// Methodology: runStoreSuite is the store contract as tests: write-once Put, Get and Has,
// Delete, List across continuation pages and past its limit, name validation, and the object
// size ceiling. TestWriterRoundTrip then runs the lake writer, the store's consumer, over the
// fake: a commit lands as a part and a record, a re-flush of the same batch is harmless, and
// the writer's own reads (Get of a record) see what the store holds.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/lake"
	"github.com/danmestas/dagnats/lake/writer"
)

func runStoreSuite(t *testing.T, newStore func(t *testing.T) *Store) {
	t.Helper()
	cases := map[string]func(t *testing.T, s *Store){
		"PutGetHas":  suitePutGetHas,
		"WriteOnce":  suiteWriteOnce,
		"Delete":     suiteDelete,
		"ListPaging": suiteListPaging,
		"ListLimit":  suiteListLimit,
		"ListFrom":   suiteListFrom,
		"BadNames":   suiteBadNames,
		"SizeMax":    suiteSizeMax,
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) { run(t, newStore(t)) })
	}
}

func suitePutGetHas(t *testing.T, s *Store) {
	ctx := context.Background()
	if err := s.Put(ctx, "a/b/obj", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "a/b/obj")
	if err != nil || string(got) != "hello" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	if has, err := s.Has(ctx, "a/b/obj"); err != nil || !has {
		t.Fatalf("Has present = %v, %v", has, err)
	}
	if has, err := s.Has(ctx, "a/b/none"); err != nil || has {
		t.Fatalf("Has absent = %v, %v", has, err)
	}
	if err := s.Put(ctx, "empty", nil); err != nil {
		t.Fatalf("Put nil: %v", err)
	}
	if got, err := s.Get(ctx, "empty"); err != nil || len(got) != 0 {
		t.Fatalf("Get empty = %q, %v", got, err)
	}
}

func suiteWriteOnce(t *testing.T, s *Store) {
	ctx := context.Background()
	if err := s.Put(ctx, "k", []byte("first")); err != nil {
		t.Fatal(err)
	}
	err := s.Put(ctx, "k", []byte("second"))
	if !errors.Is(err, ErrExists) || !errors.Is(err, fs.ErrExist) {
		t.Fatalf("second Put = %v, want ErrExists and fs.ErrExist", err)
	}
	if got, _ := s.Get(ctx, "k"); string(got) != "first" {
		t.Fatalf("object replaced: %q", got)
	}
	_, err = s.Get(ctx, "missing")
	if !errors.Is(err, ErrNotFound) || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Get missing = %v, want ErrNotFound and fs.ErrNotExist", err)
	}
	if errors.Is(err, fs.ErrExist) {
		t.Fatal("not-found matches fs.ErrExist")
	}
}

func suiteDelete(t *testing.T, s *Store) {
	ctx := context.Background()
	if err := s.Delete(ctx, "never"); err != nil {
		t.Fatalf("Delete absent: %v", err)
	}
	if err := s.Put(ctx, "gone", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	if has, err := s.Has(ctx, "gone"); err != nil || has {
		t.Fatalf("Has after Delete = %v, %v", has, err)
	}
	if err := s.Put(ctx, "gone", []byte("again")); err != nil {
		t.Fatalf("Put after Delete: %v", err)
	}
}

func suiteListPaging(t *testing.T, s *Store) {
	ctx := context.Background()
	total := listPageMax + 5
	for i := 0; i < total; i++ {
		if err := s.Put(ctx, fmt.Sprintf("p/%05d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put(ctx, "other/x", []byte("x")); err != nil {
		t.Fatal(err)
	}
	names, err := s.List(ctx, "p/", total)
	if err != nil || len(names) != total {
		t.Fatalf("List across pages = %d names, %v; want %d", len(names), err, total)
	}
	if names[0] != "p/00000" || names[total-1] != fmt.Sprintf("p/%05d", total-1) {
		t.Fatalf("names not prefix-trimmed or ordered: %q ... %q", names[0], names[total-1])
	}
	if all, err := s.List(ctx, "", total+1); err != nil || len(all) != total+1 {
		t.Fatalf("List all = %d, %v", len(all), err)
	}
}

func suiteListLimit(t *testing.T, s *Store) {
	ctx := context.Background()
	for _, name := range []string{"l/a", "l/b", "l/c"} {
		if err := s.Put(ctx, name, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	names, err := s.List(ctx, "l/", 3)
	if err != nil || !reflect.DeepEqual(names, []string{"l/a", "l/b", "l/c"}) {
		t.Fatalf("List at the limit = %v, %v", names, err)
	}
	if names, err = s.List(ctx, "l/", 2); err == nil || names != nil {
		t.Fatalf("List over the limit = %v, %v; want an error and no names", names, err)
	}
	if _, err = s.List(ctx, "l/", 0); err == nil {
		t.Fatal("List accepted limit 0")
	}
}

func suiteListFrom(t *testing.T, s *Store) {
	ctx := context.Background()
	total := listPageMax + 5
	for i := 0; i < total; i++ {
		if err := s.Put(ctx, fmt.Sprintf("f/%05d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	names, more, err := s.ListFrom(ctx, "f/", "f/00002", 3)
	if err != nil || !more || !reflect.DeepEqual(names, []string{"f/00003", "f/00004", "f/00005"}) {
		t.Fatalf("ListFrom next 3 = %v, %v, %v", names, more, err)
	}
	// startAfter need not exist; exactly the remainder is not "more".
	last := fmt.Sprintf("f/%05d", total-3)
	names, more, err = s.ListFrom(ctx, "f/", "f/00000x", total)
	if err != nil || more || len(names) != total-1 || names[0] != "f/00001" {
		t.Fatalf("ListFrom across pages = %d names, more %v, %v", len(names), more, err)
	}
	names, more, err = s.ListFrom(ctx, "f/", last, 2)
	if err != nil || more || len(names) != 2 {
		t.Fatalf("ListFrom the exact tail = %v, %v, %v", names, more, err)
	}
	names, more, err = s.ListFrom(ctx, "f/", fmt.Sprintf("f/%05d", total-1), 5)
	if err != nil || more || len(names) != 0 {
		t.Fatalf("ListFrom past the end = %v, %v, %v", names, more, err)
	}
	if _, _, err = s.ListFrom(ctx, "f/", "", 0); err == nil {
		t.Fatal("ListFrom accepted limit 0")
	}
}

func suiteBadNames(t *testing.T, s *Store) {
	ctx := context.Background()
	for _, bad := range []string{"", "/abs", "a//b", "a/./b", "a/../b", "..", "a\\b", "a/"} {
		if err := s.Put(ctx, bad, []byte("x")); err == nil {
			t.Fatalf("Put accepted %q", bad)
		}
		if _, err := s.Get(ctx, bad); err == nil {
			t.Fatalf("Get accepted %q", bad)
		}
		if _, err := s.Has(ctx, bad); err == nil {
			t.Fatalf("Has accepted %q", bad)
		}
		if err := s.Delete(ctx, bad); err == nil {
			t.Fatalf("Delete accepted %q", bad)
		}
	}
}

func suiteSizeMax(t *testing.T, s *Store) {
	ctx := context.Background()
	small := *s
	small.sizeMax = 8
	if err := small.Put(ctx, "ok8", []byte("12345678")); err != nil {
		t.Fatalf("Put at the ceiling: %v", err)
	}
	err := small.Put(ctx, "big", []byte("123456789"))
	if err == nil || errors.Is(err, ErrExists) {
		t.Fatalf("Put over the ceiling = %v", err)
	}
	if has, _ := s.Has(ctx, "big"); has {
		t.Fatal("over-ceiling object was stored")
	}
	if err := s.Put(ctx, "wide", []byte("123456789")); err != nil {
		t.Fatal(err)
	}
	if _, err := small.Get(ctx, "wide"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("Get over the ceiling = %v", err)
	}
}

func TestObjectSizeMaxConfig(t *testing.T) {
	cfg := Config{Bucket: "bkt", AccessKey: "a", SecretKey: "s"}
	s, err := New(cfg)
	if err != nil || s.sizeMax != ObjectSizeMaxDefault {
		t.Fatalf("default ceiling = %d, %v", s.sizeMax, err)
	}
	cfg.ObjectSizeMax = 1 << 20
	if s, err = New(cfg); err != nil || s.sizeMax != 1<<20 {
		t.Fatalf("explicit ceiling = %v, %v", s, err)
	}
	cfg.ObjectSizeMax = -1
	if _, err = New(cfg); err == nil {
		t.Fatal("New accepted a negative ceiling")
	}
}

func TestWriterRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore(t)
	cfg := writer.Config{
		Table: "deliveries", Stream: "NOTIFY_DELIVERY",
		Schema: lake.Schema{Version: 1, Columns: []lake.Column{
			{Name: "org_id", Type: lake.TypeString}, {Name: "attempts", Type: lake.TypeInt64},
		}},
	}
	flush := func() []lake.Commit {
		w, err := writer.New(s, "notify", cfg)
		if err != nil {
			t.Fatal(err)
		}
		at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
		for seq := int64(1); seq <= 3; seq++ {
			row := writer.Row{
				EventID: fmt.Sprintf("evt-%d", seq), EventTime: at, StreamSeq: seq,
				Payload: `{"id":"x"}`,
				Values:  map[string]any{"org_id": "o1", "attempts": seq},
			}
			if err := w.Append(row); err != nil {
				t.Fatal(err)
			}
		}
		commits, err := w.Flush(ctx)
		if err != nil {
			t.Fatalf("Flush: %v", err)
		}
		return commits
	}
	commits := flush()
	if len(commits) != 1 || commits[0].Rows != 3 || commits[0].SeqMax != 3 {
		t.Fatalf("commits = %+v", commits)
	}
	names, err := s.List(ctx, "notify/deliveries/", 10)
	if err != nil || len(names) != 2 {
		t.Fatalf("listed %v, %v; want a part and its record", names, err)
	}
	part, err := s.Get(ctx, commits[0].Key)
	if err != nil || !strings.HasPrefix(string(part), "PAR1") {
		t.Fatalf("part is not Parquet: %.8q, %v", part, err)
	}
	recordKey := strings.TrimSuffix(commits[0].Key, ".parquet") + ".json"
	record, err := s.Get(ctx, recordKey)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := lake.DecodeCommit(recordKey[strings.LastIndex(recordKey, "/")+1:], record)
	if err != nil || decoded.Digest != commits[0].Digest || decoded.Rows != 3 {
		t.Fatalf("record = %+v, %v", decoded, err)
	}
	if again := flush(); len(again) != len(commits) {
		t.Fatalf("re-flush of the same batch = %+v", again)
	}
	if names, err = s.List(ctx, "notify/deliveries/", 10); err != nil || len(names) != 2 {
		t.Fatalf("re-flush changed the listing: %v, %v", names, err)
	}
}
