// Methodology: the Store contract is checked against two fakes and the LocalStore. memStore is
// atomic create-only, as a conditional-PUT store (R2, S3, Tigris) is. checkFirst is what a store
// without conditional writes (Garage v2.1.0) does: HEAD then PUT, with a hook that holds racing
// writers between the check and the write so both pass the check. The writer must stay correct
// over both, and must reach a commit with nothing but Put and Get.
package writer

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/lake"
)

type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	calls   []string
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}} }

func (m *memStore) Put(ctx context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "put "+key)
	if _, found := m.objects[key]; found {
		return fmt.Errorf("mem: put %s: %w", key, fs.ErrExist)
	}
	m.objects[key] = append([]byte(nil), data...)
	return nil
}

func (m *memStore) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "get "+key)
	data, found := m.objects[key]
	if !found {
		return nil, fmt.Errorf("mem: get %s: %w", key, fs.ErrNotExist)
	}
	return append([]byte(nil), data...), nil
}

// checkFirst checks for the key, runs between (if set) and then writes, overwriting.
type checkFirst struct {
	*memStore
	between func(key string)
}

func (c *checkFirst) Put(ctx context.Context, key string, data []byte) error {
	if _, err := c.memStore.Get(ctx, key); err == nil {
		return fmt.Errorf("checkfirst: put %s: %w", key, fs.ErrExist)
	}
	if c.between != nil {
		c.between(key)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.objects[key] = append([]byte(nil), data...)
	return nil
}

func TestStore_PutIsCreateOnly(t *testing.T) {
	stores := map[string]Store{"local": NewLocalStore(t.TempDir()), "mem": newMemStore()}
	for name, store := range stores {
		key := "p/t/dt=2026-10-01/part-a.json"
		if _, err := store.Get(t.Context(), key); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s: Get of a missing key = %v, want fs.ErrNotExist", name, err)
		}
		if err := store.Put(t.Context(), key, []byte("first")); err != nil {
			t.Fatalf("%s: first Put: %v", name, err)
		}
		if err := store.Put(t.Context(), key, []byte("second")); !errors.Is(err, fs.ErrExist) {
			t.Fatalf("%s: second Put = %v, want fs.ErrExist", name, err)
		}
		data, err := store.Get(t.Context(), key)
		if err != nil || string(data) != "first" {
			t.Fatalf("%s: Get = %q, %v; the second Put replaced the object", name, data, err)
		}
	}
}

func TestFlush_CommitsWithOnlyPutAndGet(t *testing.T) {
	store := newMemStore()
	writer, err := New(store, "notify", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	commits := flushRows(t, writer, testRow(1, 1))
	prefix := "notify/deliveries/dt=2026-10-01/" + strings.TrimSuffix(commits[0].Part, ".parquet")
	want := []string{"put " + prefix + ".parquet", "put " + prefix + ".json"}
	if fmt.Sprint(store.calls) != fmt.Sprint(want) {
		t.Fatalf("calls = %v, want %v", store.calls, want)
	}
	store.calls = nil
	flushRows(t, writer, testRow(1, 1))
	want = append(want, "get "+prefix+".json")
	if fmt.Sprint(store.calls) != fmt.Sprint(want) {
		t.Fatalf("retry calls = %v, want %v", store.calls, want)
	}
}

func TestFlush_KeysStayUnderTheWritersProgram(t *testing.T) {
	store := newMemStore()
	lode, err := New(store, "lode", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	flushRows(t, lode, testRow(1, 1), testRow(2, 2))
	if len(store.objects) != 4 {
		t.Fatalf("objects = %d, want a part and a record per date", len(store.objects))
	}
	for key := range store.objects {
		if !strings.HasPrefix(key, "lode/deliveries/dt=") {
			t.Fatalf("key %q is outside lode/", key)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("a key outside the program did not panic")
		}
	}()
	lode.key("../2026-10-01", "part-x.json")
}

// TestFlush_CheckFirstRaceOnOnePartKeyStaysCorrect holds two writers of one batch between the
// check and the write of the part key, so both pass the check and both write it. The record
// races the same way. The result must still be one part and one record with matching digests.
func TestFlush_CheckFirstRaceOnOnePartKeyStaysCorrect(t *testing.T) {
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	store := &checkFirst{memStore: newMemStore()}
	store.between = func(key string) {
		if !strings.HasSuffix(key, ".parquet") {
			return
		}
		arrived <- struct{}{}
		select {
		case <-release:
		case <-time.After(10 * time.Second):
			t.Error("racing writer never released")
		}
	}
	results := make([][]lake.Commit, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range results {
		writer, err := New(store, "notify", testConfig())
		if err != nil {
			t.Fatal(err)
		}
		writer.now = func() time.Time { return time.Unix(int64(100*(i+1)), 0) }
		if err := writer.Append(testRow(1, 1)); err != nil {
			t.Fatal(err)
		}
		wg.Go(func() { results[i], errs[i] = writer.Flush(context.Background()) })
	}
	for range 2 {
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			t.Fatal("both writers did not pass the check")
		}
	}
	close(release)
	wg.Wait()
	assertRaceOutcome(t, store.memStore, results, errs)
}

func assertRaceOutcome(t *testing.T, store *memStore, results [][]lake.Commit, errs []error) {
	t.Helper()
	for i := range results {
		if errs[i] != nil || len(results[i]) != 1 {
			t.Fatalf("writer %d: %+v, %v", i, results[i], errs[i])
		}
	}
	if results[0][0].Key != results[1][0].Key || results[0][0].Digest != results[1][0].Digest {
		t.Fatalf("racing writers disagree: %+v vs %+v", results[0][0], results[1][0])
	}
	if len(store.objects) != 2 {
		t.Fatalf("objects = %d, want one part and one record", len(store.objects))
	}
	recordKey := strings.TrimSuffix(results[0][0].Key, ".parquet") + ".json"
	commit, err := lake.DecodeCommit(recordKey, store.objects[recordKey])
	if err != nil || commit.Digest != results[0][0].Digest {
		t.Fatalf("surviving record = %+v, %v", commit, err)
	}
	if len(store.objects[results[0][0].Key]) != commit.Bytes {
		t.Fatalf("part is %d bytes, record says %d", len(store.objects[results[0][0].Key]), commit.Bytes)
	}
}
