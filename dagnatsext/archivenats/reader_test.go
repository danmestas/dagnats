// dagnatsext/archivenats/reader_test.go
// The archive range protocol from the reader's side: the subject shape,
// the literal request and reply keys, paging, split-message reassembly,
// bounded busy retries, and every error a read must surface instead of
// returning a partial range.
//
// Methodology: one embedded NATS server per test, with the fake archive
// of internal/archivetest (or a raw subscriber when a test needs a
// literal reply) on the range subject. Timeouts and backoffs are
// shortened through archivenats.Options so every wait is bounded; every
// test asserts what a read returns and what it must not.
package archivenats_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatsext"
	"github.com/danmestas/dagnats/dagnatsext/archivenats"
	"github.com/danmestas/dagnats/internal/archivetest"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/nats-io/nats.go"
)

const (
	testStream = "AGENT_JOURNAL"
	testDomain = "eu"
	// testEpoch is above 2^53, where a float would lose digits.
	testEpoch int64 = 1_700_000_000_123_456_789
	caseTime        = 10 * time.Second
)

var fastOptions = archivenats.Options{
	RequestTimeout: 300 * time.Millisecond, ReadTimeout: 5 * time.Second,
	BusyRetryMax: 3, BusyBackoff: 10 * time.Millisecond,
}

func messages(subject string, count int) []dagnatsext.ArchivedMessage {
	out := make([]dagnatsext.ArchivedMessage, 0, count)
	at := time.Date(2026, 11, 1, 12, 0, 0, 123456000, time.UTC)
	for i := 1; i <= count; i++ {
		out = append(out, dagnatsext.ArchivedMessage{
			Seq: uint64(i), Subject: subject, Time: at.Add(time.Duration(i) * time.Second),
			Headers: map[string][]string{"K": {"v" + strings.Repeat("x", i)}},
			Data:    []byte(`{"n":` + strings.Repeat("1", i) + `}`),
		})
	}
	return out
}

func read(
	t *testing.T, nc *nats.Conn, rng dagnatsext.ArchiveRange,
) (dagnatsext.ArchivedRange, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTime)
	defer cancel()
	reader := archivenats.NewReader(nc, testDomain, testStream, fastOptions)
	return reader.ReadRange(ctx, rng)
}

// rawArchive answers every range request with the literal reply.
func rawArchive(t *testing.T, nc *nats.Conn, reply string) {
	t.Helper()
	sub, err := nc.Subscribe(dagnatsext.ArchiveRangeSubject(testDomain, testStream),
		func(m *nats.Msg) {
			if err := m.Respond([]byte(reply)); err != nil {
				t.Errorf("respond: %v", err)
			}
		})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() {
		if err := sub.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
			t.Errorf("unsubscribe: %v", err)
		}
	})
}

func TestArchiveRangeSubject(t *testing.T) {
	if got := dagnatsext.ArchiveRangeSubject("eu", "AGENT_JOURNAL"); got !=
		"lode.stream.eu.AGENT_JOURNAL.range" {
		t.Errorf("subject = %q", got)
	}
	if got := dagnatsext.ArchiveRangeSubject("", "AGENT_JOURNAL"); got !=
		"lode.stream.default.AGENT_JOURNAL.range" {
		t.Errorf("empty domain subject = %q, want the default token", got)
	}
	if !strings.HasPrefix(dagnatsext.ArchiveRangeSubject("eu", "S"), dagnatsext.ArchiveSubjectPrefix) {
		t.Error("subject is not under ArchiveSubjectPrefix")
	}
}

func TestArchiveRead_RequestCarriesLiteralKeysAndIntegerEpoch(t *testing.T) {
	_, nc := natstest.StartServer(t)
	fake := archivetest.Serve(t, nc, testDomain, testStream, testEpoch, messages("journal.r.s", 3))
	got, err := read(t, nc, dagnatsext.ArchiveRange{
		Epoch: testEpoch, Subject: "journal.r.s", FromSeq: 1, ToSeq: 4,
	})
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(got.Messages) != 3 {
		t.Fatalf("messages = %d, want 3", len(got.Messages))
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(fake.LastRequest(), &keys); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	want := map[string]string{
		"epoch": "1700000000123456789", "subject": `"journal.r.s"`, "from_seq": "1",
		"to_seq": "4", "offset": "0", "max_bytes": "524288",
	}
	if len(keys) != len(want) {
		t.Errorf("request keys = %v, want exactly %v", keys, want)
	}
	for key, value := range want {
		if string(keys[key]) != value {
			t.Errorf("request %s = %s, want %s", key, keys[key], value)
		}
	}
}

func TestArchiveRead_DecodesLiteralReply(t *testing.T) {
	_, nc := natstest.StartServer(t)
	reply := `{"messages":[{"seq":7,"subject":"journal.r.s",` +
		`"time":"2026-11-01T12:00:00.123456Z","headers":{"A":["1","2"]},` +
		`"data":"aGVsbG8="}],"next_seq":9,"next_offset":0,"more":false,` +
		`"gaps":[{"from":8,"to":8}],"done":true}`
	rawArchive(t, nc, reply)
	got, err := read(t, nc, dagnatsext.ArchiveRange{Epoch: 1, FromSeq: 1, ToSeq: 9})
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(got.Messages) != 1 || string(got.Messages[0].Data) != "hello" {
		t.Fatalf("messages = %+v, want one with data hello", got.Messages)
	}
	m := got.Messages[0]
	if m.Seq != 7 || !slices.Equal(m.Headers["A"], []string{"1", "2"}) ||
		m.Time.Nanosecond() != 123456000 {
		t.Errorf("message = %+v", m)
	}
	if !slices.Equal(got.Gaps, []dagnatsext.ArchiveGap{{From: 8, To: 8}}) {
		t.Errorf("gaps = %v, want [{8 8}]", got.Gaps)
	}
}

func TestArchiveRead_PagesInSeqOrderAndStopsAtLimit(t *testing.T) {
	_, nc := natstest.StartServer(t)
	all := messages("journal.r.s", 9)
	fake := archivetest.Serve(t, nc, testDomain, testStream, testEpoch, all)
	fake.PageMessages = 2
	got, err := read(t, nc, dagnatsext.ArchiveRange{
		Epoch: testEpoch, Subject: "journal.r.s", FromSeq: 1, ToSeq: 10,
	})
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(got.Messages) != 9 || fake.Requests.Load() < 5 {
		t.Fatalf("got %d messages in %d requests, want 9 over at least 5 pages",
			len(got.Messages), fake.Requests.Load())
	}
	for i, m := range got.Messages {
		if m.Seq != all[i].Seq || !bytes.Equal(m.Data, all[i].Data) {
			t.Errorf("message %d = seq %d, want seq %d", i, m.Seq, all[i].Seq)
		}
	}
	limited, err := read(t, nc, dagnatsext.ArchiveRange{
		Epoch: testEpoch, Subject: "journal.r.s", FromSeq: 4, ToSeq: 10, Limit: 1,
	})
	if err != nil || len(limited.Messages) != 1 || limited.Messages[0].Seq != 4 {
		t.Errorf("limit 1 from 4 = %+v, %v; want only seq 4", limited.Messages, err)
	}
}

func TestArchiveRead_SplitMessageReassemblesByteIdentical(t *testing.T) {
	_, nc := natstest.StartServer(t)
	big := bytes.Repeat([]byte("0123456789abcdef"), 700) // 11200 bytes
	all := []dagnatsext.ArchivedMessage{
		{Seq: 1, Subject: "journal.r.s", Data: []byte("a")},
		{Seq: 2, Subject: "journal.r.s", Data: big, Headers: map[string][]string{"H": {"x"}}},
		{Seq: 3, Subject: "journal.r.s", Data: []byte("c")},
	}
	fake := archivetest.Serve(t, nc, testDomain, testStream, testEpoch, all)
	fake.PageBytes = 1000
	got, err := read(t, nc, dagnatsext.ArchiveRange{
		Epoch: testEpoch, Subject: "journal.r.s", FromSeq: 1, ToSeq: 4,
	})
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(got.Messages) != 3 || !bytes.Equal(got.Messages[1].Data, big) {
		t.Fatalf("got %d messages, split one intact = %v", len(got.Messages),
			len(got.Messages) == 3 && bytes.Equal(got.Messages[1].Data, big))
	}
	if got.Messages[1].Headers["H"][0] != "x" || fake.Requests.Load() < 12 {
		t.Errorf("headers %v over %d requests, want the first part's over >= 12",
			got.Messages[1].Headers, fake.Requests.Load())
	}
}

func TestArchiveRead_BusyIsRetriedThenBounded(t *testing.T) {
	_, nc := natstest.StartServer(t)
	fake := archivetest.Serve(t, nc, testDomain, testStream, testEpoch, messages("s", 2))
	fake.BusyFor = 2
	got, err := read(t, nc, dagnatsext.ArchiveRange{Epoch: testEpoch, FromSeq: 1, ToSeq: 3})
	if err != nil || len(got.Messages) != 2 {
		t.Fatalf("busy twice then served: %d messages, %v", len(got.Messages), err)
	}
	fake.Requests.Store(0)
	fake.BusyFor = 1 << 40
	_, err = read(t, nc, dagnatsext.ArchiveRange{Epoch: testEpoch, FromSeq: 1, ToSeq: 3})
	if !errors.Is(err, dagnatsext.ErrArchiveBusy) {
		t.Fatalf("busy forever: err = %v, want ErrArchiveBusy", err)
	}
	if n := fake.Requests.Load(); n != int64(fastOptions.BusyRetryMax+1) {
		t.Errorf("busy forever made %d requests, want %d", n, fastOptions.BusyRetryMax+1)
	}
}

func TestArchiveRead_ErrorRepliesFailTheRead(t *testing.T) {
	_, nc := natstest.StartServer(t)
	fake := archivetest.Serve(t, nc, testDomain, testStream, testEpoch, messages("s", 2))
	_, err := read(t, nc, dagnatsext.ArchiveRange{Epoch: testEpoch + 1, FromSeq: 1, ToSeq: 3})
	if !errors.Is(err, dagnatsext.ErrArchiveUnknownEpoch) {
		t.Errorf("wrong epoch: err = %v, want ErrArchiveUnknownEpoch", err)
	}
	fake.Code, fake.Floor = dagnatsext.ArchiveCodeNotArchived, 1
	_, err = read(t, nc, dagnatsext.ArchiveRange{Epoch: testEpoch, FromSeq: 1, ToSeq: 3})
	var archiveErr *dagnatsext.ArchiveError
	if !errors.Is(err, dagnatsext.ErrArchiveNotArchived) || !errors.As(err, &archiveErr) ||
		archiveErr.Floor != 1 {
		t.Errorf("not_archived: err = %v, want ErrArchiveNotArchived with floor 1", err)
	}
	if errors.Is(err, dagnatsext.ErrArchiveBusy) {
		t.Error("not_archived must not read as busy")
	}
}

func TestArchiveRead_NoResponderAndTimeoutFail(t *testing.T) {
	_, nc := natstest.StartServer(t)
	_, err := read(t, nc, dagnatsext.ArchiveRange{Epoch: testEpoch, FromSeq: 1, ToSeq: 3})
	if !errors.Is(err, dagnatsext.ErrArchiveUnavailable) {
		t.Errorf("no responder: err = %v, want ErrArchiveUnavailable", err)
	}
	fake := archivetest.Serve(t, nc, testDomain, testStream, testEpoch, messages("s", 2))
	fake.Silent = true
	start := time.Now()
	got, err := read(t, nc, dagnatsext.ArchiveRange{Epoch: testEpoch, FromSeq: 1, ToSeq: 3})
	if err == nil || len(got.Messages) != 0 {
		t.Fatalf("silent archive: %d messages, err %v; want a failure", len(got.Messages), err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("silent archive took %v, want the 300ms request bound", elapsed)
	}
}

func TestArchiveRead_ReplyOutsideRangeIsAProtocolError(t *testing.T) {
	_, nc := natstest.StartServer(t)
	reply := `{"messages":[{"seq":5,"subject":"s","time":"2026-11-01T12:00:00.000000Z",` +
		`"data":""}],"done":true}`
	rawArchive(t, nc, reply)
	got, err := read(t, nc, dagnatsext.ArchiveRange{Epoch: 1, FromSeq: 1, ToSeq: 3})
	if !errors.Is(err, dagnatsext.ErrArchiveProtocol) {
		t.Errorf("seq 5 for [1,3): err = %v, want ErrArchiveProtocol", err)
	}
	if len(got.Messages) != 0 {
		t.Errorf("a failed read returned %d messages", len(got.Messages))
	}
}

func TestArchiveRead_EmptyPagesAreNotTheEnd(t *testing.T) {
	_, nc := natstest.StartServer(t)
	all := []dagnatsext.ArchivedMessage{
		{Seq: 10, Subject: "s", Data: []byte("a")}, {Seq: 11, Subject: "s", Data: []byte("b")},
	}
	fake := archivetest.Serve(t, nc, testDomain, testStream, testEpoch, all)
	fake.EmptyPages = 3
	got, err := read(t, nc, dagnatsext.ArchiveRange{Epoch: testEpoch, FromSeq: 1, ToSeq: 12})
	if err != nil {
		t.Fatalf("ReadRange over empty pages: %v", err)
	}
	if len(got.Messages) != 2 || got.Messages[0].Seq != 10 || fake.Requests.Load() != 4 {
		t.Fatalf("got %d messages over %d requests, want seq 10 and 11 after 3 empty pages",
			len(got.Messages), fake.Requests.Load())
	}
}

func TestArchiveRead_StalledNextSeqFails(t *testing.T) {
	_, nc := natstest.StartServer(t)
	// An empty page that does not move next_seq would page forever.
	rawArchive(t, nc, `{"messages":[],"next_seq":1,"next_offset":0,"more":false,"done":false}`)
	got, err := read(t, nc, dagnatsext.ArchiveRange{Epoch: 1, FromSeq: 1, ToSeq: 9})
	if !errors.Is(err, dagnatsext.ErrArchiveProtocol) {
		t.Fatalf("stalled next_seq: err = %v, want ErrArchiveProtocol", err)
	}
	if len(got.Messages) != 0 {
		t.Errorf("a stalled read returned %d messages", len(got.Messages))
	}
}

func TestArchiveRead_NotIndexedAndUnknownCodesFailWithoutRetry(t *testing.T) {
	_, nc := natstest.StartServer(t)
	fake := archivetest.Serve(t, nc, testDomain, testStream, testEpoch, messages("s", 2))
	for _, code := range []string{dagnatsext.ArchiveCodeNotIndexed, "no_such_code"} {
		fake.Requests.Store(0)
		fake.Code = code
		_, err := read(t, nc, dagnatsext.ArchiveRange{Epoch: testEpoch, FromSeq: 1, ToSeq: 3})
		var archiveErr *dagnatsext.ArchiveError
		if !errors.As(err, &archiveErr) || archiveErr.Code != code ||
			!strings.Contains(err.Error(), code) {
			t.Errorf("%s: err = %v, want an ArchiveError naming the code", code, err)
		}
		if n := fake.Requests.Load(); n != 1 {
			t.Errorf("%s: %d requests, want 1 (no retry)", code, n)
		}
	}
	fake.Code = dagnatsext.ArchiveCodeNotIndexed
	_, err := read(t, nc, dagnatsext.ArchiveRange{Epoch: testEpoch, FromSeq: 1, ToSeq: 3})
	if !errors.Is(err, dagnatsext.ErrArchiveNotIndexed) || errors.Is(err, dagnatsext.ErrArchiveBusy) {
		t.Errorf("not_indexed: err = %v, want ErrArchiveNotIndexed and not busy", err)
	}
}
