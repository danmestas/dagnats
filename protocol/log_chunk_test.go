// protocol/log_chunk_test.go
// Tests for LogChunk (the BUILD_LOGS wire type, #624) and the bounds
// constants it and its consumers rely on. Methodology: pure unit tests,
// no NATS. Positive space asserts a round trip preserves every field
// exactly; negative space asserts the bounds table is internally
// consistent (a chunk can never legally exceed a step's total budget).
package protocol

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLogChunkJSONRoundTrip(t *testing.T) {
	want := LogChunk{
		Seq:    42,
		TS:     time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Stream: LogStreamOut,
		Data:   []byte("hello world"),
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got LogChunk
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Seq != want.Seq || got.Stream != want.Stream ||
		string(got.Data) != string(want.Data) || !got.TS.Equal(want.TS) {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, want)
	}
	// snake_case wire keys, per the design.
	for _, key := range []string{`"seq"`, `"ts"`, `"stream"`, `"data"`} {
		if !jsonContains(data, key) {
			t.Fatalf("marshaled JSON missing key %s: %s", key, data)
		}
	}
}

func jsonContains(data []byte, key string) bool {
	return len(data) > 0 && (indexOf(string(data), key) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestLogChunkBoundsAreSane(t *testing.T) {
	if LogChunkBytesMax <= 0 {
		t.Fatalf("LogChunkBytesMax must be positive, got %d", LogChunkBytesMax)
	}
	if LogStepBytesMax <= 0 {
		t.Fatalf("LogStepBytesMax must be positive, got %d", LogStepBytesMax)
	}
	if LogChunkBytesMax >= LogStepBytesMax {
		t.Fatalf(
			"LogChunkBytesMax (%d) must be < LogStepBytesMax (%d)",
			LogChunkBytesMax, LogStepBytesMax,
		)
	}
	if LogReadChunksMax <= 0 {
		t.Fatalf("LogReadChunksMax must be positive, got %d", LogReadChunksMax)
	}
}
