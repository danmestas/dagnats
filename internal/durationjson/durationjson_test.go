package durationjson

// Methodology: pure decode tests on small local structs, no NATS. They
// pin the two edges the dag tests don't reach: key matching is
// case-insensitive like encoding/json, and a nested object whose key
// shares a duration field's name triggers the slow path without
// touching that nested value.

import (
	"encoding/json"
	"testing"
	"time"
)

type sample struct {
	Wait   time.Duration   `json:"wait"`
	Name   string          `json:"name"`
	Nested json.RawMessage `json:"nested"`
}

func (s *sample) UnmarshalJSON(data []byte) error {
	type plain sample
	return Unmarshal(data, (*plain)(s))
}

func TestUnmarshalMatchesKeysCaseInsensitively(t *testing.T) {
	var got sample
	if err := json.Unmarshal([]byte(`{"WAIT": "3s", "name": "n"}`), &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Wait != 3*time.Second {
		t.Fatalf("wait = %v, want 3s", got.Wait)
	}
	if got.Name != "n" {
		t.Fatalf("name = %q, want n", got.Name)
	}
}

func TestUnmarshalLeavesNestedValuesAlone(t *testing.T) {
	var got sample
	data := []byte(`{"wait": 5, "nested": {"wait": "1h"}}`)
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Wait != 5 {
		t.Fatalf("wait = %d, want 5ns", got.Wait)
	}
	if string(got.Nested) != `{"wait": "1h"}` {
		t.Fatalf("nested rewritten: %s", got.Nested)
	}
}

func TestUnmarshalRejectsBadStrings(t *testing.T) {
	var got sample
	if err := json.Unmarshal([]byte(`{"wait": "later"}`), &got); err == nil {
		t.Fatal("want an error for a malformed duration")
	}
	if err := json.Unmarshal([]byte(`{"wait": "-2m"}`), &got); err != nil {
		t.Fatalf("negative durations parse like time.ParseDuration: %v", err)
	}
	if got.Wait != -2*time.Minute {
		t.Fatalf("wait = %v, want -2m", got.Wait)
	}
}
