// dag/respond_config_test.go
//
// Methodology: pure unit tests. RespondConfig defaults are applied via
// Defaulted(); each test pins one field's zero-value behavior. Two
// assertions per test (positive + negative space). No NATS, no I/O.
package dag

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStepTypeRespondString(t *testing.T) {
	got := StepTypeRespond.String()
	if got != "respond" {
		t.Fatalf("got %q, want respond", got)
	}
	// Negative: StepTypeRespond is distinct from all other types.
	if StepTypeRespond == StepTypeNormal {
		t.Fatal("StepTypeRespond must not collide with StepTypeNormal")
	}
}

func TestStepTypeRespondJSONRoundTrip(t *testing.T) {
	raw, err := json.Marshal(StepTypeRespond)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), "respond") {
		t.Fatalf("marshal output %s missing 'respond'", raw)
	}
	var got StepType
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != StepTypeRespond {
		t.Fatalf("round-trip %v, want StepTypeRespond", got)
	}
}

func TestRespondConfigDefaultStatus(t *testing.T) {
	cfg := RespondConfig{}.Defaulted()
	if cfg.Status != 200 {
		t.Fatalf("default Status = %d, want 200", cfg.Status)
	}
	// Negative: an explicit non-zero status is preserved.
	cfg = RespondConfig{Status: 201}.Defaulted()
	if cfg.Status != 201 {
		t.Fatalf("explicit Status = %d, want 201", cfg.Status)
	}
}

func TestRespondConfigDefaultContentType(t *testing.T) {
	cfg := RespondConfig{}.Defaulted()
	if cfg.ContentType != "application/json" {
		t.Fatalf(
			"default ContentType = %q, want application/json",
			cfg.ContentType,
		)
	}
	cfg = RespondConfig{ContentType: "text/plain"}.Defaulted()
	if cfg.ContentType != "text/plain" {
		t.Fatalf(
			"explicit ContentType = %q, want text/plain",
			cfg.ContentType,
		)
	}
}

func TestRespondConfigBodyFromBlankMeansUpstream(t *testing.T) {
	cfg := RespondConfig{}.Defaulted()
	if cfg.BodyFrom != "" {
		t.Fatalf("default BodyFrom = %q, want empty", cfg.BodyFrom)
	}
	// Negative: explicit dotpath preserved.
	cfg = RespondConfig{BodyFrom: "data.result"}.Defaulted()
	if cfg.BodyFrom != "data.result" {
		t.Fatalf(
			"explicit BodyFrom = %q, want data.result",
			cfg.BodyFrom,
		)
	}
}

func TestRespondConfigHeadersDefaultEmpty(t *testing.T) {
	cfg := RespondConfig{}.Defaulted()
	if len(cfg.Headers) != 0 {
		t.Fatalf("default Headers should be empty, got %v", cfg.Headers)
	}
	cfg = RespondConfig{
		Headers: map[string]string{"X-Foo": "bar"},
	}.Defaulted()
	if cfg.Headers["X-Foo"] != "bar" {
		t.Fatalf("explicit Headers lost: %v", cfg.Headers)
	}
}

func TestParseRespondConfigWrongType(t *testing.T) {
	step := StepDef{
		ID: "s", Type: StepTypeNormal,
		Config: json.RawMessage(`{"status":200}`),
	}
	_, err := ParseRespondConfig(step)
	if err == nil {
		t.Fatal("expected error for non-respond step type")
	}
	if !strings.Contains(err.Error(), "Respond") {
		t.Fatalf("err = %v, want mention of Respond", err)
	}
}

func TestParseRespondConfigNilConfig(t *testing.T) {
	step := StepDef{ID: "s", Type: StepTypeRespond, Config: nil}
	_, err := ParseRespondConfig(step)
	if err == nil {
		t.Fatal("expected error for nil config on respond step")
	}
	if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("err = %v, want mention of nil", err)
	}
}

func TestParseRespondConfigRoundTrip(t *testing.T) {
	cfg := RespondConfig{
		Status:      201,
		ContentType: "application/json",
		BodyFrom:    "output.payload",
		Headers:     map[string]string{"X-Trace-Id": "t-1"},
	}
	step := StepDef{
		ID:     "s",
		Type:   StepTypeRespond,
		Config: MarshalConfig(&cfg),
	}
	got, err := ParseRespondConfig(step)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.Status != 201 {
		t.Fatalf("Status = %d, want 201", got.Status)
	}
	if got.Headers["X-Trace-Id"] != "t-1" {
		t.Fatalf("Headers lost: %v", got.Headers)
	}
}
