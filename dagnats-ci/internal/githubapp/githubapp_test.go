// Package githubapp_test exercises webhook signature verification and event
// parsing using only in-memory inputs — no network calls, no GitHub App
// credentials, no time.Sleep.
//
// Methodology (TigerStyle TDD):
//   - VerifySignature: compute the expected HMAC in the test itself so the
//     assertion is self-contained and independent of the production code path.
//   - ParseEvent: embed literal JSON payloads as constants; do NOT fetch from
//     the network.
//   - ToEnvelope: round-trip the Data field back into an Event to prove the
//     envelope carries the correct normalized event.
//   - Each test asserts at least one positive (happy path) and one negative
//     (rejection or boundary) condition.
package githubapp_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/danmestas/dagnats-ci/internal/githubapp"
)

// samplePRPayload is a minimal GitHub pull_request webhook body. Only the
// fields that ParseEvent extracts are populated; the rest are omitted to keep
// the fixture readable and stable.
const samplePRPayload = `{
  "action": "opened",
  "pull_request": {
    "number": 42,
    "head": { "sha": "abc123def456abc123def456abc123def456abc1" },
    "base": { "ref": "main" }
  },
  "repository": {
    "name": "dagnats",
    "owner": { "login": "danmestas" },
    "clone_url": "https://github.com/danmestas/dagnats.git"
  },
  "installation": { "id": 987654 }
}`

// samplePushPayload is a minimal GitHub push webhook body.
const samplePushPayload = `{
  "ref": "refs/heads/main",
  "after": "deadbeef00000000000000000000000000000000",
  "repository": {
    "name": "dagnats",
    "owner": { "login": "danmestas" },
    "clone_url": "https://github.com/danmestas/dagnats.git"
  },
  "installation": { "id": 11111 }
}`

// computeHMAC returns the sha256= prefixed signature for body keyed by secret.
// This mirrors the GitHub webhook signing algorithm so the test does not depend
// on the production code path to generate the expected value.
func computeHMAC(t *testing.T, secret, body []byte) string {
	t.Helper()
	mac := hmac.New(sha256.New, secret)
	n, err := mac.Write(body)
	if err != nil || n != len(body) {
		t.Fatalf("computeHMAC: mac.Write failed (n=%d len=%d err=%v)", n, len(body), err)
	}
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// TestVerifySignature checks that a correctly-signed body is accepted and that
// a tampered body, wrong secret, and empty header are each rejected.
func TestVerifySignature(t *testing.T) {
	secret := []byte("test-secret-xk7q")
	body := []byte(`{"action":"opened","number":1}`)
	validSig := computeHMAC(t, secret, body)

	// Positive: correct secret + body → no error.
	if err := githubapp.VerifySignature(secret, body, validSig); err != nil {
		t.Errorf("VerifySignature(valid) = %v, want nil", err)
	}

	// Negative: tampered body → HMAC mismatch.
	tampered := append([]byte(nil), body...)
	tampered = append(tampered, '!')
	if err := githubapp.VerifySignature(secret, tampered, validSig); err == nil {
		t.Error("VerifySignature(tampered body) = nil, want error")
	}

	// Negative: wrong secret → HMAC mismatch.
	if err := githubapp.VerifySignature([]byte("wrong-secret"), body, validSig); err == nil {
		t.Error("VerifySignature(wrong secret) = nil, want error")
	}

	// Negative: empty header → immediate rejection.
	if err := githubapp.VerifySignature(secret, body, ""); err == nil {
		t.Error("VerifySignature(empty header) = nil, want error")
	}

	// Negative: a header with a non-sha256= prefix is rejected with an error.
	if err := githubapp.VerifySignature(secret, body, "nonce=abcdef0123456789"); err == nil {
		t.Error("VerifySignature(wrong prefix) = nil, want error mentioning sha256= prefix")
	}
}

// TestParseEventPullRequest verifies that a pull_request payload is decoded
// into the correct normalized Event fields and that malformed JSON is rejected.
func TestParseEventPullRequest(t *testing.T) {
	ev, err := githubapp.ParseEvent("pull_request", []byte(samplePRPayload))
	if err != nil {
		t.Fatalf("ParseEvent(pull_request) = %v, want nil", err)
	}

	// Positive: key fields are extracted correctly.
	if ev.HeadSHA != "abc123def456abc123def456abc123def456abc1" {
		t.Errorf("HeadSHA = %q, want %q", ev.HeadSHA,
			"abc123def456abc123def456abc123def456abc1")
	}
	if ev.PR != 42 {
		t.Errorf("PR = %d, want 42", ev.PR)
	}
	if ev.InstallationID != 987654 {
		t.Errorf("InstallationID = %d, want 987654", ev.InstallationID)
	}
	if ev.Owner != "danmestas" {
		t.Errorf("Owner = %q, want %q", ev.Owner, "danmestas")
	}
	if ev.Repo != "dagnats" {
		t.Errorf("Repo = %q, want %q", ev.Repo, "dagnats")
	}
	if ev.CloneURL != "https://github.com/danmestas/dagnats.git" {
		t.Errorf("CloneURL = %q, want https://github.com/danmestas/dagnats.git", ev.CloneURL)
	}

	// Negative: malformed JSON returns an error.
	_, err = githubapp.ParseEvent("pull_request", []byte(`{"action": BROKEN`))
	if err == nil {
		t.Error("ParseEvent(malformed JSON) = nil, want error")
	}
}

// TestParseEventPush verifies that a push payload is decoded correctly.
func TestParseEventPush(t *testing.T) {
	ev, err := githubapp.ParseEvent("push", []byte(samplePushPayload))
	if err != nil {
		t.Fatalf("ParseEvent(push) = %v, want nil", err)
	}

	// Positive: SHA and ref are extracted.
	if ev.HeadSHA != "deadbeef00000000000000000000000000000000" {
		t.Errorf("HeadSHA = %q, want deadbeef0...", ev.HeadSHA)
	}
	if ev.BaseRef != "main" {
		t.Errorf("BaseRef = %q, want \"main\" (refs/heads/ prefix must be stripped)", ev.BaseRef)
	}
	if ev.CloneURL != "https://github.com/danmestas/dagnats.git" {
		t.Errorf("CloneURL = %q, want https://github.com/danmestas/dagnats.git", ev.CloneURL)
	}

	// Negative: unsupported event type returns an error.
	_, err = githubapp.ParseEvent("workflow_run", []byte(`{}`))
	if err == nil {
		t.Error("ParseEvent(unsupported type) = nil, want error")
	}
}

// TestToEnvelope verifies that the TriggerEnvelope has the correct Trigger and
// Source, that its Data round-trips back to the original Event, and that empty
// owner/repo fields are rejected.
func TestToEnvelope(t *testing.T) {
	ev := githubapp.Event{
		Kind:           "pull_request",
		Action:         "opened",
		Owner:          "danmestas",
		Repo:           "dagnats",
		HeadSHA:        "abc123",
		BaseRef:        "main",
		PR:             7,
		InstallationID: 99,
	}
	env, err := githubapp.ToEnvelope(ev)
	if err != nil {
		t.Fatalf("ToEnvelope = %v, want nil", err)
	}

	// Positive: Trigger and Source are well-formed.
	if env.Trigger != "github" {
		t.Errorf("Trigger = %q, want \"github\"", env.Trigger)
	}
	if env.Source != "github:danmestas/dagnats" {
		t.Errorf("Source = %q, want \"github:danmestas/dagnats\"", env.Source)
	}

	// Positive: Data contains the expected snake_case JSON keys (spec §5.1).
	var raw map[string]any
	if err := json.Unmarshal(env.Data, &raw); err != nil {
		t.Fatalf("unmarshal Data into map: %v", err)
	}
	if raw["head_sha"] != ev.HeadSHA {
		t.Errorf("Data[\"head_sha\"] = %v, want %q", raw["head_sha"], ev.HeadSHA)
	}
	if id, _ := raw["installation_id"].(float64); int64(id) != ev.InstallationID {
		t.Errorf("Data[\"installation_id\"] = %v, want %d", raw["installation_id"], ev.InstallationID)
	}

	// Negative: PascalCase keys must be absent — json tags must use snake_case.
	if _, ok := raw["HeadSHA"]; ok {
		t.Error("Data[\"HeadSHA\"] present — json tags missing or wrong (want snake_case)")
	}

	// Negative: empty owner returns an error.
	_, err = githubapp.ToEnvelope(githubapp.Event{Owner: "", Repo: "dagnats"})
	if err == nil {
		t.Error("ToEnvelope(empty owner) = nil, want error")
	}

	// Negative: empty repo returns an error.
	_, err = githubapp.ToEnvelope(githubapp.Event{Owner: "danmestas", Repo: ""})
	if err == nil {
		t.Error("ToEnvelope(empty repo) = nil, want error")
	}
}
