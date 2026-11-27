// scope_refusal_test.go
// Pins #1026: a valid worker token refused for a task type or worker group
// outside its scope gets a JSON 403 that names its own scope in fields, not
// only inside the English sentence, so a caller such as a forge fronting the
// bridge can tell the token's holder which group the token belongs to.
//
// Methodology: real NATS, real bridge, a token minted for task type "echo"
// and group "quarry" (newConnectGroupHarness). Poll and connect are each
// refused for another group, and poll for another task type. Negative
// space: an unknown token still gets the bare 401 and learns nothing.
package bridge

import (
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// readScopeRefusal reads a 403 body as JSON, failing on any other answer.
func readScopeRefusal(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %q)", resp.StatusCode, raw)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want JSON (body %q)", ct, raw)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("403 body is not JSON: %v (%q)", err, raw)
	}
	return body
}

// requireScope asserts the fields every scope refusal carries.
func requireScope(t *testing.T, body map[string]any, code, requested string) {
	t.Helper()
	if body["code"] != code || body["requested"] != requested {
		t.Fatalf("code %v requested %v, want %s and %q", body["code"], body["requested"], code, requested)
	}
	if msg, _ := body["error"].(string); !strings.Contains(msg, requested) {
		t.Fatalf("error %q should keep the sentence naming %q", body["error"], requested)
	}
	groups, _ := body["token_worker_groups"].([]any)
	types, _ := body["token_task_types"].([]any)
	if !slices.Equal(groups, []any{"quarry"}) || !slices.Equal(types, []any{"echo"}) {
		t.Fatalf("token scope groups %v types %v, want [quarry] and [echo]", groups, types)
	}
}

func postScopedPoll(t *testing.T, url, bearer, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest("POST", url+"/v1/tasks/poll", strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	return resp
}

func TestScopeRefusalsNameTheTokensScope(t *testing.T) {
	h := newConnectGroupHarness(t)
	bearer := h.mint(t, []string{"quarry"})

	body := readScopeRefusal(t, postScopedPoll(t, h.url, bearer,
		`{"task_types":["echo"],"worker_group":"other","timeout_ms":100}`))
	requireScope(t, body, "worker_group_not_allowed", "other")

	body = readScopeRefusal(t, postScopedPoll(t, h.url, bearer,
		`{"task_types":["build"],"worker_group":"quarry","timeout_ms":100}`))
	requireScope(t, body, "task_type_not_allowed", "build")

	resp, cancel := h.connect(t, bearer, "scope-refused", "other")
	defer cancel()
	requireScope(t, readScopeRefusal(t, resp), "worker_group_not_allowed", "other")

	unknown := postScopedPoll(t, h.url, "not-a-token",
		`{"task_types":["echo"],"worker_group":"other","timeout_ms":100}`)
	raw, _ := io.ReadAll(unknown.Body)
	unknown.Body.Close()
	if unknown.StatusCode != http.StatusUnauthorized || strings.Contains(string(raw), "quarry") {
		t.Fatalf("unknown token: status %d body %q, want a bare 401", unknown.StatusCode, raw)
	}
}
