// bridge/connect_worker_group_test.go
// Tests for #719: a bridge worker's registration records the worker
// groups it drains, so /v1/workers can answer "who serves group G".
//
// Methodology: real NATS server, real workertoken.Store, httptest
// server driving POST /v1/workers/connect. Connect is a long-lived SSE
// stream, so each successful connection is held open while its
// registration is read back from the worker directory -- the same
// value GET /v1/workers serializes -- and closed afterwards. Every
// resolver branch is covered through the real HTTP path: a named group
// is validated exactly as poll validates it, and an omitted group
// records the token's own scope rather than a false "ungrouped".
package bridge

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go/jetstream"
)

// connectGroupHarness is one bridge + token store per test.
type connectGroupHarness struct {
	url   string
	store *workertoken.Store
	dir   *worker.Directory
}

func newConnectGroupHarness(t *testing.T) connectGroupHarness {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := openTokenStore(t, js)
	b := newTestBridge(t, nc)
	b.token = "admin-secret"
	b.SetTokenStore(store)
	ts := httptest.NewServer(b.Handler())
	t.Cleanup(ts.Close)
	return connectGroupHarness{
		url: ts.URL, store: store, dir: worker.NewDirectory(js),
	}
}

// mint returns a bearer scoped to the echo task type and to groups
// (nil means unscoped by group).
func (h connectGroupHarness) mint(t *testing.T, groups []string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, bearer, err := h.store.Mint(ctx, "w", []string{"echo"}, groups, "t")
	if err != nil {
		t.Fatalf("Mint(groups=%v): %v", groups, err)
	}
	return bearer
}

// connect issues a connect naming group ("" omits the field). The
// caller closes resp.Body and calls cancel when done.
func (h connectGroupHarness) connect(
	t *testing.T, bearer, workerID, group string,
) (*http.Response, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	groupField := ""
	if group != "" {
		groupField = fmt.Sprintf(`,"worker_group":%q`, group)
	}
	body := fmt.Sprintf(
		`{"worker_id":%q,"task_types":["echo"],"max_tasks":1%s}`,
		workerID, groupField,
	)
	req, err := http.NewRequestWithContext(
		ctx, "POST", h.url+"/v1/workers/connect", strings.NewReader(body),
	)
	if err != nil {
		cancel()
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // caller closes
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	return resp, cancel
}

// TestConnectRecordsWorkerGroup covers every successful resolver
// branch: a named group, and an omitted group under each token scope.
func TestConnectRecordsWorkerGroup(t *testing.T) {
	cases := []struct {
		name        string
		tokenGroups []string
		requested   string
		want        []string
	}{
		{"named group", []string{"quarry"}, "quarry", []string{"quarry"}},
		{"omitted, single-group token", []string{"quarry"}, "", []string{"quarry"}},
		{"omitted, multi-group token", []string{"a", "b"}, "", []string{"a", "b"}},
		{"omitted, unscoped token", nil, "", nil},
		{"named, unscoped token", nil, "gpu", []string{"gpu"}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newConnectGroupHarness(t)
			workerID := fmt.Sprintf("w%d", i)
			resp, cancel := h.connect(t, h.mint(t, tc.tokenGroups),
				workerID, tc.requested)
			defer cancel()
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("connect status = %d, want 200", resp.StatusCode)
			}
			got := findWorker(t, h.dir, workerID).WorkerGroups
			if !slices.Equal(got, tc.want) {
				t.Fatalf("WorkerGroups = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestConnectRefusesGroupAsPollDoes pins that a named group is refused
// exactly where a poll would refuse it, and that no registration is
// written for a refused connect.
func TestConnectRefusesGroupAsPollDoes(t *testing.T) {
	cases := []struct {
		name        string
		tokenGroups []string
		requested   string
		wantStatus  int
		wantInBody  string
	}{
		{"group outside token scope", []string{"quarry"}, "other",
			http.StatusForbidden, `"quarry"`},
		{"invalid group value", nil, "has.dot",
			http.StatusBadRequest, "worker group"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newConnectGroupHarness(t)
			workerID := fmt.Sprintf("refused%d", i)
			resp, cancel := h.connect(t, h.mint(t, tc.tokenGroups),
				workerID, tc.requested)
			defer cancel()
			raw, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %q)",
					resp.StatusCode, tc.wantStatus, raw)
			}
			if !strings.Contains(string(raw), tc.wantInBody) {
				t.Fatalf("body %q lacks %q", raw, tc.wantInBody)
			}
			if _, present := workerPresent(t, h.dir, workerID); present {
				t.Fatalf("refused connect still registered %q", workerID)
			}
		})
	}
}
