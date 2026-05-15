package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
)

// extra_pages.go houses the PR 4 surfaces: triggers list/detail, DLQ
// list/detail, audit log, and the small run-id-lookup redirect. Each
// page mirrors the established pattern: handler → build view → render.
// Keeping these in a separate file from pages.go preserves git
// blame on the PR 1–3 surfaces.

// serveRunIDLookup redirects /console/runs/lookup?id=<id> to the
// run-detail page. Empty / whitespace input is treated as a noop and
// 302s back to the runs list — operators get a tactile signal that
// the input was seen but no navigation happened. Datastar's runtime
// URL interpolation pattern is awkward for this case, so a tiny
// server-side redirect is the simpler ousterhoutian path.
func serveRunIDLookup(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveRunIDLookup: w is nil")
	}
	if r == nil {
		panic("serveRunIDLookup: r is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		http.Redirect(w, r, "/console/runs", http.StatusFound)
		return
	}
	// Sanity: a run id can't have a slash. If the operator pasted a
	// URL fragment we strip everything down to the trailing segment.
	if idx := strings.LastIndex(id, "/"); idx >= 0 {
		id = id[idx+1:]
	}
	if id == "" || strings.Contains(id, " ") {
		serveNotFound(w, r, ts, cfg)
		return
	}
	http.Redirect(w, r,
		"/console/runs/"+id, http.StatusFound)
}

// TriggersListView is the binding for /console/triggers.
type TriggersListView struct {
	TypeFilter string
	Total      int
	Rows       []TriggerRow
}

// TriggerRow is one row on the triggers list page. The Kind / Target
// fields mirror the workflow-detail TriggerLine shape so the operator
// sees consistent labels across surfaces.
type TriggerRow struct {
	ID            string
	Kind          string
	Target        string
	Workflow      string
	Enabled       bool
	StatusLabel   string
	StatusIcon    string
	LastFiredText string
}

// servePageTriggersList renders /console/triggers.
func servePageTriggersList(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageTriggersList: w is nil")
	}
	if r == nil {
		panic("servePageTriggersList: r is nil")
	}
	ds, ok := requireData(w, cfg, "triggers-list")
	if !ok {
		return
	}
	view := buildTriggersView(r.Context(), ds, r.URL.Query())
	renderPage(w, r, ts, cfg, "triggers-list", pageData{
		Title:   "Triggers",
		Section: "triggers",
		Page:    view,
	})
}

// buildTriggersView pulls the trigger list from the DataSource and
// projects it into TriggerRow values. Filter on ?type=<kind>.
func buildTriggersView(
	ctx context.Context, ds DataSource, q map[string][]string,
) TriggersListView {
	if ds == nil {
		panic("buildTriggersView: ds is nil")
	}
	if ctx == nil {
		panic("buildTriggersView: ctx is nil")
	}
	typeFilter := firstQueryValue(q, "type")
	defs, _ := ds.ListTriggers(ctx)
	rows := make([]TriggerRow, 0, len(defs))
	for _, t := range defs {
		row := triggerRowFromDef(t)
		if typeFilter != "" && row.Kind != typeFilter {
			continue
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ID < rows[j].ID
	})
	return TriggersListView{
		TypeFilter: typeFilter,
		Total:      len(rows),
		Rows:       rows,
	}
}

// triggerRowFromDef projects a TriggerDef into a render-ready row.
func triggerRowFromDef(t trigger.TriggerDef) TriggerRow {
	kind, target := triggerKindAndTarget(t)
	row := TriggerRow{
		ID:       t.ID,
		Kind:     kind,
		Target:   target,
		Workflow: t.WorkflowID,
		Enabled:  t.Enabled,
	}
	if t.Enabled {
		row.StatusLabel = "enabled"
		row.StatusIcon = "✓"
	} else {
		row.StatusLabel = "disabled"
		row.StatusIcon = "⊘"
	}
	return row
}

// TriggerDetailView powers /console/triggers/<id>.
type TriggerDetailView struct {
	ID             string
	Kind           string
	Target         string
	Workflow       string
	Enabled        bool
	StatusLabel    string
	StatusIcon     string
	ConfigJSON     string
	SignatureOn    bool
	NotFound       bool
	RecentFirings  []TriggerFiringRow
	NextFireText   string
	NextFireMethod string
}

// TriggerFiringRow is one row in the "recent activity" panel. Empty
// when no firings have been recorded yet.
type TriggerFiringRow struct {
	FiredAt    string
	Outcome    string
	OutcomeRaw string
	RunID      string
	RunIDShort string
	Skipped    bool
}

// servePageTriggerDetail renders /console/triggers/<id>.
func servePageTriggerDetail(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageTriggerDetail: w is nil")
	}
	if r == nil {
		panic("servePageTriggerDetail: r is nil")
	}
	id := strings.TrimPrefix(r.URL.Path, "/console/triggers/")
	if id == "" || strings.Contains(id, "/") {
		serveNotFound(w, r, ts, cfg)
		return
	}
	ds, ok := requireData(w, cfg, "trigger-detail")
	if !ok {
		return
	}
	view := buildTriggerDetail(r.Context(), ds, id)
	renderPage(w, r, ts, cfg, "trigger-detail", pageData{
		Title:   "Trigger " + id,
		Section: "triggers",
		Page:    view,
	})
}

// buildTriggerDetail looks up the trigger and shapes the detail view.
func buildTriggerDetail(
	ctx context.Context, ds DataSource, id string,
) TriggerDetailView {
	if ds == nil {
		panic("buildTriggerDetail: ds is nil")
	}
	if id == "" {
		panic("buildTriggerDetail: id is empty")
	}
	defs, _ := ds.ListTriggers(ctx)
	for _, t := range defs {
		if t.ID != id {
			continue
		}
		return populateTriggerDetail(t)
	}
	return TriggerDetailView{ID: id, NotFound: true}
}

// populateTriggerDetail assembles the per-trigger detail view from
// one TriggerDef. Pulled out so buildTriggerDetail stays under 70
// lines.
func populateTriggerDetail(t trigger.TriggerDef) TriggerDetailView {
	kind, target := triggerKindAndTarget(t)
	cfgJSON, _ := json.MarshalIndent(triggerConfigOf(t), "", "  ")
	view := TriggerDetailView{
		ID:         t.ID,
		Kind:       kind,
		Target:     target,
		Workflow:   t.WorkflowID,
		Enabled:    t.Enabled,
		ConfigJSON: string(cfgJSON),
	}
	if t.Enabled {
		view.StatusLabel = "enabled"
		view.StatusIcon = "✓"
	} else {
		view.StatusLabel = "disabled"
		view.StatusIcon = "⊘"
	}
	if t.Webhook != nil && t.Webhook.Secret != "" {
		view.SignatureOn = true
	}
	if t.HTTP != nil && t.HTTP.Secret != "" {
		view.SignatureOn = true
	}
	view.NextFireText, view.NextFireMethod = triggerNextFire(t)
	return view
}

// triggerNextFire describes when/how the trigger is expected to fire
// next. For cron triggers we surface "via cron <expr>"; for non-cron
// kinds we report the firing source. The exact next-fire wallclock
// is not computed here — that lives in the scheduler and would
// require time-zone plumbing the console doesn't have.
func triggerNextFire(t trigger.TriggerDef) (string, string) {
	switch {
	case t.Cron != nil:
		return "computed by scheduler — " + t.Cron.Expression,
			"cron"
	case t.Subject != nil:
		return "on NATS subject match", "subject"
	case t.HTTP != nil:
		return "on HTTP request", "http"
	case t.Webhook != nil:
		return "on webhook delivery", "webhook"
	}
	return "unknown", "unknown"
}

// triggerConfigOf returns the active per-kind config block so the
// JSON pretty-print only shows the relevant section. Empty for
// unknown kinds.
func triggerConfigOf(t trigger.TriggerDef) any {
	switch {
	case t.Cron != nil:
		return t.Cron
	case t.Subject != nil:
		return t.Subject
	case t.HTTP != nil:
		return t.HTTP
	case t.Webhook != nil:
		return t.Webhook
	}
	return struct{}{}
}

// DLQListView powers /console/dlq.
type DLQListView struct {
	ReasonFilter string
	Total        int
	Rows         []DLQRow
	ReadOnly     bool
}

// DLQRow is one row on the DLQ list page.
type DLQRow struct {
	Sequence       uint64
	ReasonShort    string
	ReasonFull     string
	Workflow       string
	OriginalRunID  string
	OriginalRunIDS string
	FailedAt       string
	Attempts       int
	BodyPreserved  bool
}

// servePageDLQList renders /console/dlq.
func servePageDLQList(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageDLQList: w is nil")
	}
	if r == nil {
		panic("servePageDLQList: r is nil")
	}
	ds, ok := requireData(w, cfg, "dlq-list")
	if !ok {
		return
	}
	view := buildDLQView(r.Context(), ds, r.URL.Query())
	view.ReadOnly = cfg.ReadOnly
	renderPage(w, r, ts, cfg, "dlq-list", pageData{
		Title:   "DLQ",
		Section: "dlq",
		Page:    view,
	})
}

// buildDLQView pulls the recent dead letters and shapes them as rows.
func buildDLQView(
	ctx context.Context, ds DataSource, q map[string][]string,
) DLQListView {
	if ds == nil {
		panic("buildDLQView: ds is nil")
	}
	if ctx == nil {
		panic("buildDLQView: ctx is nil")
	}
	const dlqMax = 100
	views, _ := ds.ListDeadLetters(ctx, dlqMax)
	reasonFilter := firstQueryValue(q, "reason")
	rows := make([]DLQRow, 0, len(views))
	for _, v := range views {
		row := dlqRowFromView(v)
		if reasonFilter != "" &&
			classifyDLQReason(v.Error) != reasonFilter {
			continue
		}
		rows = append(rows, row)
	}
	return DLQListView{
		ReasonFilter: reasonFilter,
		Total:        len(rows),
		Rows:         rows,
	}
}

// dlqRowFromView projects a DeadLetterView into a DLQRow.
func dlqRowFromView(v api.DeadLetterView) DLQRow {
	const reasonPreview = 80
	short := v.Error
	if len(short) > reasonPreview {
		short = short[:reasonPreview] + "…"
	}
	return DLQRow{
		Sequence:       v.Sequence,
		ReasonShort:    short,
		ReasonFull:     v.Error,
		Workflow:       extractWorkflowFromTask(v.Task),
		OriginalRunID:  v.RunID,
		OriginalRunIDS: shortRunID(v.RunID),
		FailedAt:       v.Timestamp.UTC().Format(time.RFC3339),
		Attempts:       v.DeliveryCount,
		BodyPreserved:  v.BodyPreserved,
	}
}

// classifyDLQReason buckets a DLQ error string into a small reason set:
// timeout / panic / unrecoverable / max-attempts / other. Best-effort
// substring match; new reasons fall into "other" until they're added.
func classifyDLQReason(errMsg string) string {
	lc := strings.ToLower(errMsg)
	switch {
	case strings.Contains(lc, "timeout"),
		strings.Contains(lc, "timed out"):
		return "timeout"
	case strings.Contains(lc, "panic"):
		return "panic"
	case strings.Contains(lc, "unrecoverable"):
		return "unrecoverable"
	case strings.Contains(lc, "max attempts"),
		strings.Contains(lc, "max-attempts"),
		strings.Contains(lc, "delivery limit"):
		return "max-attempts"
	}
	return "other"
}

// extractWorkflowFromTask returns the workflow name encoded in a task
// subject. Task subjects follow the pattern "task.<workflow>.<step>";
// the function returns the middle segment when present. Falls back to
// the raw task on parse failure.
func extractWorkflowFromTask(task string) string {
	if task == "" {
		return ""
	}
	parts := strings.SplitN(task, ".", 3)
	if len(parts) < 2 {
		return task
	}
	if parts[0] == "task" && len(parts) >= 2 {
		return parts[1]
	}
	return parts[0]
}

// DLQDetailView powers /console/dlq/<seq>.
type DLQDetailView struct {
	Sequence       uint64
	ReasonFull     string
	ReasonClass    string
	Workflow       string
	OriginalRunID  string
	OriginalRunIDS string
	StepID         string
	Task           string
	FailedAt       string
	Attempts       int
	Consumer       string
	BodyPreview    string
	BodyPreserved  bool
	NotFound       bool
	ReadOnly       bool
	CSRFToken      string
}

// servePageDLQDetail renders /console/dlq/<seq>.
func servePageDLQDetail(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageDLQDetail: w is nil")
	}
	if r == nil {
		panic("servePageDLQDetail: r is nil")
	}
	seqStr := strings.TrimPrefix(r.URL.Path, "/console/dlq/")
	if seqStr == "" || strings.Contains(seqStr, "/") {
		serveNotFound(w, r, ts, cfg)
		return
	}
	ds, ok := requireData(w, cfg, "dlq-detail")
	if !ok {
		return
	}
	view := buildDLQDetail(r.Context(), ds, seqStr)
	view.ReadOnly = cfg.ReadOnly
	view.CSRFToken = csrfTokenFor(r)
	if view.NotFound {
		serveNotFound(w, r, ts, cfg)
		return
	}
	renderPage(w, r, ts, cfg, "dlq-detail", pageData{
		Title:   fmt.Sprintf("DLQ #%d", view.Sequence),
		Section: "dlq",
		Page:    view,
	})
}

// buildDLQDetail looks up a single DLQ entry by sequence string.
func buildDLQDetail(
	ctx context.Context, ds DataSource, seqStr string,
) DLQDetailView {
	if ds == nil {
		panic("buildDLQDetail: ds is nil")
	}
	if seqStr == "" {
		panic("buildDLQDetail: seqStr is empty")
	}
	seq, err := parseDLQSequence(seqStr)
	if err != nil {
		return DLQDetailView{NotFound: true}
	}
	const dlqMax = 500
	views, _ := ds.ListDeadLetters(ctx, dlqMax)
	for _, v := range views {
		if v.Sequence != seq {
			continue
		}
		return dlqDetailFromView(v)
	}
	return DLQDetailView{NotFound: true}
}

// dlqDetailFromView populates a DLQDetailView from one DeadLetterView.
func dlqDetailFromView(v api.DeadLetterView) DLQDetailView {
	const bodyPreviewMax = 4000
	bodyStr := string(v.Body)
	if len(bodyStr) > bodyPreviewMax {
		bodyStr = bodyStr[:bodyPreviewMax] + "\n... (truncated)"
	}
	return DLQDetailView{
		Sequence:       v.Sequence,
		ReasonFull:     v.Error,
		ReasonClass:    classifyDLQReason(v.Error),
		Workflow:       extractWorkflowFromTask(v.Task),
		OriginalRunID:  v.RunID,
		OriginalRunIDS: shortRunID(v.RunID),
		StepID:         v.StepID,
		Task:           v.Task,
		FailedAt:       v.Timestamp.UTC().Format(time.RFC3339),
		Attempts:       v.DeliveryCount,
		Consumer:       v.Consumer,
		BodyPreview:    bodyStr,
		BodyPreserved:  v.BodyPreserved,
	}
}

// parseDLQSequence parses a uint64 sequence string. Returns the
// parsed value or an error on garbage / overflow / negative input.
func parseDLQSequence(s string) (uint64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty sequence")
	}
	var v uint64
	for _, b := range []byte(s) {
		if b < '0' || b > '9' {
			return 0, fmt.Errorf("invalid sequence %q", s)
		}
		v = v*10 + uint64(b-'0')
	}
	if v == 0 {
		return 0, fmt.Errorf("sequence must be > 0")
	}
	return v, nil
}

// csrfTokenFor returns a small per-request token tied to the actor's
// session. The token is just a server-side opaque string the form
// echoes back; the server compares the echo with the freshly-generated
// value carried in a hidden header. Since the console is single-origin
// and same-binary, this is a soft defence: the real boundary is
// authMiddleware. Operators who flip CONSOLE_READ_ONLY=true also
// receive a 405 long before the token check runs.
func csrfTokenFor(r *http.Request) string {
	if r == nil {
		return ""
	}
	actor, _ := ActorFrom(r.Context())
	return actor.Display() + ":" + r.URL.Path
}

// AuditLogView powers /console/ops/audit.
type AuditLogView struct {
	ActorFilter  string
	ActionFilter string
	Total        int
	Rows         []AuditRow
}

// AuditRow is one rendered audit-log entry.
type AuditRow struct {
	Time     string
	TimeRel  string
	Actor    string
	Action   string
	Target   string
	Outcome  string
	DataJSON string
}

// servePageAuditLog renders /console/ops/audit.
func servePageAuditLog(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageAuditLog: w is nil")
	}
	if r == nil {
		panic("servePageAuditLog: r is nil")
	}
	ds, ok := requireData(w, cfg, "audit-log")
	if !ok {
		return
	}
	view := buildAuditView(r.Context(), ds, r.URL.Query())
	renderPage(w, r, ts, cfg, "audit-log", pageData{
		Title:   "Audit log",
		Section: "ops",
		Page:    view,
	})
}

// buildAuditView reads recent audit events and applies filter params.
func buildAuditView(
	ctx context.Context, ds DataSource, q map[string][]string,
) AuditLogView {
	if ds == nil {
		panic("buildAuditView: ds is nil")
	}
	if ctx == nil {
		panic("buildAuditView: ctx is nil")
	}
	const auditMax = 200
	events, _ := ds.ListAuditEvents(ctx, auditMax)
	actorFilter := firstQueryValue(q, "actor")
	actionFilter := firstQueryValue(q, "action")
	rows := make([]AuditRow, 0, len(events))
	for _, e := range events {
		if actorFilter != "" && e.Actor != actorFilter {
			continue
		}
		if actionFilter != "" && e.Action != actionFilter {
			continue
		}
		rows = append(rows, auditRowFromEvent(e))
	}
	return AuditLogView{
		ActorFilter:  actorFilter,
		ActionFilter: actionFilter,
		Total:        len(rows),
		Rows:         rows,
	}
}

// auditRowFromEvent projects one AuditEvent into the render shape.
func auditRowFromEvent(e AuditEvent) AuditRow {
	dataBytes, _ := json.Marshal(e.Data)
	return AuditRow{
		Time:     e.Time.UTC().Format(time.RFC3339),
		TimeRel:  formatDuration(time.Since(e.Time)) + " ago",
		Actor:    e.Actor,
		Action:   e.Action,
		Target:   e.Target,
		Outcome:  e.Outcome,
		DataJSON: string(dataBytes),
	}
}
