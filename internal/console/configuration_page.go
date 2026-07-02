// configuration_page.go owns /console/config — the one-screen
// deployment self-portrait (#312, parent ADR-015 R3). The page
// answers: how many of each object exist, what URLs is this serving,
// which JetStream resources are provisioned, which workers are
// plugged in, which trigger types are registered, what binary is
// running.
//
// Adapter-first layout per iii's config.tsx: sections are grouped
// by the underlying system (Endpoints → JetStream resources →
// Workers → Trigger types) rather than by concept. The counts strip
// at the top is the only "summary" affordance; everything below is
// resource-oriented.
//
// The page reuses cfg.Build for the build line (already plumbed at
// handler.go:56). Only NATS server version and Go runtime version
// are net-new; the DataSource surfaces the former via
// ConfigSnapshot, the latter comes from runtime.Version().
//
// YAML export is rendered inline (no separate modal partial) — the
// modal markup lives in configuration.html and toggles via a
// <details> element so the page works without JS. The YAML payload
// is the same struct shape ConfigSnapshot already carries; future
// PR 3 of #273 can promote this string to a typed manifest.
package console

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/danmestas/dagnats/worker"
)

// ConfigPageView is the binding the configuration.html template
// renders. Header carries the counts strip via the shared page-header
// partial; the per-section slices fill the rest.
type ConfigPageView struct {
	Header        PageHeader
	AccessPosture AccessPostureView
	Endpoints     []EndpointCard
	Streams       []ConfigStreamRow
	KVBuckets     []KVBucketInfo
	WorkerGroups  []WorkerGroup
	TriggerTypes  []TriggerTypeRow
	Invariants    []InvariantRow
	Build         BuildFooter
	YAMLExport    string
}

// AccessPostureView is the static counterpart to the Audit log: it
// reports the auth gate the console resolved at startup and whether
// mutations are disabled. Every field is read from the Config the handler
// already holds (cfg.AuthMode, cfg.ReadOnly) — no DataSource round-trip,
// no fabricated runtime state.
type AccessPostureView struct {
	Modes       []AuthModePill
	ActiveNote  string // actor-source note keyed off the active AuthMode
	ReadOnly    bool
	ReadOnlyEnv string // "CONSOLE_READ_ONLY"
	AuditHref   string // "/console/audit" — a real, navigable route
	SourceRef   string // "console/auth.go" — header provenance label
}

// AuthModePill is one chip in the auth-mode strip. Active marks the mode
// the console actually resolved (exactly one is active).
type AuthModePill struct {
	Label  string
	Active bool
}

// InvariantRow is one row in the engine-invariants table — a compile-time
// constant the engine enforces, surfaced as reference documentation. Tone
// types the Source pill; Source is always "hardcoded" for these.
type InvariantRow struct {
	Constant string
	Value    string
	Governs  string
	Source   string
	Tone     TileTone
}

// EndpointCard is one card in the endpoints panel. URL is the
// operator-facing address; Subtitle is a short orientation string
// shown beneath the title. Iconography is keyed by the Kind enum so
// CSS owns the glyph rather than the Go side baking in a class.
type EndpointCard struct {
	Title    string
	Subtitle string
	URL      string
	Kind     string // "console" | "nats" | "otlp" | "monitor" | "bridge"
}

// ConfigStreamRow is one row in the JetStream streams table. Empty Size /
// Messages render as "—" so an unprovisioned stream doesn't lie about
// zero state.
type ConfigStreamRow struct {
	Name        string
	Messages    uint64
	Bytes       uint64
	Retention   string
	Provisioned bool
}

// WorkerGroup aggregates registered workers by their primary task
// type so the page renders one row per group (matching iii's
// "Worker Pools" section). Members carries the raw registrations
// for an operator drilling down.
type WorkerGroup struct {
	Name     string
	Count    int
	LastSeen string
	Members  []worker.WorkerRegistration
}

// TriggerTypeRow is one cell in the registered-trigger-types grid.
// Description orients the operator on what the kind does. Built-in
// for v1 — #273 Phase 2 will populate this from the registry once
// the trigger-type API lands.
type TriggerTypeRow struct {
	Name        string
	Description string
	Glyph       template.HTML
}

// BuildFooter is the one-line build/runtime signature rendered at
// the bottom of the page. DagnatsBuild is cfg.Build (already plumbed,
// see handler.go); NATSServerVersion comes from
// nc.ConnectedServerVersion() via ConfigSnapshot; GoVersion is
// runtime.Version().
type BuildFooter struct {
	DagnatsBuild      string
	NATSServerVersion string
	GoVersion         string
}

// servePageConfiguration renders /console/config.
func servePageConfiguration(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageConfiguration: w is nil")
	}
	if r == nil {
		panic("servePageConfiguration: r is nil")
	}
	view := buildConfigView(r.Context(), cfg)
	renderPage(w, r, ts, cfg, "configuration", pageData{
		Title:   "Config",
		Section: "config",
		Page:    view,
	})
}

// buildConfigView assembles the full page view from the available
// data sources. Each section degrades to a clear empty state when
// its source is nil / unreachable — the page is observational, so
// a missing piece never blocks the rest from rendering.
func buildConfigView(ctx context.Context, cfg Config) ConfigPageView {
	snap := fetchConfigSnapshot(ctx, cfg.Data)
	view := ConfigPageView{
		AccessPosture: buildAccessPosture(cfg),
		Endpoints:     buildEndpoints(cfg, snap),
		Streams:       buildStreamRows(snap),
		KVBuckets:     snap.KVBuckets,
		WorkerGroups:  groupWorkers(snap.Workers),
		TriggerTypes:  builtInTriggerTypes(),
		Invariants:    engineInvariants(),
		Build: BuildFooter{
			DagnatsBuild:      consoleBuildLabel(cfg.Build),
			NATSServerVersion: snap.NATSServerVersion,
			GoVersion:         runtime.Version(),
		},
	}
	view.Header = buildConfigHeader(ctx, cfg.Data, snap)
	view.YAMLExport = renderConfigYAML(cfg, view, snap)
	return view
}

// fetchConfigSnapshot calls into the DataSource for the resources
// the page surfaces. Returns a zero-value snapshot when ds is nil so
// the renderer paints the empty-state shell consistently.
func fetchConfigSnapshot(
	ctx context.Context, ds DataSource,
) ConfigSnapshot {
	if ds == nil {
		return ConfigSnapshot{}
	}
	snap, err := ds.ConfigSnapshot(ctx)
	if err != nil {
		return ConfigSnapshot{}
	}
	return snap
}

// buildConfigHeader builds the R5 counts strip at the top of the
// page. Counts come from the established list calls so we don't
// duplicate enumeration into the snapshot.
func buildConfigHeader(
	ctx context.Context, ds DataSource, snap ConfigSnapshot,
) PageHeader {
	tiles := configTiles(ctx, ds, snap)
	header, err := NewPageHeader(PageHeader{
		Title:    "Config",
		Subtitle: "Live shape of this deployment.",
		Tiles:    tiles,
	})
	if err != nil {
		// Falls back to a bare title — a tile-validation bug must
		// not 500 the page. Production hits this path only on a
		// programmer error in tile construction.
		return PageHeader{Title: "Config"}
	}
	return header
}

// configTiles assembles the six count tiles. Each tile reports the
// adjacent inventory at a glance: workflows, triggers, workers,
// streams, KV buckets, DLQ entries. Counts default to 0 when the
// DataSource is absent.
func configTiles(
	ctx context.Context, ds DataSource, snap ConfigSnapshot,
) []Tile {
	wf, trg, dlq := configListCounts(ctx, ds)
	return []Tile{
		{Label: "WORKFLOWS", Count: wf, Tone: ToneDefault,
			Href: "/console/workflows"},
		{Label: "TRIGGERS", Count: trg, Tone: ToneInfo,
			Href: "/console/triggers"},
		{Label: "WORKERS", Count: len(snap.Workers), Tone: ToneSuccess,
			Href: "/console/workers"},
		{Label: "STREAMS", Count: len(snap.Streams), Tone: ToneDefault},
		{Label: "KV BUCKETS", Count: len(snap.KVBuckets), Tone: ToneDefault,
			Href: "/console/kv"},
		{Label: "DLQ", Count: dlq, Tone: dlqTone(dlq),
			Href: "/console/dlq"},
	}
}

// configListCounts pulls the three list-derived counts in one call
// site so configTiles stays readable. Errors collapse to zero — the
// page renders empty state rather than 500ing on a transient list
// failure.
func configListCounts(
	ctx context.Context, ds DataSource,
) (workflows, triggers, dlq int) {
	if ds == nil {
		return 0, 0, 0
	}
	if wfs, err := ds.ListWorkflows(ctx); err == nil {
		workflows = len(wfs)
	}
	if trgs, err := ds.ListTriggers(ctx); err == nil {
		triggers = len(trgs)
	}
	const dlqProbe = 200 // bounded probe; tile is "any in flight?"
	if dl, err := ds.ListDeadLetters(ctx, dlqProbe); err == nil {
		dlq = len(dl)
	}
	return workflows, triggers, dlq
}

// dlqTone maps a DLQ count to a tone — danger when anything is in
// the queue (operator attention), default when empty.
func dlqTone(count int) TileTone {
	if count > 0 {
		return ToneDanger
	}
	return ToneDefault
}

// buildEndpoints assembles the endpoints panel (iii's adapter-first
// equivalent). Always renders the console listener; appends NATS and
// observability cards when the snapshot supplies them.
func buildEndpoints(cfg Config, snap ConfigSnapshot) []EndpointCard {
	cards := make([]EndpointCard, 0, 4)
	cards = append(cards, EndpointCard{
		Title:    "Console",
		Subtitle: "HTTP listener",
		URL:      cfg.HTTPAddr,
		Kind:     "console",
	})
	if snap.NATSURL != "" {
		cards = append(cards, EndpointCard{
			Title:    "NATS",
			Subtitle: "JetStream + KV",
			URL:      snap.NATSURL,
			Kind:     "nats",
		})
	}
	if snap.OTLPEndpoint != "" {
		cards = append(cards, EndpointCard{
			Title:    "OTLP exporter",
			Subtitle: "Traces + metrics",
			URL:      snap.OTLPEndpoint,
			Kind:     "otlp",
		})
	}
	return cards
}

// buildStreamRows converts ConfigSnapshot.Streams into render rows.
// An empty input slice means the snapshot couldn't talk to
// JetStream — the template renders the empty-state row. The config
// page renders a subset of the snapshot fields (name, counts,
// retention, provisioned); subjects + consumer counts surface on the
// dedicated /console/streams page instead.
func buildStreamRows(snap ConfigSnapshot) []ConfigStreamRow {
	out := make([]ConfigStreamRow, 0, len(snap.Streams))
	for _, s := range snap.Streams {
		out = append(out, ConfigStreamRow{
			Name:        s.Name,
			Messages:    s.Messages,
			Bytes:       s.Bytes,
			Retention:   s.Retention,
			Provisioned: s.Provisioned,
		})
	}
	return out
}

// groupWorkers folds the raw worker registrations into one row per
// task-type. iii uses worker pools as the primary grouping; we
// follow because the operator question is "what kinds of tasks can
// this deployment run?", not "what processes are alive?". The
// process-level view lives at /console/ops/workers.
func groupWorkers(
	regs []worker.WorkerRegistration,
) []WorkerGroup {
	if len(regs) == 0 {
		return nil
	}
	groups := make(map[string][]worker.WorkerRegistration)
	for _, reg := range regs {
		key := primaryTaskType(reg)
		groups[key] = append(groups[key], reg)
	}
	out := make([]WorkerGroup, 0, len(groups))
	for name, members := range groups {
		out = append(out, WorkerGroup{
			Name:     name,
			Count:    len(members),
			LastSeen: latestSeen(members),
			Members:  members,
		})
	}
	// Stable ordering keeps DOM substring assertions in tests
	// deterministic regardless of map iteration order.
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

// primaryTaskType picks the canonical group label for a worker.
// Joins the task-types slice with "+" when a worker advertises
// multiple; falls back to the worker ID for ungrouped pools.
func primaryTaskType(reg worker.WorkerRegistration) string {
	if len(reg.TaskTypes) == 0 {
		return reg.WorkerID
	}
	if len(reg.TaskTypes) == 1 {
		return reg.TaskTypes[0]
	}
	parts := make([]string, len(reg.TaskTypes))
	copy(parts, reg.TaskTypes)
	sort.Strings(parts)
	return strings.Join(parts, "+")
}

// latestSeen formats the freshest LastSeen across the group's
// members. Returns "—" when no member carried the field (older
// registrations from before #289 may not have it).
func latestSeen(regs []worker.WorkerRegistration) string {
	var newest time.Time
	for _, r := range regs {
		if r.LastSeen.After(newest) {
			newest = r.LastSeen
		}
	}
	if newest.IsZero() {
		return "—"
	}
	return formatRelative(newest, time.Now())
}

// formatRelative renders a coarse "N s/m/h/d ago" string. Mirrors
// the format used in the DLQ / runs lists so the operator reads
// staleness consistently across pages.
func formatRelative(t, now time.Time) string {
	d := now.Sub(t)
	switch {
	case d < time.Second:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	return fmt.Sprintf("%dd ago", int(d.Hours()/24))
}

// builtInTriggerTypes returns the registered-trigger-types section
// data. Hard-coded to the 4 built-ins until #273 Phase 2.1 surfaces
// a real registry endpoint. Each entry carries the same glyph the
// triggers list uses (triggerKindGlyph in handler.go).
func builtInTriggerTypes() []TriggerTypeRow {
	return []TriggerTypeRow{
		{Name: "cron", Description: "Time-based firings on a schedule.",
			Glyph: triggerKindGlyph("cron")},
		{Name: "subject", Description: "Fire on a NATS subject match.",
			Glyph: triggerKindGlyph("subject")},
		{Name: "webhook", Description: "Fire on an inbound webhook POST.",
			Glyph: triggerKindGlyph("webhook")},
		{Name: "http", Description: "Fire on an HTTP route match.",
			Glyph: triggerKindGlyph("http")},
	}
}

// buildAccessPosture renders the access-posture card from the Config the
// handler already holds. The four-mode strip iterates the AuthMode enum in
// declaration order and marks the resolved mode active; the actor note is
// keyed off that mode. All data is config the console already knows — no
// read, no fabrication.
func buildAccessPosture(cfg Config) AccessPostureView {
	modes := []AuthMode{
		AuthLoopback, AuthForwarded, AuthBasic, AuthDisabled,
	}
	if len(modes) != 4 {
		panic("buildAccessPosture: AuthMode set drifted from 4")
	}
	pills := make([]AuthModePill, 0, len(modes))
	for _, m := range modes {
		pills = append(pills, AuthModePill{
			Label:  m.String(),
			Active: m == cfg.AuthMode,
		})
	}
	return AccessPostureView{
		Modes:       pills,
		ActiveNote:  authActorNote(cfg.AuthMode),
		ReadOnly:    cfg.ReadOnly,
		ReadOnlyEnv: "CONSOLE_READ_ONLY",
		AuditHref:   "/console/audit",
		SourceRef:   "console/auth.go",
	}
}

// authActorNote returns the one-line actor-source note for the active auth
// mode. Wording is grounded in the AuthMode doc comments (auth.go:17-28),
// not invented.
func authActorNote(mode AuthMode) string {
	switch mode {
	case AuthLoopback:
		return "actor implicit (loopback bind)"
	case AuthForwarded:
		return "actor from X-Forwarded-User"
	case AuthBasic:
		return `actor is "console"`
	case AuthDisabled:
		return "no auth — listener refuses to serve"
	default:
		return "unknown auth mode"
	}
}

// engineInvariants returns the static engine-invariants table: the
// compile-time constants the engine enforces, surfaced as reference
// documentation. Each value is cited to its source so the table stays
// auditable. AckWait is split into two scoped rows because the worker task
// consumer (5m, worker/consumer_naming.go:14) and the WORKFLOW_HISTORY
// consumer (30s default, internal/natsutil/conn.go:53-54) differ — a
// single bare "AckWait" row would fabricate one of them.
func engineInvariants() []InvariantRow {
	const src = "hardcoded"
	rows := []InvariantRow{
		{"AckWait (WORKFLOW_HISTORY consumer)", "30s",
			"history consumer ack timeout (conn.go:53-54)", src, ToneDefault},
		{"AckWait (worker task consumer)", "5m (default)",
			"task consumer ack timeout (consumer_naming.go:14)", src, ToneDefault},
		{"MaxDeliver", "-1 (unlimited)",
			"engine retries via NakWithDelay, not redelivery cap (worker.go:577)",
			src, ToneDefault},
		{"WORKFLOW_HISTORY dedup", "5s",
			"duplicate-publish window on history.> (conn.go:31)", src, ToneDefault},
		{"DEAD_LETTERS dedup", "24h",
			"dead-letter dedup window (conn.go:63)", src, ToneDefault},
		{"TELEMETRY retention", "7 days / 1 GiB",
			"telemetry stream age/size cap (conn.go:228-229)", src, ToneDefault},
		{"TELEMETRY dedup", "5s",
			"telemetry duplicate window (conn.go:230)", src, ToneDefault},
		{"workers KV TTL", "60s",
			"worker heartbeat liveness — expiry = stale (conn.go:106)", src,
			ToneDefault},
		{"worker_status KV TTL", "120s",
			"per-worker counter snapshot expiry (conn.go:112)", src, ToneDefault},
		{"idempotency_keys TTL", "24h",
			"generic idempotency replay window (conn.go:131)", src, ToneDefault},
		{"http_idempotency TTL", "1h",
			"HTTP trigger dedup window (conn.go:143)", src, ToneDefault},
		{"approval_tokens TTL", "168h (7d)",
			"human-approval token expiry (conn.go:121)", src, ToneDefault},
		{"sticky_bindings TTL", "25h",
			"worker affinity lifetime (conn.go:148)", src, ToneDefault},
		{"debounce_state TTL", "14d",
			"trigger debounce timer cleanup (conn.go:126)", src, ToneDefault},
	}
	if len(rows) != 14 {
		panic("engineInvariants: row count drifted")
	}
	return rows
}

// renderConfigYAML produces the deployment-shape YAML the export
// modal displays. The output is intentionally human-shaped and not
// re-parseable by the engine — it's a snapshot, not a manifest.
// #273 Port 3 will promote this to a typed manifest with round-trip
// guarantees.
func renderConfigYAML(
	cfg Config, view ConfigPageView, snap ConfigSnapshot,
) string {
	var b strings.Builder
	b.Grow(2048)
	b.WriteString("# dagnats deployment snapshot\n")
	b.WriteString("# Generated by /console/config — observational only.\n\n")
	b.WriteString("build:\n")
	b.WriteString("  dagnats: ")
	b.WriteString(yamlString(view.Build.DagnatsBuild))
	b.WriteString("\n")
	b.WriteString("  nats_server: ")
	b.WriteString(yamlString(view.Build.NATSServerVersion))
	b.WriteString("\n")
	b.WriteString("  go: ")
	b.WriteString(yamlString(view.Build.GoVersion))
	b.WriteString("\n\n")
	b.WriteString("endpoints:\n")
	b.WriteString("  console: ")
	b.WriteString(yamlString(cfg.HTTPAddr))
	b.WriteString("\n")
	if snap.NATSURL != "" {
		b.WriteString("  nats: ")
		b.WriteString(yamlString(snap.NATSURL))
		b.WriteString("\n")
	}
	if snap.OTLPEndpoint != "" {
		b.WriteString("  otlp: ")
		b.WriteString(yamlString(snap.OTLPEndpoint))
		b.WriteString("\n")
	}
	b.WriteString("\nstreams:\n")
	writeYAMLStreams(&b, view.Streams)
	b.WriteString("\nkv_buckets:\n")
	writeYAMLBuckets(&b, view.KVBuckets)
	b.WriteString("\nworker_groups:\n")
	writeYAMLWorkers(&b, view.WorkerGroups)
	b.WriteString("\ntrigger_types:\n")
	for _, t := range view.TriggerTypes {
		b.WriteString("  - ")
		b.WriteString(yamlString(t.Name))
		b.WriteString("\n")
	}
	return b.String()
}

// writeYAMLStreams emits the streams: section. Pulled out so
// renderConfigYAML stays within the 70-line ceiling.
func writeYAMLStreams(b *strings.Builder, rows []ConfigStreamRow) {
	if len(rows) == 0 {
		b.WriteString("  [] # JetStream account info unreachable\n")
		return
	}
	const maxRows = 64
	for i := 0; i < len(rows) && i < maxRows; i++ {
		s := rows[i]
		b.WriteString("  - name: ")
		b.WriteString(yamlString(s.Name))
		b.WriteString("\n")
		b.WriteString("    messages: ")
		b.WriteString(fmt.Sprintf("%d", s.Messages))
		b.WriteString("\n")
		b.WriteString("    bytes: ")
		b.WriteString(fmt.Sprintf("%d", s.Bytes))
		b.WriteString("\n")
		if s.Retention != "" {
			b.WriteString("    retention: ")
			b.WriteString(yamlString(s.Retention))
			b.WriteString("\n")
		}
	}
}

// writeYAMLBuckets emits the kv_buckets: section.
func writeYAMLBuckets(b *strings.Builder, rows []KVBucketInfo) {
	if len(rows) == 0 {
		b.WriteString("  []\n")
		return
	}
	const maxRows = 64
	for i := 0; i < len(rows) && i < maxRows; i++ {
		r := rows[i]
		b.WriteString("  - name: ")
		b.WriteString(yamlString(r.Name))
		b.WriteString("\n")
		b.WriteString("    keys: ")
		b.WriteString(fmt.Sprintf("%d", r.Keys))
		b.WriteString("\n")
	}
}

// writeYAMLWorkers emits the worker_groups: section.
func writeYAMLWorkers(b *strings.Builder, groups []WorkerGroup) {
	if len(groups) == 0 {
		b.WriteString("  []\n")
		return
	}
	const maxRows = 64
	for i := 0; i < len(groups) && i < maxRows; i++ {
		g := groups[i]
		b.WriteString("  - name: ")
		b.WriteString(yamlString(g.Name))
		b.WriteString("\n")
		b.WriteString("    count: ")
		b.WriteString(fmt.Sprintf("%d", g.Count))
		b.WriteString("\n")
	}
}

// yamlString quotes the string when it could be misread by a YAML
// parser (contains ':', '#', leading '-', etc). Plain identifiers
// emit unquoted to keep the snapshot readable. Empty string emits
// as quoted "" so the key isn't dropped by readers expecting a value.
func yamlString(s string) string {
	if s == "" {
		return `""`
	}
	const unsafe = ":#-{}[],&*!|>'\"%@`"
	for i := 0; i < len(s); i++ {
		if strings.ContainsRune(unsafe, rune(s[i])) ||
			s[i] == ' ' || s[i] == '\t' {
			return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
		}
	}
	return s
}
