package console

import (
	"fmt"
	"net/http"
)

// ServerHealthView powers /console/server: the embedded NATS server's
// identity plus its JetStream account capacity. Note carries a
// read-failure explanation when the DataSource read degrades, empty
// otherwise — mirroring the consumers page's degrade-don't-500 contract.
type ServerHealthView struct {
	Header PageHeader
	Health ServerHealth
	Note   string

	// Human-readable byte strings derived from Health so the template
	// stays logic-light. A non-positive Max renders as the unlimited
	// glyph rather than a humanized negative.
	StoreUsedHuman  string
	StoreMaxHuman   string
	MemoryUsedHuman string
	MemoryMaxHuman  string
}

// servePageServer renders /console/server. A ServerHealth read failure
// degrades to zero-valued fields + an explanatory Note rather than a
// 500, matching how the streams / consumers / config pages survive a
// JetStream outage — operators see the page shell, not an error screen.
func servePageServer(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageServer: w is nil")
	}
	if r == nil {
		panic("servePageServer: r is nil")
	}
	data, ok := requireData(w, cfg, "read server health")
	if !ok {
		return
	}
	health, err := data.ServerHealth(r.Context())
	note := ""
	if err != nil {
		cfg.Logger.Error("console: server health", "err", err)
		health = ServerHealth{}
		note = "Server health could not be read from JetStream right " +
			"now; capacity fields are blank until the read succeeds."
	}
	view := ServerHealthView{
		Header:          buildServerHeader(health),
		Health:          health,
		Note:            note,
		StoreUsedHuman:  humanBytes(health.StoreUsed),
		StoreMaxHuman:   humanBytesMax(health.StoreMax),
		MemoryUsedHuman: humanBytes(health.MemoryUsed),
		MemoryMaxHuman:  humanBytesMax(health.MemoryMax),
	}
	renderPage(w, r, ts, cfg, "server", pageData{
		Title:   "Server",
		Section: "server",
		Page:    view,
	})
}

// buildServerHeader assembles the count strip for the server page:
// stream + consumer account totals and cumulative API activity. These
// are neutral facts, not alarms — the account has no configured tier
// ceiling here (limits are unlimited), and API errors are cumulative
// since boot and dominated by benign "not found" startup probes, so
// neither is surfaced as a danger signal. The storage-headroom alarm
// (used vs the server-wide store ceiling) and live traffic land with
// the Varz/Jsz enrichment, which needs the embedded server handle.
func buildServerHeader(h ServerHealth) PageHeader {
	tiles := []Tile{
		{Label: "streams", Count: h.Streams, Tone: ToneInfo,
			Tooltip: "Streams on this JetStream account (includes KV-backing streams)"},
		{Label: "consumers", Count: h.Consumers, Tone: ToneInfo,
			Tooltip: "Consumers defined on this account"},
		{Label: "API calls", Count: int(h.APITotal), Tone: ToneDefault,
			Tooltip: "JetStream API calls since boot (cumulative)"},
	}
	header, err := NewPageHeader(PageHeader{
		Title:    "Server",
		Subtitle: "Embedded NATS server identity and JetStream account stats.",
		Tiles:    tiles,
	})
	if err != nil {
		return PageHeader{Title: "Server"}
	}
	return header
}

// humanBytes renders a byte count as a compact IEC string (KiB/MiB/…),
// trimming to one decimal place above the kibibyte threshold. Used by
// the server page to show JetStream store/memory usage as something an
// operator can read at a glance rather than a raw integer.
func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for value := n / unit; value >= unit; value /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// humanBytesMax renders a tier ceiling: a non-positive max means the
// tier is unlimited, shown as the infinity glyph rather than a humanized
// negative byte count.
func humanBytesMax(max int64) string {
	if max <= 0 {
		return "∞"
	}
	return humanBytes(uint64(max))
}
