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

	// Traffic + host byte strings, populated on the rich (HasStats) path.
	// In/OutBytes and Mem are int64 from Varz; humanBytesSigned clamps a
	// negative to 0 B before humanizing.
	InBytesHuman  string
	OutBytesHuman string
	MemHuman      string
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
		InBytesHuman:    humanBytesSigned(health.InBytes),
		OutBytesHuman:   humanBytesSigned(health.OutBytes),
		MemHuman:        humanBytesSigned(health.MemBytes),
	}
	renderPage(w, r, ts, cfg, "server", pageData{
		Title:   "Server",
		Section: "server",
		Page:    view,
	})
}

// buildServerHeader assembles the count strip for the server page. When
// the embedded server's stats were read (HasStats) it surfaces the real
// alarms: storage headroom (danger at >=85%) and slow consumers (danger
// at any non-zero count — a disconnected-because-slow client is a real
// problem), alongside live connection + subscription counts. Without the
// stats handle it falls back to the neutral lean strip (stream + consumer
// account totals and cumulative API activity) — facts, not alarms, since
// the account tier is unlimited and API errors are dominated by benign
// startup probes.
func buildServerHeader(h ServerHealth) PageHeader {
	subtitle := "Embedded NATS server identity and JetStream account stats."
	tiles := leanServerTiles(h)
	if h.HasStats {
		subtitle = "Embedded NATS server health — identity, JetStream capacity, live traffic."
		tiles = richServerTiles(h)
	}
	header, err := NewPageHeader(PageHeader{
		Title:    "Server",
		Subtitle: subtitle,
		Tiles:    tiles,
	})
	if err != nil {
		return PageHeader{Title: "Server"}
	}
	return header
}

// richServerTiles is the HasStats count strip: storage headroom and slow
// consumers as real alarms, plus live connection + subscription counts.
func richServerTiles(h ServerHealth) []Tile {
	storeTone := ToneInfo
	if h.StorePct >= 85 {
		storeTone = ToneDanger
	}
	slowTone := ToneSuccess
	if h.SlowConsumers > 0 {
		slowTone = ToneDanger
	}
	return []Tile{
		{Label: "storage %", Count: h.StorePct, Tone: storeTone,
			Tooltip: "Store used vs the server-wide JetStream ceiling"},
		{Label: "connections", Count: h.Connections, Tone: ToneInfo,
			Tooltip: "Live client connections to this server"},
		{Label: "subscriptions", Count: int(h.Subscriptions), Tone: ToneInfo,
			Tooltip: "Active subscriptions across all connections"},
		{Label: "slow consumers", Count: int(h.SlowConsumers), Tone: slowTone,
			Tooltip: "Clients disconnected since boot for being slow consumers"},
	}
}

// leanServerTiles is the no-stats fallback strip: neutral account facts.
func leanServerTiles(h ServerHealth) []Tile {
	return []Tile{
		{Label: "streams", Count: h.Streams, Tone: ToneInfo,
			Tooltip: "Streams on this JetStream account (includes KV-backing streams)"},
		{Label: "consumers", Count: h.Consumers, Tone: ToneInfo,
			Tooltip: "Consumers defined on this account"},
		{Label: "API calls", Count: int(h.APITotal), Tone: ToneDefault,
			Tooltip: "JetStream API calls since boot (cumulative)"},
	}
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

// humanBytesSigned humanizes a signed byte count (Varz reports In/Out
// bytes and resident memory as int64), clamping a negative to zero so a
// transient negative never renders as a humanized negative.
func humanBytesSigned(n int64) string {
	if n < 0 {
		return humanBytes(0)
	}
	return humanBytes(uint64(n))
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
