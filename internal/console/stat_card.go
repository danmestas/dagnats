package console

// stat_card.go owns the shared stat-card component the stream-detail and
// trigger-detail views render (Batch 6). A StatCard is a titled section
// card holding a definition list of label/value rows; the matching
// partial lives at templates/components/stat_card.html. Sharing one type
// + one partial keeps the two detail pages from duplicating card markup
// (Ousterhout: reuse).

// StatRow is one label/value pair in a StatCard's definition list. Value
// is rendered verbatim — callers pass "—" for unknown/absent fields so
// the card never fabricates data (Norman honesty). Mono renders the
// value in the monospace face (subjects, sequence numbers, sizes).
type StatRow struct {
	Label string
	Value string
	Mono  bool
}

// StatCard is a titled section card holding a strip of StatRows. Title
// is the H2; Stats is the definition list. An empty Stats slice renders
// just the header — callers should instead omit the card in that case.
type StatCard struct {
	Title string
	Stats []StatRow
}

// dash is the honest placeholder for an unknown/absent value. Centralised
// so every detail-view field that lacks data renders the same em-dash
// rather than a fabricated zero.
const dash = "—"

// statValueOr returns value when non-empty, else the honest dash. Lets
// view builders write `statValueOr(s.MaxAge)` without an inline if at
// every call site.
func statValueOr(value string) string {
	if value == "" {
		return dash
	}
	return value
}
