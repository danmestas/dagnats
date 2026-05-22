// Page header partial: a single header primitive shared by every list
// page in the console (#274 R5, ADR-015). The partial renders a title
// row + optional subtitle + a strip of count "tiles" that each carry
// a typed Tone — see the TileTone constants below.
//
// Why typed Tone: the four list pages each construct a few tiles and a
// stringly-typed "tone" field would let any caller drift the palette
// over time. Const + validate() catches the drift at view-assembly
// time (NewPageHeader returns error), keeping the operator-facing
// palette uniform.
//
// The partial owns markup; this Go file owns the type + validation.
// The four callers — workflows / runs / triggers / dlq list — build a
// PageHeader and pass it to renderPage as part of pageData.
package console

import "fmt"

// TileTone is a closed set of tone tokens that map to a colour class
// on the rendered tile. Adding a tone requires editing this file, the
// CSS, and the validate() switch — that's intentional friction.
type TileTone string

const (
	// ToneDefault renders the tile with neutral chrome (border + ink).
	// Use for the "total" tile that every page has at position 0.
	ToneDefault TileTone = "default"
	// ToneSuccess pairs with --status-completed. Used for "active",
	// "redrive-eligible", "healthy" counts.
	ToneSuccess TileTone = "success"
	// ToneWarning pairs with --status-pending. Used for in-flight /
	// running / awaiting-action counts.
	ToneWarning TileTone = "warning"
	// ToneDanger pairs with --status-failed. Used for failure /
	// expired counts.
	ToneDanger TileTone = "danger"
	// ToneInfo pairs with --status-running (operator blue). Used for
	// "configured", "enabled", "draft" counts that are informational
	// rather than urgent.
	ToneInfo TileTone = "info"
)

// PageHeader is the binding the page_header partial reads. Callers
// build one per page and hand it to renderPage via pageData.
//
// Title is the H1 text. IconClass is an optional CSS class for a
// leading glyph (e.g. an SVG-via-mask icon); empty string omits the
// icon container. Subtitle, when non-empty, renders below the title
// in --text-secondary.
//
// TitleGlossaryTerm, when non-empty, wraps the title in the glossary
// tooltip helper (tooltipAs) so the H1 becomes a defined-term reference.
// Used on pages whose title is also a domain term (e.g. "Triggers").
//
// Tiles is the count strip. Empty slice renders just the title row;
// every tile must validate (see validate()).
type PageHeader struct {
	Title             string
	IconClass         string
	Subtitle          string
	TitleGlossaryTerm string
	Tiles             []Tile
}

// Tile is one count cell in the page header strip. Count is the
// integer to render; Label is the operator-facing text below it.
//
// Href, when non-empty, wraps the tile in an anchor — used for the
// dashboard-style drill-through ("3 failed" → /console/runs?status=failed).
// Tooltip, when non-empty, populates the title attribute (no glossary
// lookup — this is a free-form helper, not a defined-term reference).
type Tile struct {
	Label   string
	Count   int
	Tone    TileTone
	Href    string
	Tooltip string
}

// NewPageHeader builds and validates a PageHeader. Returns an error
// when any tile carries an unknown Tone or an empty Label, or when
// Title is empty. Callers should treat validation errors as
// programmer errors — log and fall back to a bare H1 rather than 500.
//
// Validation happens at construction time (not render time) so the
// failure surfaces in the handler's request log next to the page name,
// not buried inside a template-execution error.
func NewPageHeader(h PageHeader) (PageHeader, error) {
	if err := h.validate(); err != nil {
		return PageHeader{}, fmt.Errorf("page header: %w", err)
	}
	return h, nil
}

// validate checks invariants on the header itself. Pulled out so tests
// can hit the validation surface without going through the constructor.
func (h PageHeader) validate() error {
	if h.Title == "" {
		return fmt.Errorf("title is empty")
	}
	const tilesMax = 16
	if len(h.Tiles) > tilesMax {
		return fmt.Errorf("too many tiles: %d > %d", len(h.Tiles), tilesMax)
	}
	for i := range h.Tiles {
		if err := h.Tiles[i].validate(); err != nil {
			return fmt.Errorf("tile %d: %w", i, err)
		}
	}
	return nil
}

// validate checks one tile is renderable. Unknown Tone is the bug
// this prevents — a stringly-typed tone would let a caller pass
// "succes" (typo) and quietly render a default-tone tile.
func (t Tile) validate() error {
	if t.Label == "" {
		return fmt.Errorf("label is empty")
	}
	if t.Count < 0 {
		return fmt.Errorf("count is negative: %d", t.Count)
	}
	switch t.Tone {
	case ToneDefault, ToneSuccess, ToneWarning, ToneDanger, ToneInfo:
		return nil
	}
	return fmt.Errorf("unknown tone: %q", string(t.Tone))
}

// ToneClass maps a TileTone to the CSS class the partial emits. The
// partial calls this through the funcMap; exposing it keeps the
// mapping in one place so a future palette refactor only touches Go.
func (t TileTone) Class() string {
	switch t {
	case ToneSuccess:
		return "tile-tone-success"
	case ToneWarning:
		return "tile-tone-warning"
	case ToneDanger:
		return "tile-tone-danger"
	case ToneInfo:
		return "tile-tone-info"
	}
	return "tile-tone-default"
}
