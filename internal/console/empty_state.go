// Empty-state partial — the shared educational empty body shown on
// every list page (#274 R4, #310). Mirrors iii's EmptyState
// (`/iii/console/packages/console-frontend/src/components/ui/empty-state.tsx`):
// the information hierarchy is icon → title → description → optional
// primary action. dagnats keeps the visual stack and the typed Go
// surface here; the rendered styles live in app.css.
//
// Why typed: same reason as PageHeader — caller drift on a stringly
// "icon" or unbounded action shape would let the four list pages
// disagree on the empty body. Construction-time validation in
// NewEmptyState surfaces the bug in the request log next to the page
// name instead of as an empty <span> on the rendered page.
//
// Audit-adjusted shape (Ousterhout backpressure, comment on #310):
// no CSRFNonce — no POST-action consumer yet; no DocsLink — no iii
// precedent. Both can re-enter when a real use case lands.
package console

import "fmt"

// EmptyState is what the empty-state partial binds. Caller builds one
// per list page when the row set is empty, hands it to the page view,
// and the template renders it in place of the table body.
//
// Icon is a CSS class (matches the page_header.IconClass convention
// from R5) — for now dagnats uses a Unicode glyph slot indexed by
// IconKind via .console-empty-state-icon[data-kind=...]. A future
// SVG-mask icon set can supply real .icon-zap etc. classes; callers
// won't change.
//
// Title is the bold headline (e.g. "No triggers configured"); matches
// the page-header title's domain term where possible. Description is
// one short sentence under the title ("Set up HTTP, cron, or event
// triggers"). Both required.
//
// PrimaryAction is optional. When set, the partial renders a button-
// styled link. ReadOnly disables the action with a tooltip referencing
// `CONSOLE_READ_ONLY=1`; the action's Href stays in the DOM so QA can
// see what the operator would have hit.
type EmptyState struct {
	Icon          string
	Title         string
	Description   string
	PrimaryAction *EmptyStateAction
	ReadOnly      bool
}

// EmptyStateAction is the optional CTA below the description. Label is
// the button text ("Register a workflow"); Href is the destination (a
// docs page, a CLI cheat sheet, or a future create-form). dagnats list
// pages are GET-only for v1 so no CSRF nonce — add when the first
// POST-button empty state lands.
type EmptyStateAction struct {
	Label string
	Href  string
}

// NewEmptyState builds and validates an EmptyState. Pattern matches
// NewPageHeader: errors are programmer errors and callers fall back
// to an empty struct (which renders to nothing) rather than 500.
func NewEmptyState(e EmptyState) (EmptyState, error) {
	if err := e.validate(); err != nil {
		return EmptyState{}, fmt.Errorf("empty state: %w", err)
	}
	return e, nil
}

// validate enforces the construction-time contract. Title and
// Description are required; an action, when present, needs both
// Label and Href.
func (e EmptyState) validate() error {
	if e.Title == "" {
		return fmt.Errorf("title is empty")
	}
	if e.Description == "" {
		return fmt.Errorf("description is empty")
	}
	if e.PrimaryAction != nil {
		if e.PrimaryAction.Label == "" {
			return fmt.Errorf("primary action: label is empty")
		}
		if e.PrimaryAction.Href == "" {
			return fmt.Errorf("primary action: href is empty")
		}
	}
	return nil
}
