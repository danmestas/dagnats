package console

import (
	"strconv"
)

// Server-side pagination defaults. Page size is bounded so any single
// page render is cheap regardless of how many runs the operator has
// accumulated; if they want more they paginate. The size param is
// clamped to [1, pageSizeMax].
const (
	pageSizeDefault = 20
	pageSizeMax     = 100
	pageNumberMax   = 10_000 // safety bound on URL-driven loops
)

// parsePageAndSize parses URL strings into clamped page/size ints.
// Page < 1 defaults to 1; size out of bounds defaults / clamps to
// pageSizeDefault / pageSizeMax. Bounded per project rules.
func parsePageAndSize(pageStr, sizeStr string) (int, int) {
	page := 1
	if pageStr != "" {
		if v, err := strconv.Atoi(pageStr); err == nil && v >= 1 {
			if v > pageNumberMax {
				v = pageNumberMax
			}
			page = v
		}
	}
	size := pageSizeDefault
	if sizeStr != "" {
		if v, err := strconv.Atoi(sizeStr); err == nil && v >= 1 {
			if v > pageSizeMax {
				v = pageSizeMax
			}
			size = v
		}
	}
	return page, size
}

// paginate computes the safe [start, end) indices for a slice of
// length total given 1-indexed page and a positive size. Reports
// whether a next page exists. End is always clamped to total.
func paginate(total, page, size int) (int, int, bool) {
	if total < 0 {
		panic("paginate: total is negative")
	}
	if size <= 0 {
		panic("paginate: size must be positive")
	}
	start := (page - 1) * size
	if start >= total {
		return total, total, false
	}
	end := start + size
	if end > total {
		end = total
	}
	return start, end, end < total
}

// pageWindow is the derived list-page pagination chrome: the [Start,End)
// slice bounds every list builder pages its rows with, plus the
// navigation metadata every list template binds. It is domain-neutral —
// callers own what rows to paginate, how to filter them, and which of
// these fields their template surfaces; the window only does the bounds
// and metadata arithmetic and enforces that the result is self-consistent.
//
// FirstIndex / LastIndex are the 1-indexed bounds of the current page for
// the "Showing N–M of K" lede; both are zero when the page renders no
// rows so the template omits the range rather than printing "1–0".
type pageWindow struct {
	Start      int
	End        int
	HasNext    bool
	HasPrev    bool
	NextPage   int
	PrevPage   int
	Total      int
	Page       int
	Size       int
	FirstIndex int
	LastIndex  int
}

// computePageWindow derives the full pageWindow from a row count and a
// clamped 1-indexed page/size (the output of parsePageAndSize). It hides
// the offset arithmetic, the hasNext/hasPrev/next/prev derivation, and
// the empty-vs-populated lede decision behind one call so every list
// builder shares identical paging behavior. It asserts its inputs and
// its own output so an impossible page (negative bounds, a lede that
// disagrees with the slice, hasNext past the end, hasPrev on page one)
// surfaces as a programmer-error panic rather than a wrong render.
func computePageWindow(total, page, size int) pageWindow {
	if total < 0 {
		panic("computePageWindow: total is negative")
	}
	if page < 1 {
		panic("computePageWindow: page must be >= 1")
	}
	if size <= 0 {
		panic("computePageWindow: size must be positive")
	}
	start, end, hasNext := paginate(total, page, size)
	first, last := 0, 0
	if end > start {
		first = start + 1
		last = end
	}
	win := pageWindow{
		Start: start, End: end,
		HasNext: hasNext, HasPrev: page > 1,
		NextPage: page + 1, PrevPage: page - 1,
		Total: total, Page: page, Size: size,
		FirstIndex: first, LastIndex: last,
	}
	if win.Start < 0 || win.End < win.Start || win.End > total {
		panic("computePageWindow: slice bounds out of range")
	}
	if (win.FirstIndex == 0) != (win.Start == win.End) {
		panic("computePageWindow: lede disagrees with slice bounds")
	}
	if win.HasNext && win.End >= total {
		panic("computePageWindow: hasNext with no further rows")
	}
	if win.HasPrev != (win.Page > 1) {
		panic("computePageWindow: hasPrev disagrees with page number")
	}
	return win
}
