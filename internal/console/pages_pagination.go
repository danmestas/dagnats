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
