package console

// Methodology: pure unit tests for the shared list-page pagination
// chrome. computePageWindow is the one helper extracted from the
// per-domain list builders (#567); these cases pin the derived slice
// bounds and navigation metadata it produces from (total, page, size)
// across populated / empty / first / middle / last pages, and assert
// the invariants (non-negative bounds, consistent first/last, no
// impossible hasNext/hasPrev) that the helper enforces by panic.

import "testing"

func TestComputePageWindow_boundsAndMetadata(t *testing.T) {
	cases := map[string]struct {
		total, page, size int
		want              pageWindow
	}{
		"empty first page": {
			total: 0, page: 1, size: 10,
			want: pageWindow{
				Start: 0, End: 0, HasNext: false, HasPrev: false,
				NextPage: 2, PrevPage: 0, Total: 0, Page: 1, Size: 10,
				FirstIndex: 0, LastIndex: 0,
			},
		},
		"single short page": {
			total: 5, page: 1, size: 10,
			want: pageWindow{
				Start: 0, End: 5, HasNext: false, HasPrev: false,
				NextPage: 2, PrevPage: 0, Total: 5, Page: 1, Size: 10,
				FirstIndex: 1, LastIndex: 5,
			},
		},
		"middle page": {
			total: 25, page: 2, size: 10,
			want: pageWindow{
				Start: 10, End: 20, HasNext: true, HasPrev: true,
				NextPage: 3, PrevPage: 1, Total: 25, Page: 2, Size: 10,
				FirstIndex: 11, LastIndex: 20,
			},
		},
		"last partial page": {
			total: 25, page: 3, size: 10,
			want: pageWindow{
				Start: 20, End: 25, HasNext: false, HasPrev: true,
				NextPage: 4, PrevPage: 2, Total: 25, Page: 3, Size: 10,
				FirstIndex: 21, LastIndex: 25,
			},
		},
		"page past end clamps empty": {
			total: 25, page: 99, size: 10,
			want: pageWindow{
				Start: 25, End: 25, HasNext: false, HasPrev: true,
				NextPage: 100, PrevPage: 98, Total: 25, Page: 99, Size: 10,
				FirstIndex: 0, LastIndex: 0,
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := computePageWindow(tc.total, tc.page, tc.size)
			if got != tc.want {
				t.Fatalf("computePageWindow(%d,%d,%d) = %+v; want %+v",
					tc.total, tc.page, tc.size, got, tc.want)
			}
			// Negative space: an empty window must not advertise rows.
			if got.FirstIndex == 0 && got.Start != got.End {
				t.Fatalf("empty lede with non-empty slice: %+v", got)
			}
		})
	}
}

func TestComputePageWindow_rejectsInvalidInput(t *testing.T) {
	cases := map[string]struct{ total, page, size int }{
		"negative total": {total: -1, page: 1, size: 10},
		"zero page":      {total: 5, page: 0, size: 10},
		"zero size":      {total: 5, page: 1, size: 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("computePageWindow(%d,%d,%d) did not panic",
						tc.total, tc.page, tc.size)
				}
			}()
			computePageWindow(tc.total, tc.page, tc.size)
		})
	}
}
