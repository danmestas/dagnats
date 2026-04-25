package natsutil

import "testing"

func TestDeriveReplicas(t *testing.T) {
	cases := []struct {
		name     string
		routes   []string
		override int
		want     int
	}{
		{"no routes, no override", nil, 0, 1},
		{"no routes, override 3", nil, 3, 3},
		{"3-node cluster (2 routes), auto", []string{"a", "b"}, 0, 3},
		{"4-node cluster (3 routes), auto rounds down", []string{"a", "b", "c"}, 0, 3},
		{"5-node cluster (4 routes), auto", []string{"a", "b", "c", "d"}, 0, 5},
		{"6-node cluster (5 routes), auto caps at 5", []string{"a", "b", "c", "d", "e"}, 0, 5},
		{"override beats auto", []string{"a", "b", "c", "d"}, 3, 3},
		{"override 1 in cluster", []string{"a", "b"}, 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeriveReplicas(tc.routes, tc.override)
			if got != tc.want {
				t.Errorf("DeriveReplicas(%v, %d) = %d, want %d",
					tc.routes, tc.override, got, tc.want)
			}
		})
	}
}
