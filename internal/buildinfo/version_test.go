// internal/buildinfo/version_test.go
// Tests for the build version string shared by cli and the program
// version-range check.
// Methodology: unit test of the unstamped default; the ldflags path is
// covered by the Makefile and Dockerfile builds.
package buildinfo

import "testing"

func TestVersionDefaultsToDev(t *testing.T) {
	// Positive: an unstamped build reports "dev".
	if Version != "dev" {
		t.Fatalf("Version = %q, want %q", Version, "dev")
	}
	// Negative: never empty, so callers need no fallback.
	if Version == "" {
		t.Fatal("Version must not be empty")
	}
}
