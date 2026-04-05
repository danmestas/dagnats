package natsutil

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
)

// RequireServerVersion checks that the connected NATS server meets the
// minimum version requirement. This gates features like atomic batch
// publish that require server-side support (NATS >= 2.12).
func RequireServerVersion(nc *nats.Conn, minimum string) error {
	if nc == nil {
		panic("RequireServerVersion: nc must not be nil")
	}
	if minimum == "" {
		panic("RequireServerVersion: minimum must not be empty")
	}

	serverVersion := nc.ConnectedServerVersion()
	if serverVersion == "" {
		return fmt.Errorf(
			"natsutil: cannot determine server version",
		)
	}

	minMajor, minMinor, minPatch, err := parseVersion(minimum)
	if err != nil {
		return fmt.Errorf(
			"natsutil: invalid minimum version %q: %w",
			minimum, err,
		)
	}

	curMajor, curMinor, curPatch, err := parseVersion(serverVersion)
	if err != nil {
		return fmt.Errorf(
			"natsutil: invalid server version %q: %w",
			serverVersion, err,
		)
	}

	if !versionAtLeast(
		curMajor, curMinor, curPatch,
		minMajor, minMinor, minPatch,
	) {
		return fmt.Errorf(
			"natsutil: server version %s < required %s",
			serverVersion, minimum,
		)
	}
	return nil
}

// parseVersion splits a "major.minor.patch" string into integers.
func parseVersion(v string) (int, int, int, error) {
	parts := strings.SplitN(v, ".", 3)
	if len(parts) != 3 {
		return 0, 0, 0, fmt.Errorf("expected major.minor.patch")
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, 0, err
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, 0, err
	}
	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, 0, 0, err
	}
	return major, minor, patch, nil
}

// versionAtLeast returns true if cur >= min using semantic ordering.
func versionAtLeast(
	curMajor, curMinor, curPatch int,
	minMajor, minMinor, minPatch int,
) bool {
	if curMajor != minMajor {
		return curMajor > minMajor
	}
	if curMinor != minMinor {
		return curMinor > minMinor
	}
	return curPatch >= minPatch
}
