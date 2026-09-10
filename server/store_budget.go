package server

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
)

// storeBudgetWarnThresholdPct is the fraction of available disk bytes past
// which an effective max_store_bytes gets a loud warning: too little is
// left over for the OS and everything else sharing the host (issue #687
// option 4). In practice this only fires for an EXPLICIT max_store_bytes:
// deriveMaxStoreBytes halves available disk by construction, so a derived
// value is always <= 50% and can never cross an 80% threshold on its own.
const storeBudgetWarnThresholdPct = 0.8

// maxProbePathLen bounds the paths statfsAvailableBytes/nearestExistingAncestor
// walk, matching ConfigWithPath's existing configPath bound.
const maxProbePathLen = 4096

// availableDiskBytesFn resolves the bytes available to an unprivileged
// writer on the filesystem holding a path. A var indirection so tests can
// fake disk sizes deterministically without touching the real filesystem.
var availableDiskBytesFn = statfsAvailableBytes

// deriveMaxStoreBytes computes what MaxStoreBytes resolves to when the
// operator leaves it at the 0 "unset" sentinel (see DefaultConfig): half of
// what is available on the filesystem holding dataDir, capped at
// defaultMaxStoreBytes so a large host still gets a bounded default. This
// is the fix for #687: an unconditional 10 GiB default silently
// overcommitted a 2 GB host, driving JetStream's filestore index past
// MemoryHigh and stalling the whole box (6300 major faults/sec, 57-61%
// host I/O stall) while the engine kept reporting healthy.
//
// When dataDir cannot be probed (e.g. it does not exist yet and has no
// existing ancestor), this falls back to the compiled-in cap and logs why
// -- an unresolved disk size must never leave MaxStoreBytes at 0, which
// downstream validation treats as a programmer error.
func deriveMaxStoreBytes(dataDir string) int64 {
	if dataDir == "" {
		panic("deriveMaxStoreBytes: dataDir is empty")
	}

	available, err := availableDiskBytesFn(dataDir)
	if err != nil {
		slog.Warn(
			"max_store_bytes unset and disk size could not be determined; "+
				"falling back to the compiled-in cap",
			"data_dir", dataDir,
			"cap_bytes", defaultMaxStoreBytes,
			"error", err)
		return defaultMaxStoreBytes
	}
	if available < 0 {
		panic(fmt.Sprintf(
			"deriveMaxStoreBytes: availableDiskBytesFn returned negative %d",
			available))
	}

	half := available / 2
	if half > defaultMaxStoreBytes {
		return defaultMaxStoreBytes
	}
	return half
}

// warnIfStoreBudgetTooLarge logs a plain warning when the effective
// max_store_bytes reserves more than storeBudgetWarnThresholdPct of the
// disk available at dataDir. That large a budget leaves too little
// headroom for the OS, other services, and JetStream's own filestore index
// -- the #687 incident shape reached via an explicit config rather than
// the default (a derived value can never cross this threshold; see
// storeBudgetWarnThresholdPct). A probe failure here is not fatal: this
// warning is a courtesy, not a gate, so it simply stays silent when the
// disk size cannot be determined.
func warnIfStoreBudgetTooLarge(dataDir string, effective int64) {
	if dataDir == "" {
		panic("warnIfStoreBudgetTooLarge: dataDir is empty")
	}
	if effective <= 0 {
		panic(fmt.Sprintf(
			"warnIfStoreBudgetTooLarge: effective must be positive, got %d",
			effective))
	}

	available, err := availableDiskBytesFn(dataDir)
	if err != nil || available <= 0 {
		return
	}
	if float64(effective) <= float64(available)*storeBudgetWarnThresholdPct {
		return
	}

	slog.Warn(
		"max_store_bytes reserves most of the available disk; "+
			"JetStream will have little headroom left for the OS "+
			"and everything else on this host",
		"max_store_bytes", effective,
		"available_disk_bytes", available,
		"data_dir", dataDir)
}

// statfsAvailableBytes returns the unprivileged-available bytes on the
// filesystem holding path, probing the nearest existing ancestor when path
// itself does not exist yet -- DataDir is frequently unwritten at
// config-resolution time, before the server has created it.
func statfsAvailableBytes(path string) (int64, error) {
	if path == "" {
		panic("statfsAvailableBytes: path is empty")
	}
	if len(path) > maxProbePathLen {
		panic("statfsAvailableBytes: path exceeds max length")
	}

	probePath, err := nearestExistingAncestor(path)
	if err != nil {
		return 0, err
	}
	if probePath == "" {
		panic("statfsAvailableBytes: nearestExistingAncestor returned empty path with nil error")
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(probePath, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", probePath, err)
	}
	available := int64(stat.Bavail) * int64(stat.Bsize)
	if available < 0 {
		return 0, fmt.Errorf(
			"statfs %s: computed negative available bytes", probePath)
	}
	return available, nil
}

// nearestExistingAncestor walks up path's directory tree until it finds a
// directory that exists, bounded so a pathological input cannot loop
// forever. dataDir is frequently unwritten at config-resolution time, so
// this lets the disk probe fall back to whatever ancestor (e.g. the parent
// of ~/Library/Application Support/dagnats) is already there.
func nearestExistingAncestor(path string) (string, error) {
	if path == "" {
		panic("nearestExistingAncestor: path is empty")
	}
	if len(path) > maxProbePathLen {
		panic("nearestExistingAncestor: path exceeds max length")
	}
	const maxAncestorHops = 64

	dir := filepath.Clean(path)
	for hops := 0; hops < maxAncestorHops; hops++ {
		if _, err := os.Stat(dir); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("no existing ancestor found for %s", path)
}
