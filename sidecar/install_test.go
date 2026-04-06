// Tests for binary detection and installation.
//
// Methodology: Unit tests for FindBinary, BinDir, and
// DownloadURL use real filesystem with temp dirs. The
// Install flow is tested against a local HTTP server
// serving a real tar.gz archive with a dummy binary.
// No external network access required.

package sidecar

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestFindBinary_OnPath(t *testing.T) {
	// "ls" is always on PATH in Unix-like systems.
	path, err := FindBinary("ls")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if path == "" {
		t.Fatal("expected non-empty path for ls")
	}
}

func TestFindBinary_InDagnatsDir(t *testing.T) {
	// Create a temp dir to act as ~/.dagnats/bin/.
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, binDirName)
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	fakeBin := filepath.Join(binDir, "fakebinary")
	if err := os.WriteFile(
		fakeBin, []byte("#!/bin/sh\n"), 0o755,
	); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}

	// Override HOME so binDirPath resolves to our temp.
	origHome := os.Getenv("HOME")
	t.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", origHome)

	path, err := FindBinary("fakebinary")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if path != fakeBin {
		t.Fatalf(
			"expected path %q, got %q", fakeBin, path,
		)
	}
}

func TestFindBinary_NotFound(t *testing.T) {
	path, err := FindBinary(
		"definitely_not_a_real_binary_xyz123",
	)
	if err == nil {
		t.Fatal("expected error for missing binary")
	}
	if path != "" {
		t.Fatalf("expected empty path, got %q", path)
	}
}

func TestBinDir(t *testing.T) {
	tmpDir := t.TempDir()

	origHome := os.Getenv("HOME")
	t.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", origHome)

	dir, err := BinDir()
	if err != nil {
		t.Fatalf("BinDir error: %v", err)
	}

	expected := filepath.Join(tmpDir, binDirName)
	if dir != expected {
		t.Fatalf("expected %q, got %q", expected, dir)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat bin dir: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("bin dir is not a directory")
	}
}

func TestDownloadURL_Otelcol(t *testing.T) {
	url, err := DownloadURL(
		"otelcol", "0.102.0", "darwin", "arm64",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "https://github.com/" +
		"open-telemetry/" +
		"opentelemetry-collector-releases/" +
		"releases/download/v0.102.0/" +
		"otelcol_0.102.0_darwin_arm64.tar.gz"
	if url != expected {
		t.Fatalf("expected:\n  %s\ngot:\n  %s", expected, url)
	}
}

func TestDownloadURL_Otlp2parquet(t *testing.T) {
	url, err := DownloadURL(
		"otlp2parquet", "0.5.0", "linux", "amd64",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "https://github.com/" +
		"smithclay/otlp2parquet/" +
		"releases/download/v0.5.0/" +
		"otlp2parquet-linux-amd64.tar.gz"
	if url != expected {
		t.Fatalf("expected:\n  %s\ngot:\n  %s", expected, url)
	}
}

func TestDownloadURL_Unknown(t *testing.T) {
	_, err := DownloadURL(
		"unknown", "1.0.0", "linux", "amd64",
	)
	if err == nil {
		t.Fatal("expected error for unknown binary")
	}
}

func TestInstallFromMockServer(t *testing.T) {
	tmpDir := t.TempDir()

	origHome := os.Getenv("HOME")
	t.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", origHome)

	// Build a tar.gz containing a fake "testbin" file.
	archive := buildTarGz(t, "testbin", "#!/bin/sh\necho hi\n")

	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(
					"Content-Type",
					"application/gzip",
				)
				w.Write(archive)
			},
		),
	)
	defer server.Close()

	err := installFromURL("testbin", server.URL+"/test.tar.gz")
	if err != nil {
		t.Fatalf("installFromURL error: %v", err)
	}

	// Verify binary exists and is executable.
	binPath := filepath.Join(
		tmpDir, binDirName, "testbin",
	)
	info, err := os.Stat(binPath)
	if err != nil {
		t.Fatalf("binary not found: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatal("binary is not executable")
	}

	// Verify content.
	data, err := os.ReadFile(binPath)
	if err != nil {
		t.Fatalf("read binary: %v", err)
	}
	if string(data) != "#!/bin/sh\necho hi\n" {
		t.Fatalf(
			"unexpected content: %q", string(data),
		)
	}
}

func TestInstallFromMockServer_NotFound(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
		),
	)
	defer server.Close()

	err := installFromURL("testbin", server.URL+"/missing")
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
}

func TestExtractBinary_NotInArchive(t *testing.T) {
	archive := buildTarGz(t, "other", "data")

	tmpDir := t.TempDir()
	dest := filepath.Join(tmpDir, "wanted")

	err := extractBinary(
		bytes.NewReader(archive), "wanted", dest,
	)
	if err == nil {
		t.Fatal("expected error when binary not in archive")
	}

	// Verify dest was not created.
	if _, statErr := os.Stat(dest); statErr == nil {
		t.Fatal("dest file should not exist")
	}
}

// buildTarGz creates a tar.gz archive with a single file.
func buildTarGz(
	t *testing.T, name, content string,
) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	hdr := &tar.Header{
		Name: name,
		Mode: 0o755,
		Size: int64(len(content)),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("tar header: %v", err)
	}
	if _, err := fmt.Fprint(tw, content); err != nil {
		t.Fatalf("tar write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	return buf.Bytes()
}
