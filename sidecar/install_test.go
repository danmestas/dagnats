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
	"strings"
	"testing"
)

func TestBuildLocal_GoNotFound(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	err := BuildLocal("test", "./cmd/test/")

	// Positive: should return an error.
	if err == nil {
		t.Fatal("expected error when go not on PATH")
	}

	// Positive: error should mention go.
	if !strings.Contains(err.Error(), "go") {
		t.Errorf(
			"error = %q, want mention of 'go'", err.Error(),
		)
	}
}

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
	t.Setenv("HOME", tmpDir)

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

	t.Setenv("HOME", tmpDir)

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
		"otlp2parquet", "0.9.1", "linux", "amd64",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Upstream switched to a "cli-" infix in the release filename
	// at v0.8.0 and the previous "otlp2parquet-<os>-<arch>.tar.gz"
	// pattern produces a 404 against any release from v0.8.0 onward.
	expected := "https://github.com/" +
		"smithclay/otlp2parquet/" +
		"releases/download/v0.9.1/" +
		"otlp2parquet-cli-linux-amd64.tar.gz"
	if url != expected {
		t.Fatalf("expected:\n  %s\ngot:\n  %s", expected, url)
	}
}

func TestOtlp2parquetDefaultVersionExistsUpstream(t *testing.T) {
	// Guards against the failure mode that produced #186: the
	// hardcoded default version drifted past upstream's actual
	// releases (we shipped v0.11.0 when only v0.9.x existed),
	// so the install command always 404'd. The default version
	// must be a release that actually exists.
	spec, ok := knownBinaries["otlp2parquet"]
	if !ok {
		t.Fatal("otlp2parquet missing from knownBinaries")
	}
	if spec.version == "" {
		t.Fatal("otlp2parquet default version is empty")
	}

	// Anchor on the version known-good at the time of the fix.
	// Bumping this requires verifying the new tag exists at
	// https://github.com/smithclay/otlp2parquet/releases.
	const knownGood = "0.9.1"
	if spec.version != knownGood {
		t.Errorf(
			"default otlp2parquet version = %q; "+
				"want %q (verify upstream before bumping)",
			spec.version, knownGood,
		)
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

	t.Setenv("HOME", tmpDir)

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

func TestLocalBinariesPkgExists(t *testing.T) {
	// Guards against the failure mode of #187: localBinaries[i].Pkg
	// pointed at "./cmd/mcp-duckdb/" but the actual directory is
	// "./cmd/dagnats-mcp-duckdb/", so BuildLocal could never succeed
	// even when Go was on PATH inside the source tree.
	modRoot, err := findModuleRoot()
	if err != nil {
		t.Skipf("not in source tree: %v", err)
	}

	if len(localBinaries) == 0 {
		t.Fatal("localBinaries is empty; nothing to validate")
	}

	for _, lb := range localBinaries {
		rel := strings.TrimPrefix(lb.Pkg, "./")
		full := filepath.Join(modRoot, rel)

		info, statErr := os.Stat(full)
		if statErr != nil {
			t.Errorf(
				"localBinary %q: Pkg %q resolves to %q, "+
					"which does not exist: %v",
				lb.Name, lb.Pkg, full, statErr,
			)
			continue
		}
		if !info.IsDir() {
			t.Errorf(
				"localBinary %q: Pkg %q is not a directory",
				lb.Name, lb.Pkg,
			)
		}
	}
}

func TestInstallAll_SoftFailsMCPDuckDB(t *testing.T) {
	// After #187: when BuildLocal cannot succeed for
	// dagnats-mcp-duckdb (e.g., Go not on PATH or not in the
	// dagnats source tree), InstallAll skips it with a notice
	// rather than failing the whole install. otelcol and
	// otlp2parquet remain hard-required.
	tmp := t.TempDir()
	binDir := filepath.Join(tmp, binDirName)
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Pre-populate fake otelcol and otlp2parquet so the
	// download-required binaries are "found" and the install
	// loop reaches the local-build loop.
	for _, name := range []string{"otelcol", "otlp2parquet"} {
		if err := os.WriteFile(
			filepath.Join(binDir, name),
			[]byte("#!/bin/sh\n"), 0o755,
		); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	t.Setenv("HOME", tmp)
	// PATH points at an empty dir, so `go` is unreachable and
	// BuildLocal will fail with "go not found on PATH".
	t.Setenv("PATH", t.TempDir())

	var buf bytes.Buffer
	err := InstallAll(&buf)
	if err != nil {
		t.Fatalf(
			"InstallAll should succeed when only "+
				"dagnats-mcp-duckdb fails to build, got: %v\n"+
				"output: %s",
			err, buf.String(),
		)
	}

	out := buf.String()
	if !strings.Contains(out, "dagnats-mcp-duckdb") {
		t.Errorf(
			"expected dagnats-mcp-duckdb notice in output, "+
				"got: %s", out,
		)
	}
	if !strings.Contains(out, "MCP") &&
		!strings.Contains(out, "mcp") {
		t.Errorf(
			"expected user-facing MCP notice in output, "+
				"got: %s", out,
		)
	}
}

func TestKnownBinaries_IncludesMCPDuckDB(t *testing.T) {
	// #188: dagnats-mcp-duckdb must be a first-class download
	// like otelcol/otlp2parquet so prebuilt-binary hosts can
	// install it without Go or a CGO toolchain.
	spec, ok := knownBinaries["dagnats-mcp-duckdb"]
	if !ok {
		t.Fatal(
			"knownBinaries must include dagnats-mcp-duckdb",
		)
	}
	if spec.urlFmt == "" {
		t.Fatal("dagnats-mcp-duckdb URL template must be set")
	}
	if spec.version == "" {
		t.Fatal("dagnats-mcp-duckdb default version must be set")
	}
}

func TestDownloadURL_MCPDuckDB_LinuxAMD64(t *testing.T) {
	// #188: the URL produced for linux/amd64 must point at
	// the dagnats GitHub release artifact for the named
	// version and platform pair.
	spec, ok := knownBinaries["dagnats-mcp-duckdb"]
	if !ok {
		t.Fatal("dagnats-mcp-duckdb missing from knownBinaries")
	}

	url, err := DownloadURL(
		"dagnats-mcp-duckdb", spec.version, "linux", "amd64",
	)
	if err != nil {
		t.Fatalf("DownloadURL: unexpected error: %v", err)
	}

	// Positive: URL must embed both the platform pair and
	// the dagnats release host.
	if !strings.Contains(url, "linux-amd64") {
		t.Errorf(
			"URL must embed linux-amd64 platform pair: %s",
			url,
		)
	}
	if !strings.Contains(url, "danmestas/dagnats") {
		t.Errorf(
			"URL must reference the dagnats release: %s", url,
		)
	}

	// Negative: must not collide with otelcol upstream.
	if strings.Contains(url, "open-telemetry") {
		t.Errorf(
			"URL must not point at otelcol upstream: %s", url,
		)
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
