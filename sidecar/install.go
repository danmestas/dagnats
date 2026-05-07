package sidecar

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	downloadTimeout = 30 * time.Second
	maxDownloadSize = 200 * 1024 * 1024 // 200 MB

	defaultOtelcolVersion      = "0.102.0"
	defaultOtlp2parquetVersion = "0.9.1"

	binDirName = ".dagnats/bin"
	dirPerms   = 0o755
	binPerms   = 0o755
)

// knownBinaries maps binary names to their download URL
// templates and default versions.
var knownBinaries = map[string]binarySpec{
	"otelcol": {
		version: defaultOtelcolVersion,
		urlFmt: "https://github.com/" +
			"open-telemetry/" +
			"opentelemetry-collector-releases/" +
			"releases/download/v%s/" +
			"otelcol_%s_%s_%s.tar.gz",
	},
	"otlp2parquet": {
		version: defaultOtlp2parquetVersion,
		// Upstream uses an "otlp2parquet-cli-" prefix on its
		// release tarballs (since v0.8.0). See #186.
		urlFmt: "https://github.com/" +
			"smithclay/otlp2parquet/" +
			"releases/download/v%s/" +
			"otlp2parquet-cli-%s-%s.tar.gz",
	},
}

type binarySpec struct {
	version string
	urlFmt  string
}

// FindBinary checks PATH first, then ~/.dagnats/bin/.
// Returns the full path if found, empty string + error
// if not.
func FindBinary(name string) (string, error) {
	if name == "" {
		panic("FindBinary: name is empty")
	}

	// Check PATH first.
	if path, err := exec.LookPath(name); err == nil {
		return path, nil
	}

	// Check ~/.dagnats/bin/.
	binDir, err := binDirPath()
	if err != nil {
		return "", fmt.Errorf("resolve bin dir: %w", err)
	}

	candidate := filepath.Join(binDir, name)
	if _, err := os.Stat(candidate); err == nil {
		return candidate, nil
	}

	return "", fmt.Errorf(
		"%q not found on PATH or in %s", name, binDir,
	)
}

// BinDir returns ~/.dagnats/bin/, creating it if needed.
func BinDir() (string, error) {
	dir, err := binDirPath()
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(dir, dirPerms); err != nil {
		return "", fmt.Errorf("create bin dir: %w", err)
	}

	return dir, nil
}

// binDirPath returns the bin directory path without
// creating it.
func binDirPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("get home dir: %w", err)
	}
	return filepath.Join(home, binDirName), nil
}

// DownloadURL builds the download URL for a known binary.
// Returns an error for unknown binary names.
func DownloadURL(
	name, version, goos, goarch string,
) (string, error) {
	if name == "" {
		panic("DownloadURL: name is empty")
	}
	if version == "" {
		panic("DownloadURL: version is empty")
	}

	switch name {
	case "otelcol":
		spec := knownBinaries["otelcol"]
		return fmt.Sprintf(
			spec.urlFmt, version, version, goos, goarch,
		), nil
	case "otlp2parquet":
		spec := knownBinaries["otlp2parquet"]
		return fmt.Sprintf(
			spec.urlFmt, version, goos, goarch,
		), nil
	default:
		return "", fmt.Errorf("unknown binary %q", name)
	}
}

// Install downloads a binary to ~/.dagnats/bin/.
// Uses runtime.GOOS and runtime.GOARCH for platform
// detection.
func Install(name, version string) error {
	if name == "" {
		panic("Install: name is empty")
	}
	if version == "" {
		panic("Install: version is empty")
	}

	url, err := DownloadURL(
		name, version, runtime.GOOS, runtime.GOARCH,
	)
	if err != nil {
		return err
	}

	return installFromURL(name, url)
}

// installFromURL downloads a tar.gz from url, extracts the
// named binary, and places it in ~/.dagnats/bin/.
func installFromURL(name, url string) error {
	binDir, err := BinDir()
	if err != nil {
		return err
	}

	data, err := downloadFile(url)
	if err != nil {
		return fmt.Errorf("download %s: %w", name, err)
	}
	defer data.Close()

	dest := filepath.Join(binDir, name)
	return extractBinary(data, name, dest)
}

// downloadFile fetches a URL with a bounded timeout and
// size. Returns the response body; caller must close it.
func downloadFile(url string) (io.ReadCloser, error) {
	if url == "" {
		panic("downloadFile: url is empty")
	}

	client := &http.Client{Timeout: downloadTimeout}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf(
			"HTTP %d from %s", resp.StatusCode, url,
		)
	}

	bounded := io.LimitReader(resp.Body, maxDownloadSize)
	return &boundedReadCloser{
		Reader: bounded,
		Closer: resp.Body,
	}, nil
}

type boundedReadCloser struct {
	io.Reader
	io.Closer
}

// extractBinary reads a tar.gz stream, finds an entry
// matching name, and writes it to dest with executable
// permissions.
func extractBinary(
	r io.Reader, name, dest string,
) error {
	if r == nil {
		panic("extractBinary: reader is nil")
	}
	if name == "" {
		panic("extractBinary: name is empty")
	}

	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("gzip open: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	const maxEntries = 1000

	for i := 0; i < maxEntries; i++ {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar read: %w", err)
		}

		base := filepath.Base(hdr.Name)
		if base != name {
			continue
		}

		return writeExecutable(tr, dest)
	}

	return fmt.Errorf(
		"binary %q not found in archive", name,
	)
}

// writeExecutable writes from r to dest with 0755
// permissions, using a temp file + rename for atomicity.
func writeExecutable(r io.Reader, dest string) error {
	if r == nil {
		panic("writeExecutable: reader is nil")
	}
	if dest == "" {
		panic("writeExecutable: dest is empty")
	}

	dir := filepath.Dir(dest)
	tmp, err := os.CreateTemp(dir, "install-*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}

	_, err = io.Copy(tmp, r)
	closeErr := tmp.Close()
	if err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("write binary: %w", err)
	}
	if closeErr != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("close temp: %w", closeErr)
	}

	if err := os.Chmod(tmp.Name(), binPerms); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("chmod: %w", err)
	}

	if err := os.Rename(tmp.Name(), dest); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}

// localBinary describes a binary built from the local module.
type localBinary struct {
	Name string
	Pkg  string
}

// localBinaries lists binaries built from the local Go module.
var localBinaries = []localBinary{
	{Name: "dagnats-mcp-duckdb", Pkg: "./cmd/mcp-duckdb/"},
}

// BuildLocal builds a binary from the local Go module using
// go build. Requires go on PATH.
func BuildLocal(name, pkg string) error {
	if name == "" {
		panic("BuildLocal: name is empty")
	}
	if pkg == "" {
		panic("BuildLocal: pkg is empty")
	}

	goPath, err := exec.LookPath("go")
	if err != nil {
		return fmt.Errorf(
			"go not found on PATH: %w", err,
		)
	}

	modRoot, err := findModuleRoot()
	if err != nil {
		return fmt.Errorf("find module root: %w", err)
	}

	binDir, err := BinDir()
	if err != nil {
		return fmt.Errorf("resolve bin dir: %w", err)
	}

	dest := filepath.Join(binDir, name)
	cmd := exec.Command(goPath, "build", "-o", dest, pkg)
	cmd.Dir = modRoot
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go build %s: %w", name, err)
	}
	return nil
}

// findModuleRoot returns the directory containing go.mod.
func findModuleRoot() (string, error) {
	cmd := exec.Command("go", "env", "GOMOD")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("go env GOMOD: %w", err)
	}
	gomod := strings.TrimSpace(string(out))
	if gomod == "" || gomod == os.DevNull {
		return "", fmt.Errorf("not in a Go module")
	}
	return filepath.Dir(gomod), nil
}

// InstallAll checks for otelcol, otlp2parquet, and
// dagnats-mcp-duckdb, installs any that are missing.
// Uses prebuilt downloads where available, falls back to
// building from source for otlp2parquet (Rust/cargo) and
// dagnats-mcp-duckdb (Go).
func InstallAll(w io.Writer) error {
	if w == nil {
		panic("InstallAll: writer is nil")
	}

	// Download-based binaries.
	names := []string{"otelcol", "otlp2parquet"}
	for _, name := range names {
		path, err := FindBinary(name)
		if err == nil {
			fmt.Fprintf(w, "✓ %s found at %s\n", name, path)
			continue
		}

		spec, ok := knownBinaries[name]
		if !ok {
			return fmt.Errorf("unknown binary %q", name)
		}

		fmt.Fprintf(
			w, "⬇ installing %s v%s...\n",
			name, spec.version,
		)

		if err := Install(name, spec.version); err != nil {
			if name == "otlp2parquet" {
				fmt.Fprintf(w,
					"  download failed, building from source...\n")
				if buildErr := buildFromSource(
					w, name, "cargo",
					"github.com/smithclay/otlp2parquet",
				); buildErr != nil {
					return fmt.Errorf(
						"install %s: download: %w, build: %v",
						name, err, buildErr)
				}
				continue
			}
			return fmt.Errorf("install %s: %w", name, err)
		}

		fmt.Fprintf(w, "✓ %s installed\n", name)
	}

	// Local Go binaries: build from in-repo source.
	for _, lb := range localBinaries {
		path, err := FindBinary(lb.Name)
		if err == nil {
			fmt.Fprintf(w, "✓ %s found at %s\n",
				lb.Name, path)
			continue
		}

		fmt.Fprintf(w, "🔨 building %s...\n", lb.Name)
		if err := BuildLocal(lb.Name, lb.Pkg); err != nil {
			return fmt.Errorf(
				"build %s: %w", lb.Name, err,
			)
		}
		fmt.Fprintf(w, "✓ %s built\n", lb.Name)
	}

	return nil
}

// buildFromSource clones a repo and builds with cargo or go.
// For cargo (Rust): clones, runs cargo build --release, copies
// binary to ~/.dagnats/bin/.
func buildFromSource(
	w io.Writer,
	name, toolchain, repo string,
) error {
	if name == "" {
		panic("buildFromSource: name is empty")
	}

	binDir, err := BinDir()
	if err != nil {
		return fmt.Errorf("bin dir: %w", err)
	}
	dest := filepath.Join(binDir, name)

	switch toolchain {
	case "cargo":
		return buildWithCargo(w, name, repo, dest)
	default:
		return fmt.Errorf("unsupported toolchain: %s",
			toolchain)
	}
}

// buildWithCargo clones a git repo to a temp dir, builds
// with cargo, and copies the release binary to dest.
func buildWithCargo(
	w io.Writer,
	name, repo, dest string,
) error {
	if _, err := exec.LookPath("cargo"); err != nil {
		return fmt.Errorf(
			"cargo not found — install Rust: https://rustup.rs")
	}

	tmpDir, err := os.MkdirTemp("", "dagnats-build-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Clone
	cloneURL := "https://" + repo
	fmt.Fprintf(w, "  cloning %s...\n", repo)
	clone := exec.Command(
		"git", "clone", "--depth=1", cloneURL, tmpDir)
	clone.Stdout = w
	clone.Stderr = w
	if err := clone.Run(); err != nil {
		return fmt.Errorf("git clone: %w", err)
	}

	// Build
	fmt.Fprintf(w,
		"  building with cargo"+
			" (this may take a few minutes)...\n")
	build := exec.Command("cargo", "build", "--release")
	build.Dir = tmpDir
	build.Stdout = w
	build.Stderr = w
	if err := build.Run(); err != nil {
		return fmt.Errorf("cargo build: %w", err)
	}

	// Copy binary
	src := filepath.Join(
		tmpDir, "target", "release", name,
	)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf(
			"built binary not found at %s", src)
	}

	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read binary: %w", err)
	}

	if err := os.WriteFile(dest, data, binPerms); err != nil {
		return fmt.Errorf("write binary: %w", err)
	}

	fmt.Fprintf(w, "✓ %s built and installed\n", name)
	return nil
}
