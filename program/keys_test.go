// program/keys_test.go
// Tests for LoadOrCreateKey, the program's own NKey identity.
// Methodology: real files in t.TempDir(). The seed file is inspected on
// disk (mode, bytes, mtime) across calls; the Key value is printed every
// way fmt can print it to show the seed does not leave through it.
package program_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nkeys"
)

func TestKeys_SeedWrittenOnceMode0600(t *testing.T) {
	path := filepath.Join(t.TempDir(), "toy.seed")
	first, err := program.LoadOrCreateKey(path)
	if err != nil {
		t.Fatalf("first LoadOrCreateKey: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// Positive: the seed is private to its owner.
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("seed mode = %o, want 600", info.Mode().Perm())
	}
	seed, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	if !nkeys.IsValidPublicUserKey(first.PublicKey()) {
		t.Fatalf("PublicKey %q is not a user public key", first.PublicKey())
	}

	second, err := program.LoadOrCreateKey(path)
	if err != nil {
		t.Fatalf("second LoadOrCreateKey: %v", err)
	}
	again, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reread seed: %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("restat: %v", err)
	}
	// Positive: the second start loads the same identity.
	if second.PublicKey() != first.PublicKey() {
		t.Fatalf("public key changed: %s -> %s", first.PublicKey(), second.PublicKey())
	}
	// Negative: the file was not rewritten.
	if !bytes.Equal(seed, again) || !after.ModTime().Equal(info.ModTime()) {
		t.Fatal("seed file rewritten on second load")
	}
	assertSeedDoesNotLeak(t, first, seed)
}

func assertSeedDoesNotLeak(t *testing.T, key program.Key, seed []byte) {
	t.Helper()
	secret := strings.TrimSpace(string(seed))
	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		// Negative: no formatting of Key prints the seed.
		if strings.Contains(fmt.Sprintf(format, key), secret) {
			t.Fatalf("fmt %s of Key printed the seed", format)
		}
	}
	// Positive: what it does print is the public key.
	if fmt.Sprint(key) != key.PublicKey() {
		t.Fatalf("Key prints %q, want its public key", fmt.Sprint(key))
	}
}

func TestKeys_RefusesReadableSeedAndGarbage(t *testing.T) {
	dir := t.TempDir()
	loose := filepath.Join(dir, "loose.seed")
	if _, err := program.LoadOrCreateKey(loose); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := os.Chmod(loose, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	// Negative: a seed others can read is refused, not silently used.
	if _, err := program.LoadOrCreateKey(loose); err == nil {
		t.Fatal("LoadOrCreateKey accepted a 0644 seed")
	}
	garbage := filepath.Join(dir, "garbage.seed")
	if err := os.WriteFile(garbage, []byte("not a seed"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Negative: a file that is not a user seed is refused.
	if _, err := program.LoadOrCreateKey(garbage); err == nil {
		t.Fatal("LoadOrCreateKey accepted garbage")
	}
	// Negative: a missing parent directory is an error, not created.
	if _, err := program.LoadOrCreateKey(filepath.Join(dir, "no", "such.seed")); err == nil {
		t.Fatal("LoadOrCreateKey created a missing directory")
	}
}
