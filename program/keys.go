package program

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// seedFileMode is the only mode a seed file may have: readable by its
// owner and nobody else.
const seedFileMode fs.FileMode = 0o600

// seedFileBytesMax bounds the read; a user seed is 58 bytes.
const seedFileBytesMax = 256

// Key is a program's NKey identity. The seed stays inside: Key exposes
// the public key and a connect option that signs with the seed, and
// prints as its public key.
type Key struct {
	pair   nkeys.KeyPair
	public string
}

// LoadOrCreateKey returns the identity stored at seedPath, generating a
// user key pair and writing its seed there (mode 0600, exclusive create)
// when the file does not exist. An existing seed is read, never
// rewritten; one readable by group or others is refused. The parent
// directory must exist.
func LoadOrCreateKey(seedPath string) (Key, error) {
	if seedPath == "" {
		panic("program.LoadOrCreateKey: seedPath must not be empty")
	}
	key, err := createKey(seedPath)
	if errors.Is(err, fs.ErrExist) {
		key, err = loadKey(seedPath)
	}
	if err != nil {
		return Key{}, err
	}
	if !nkeys.IsValidPublicUserKey(key.public) {
		panic("program.LoadOrCreateKey: key is not a user key")
	}
	return key, nil
}

// createKey writes a fresh seed with O_EXCL, so two processes racing on
// a first start cannot both believe their seed is the stored one.
func createKey(seedPath string) (Key, error) {
	file, err := os.OpenFile(seedPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, seedFileMode)
	if err != nil {
		return Key{}, err
	}
	pair, err := nkeys.CreateUser()
	if err != nil {
		return Key{}, errors.Join(err, file.Close(), os.Remove(seedPath))
	}
	seed, err := pair.Seed()
	if err != nil {
		return Key{}, errors.Join(err, file.Close(), os.Remove(seedPath))
	}
	// Seed returns the pair's own buffer, so it is written, not wiped.
	_, err = file.Write(seed)
	if err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return Key{}, errors.Join(fmt.Errorf("program: write seed: %w", err), os.Remove(seedPath))
	}
	return newKey(pair)
}

func loadKey(seedPath string) (Key, error) {
	info, err := os.Stat(seedPath)
	if err != nil {
		return Key{}, err
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return Key{}, fmt.Errorf(
			"program: seed %s has mode %o; it must be %o", seedPath, perm, seedFileMode)
	}
	if info.Size() > seedFileBytesMax {
		return Key{}, fmt.Errorf("program: seed %s is %d bytes, not a seed", seedPath, info.Size())
	}
	raw, err := os.ReadFile(seedPath)
	if err != nil {
		return Key{}, err
	}
	defer wipe(raw)
	pair, err := nkeys.FromSeed(bytes.TrimSpace(raw))
	if err != nil {
		return Key{}, fmt.Errorf("program: seed %s: %w", seedPath, err)
	}
	return newKey(pair)
}

func newKey(pair nkeys.KeyPair) (Key, error) {
	public, err := pair.PublicKey()
	if err != nil {
		return Key{}, err
	}
	if !nkeys.IsValidPublicUserKey(public) {
		return Key{}, fmt.Errorf("program: seed is not a user seed (public key %s)", public)
	}
	return Key{pair: pair, public: public}, nil
}

// PublicKey is the "U..." key dagnats binds the program's user to. It
// is the only part of the identity that ever leaves the process.
func (k Key) PublicKey() string {
	if k.pair == nil {
		panic("program.Key.PublicKey: zero Key")
	}
	return k.public
}

// ConnectOption authenticates a connection as this key.
func (k Key) ConnectOption() nats.Option {
	if k.pair == nil {
		panic("program.Key.ConnectOption: zero Key")
	}
	pair := k.pair
	return nats.Nkey(k.public, pair.Sign)
}

// String prints the public key, so logging a Key cannot leak the seed.
func (k Key) String() string { return k.public }

// GoString keeps %#v to the public key as well.
func (k Key) GoString() string { return "program.Key(" + k.public + ")" }

func wipe(b []byte) {
	for i := range b {
		b[i] = 'x'
	}
}
