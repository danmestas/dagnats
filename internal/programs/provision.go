package programs

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go/jetstream"
)

// programLockStripes bounds the per-program locks: a program's name
// always hashes to the same stripe, and unrelated programs that share a
// stripe only wait on each other's provisioning.
const programLockStripes = 64

// programLocks serialize one program's registrations from commit through
// provisioning on this node. Taken before admitMu, never after it.
type programLocks [programLockStripes]sync.Mutex

func (r *Registrar) programLock(name string) *sync.Mutex {
	if name == "" {
		panic("programs.programLock: name was validated")
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(name)) // a hash.Hash32 Write never fails
	return &r.locks[hash.Sum32()%programLockStripes]
}

// provision applies m at g. When that fails part way, it puts back the
// configs the existing streams and buckets had before, so the record the
// caller then restores still describes what is applied.
func (r *Registrar) provision(ctx context.Context, m program.Manifest, g program.Grant) error {
	if ctx == nil {
		panic("programs.provision: ctx must not be nil")
	}
	before, err := r.liveConfigs(ctx, m)
	if err != nil {
		return err
	}
	err = program.Provision(ctx, r.js, m, g)
	if err == nil {
		return nil
	}
	return errors.Join(err, r.restoreConfigs(before))
}

// liveConfigs returns the configs of m's streams and bucket streams that
// exist now.
func (r *Registrar) liveConfigs(
	ctx context.Context, m program.Manifest,
) ([]jetstream.StreamConfig, error) {
	names := make([]string, 0, len(m.Streams)+len(m.Buckets))
	for _, s := range m.Streams {
		names = append(names, s.Name)
	}
	for _, b := range m.Buckets {
		names = append(names, "KV_"+b.Name)
	}
	configs := make([]jetstream.StreamConfig, 0, len(names))
	for _, name := range names {
		info, err := r.streamInfo(ctx, name)
		if err != nil {
			return nil, err
		}
		if info != nil {
			configs = append(configs, info.Config)
		}
	}
	return configs, nil
}

// restoreConfigs writes back configs on its own context, so a cancelled
// registration still restores.
func (r *Registrar) restoreConfigs(configs []jetstream.StreamConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var errs []error
	for _, config := range configs {
		if _, err := r.js.UpdateStream(ctx, config); err != nil {
			errs = append(errs, fmt.Errorf("restore stream %s: %w", config.Name, err))
		}
	}
	return errors.Join(errs...)
}
