package programs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go/jetstream"
)

// operatorNeeded ends every destructive-change refusal: registration
// never deletes or truncates what a program holds, and there is no
// unregister yet, so the change has to be made by hand.
const operatorNeeded = "; this would destroy data the program holds, so an operator " +
	"must make the change by hand"

// checkRemovals refuses a changed manifest that drops a stream or bucket
// the stored one declared, or lowers a retention limit, all of which
// Provision would otherwise apply: it updates existing streams in place.
// A MaxAge or TTL of 0 is unlimited.
func checkRemovals(stored, changed program.Manifest) error {
	if stored.Name != changed.Name {
		panic("programs.checkRemovals: an update keeps the program's name")
	}
	for _, old := range stored.Streams {
		index := slices.IndexFunc(changed.Streams, func(s program.StreamSpec) bool {
			return s.Name == old.Name
		})
		if index < 0 {
			return destructive("stream %q is no longer declared", old.Name)
		}
		if lowersLimit(old.MaxAge, changed.Streams[index].MaxAge) {
			return destructive("stream %q MaxAge drops from %v to %v",
				old.Name, old.MaxAge, changed.Streams[index].MaxAge)
		}
	}
	for _, old := range stored.Buckets {
		index := slices.IndexFunc(changed.Buckets, func(b program.BucketSpec) bool {
			return b.Name == old.Name
		})
		if index < 0 {
			return destructive("bucket %q is no longer declared", old.Name)
		}
		if err := checkBucketLimits(old, changed.Buckets[index]); err != nil {
			return err
		}
	}
	return nil
}

func checkBucketLimits(old, changed program.BucketSpec) error {
	if old.Name != changed.Name {
		panic("programs.checkBucketLimits: compares one bucket")
	}
	if lowersLimit(old.TTL, changed.TTL) {
		return destructive("bucket %q TTL drops from %v to %v", old.Name, old.TTL, changed.TTL)
	}
	if max(changed.History, 1) < max(old.History, 1) {
		return destructive("bucket %q History drops from %d to %d",
			old.Name, max(old.History, 1), max(changed.History, 1))
	}
	return nil
}

// lowersLimit reports whether an age limit gets stricter; 0 is none.
func lowersLimit(old, changed time.Duration) bool {
	if old < 0 || changed < 0 {
		panic("programs.lowersLimit: limits are not negative")
	}
	return changed > 0 && (old == 0 || changed < old)
}

// checkLive compares m against the streams and buckets that already
// exist under its names, which a migrating program brings to its first
// registration and an update finds from the last one. Provision would
// rewrite their subjects or cap them below the bytes they hold.
func (r *Registrar) checkLive(ctx context.Context, m program.Manifest, g program.Grant) error {
	if len(g.StreamMaxBytes) != len(m.Streams) {
		panic("programs.checkLive: the grant sizes every declared stream")
	}
	for _, s := range m.Streams {
		info, err := r.streamInfo(ctx, s.Name)
		if err != nil || info == nil {
			return err
		}
		if !sameSubjects(info.Config.Subjects, s.Subjects) {
			return destructive("stream %q subjects change from %q to %q",
				s.Name, info.Config.Subjects, s.Subjects)
		}
		if size := g.StreamMaxBytes[s.Name]; uint64(size) < info.State.Bytes {
			return destructive("stream %q would be capped at %d bytes but holds %d",
				s.Name, size, info.State.Bytes)
		}
	}
	for _, b := range m.Buckets {
		info, err := r.streamInfo(ctx, "KV_"+b.Name)
		if err != nil || info == nil {
			return err
		}
		if uint64(b.MaxBytes) < info.State.Bytes {
			return destructive("bucket %q would be capped at %d bytes but holds %d",
				b.Name, b.MaxBytes, info.State.Bytes)
		}
	}
	return nil
}

// streamInfo returns nil, nil for a stream that does not exist.
func (r *Registrar) streamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
	stream, err := r.js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read stream %s: %w", name, err)
	}
	return stream.CachedInfo(), nil
}

func sameSubjects(live, declared []string) bool {
	return slices.Equal(slices.Sorted(slices.Values(live)), slices.Sorted(slices.Values(declared)))
}

func destructive(format string, args ...any) error {
	return refused(program.RefusedManifestChanged, format+operatorNeeded, args...)
}
