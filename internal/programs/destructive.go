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

// checkRemovals refuses a changed manifest that no longer declares a
// stream or bucket the stored one did: it would stay behind, holding
// data no program owns.
func checkRemovals(stored, changed program.Manifest) error {
	if stored.Name != changed.Name {
		panic("programs.checkRemovals: an update keeps the program's name")
	}
	for _, old := range stored.Streams {
		if !slices.ContainsFunc(changed.Streams, func(s program.StreamSpec) bool {
			return s.Name == old.Name
		}) {
			return destructive("stream %q is no longer declared", old.Name)
		}
	}
	for _, old := range stored.Buckets {
		if !slices.ContainsFunc(changed.Buckets, func(b program.BucketSpec) bool {
			return b.Name == old.Name
		}) {
			return destructive("bucket %q is no longer declared", old.Name)
		}
	}
	return nil
}

// checkLive compares m with the streams and buckets that already exist
// under its names: the ones an update finds from the last registration,
// and the ones a migrating program made before its first. Provision
// rewrites the fields a manifest owns in place, so it refuses a change
// to a stream's subjects, a cap below the bytes held, and a stricter
// MaxAge, TTL or History. Fields the manifest does not own (Discard,
// MaxMsgs, DenyDelete and the rest) are kept by Provision, so they
// cannot reset. A name with nothing behind it yet is skipped.
func (r *Registrar) checkLive(ctx context.Context, m program.Manifest, g program.Grant) error {
	if len(g.StreamMaxBytes) != len(m.Streams) {
		panic("programs.checkLive: the grant sizes every declared stream")
	}
	for _, s := range m.Streams {
		info, err := r.streamInfo(ctx, s.Name)
		if err != nil {
			return err
		}
		if info == nil {
			continue
		}
		if err := checkLiveStream(info, s, g.StreamMaxBytes[s.Name]); err != nil {
			return err
		}
	}
	for _, b := range m.Buckets {
		info, err := r.streamInfo(ctx, "KV_"+b.Name)
		if err != nil {
			return err
		}
		if info == nil {
			continue
		}
		if err := checkLiveBucket(info, b); err != nil {
			return err
		}
	}
	return nil
}

func checkLiveStream(info *jetstream.StreamInfo, s program.StreamSpec, size int64) error {
	if info.Config.Name != s.Name {
		panic("programs.checkLiveStream: compares one stream")
	}
	if !sameSubjects(info.Config.Subjects, s.Subjects) {
		return destructive("stream %q subjects change from %q to %q",
			s.Name, info.Config.Subjects, s.Subjects)
	}
	if uint64(size) < info.State.Bytes {
		return destructive("stream %q would be capped at %d bytes but holds %d",
			s.Name, size, info.State.Bytes)
	}
	if lowersLimit(info.Config.MaxAge, s.MaxAge) {
		return destructive("stream %q MaxAge drops from %v to %v",
			s.Name, info.Config.MaxAge, s.MaxAge)
	}
	return nil
}

// checkLiveBucket reads the bucket's backing stream: MaxAge is its TTL
// and MaxMsgsPerSubject its History.
func checkLiveBucket(info *jetstream.StreamInfo, b program.BucketSpec) error {
	if info.Config.Name != "KV_"+b.Name {
		panic("programs.checkLiveBucket: compares one bucket")
	}
	if uint64(b.MaxBytes) < info.State.Bytes {
		return destructive("bucket %q would be capped at %d bytes but holds %d",
			b.Name, b.MaxBytes, info.State.Bytes)
	}
	if lowersLimit(info.Config.MaxAge, b.TTL) {
		return destructive("bucket %q TTL drops from %v to %v", b.Name, info.Config.MaxAge, b.TTL)
	}
	history := int64(max(b.History, 1))
	if live := max(info.Config.MaxMsgsPerSubject, 1); history < live {
		return destructive("bucket %q History drops from %d to %d", b.Name, live, history)
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
