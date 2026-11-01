package program

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// Provision creates or updates the manifest's streams and buckets at
// the sizes in g. The host calls it over its own connection: a program's
// permissions grant no stream create or update, because a stream config
// can capture, mirror or republish subjects the program may not touch.
// Idempotent.
//
// An existing stream or bucket keeps every field the manifest does not
// own (Discard, MaxMsgs, DenyDelete, MaxValueSize, compression and the
// rest): only subjects, size, age limit, history, replicas and storage
// are written over its live config, so adopting a stream an operator
// made does not reset it to defaults.
func Provision(ctx context.Context, js jetstream.JetStream, m Manifest, g Grant) error {
	if ctx == nil || js == nil {
		panic("program.Provision: ctx and js must not be nil")
	}
	if err := Validate(m); err != nil {
		return err
	}
	if err := checkGrant(m, g); err != nil {
		return err
	}
	var errs []error
	for _, s := range m.Streams {
		config, err := liveStreamConfig(ctx, js, s, g)
		if err == nil {
			_, err = js.CreateOrUpdateStream(ctx, config)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("program %s: stream %s: %w", m.Name, s.Name, err))
		}
	}
	for _, b := range m.Buckets {
		config, err := liveBucketConfig(ctx, js, b, g)
		if err == nil {
			_, err = js.CreateOrUpdateKeyValue(ctx, config)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("program %s: bucket %s: %w", m.Name, b.Name, err))
		}
	}
	return errors.Join(errs...)
}

// liveStreamConfig is the stream's live config with the fields s owns
// written over it, or s's own config when the stream does not exist.
func liveStreamConfig(
	ctx context.Context, js jetstream.JetStream, s StreamSpec, g Grant,
) (jetstream.StreamConfig, error) {
	owned := streamConfig(s, g)
	stream, err := js.Stream(ctx, s.Name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return owned, nil
	}
	if err != nil {
		return jetstream.StreamConfig{}, err
	}
	live := stream.CachedInfo().Config
	live.Subjects, live.MaxBytes, live.MaxAge = owned.Subjects, owned.MaxBytes, owned.MaxAge
	live.Replicas, live.Storage = owned.Replicas, owned.Storage
	if live.Name != owned.Name {
		panic("program.liveStreamConfig: the stream read is the one declared")
	}
	return live, nil
}

// liveBucketConfig is the bucket's live config with the fields b owns
// written over it, or b's own config when the bucket does not exist.
func liveBucketConfig(
	ctx context.Context, js jetstream.JetStream, b BucketSpec, g Grant,
) (jetstream.KeyValueConfig, error) {
	owned := bucketConfig(b, g)
	kv, err := js.KeyValue(ctx, b.Name)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return owned, nil
	}
	if err != nil {
		return jetstream.KeyValueConfig{}, err
	}
	status, err := kv.Status(ctx)
	if err != nil {
		return jetstream.KeyValueConfig{}, err
	}
	live := status.Config()
	live.History, live.TTL, live.MaxBytes = owned.History, owned.TTL, owned.MaxBytes
	live.Replicas, live.Storage = owned.Replicas, owned.Storage
	if live.Bucket != owned.Bucket {
		panic("program.liveBucketConfig: the bucket read is the one declared")
	}
	return live, nil
}

func streamConfig(s StreamSpec, g Grant) jetstream.StreamConfig {
	size := g.StreamMaxBytes[s.Name]
	if size <= 0 || len(s.Subjects) == 0 {
		panic("program.streamConfig: grant size and subjects were checked")
	}
	return jetstream.StreamConfig{
		Name:     s.Name,
		Subjects: append([]string(nil), s.Subjects...),
		MaxBytes: size,
		MaxAge:   s.MaxAge,
		Replicas: g.Replicas,
		Storage:  jetstream.FileStorage,
	}
}

func bucketConfig(b BucketSpec, g Grant) jetstream.KeyValueConfig {
	if b.MaxBytes <= 0 || g.Replicas < 1 {
		panic("program.bucketConfig: size and replicas were checked")
	}
	history := b.History
	if history == 0 {
		history = 1
	}
	return jetstream.KeyValueConfig{
		Bucket:   b.Name,
		History:  history,
		TTL:      b.TTL,
		MaxBytes: b.MaxBytes,
		Replicas: g.Replicas,
		Storage:  jetstream.FileStorage,
	}
}
