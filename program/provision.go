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
		_, err := js.CreateOrUpdateStream(ctx, streamConfig(s, g))
		if err != nil {
			errs = append(errs, fmt.Errorf("program %s: stream %s: %w", m.Name, s.Name, err))
		}
	}
	for _, b := range m.Buckets {
		_, err := js.CreateOrUpdateKeyValue(ctx, bucketConfig(b, g))
		if err != nil {
			errs = append(errs, fmt.Errorf("program %s: bucket %s: %w", m.Name, b.Name, err))
		}
	}
	return errors.Join(errs...)
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
