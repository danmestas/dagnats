package trigger

// Methodology: a KeyValue wrapper that fails the claim reads/creates
// while delegating everything else to the real bucket, to drive the
// transient-error branch of the cluster claim.

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
)

type failingKV struct{ jetstream.KeyValue }

var errInjectedKV = errors.New("injected kv failure")

func (failingKV) Get(
	context.Context, string,
) (jetstream.KeyValueEntry, error) {
	return nil, errInjectedKV
}

func (failingKV) Create(
	context.Context, string, []byte, ...jetstream.KVCreateOpt,
) (uint64, error) {
	return 0, errInjectedKV
}
