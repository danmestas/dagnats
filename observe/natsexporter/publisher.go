// Package natsexporter provides OTel exporters backed by NATS
// JetStream. The TELEMETRY stream serves as a local telemetry
// store that works without external infrastructure.
package natsexporter

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const publishTimeout = 2 * time.Second

// Publisher writes telemetry data to NATS JetStream with
// Nats-Msg-Id deduplication. Shared by span, log, and metric
// exporters.
type Publisher struct {
	js jetstream.JetStream
}

// NewPublisher creates a Publisher. Panics on nil js.
func NewPublisher(js jetstream.JetStream) *Publisher {
	if js == nil {
		panic("NewPublisher: js must not be nil")
	}
	return &Publisher{js: js}
}

// Publish sends data to the given subject with dedup ID.
// Returns an error if JetStream is unreachable — the OTel SDK
// will retry per its BatchSpanProcessor config.
func (p *Publisher) Publish(
	ctx context.Context,
	subject string,
	data []byte,
	msgID string,
) error {
	if subject == "" {
		panic("Publisher.Publish: subject must not be empty")
	}
	if data == nil {
		panic("Publisher.Publish: data must not be nil")
	}
	if msgID == "" {
		panic("Publisher.Publish: msgID must not be empty")
	}

	pubCtx, cancel := context.WithTimeout(
		ctx, publishTimeout,
	)
	defer cancel()

	_, err := p.js.Publish(
		pubCtx, subject, data,
		jetstream.WithMsgID(msgID),
	)
	return err
}
