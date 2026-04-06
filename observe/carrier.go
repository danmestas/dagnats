package observe

import (
	"sort"

	"github.com/nats-io/nats.go"
)

// NATSHeaderCarrier adapts nats.Header to OTel's
// propagation.TextMapCarrier interface, enabling W3C trace-context
// injection and extraction over NATS messages.
type NATSHeaderCarrier struct {
	Header nats.Header
}

// Get returns the first value for the given key, or "" if the header is nil.
func (c NATSHeaderCarrier) Get(key string) string {
	if c.Header == nil {
		return ""
	}
	return c.Header.Get(key)
}

// Set stores a key-value pair. Panics if Header is nil (programmer error).
func (c NATSHeaderCarrier) Set(key, val string) {
	if c.Header == nil {
		panic("NATSHeaderCarrier: Set called on nil Header")
	}
	c.Header.Set(key, val)
}

// Keys returns all header keys in sorted order, or nil if Header is nil.
func (c NATSHeaderCarrier) Keys() []string {
	if c.Header == nil {
		return nil
	}
	keys := make([]string, 0, len(c.Header))
	for k := range c.Header {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
