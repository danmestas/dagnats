// Package runid mints run identifiers used as JetStream Nats-Msg-Id
// values and as the keying material for per-run subjects. The previous
// runID generator was workflowID + time.Now().UnixNano() which collided
// under concurrent HTTP triggers (two requests in the same nanosecond
// produced identical IDs, JetStream dedup dropped one workflow.started,
// and the surviving run's response delivered to both waiting handlers
// -- a cross-client data leak). This package is a single source of
// truth: callers receive 16 random bytes from the OS entropy source
// as 32 lowercase hex characters.
//
// Ousterhout note: deliberately a one-function package. Hides the
// crypto/rand call and the hex encoding behind the smallest possible
// interface so callers cannot accidentally pick a weaker source.
package runid

import (
	"crypto/rand"
	"encoding/hex"
)

// idByteCount is the number of random bytes feeding each ID. 16 bytes
// (128 bits) make a collision in 2^64 calls roughly 50/50 -- well
// past the lifetime call count of any DagNats deployment.
const idByteCount = 16

// New returns a 32-character lowercase hex string from 16 crypto-random
// bytes. Panics only if the OS entropy source is unavailable, which is
// a fatal system condition (a workflow engine cannot make any forward
// progress without unique IDs).
func New() string {
	b := make([]byte, idByteCount)
	n, err := rand.Read(b)
	if err != nil {
		panic("runid.New: crypto/rand failed: " + err.Error())
	}
	if n != idByteCount {
		panic("runid.New: short read from crypto/rand")
	}
	return hex.EncodeToString(b)
}
