// engine/ratelimit.go
// KV-backed token bucket rate limiter for task dispatch throttling.
// Uses NATS KV with CAS (Compare-And-Swap) for lock-free concurrency.
// Tokens refill proportionally based on elapsed time since last refill.
// When tokens are exhausted, returns a retryAfter duration for the caller
// to schedule a delayed re-attempt via SleepTimer.
package engine

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

const maxCASRetries = 10

type tokenBucket struct {
	Tokens     int   `json:"tokens"`
	LastRefill int64 `json:"last_refill_ns"`
	Limit      int   `json:"limit"`
	PeriodNs   int64 `json:"period_ns"`
}

// RateLimiter implements KV-backed token bucket rate limiting.
// Nil-safe: all methods are no-ops when receiver is nil, so callers
// do not need to check before calling Allow.
type RateLimiter struct {
	kv  nats.KeyValue
	now func() time.Time // injectable clock for testing
}

// NewRateLimiter creates a RateLimiter using the rate_limits KV bucket.
// Returns nil if the bucket does not exist, making rate limiting optional.
func NewRateLimiter(
	jsLegacy nats.JetStreamContext,
) *RateLimiter {
	if jsLegacy == nil {
		panic("NewRateLimiter: jsLegacy must not be nil")
	}
	kv, err := jsLegacy.KeyValue("rate_limits")
	if err != nil {
		return nil
	}
	return &RateLimiter{kv: kv, now: time.Now}
}

// Allow checks if a request is allowed under the rate limit.
// Returns allowed=true if tokens available, or allowed=false with
// retryAfter indicating when the next token refills.
// units is the number of tokens to consume per request.
func (rl *RateLimiter) Allow(
	taskType string, key string,
	limit int, period time.Duration, units int,
) (bool, time.Duration, error) {
	if rl == nil {
		return true, 0, nil
	}
	if taskType == "" {
		panic("RateLimiter.Allow: taskType must not be empty")
	}
	if key == "" {
		panic("RateLimiter.Allow: key must not be empty")
	}

	kvKey := taskType + "." + key
	for attempt := 0; attempt < maxCASRetries; attempt++ {
		allowed, retry, err := rl.tryAllow(
			kvKey, limit, period, units,
		)
		if err == nil {
			return allowed, retry, nil
		}
		// CAS conflict — retry
	}
	return false, 0, fmt.Errorf("rate limit: CAS retries exhausted")
}

// tryAllow performs a single CAS attempt to consume tokens.
func (rl *RateLimiter) tryAllow(
	kvKey string, limit int,
	period time.Duration, units int,
) (bool, time.Duration, error) {
	if kvKey == "" {
		panic("tryAllow: kvKey must not be empty")
	}
	if limit <= 0 {
		panic("tryAllow: limit must be positive")
	}

	bucket, rev, err := rl.loadBucket(kvKey, limit, period)
	if err != nil {
		return false, 0, err
	}

	bucket = rl.refill(bucket)

	if bucket.Tokens >= units {
		bucket.Tokens -= units
		return true, 0, rl.saveBucket(kvKey, bucket, rev)
	}

	// Not enough tokens — calculate retry delay.
	retryAfter := rl.timeUntilTokens(bucket, units)
	return false, retryAfter, nil
}

// loadBucket reads or initializes the token bucket from KV.
func (rl *RateLimiter) loadBucket(
	kvKey string, limit int, period time.Duration,
) (tokenBucket, uint64, error) {
	entry, err := rl.kv.Get(kvKey)
	if err == nats.ErrKeyNotFound {
		return tokenBucket{
			Tokens:     limit,
			LastRefill: rl.now().UnixNano(),
			Limit:      limit,
			PeriodNs:   period.Nanoseconds(),
		}, 0, nil
	}
	if err != nil {
		return tokenBucket{}, 0, fmt.Errorf("get %q: %w", kvKey, err)
	}

	var bucket tokenBucket
	if err := json.Unmarshal(entry.Value(), &bucket); err != nil {
		return tokenBucket{}, 0,
			fmt.Errorf("unmarshal %q: %w", kvKey, err)
	}
	return bucket, entry.Revision(), nil
}

// refill adds tokens based on elapsed time since last refill.
func (rl *RateLimiter) refill(bucket tokenBucket) tokenBucket {
	if rl == nil {
		panic("refill: nil receiver")
	}
	if bucket.PeriodNs <= 0 {
		panic("refill: bucket.PeriodNs must be positive")
	}
	now := rl.now().UnixNano()
	elapsed := now - bucket.LastRefill
	if elapsed <= 0 {
		return bucket
	}

	// Tokens refill proportionally: elapsed / period * limit
	tokensPerNs := float64(bucket.Limit) / float64(bucket.PeriodNs)
	refilled := int(float64(elapsed) * tokensPerNs)
	if refilled > 0 {
		bucket.Tokens += refilled
		if bucket.Tokens > bucket.Limit {
			bucket.Tokens = bucket.Limit
		}
		bucket.LastRefill = now
	}
	return bucket
}

// timeUntilTokens calculates how long until enough tokens refill.
func (rl *RateLimiter) timeUntilTokens(
	bucket tokenBucket, units int,
) time.Duration {
	if bucket.Limit <= 0 || bucket.PeriodNs <= 0 {
		return time.Second
	}
	needed := units - bucket.Tokens
	if needed <= 0 {
		return 0
	}
	nsPerToken := bucket.PeriodNs / int64(bucket.Limit)
	return time.Duration(int64(needed) * nsPerToken)
}

// saveBucket writes the bucket back to KV with CAS.
func (rl *RateLimiter) saveBucket(
	kvKey string, bucket tokenBucket, rev uint64,
) error {
	data, err := json.Marshal(bucket)
	if err != nil {
		return fmt.Errorf("marshal bucket: %w", err)
	}
	if rev == 0 {
		_, err = rl.kv.Create(kvKey, data)
	} else {
		_, err = rl.kv.Update(kvKey, data, rev)
	}
	return err
}
