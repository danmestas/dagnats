package engine

import "sync/atomic"

// GrantPolicyHolder holds the active *GrantPolicy behind an atomic pointer
// so a config reload can swap it live without any new watch machinery: the
// configfile watcher already re-invokes the reload closure, which calls
// Store. Concurrent dispatch goroutines call Load. The zero value is ready
// to use and Loads nil (deny-by-default) until the first Store.
type GrantPolicyHolder struct {
	v atomic.Pointer[GrantPolicy]
}

// Load returns the active policy, or nil if none has been stored. A nil
// holder (never wired) and a nil policy both deny everything, so an unset
// holder is fail-safe at every layer.
func (h *GrantPolicyHolder) Load() *GrantPolicy {
	if h == nil {
		return nil
	}
	return h.v.Load()
}

// Store swaps in a new active policy. Safe to call from the reload closure
// while dispatch goroutines call Load.
func (h *GrantPolicyHolder) Store(p *GrantPolicy) {
	h.v.Store(p)
}
