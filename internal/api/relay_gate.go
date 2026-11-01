package api

import "sync"

// Relay concurrency bounds (#885). Each program may have
// relayPerProgramMax relays in flight on a node; a request beyond that
// is refused busy at once instead of queueing, so one program flooding
// the relay cannot delay another's. relayTotalMax caps the whole node,
// and relayProgramsMax caps how many distinct programs are tracked.
const (
	relayPerProgramMax = 16
	relayTotalMax      = 256
	relayProgramsMax   = 1024
)

// relayGate counts relays in flight, per program and in total.
type relayGate struct {
	mu        sync.Mutex
	total     int
	perPrefix map[string]int
}

// acquire takes a slot for prefix, reporting false when its share or the
// node's is used up.
func (g *relayGate) acquire(prefix string) bool {
	if prefix == "" {
		panic("relayGate.acquire: prefix must not be empty")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.perPrefix == nil {
		g.perPrefix = make(map[string]int)
	}
	held, tracked := g.perPrefix[prefix]
	if g.total >= relayTotalMax || held >= relayPerProgramMax {
		return false
	}
	if !tracked && len(g.perPrefix) >= relayProgramsMax {
		return false
	}
	g.perPrefix[prefix] = held + 1
	g.total++
	return true
}

func (g *relayGate) release(prefix string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	held := g.perPrefix[prefix]
	if held <= 0 || g.total <= 0 {
		panic("relayGate.release: release without acquire")
	}
	if held == 1 {
		delete(g.perPrefix, prefix)
	} else {
		g.perPrefix[prefix] = held - 1
	}
	g.total--
}
