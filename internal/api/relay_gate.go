package api

import "sync"

// Relay concurrency bounds (#885). Each program has three shares on a
// node, one per class of relay: step events (relayEventsPerProgramMax),
// signal reads and sends, which WaitForSignal polls
// (relaySignalsPerProgramMax), and everything else, journal and stores
// (relayPerProgramMax). A
// request beyond its share is refused busy at once instead of queueing,
// so one program flooding the relay cannot delay another's, and a long
// journal replay (one view per record) cannot starve the same program's
// step events, which carry its runs forward. relayTotalMax caps the
// whole node, and relayProgramsMax how many distinct shares are tracked.
const (
	relayEventsPerProgramMax  = 8
	relaySignalsPerProgramMax = 4
	relayPerProgramMax        = 16
	relayTotalMax             = 256
	relayProgramsMax          = 1024
)

// relayClass is which share of a program's bound a relay op draws on.
type relayClass int

const (
	relayClassEvents relayClass = iota
	relayClassSignals
	relayClassOther
)

func (c relayClass) limit() int {
	switch c {
	case relayClassEvents:
		return relayEventsPerProgramMax
	case relayClassSignals:
		return relaySignalsPerProgramMax
	}
	return relayPerProgramMax
}

// relayShare is one program's share of one class.
type relayShare struct {
	prefix string
	class  relayClass
}

// relayGate counts relays in flight, per program and in total.
type relayGate struct {
	mu        sync.Mutex
	total     int
	perPrefix map[relayShare]int
}

// acquire takes a slot of class for prefix, reporting false when that
// share or the node's is used up.
func (g *relayGate) acquire(prefix string, class relayClass) bool {
	if prefix == "" {
		panic("relayGate.acquire: prefix must not be empty")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.perPrefix == nil {
		g.perPrefix = make(map[relayShare]int)
	}
	share := relayShare{prefix: prefix, class: class}
	held, tracked := g.perPrefix[share]
	if g.total >= relayTotalMax || held >= class.limit() {
		return false
	}
	if !tracked && len(g.perPrefix) >= relayProgramsMax {
		return false
	}
	g.perPrefix[share] = held + 1
	g.total++
	return true
}

func (g *relayGate) release(prefix string, class relayClass) {
	g.mu.Lock()
	defer g.mu.Unlock()
	share := relayShare{prefix: prefix, class: class}
	held := g.perPrefix[share]
	if held <= 0 || g.total <= 0 {
		panic("relayGate.release: release without acquire")
	}
	if held == 1 {
		delete(g.perPrefix, share)
	} else {
		g.perPrefix[share] = held - 1
	}
	g.total--
}
