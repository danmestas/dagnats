// A test cluster's nodes ran with nats-server logging off except
// Fatalf, so when a node failed to create a stream's store (#996:
// "error creating store for stream", err 10049) the reason it logged
// was thrown away and the failure could not be diagnosed.
//
// Methodology: pure unit tests of the node logger. Positive: Warn and
// Error lines are kept, newest last, bounded, and rendered for a dump.
// Negative: Notice, Debug and Trace lines are not kept, and Fatalf
// still reaches the bind-failure channel.
package dagnatstest

import (
	"fmt"
	"strings"
	"testing"
)

func TestNodeLog_KeepsWarnAndErrorBounded(t *testing.T) {
	l := newNodeLog()
	l.Noticef("starting %d", 1)
	l.Debugf("debug")
	l.Tracef("trace")
	l.Warnf("route %s slow", "a")
	l.Errorf("JetStream failed to create store: %s", "too many open files")
	lines := l.lines()
	if len(lines) != 2 {
		t.Fatalf("kept %d lines, want 2 (Warn and Error only): %q", len(lines), lines)
	}
	if !strings.Contains(lines[1], "[ERR] JetStream failed to create store: too many open files") {
		t.Fatalf("error line not kept verbatim: %q", lines[1])
	}

	for i := 0; i < nodeLogLinesMax+50; i++ {
		l.Warnf("line %d", i)
	}
	lines = l.lines()
	if len(lines) != nodeLogLinesMax {
		t.Fatalf("kept %d lines, want the bound %d", len(lines), nodeLogLinesMax)
	}
	want := fmt.Sprintf("[WRN] line %d", nodeLogLinesMax+49)
	if lines[len(lines)-1] != want {
		t.Fatalf("newest line = %q, want %q", lines[len(lines)-1], want)
	}
}

func TestNodeLog_FatalStillSignalsBindFailure(t *testing.T) {
	l := newNodeLog()
	l.Fatalf("can't listen on %d", 4222)
	select {
	case msg := <-l.messages:
		if msg != "can't listen on 4222" {
			t.Fatalf("fatal message = %q", msg)
		}
	default:
		t.Fatal("Fatalf did not reach the bind-failure channel")
	}
	if got := l.lines(); len(got) != 1 || !strings.Contains(got[0], "[FTL]") {
		t.Fatalf("fatal line not kept for the dump: %q", got)
	}
}
