// internal/natsutil/replica_pick_test.go

// Tests for pickReplicas, the decision behind detectReplicas.
// Methodology: pickReplicas is pure logic over a placement probe, so each
// case scripts the probe's answers and uses a zero pause. The case that
// matters is a booting cluster: its meta leader refuses three replicas
// ("no suitable peers") until it has seen enough peers (#772), so a
// refusal at three must be retried, and one that never clears must fail
// loudly instead of settling on a single replica. A cluster that knows of
// fewer than three servers (a one- or two-node cluster, such as a member of
// a supercluster) can never place three, so its first refusal is the
// answer. Real-cluster detection
// is covered in server/cluster_replicas_test.go.
package natsutil

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// scriptedProbe answers each call from answers[replicas] in order, and
// repeats the last answer once the script runs out.
type scriptedProbe struct {
	answers map[int][]bool
	calls   map[int]int
}

func (p *scriptedProbe) probe(_ context.Context, replicas int) (bool, error) {
	script := p.answers[replicas]
	n := p.calls[replicas]
	p.calls[replicas] = n + 1
	if len(script) == 0 {
		return false, nil
	}
	if n >= len(script) {
		return script[len(script)-1], nil
	}
	return script[n], nil
}

func newScriptedProbe(answers map[int][]bool) *scriptedProbe {
	return &scriptedProbe{answers: answers, calls: map[int]int{}}
}

func noPause(int) time.Duration { return 0 }

func TestPickReplicas_FivePlacedIsFive(t *testing.T) {
	p := newScriptedProbe(map[int][]bool{5: {true}})
	got, err := pickReplicas(context.Background(), p.probe, noPause, true)
	if err != nil || got != 5 {
		t.Fatalf("pickReplicas = %d, %v; want 5, nil", got, err)
	}
	if p.calls[3] != 0 {
		t.Fatalf("probed 3 replicas %d times after 5 was placed", p.calls[3])
	}
}

func TestPickReplicas_RetriesThreeWhileClusterBoots(t *testing.T) {
	p := newScriptedProbe(map[int][]bool{5: {false}, 3: {false, false, true}})
	got, err := pickReplicas(context.Background(), p.probe, noPause, true)
	if err != nil || got != 3 {
		t.Fatalf("pickReplicas = %d, %v; want 3, nil", got, err)
	}
	if p.calls[3] != 3 {
		t.Fatalf("probed 3 replicas %d times, want 3", p.calls[3])
	}
}

func TestPickReplicas_NeverPlacedFailsInsteadOfOne(t *testing.T) {
	p := newScriptedProbe(map[int][]bool{5: {false}, 3: {false}})
	got, err := pickReplicas(context.Background(), p.probe, noPause, true)
	if err == nil {
		t.Fatalf("pickReplicas = %d, nil; want an error, not a silent 1", got)
	}
	if !strings.Contains(err.Error(), "3 replicas") {
		t.Fatalf("error = %q, want it to name the refused count", err)
	}
	if p.calls[3] != replicaProbeAttemptsMax {
		t.Fatalf("probed 3 replicas %d times, want %d", p.calls[3], replicaProbeAttemptsMax)
	}
}

func TestPickReplicas_ProbeErrorStops(t *testing.T) {
	boom := errors.New("no reply")
	probe := func(context.Context, int) (bool, error) { return false, boom }
	if _, err := pickReplicas(context.Background(), probe, noPause, true); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the probe's error", err)
	}
}

func TestPickReplicas_SmallClusterAnswersOneAtOnce(t *testing.T) {
	p := newScriptedProbe(map[int][]bool{5: {false}, 3: {false}})
	got, err := pickReplicas(context.Background(), p.probe, noPause, false)
	if err != nil || got != 1 {
		t.Fatalf("pickReplicas = %d, %v; want 1, nil", got, err)
	}
	if p.calls[3] != 1 {
		t.Fatalf("probed 3 replicas %d times in a small cluster, want 1", p.calls[3])
	}
}
