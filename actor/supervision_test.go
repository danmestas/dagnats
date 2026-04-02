package actor

// Methodology: test supervision strategy decisions in isolation.
// Each strategy is a pure function from error → Directive.

import (
	"errors"
	"testing"
)

func TestOneForOneDefaultsToRestart(t *testing.T) {
	s := &OneForOne{}

	// Positive: default decision is Restart
	got := s.Decide(errors.New("boom"))
	if got != Restart {
		t.Fatalf("Decide = %v, want Restart", got)
	}

	// Positive: nil error still returns Restart
	got2 := s.Decide(nil)
	if got2 != Restart {
		t.Fatalf("Decide(nil) = %v, want Restart", got2)
	}
}

func TestOneForOneCustomDecider(t *testing.T) {
	permanent := errors.New("permanent")
	s := &OneForOne{
		Decider: func(err error) Directive {
			if errors.Is(err, permanent) {
				return Stop
			}
			return Restart
		},
	}

	// Positive: permanent error → Stop
	if got := s.Decide(permanent); got != Stop {
		t.Fatalf("Decide(permanent) = %v, want Stop", got)
	}

	// Positive: transient error → Restart
	if got := s.Decide(errors.New("transient")); got != Restart {
		t.Fatalf("Decide(transient) = %v, want Restart", got)
	}
}

func TestAllForOneDefaultsToRestart(t *testing.T) {
	s := &AllForOne{}

	// Positive: default decision
	if got := s.Decide(errors.New("boom")); got != Restart {
		t.Fatalf("Decide = %v, want Restart", got)
	}

	// Positive: RestartScope is AllChildren
	if s.RestartScope() != RestartAll {
		t.Fatalf("RestartScope = %v, want RestartAll", s.RestartScope())
	}
}

func TestOneForOneRestartScope(t *testing.T) {
	s := &OneForOne{}
	// Positive: scope is only the failed actor
	if s.RestartScope() != RestartOne {
		t.Fatalf("RestartScope = %v, want RestartOne", s.RestartScope())
	}
}

func TestAllForOneCustomDecider(t *testing.T) {
	permanent := errors.New("permanent")
	s := &AllForOne{
		Decider: func(err error) Directive {
			if errors.Is(err, permanent) {
				return Stop
			}
			return Restart
		},
	}

	// Positive: permanent error -> Stop
	if got := s.Decide(permanent); got != Stop {
		t.Fatalf("Decide(permanent) = %v, want Stop", got)
	}

	// Positive: transient error -> Restart
	got := s.Decide(errors.New("transient"))
	if got != Restart {
		t.Fatalf("Decide(transient) = %v, want Restart", got)
	}
}
