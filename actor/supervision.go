package actor

// SupervisionStrategy decides how to handle a failed child actor.
// The runtime calls Decide when an actor's Receive returns an error.
type SupervisionStrategy interface {
	// Decide returns the directive for handling the failure.
	Decide(err error) Directive

	// RestartScope controls whether a failure restarts one child
	// or all siblings under the same supervisor.
	RestartScope() RestartScope
}

// RestartScope controls which actors restart on a failure.
type RestartScope int

const (
	RestartOne RestartScope = iota // Only the failed actor
	RestartAll                     // All children of the supervisor
)

// OneForOne restarts only the failed child. The default strategy.
type OneForOne struct {
	// Decider maps errors to directives. If nil, defaults to Restart.
	Decider func(error) Directive
}

// Decide returns the directive for the given error.
func (s *OneForOne) Decide(err error) Directive {
	if s.Decider != nil {
		return s.Decider(err)
	}
	return Restart
}

// RestartScope returns RestartOne — only the failed child restarts.
func (s *OneForOne) RestartScope() RestartScope {
	return RestartOne
}

// AllForOne restarts all siblings when any child fails.
// Useful when children have interdependent state.
type AllForOne struct {
	// Decider maps errors to directives. If nil, defaults to Restart.
	Decider func(error) Directive
}

// Decide returns the directive for the given error.
func (s *AllForOne) Decide(err error) Directive {
	if s.Decider != nil {
		return s.Decider(err)
	}
	return Restart
}

// RestartScope returns RestartAll — all siblings restart.
func (s *AllForOne) RestartScope() RestartScope {
	return RestartAll
}
