package program

import "fmt"

// RegisterSubject is the request subject a program registers on. The
// registration connection is anonymous and dialed with
// nats.CustomInboxPrefix(JoinInboxPrefix), so that in a required-auth
// server its join user sees registration replies and nothing else.
const (
	RegisterSubject = "api.programs.register"
	JoinInboxPrefix = "_JOIN"
)

// Refusal codes. Each reaches the program as an *ErrRefused from Run.
const (
	RefusedRequestInvalid      = "request_invalid"
	RefusedNameInvalid         = "name_invalid"
	RefusedNameTaken           = "name_taken"
	RefusedManifestChanged     = "manifest_changed"
	RefusedJoinKeyInvalid      = "join_key_invalid"
	RefusedJoinKeyExhausted    = "join_key_exhausted"
	RefusedBudgetExceeded      = "budget_exceeded"
	RefusedVersionIncompatible = "version_incompatible"
	RefusedAuthModeUnsupported = "auth_mode_unsupported"
)

// Instance identifies the process registering, for the operator.
type Instance struct {
	Hostname string `json:"hostname"`
	Pid      int    `json:"pid"`
}

// RegisterRequest is the body of a RegisterSubject request. JoinKey may
// be empty on an open-mode server. Only the public key is sent; the
// seed never leaves the program.
type RegisterRequest struct {
	JoinKey   string   `json:"join_key,omitempty"`
	Manifest  Manifest `json:"manifest"`
	PublicKey string   `json:"public_key"`
	Instance  Instance `json:"instance"`
}

// RegisterReply is the reply: exactly one of Grant, Refused or Error.
// Error is a server fault (a KV write failed), not a refusal, and may
// succeed on retry.
type RegisterReply struct {
	Grant   *Grant      `json:"grant,omitempty"`
	Refused *ErrRefused `json:"refused,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// ErrRefused is dagnats declining to admit the program, with one of the
// Refused* codes and a human-readable detail.
type ErrRefused struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func (e *ErrRefused) Error() string {
	return fmt.Sprintf("program refused: %s: %s", e.Code, e.Detail)
}
