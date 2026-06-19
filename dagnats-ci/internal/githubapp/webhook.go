// Package githubapp implements GitHub App webhook verification and event
// normalization for the dagnats-ci add-on.
//
// Design principle: each function is a pure transformation — no global state,
// no I/O, no time.Sleep. All callers can test deterministically with
// in-memory inputs.
package githubapp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// VerifySignature confirms that signatureHeader was produced by HMAC-SHA256 of
// body keyed with secret. It uses hmac.Equal for constant-time comparison so
// signature length cannot be used as a timing oracle.
//
// An empty header is rejected immediately — GitHub always sends the header on
// authenticated deliveries; absence signals a misconfigured webhook or an
// attacker bypassing the signature step.
func VerifySignature(secret, body []byte, signatureHeader string) error {
	if signatureHeader == "" {
		return errors.New("verify signature: X-Hub-Signature-256 header is empty")
	}
	const prefix = "sha256="
	if len(signatureHeader) <= len(prefix) {
		return fmt.Errorf(
			"verify signature: header %q is too short to contain a sha256= prefix",
			signatureHeader,
		)
	}
	if !strings.HasPrefix(signatureHeader, prefix) {
		return fmt.Errorf(
			"verify signature: header %q does not start with the required sha256= prefix",
			signatureHeader,
		)
	}
	mac := hmac.New(sha256.New, secret)
	// Write never returns an error for hmac.Hash — the panic assertion below
	// would surface a regression if that contract ever changed.
	n, err := mac.Write(body)
	if err != nil || n != len(body) {
		panic(fmt.Sprintf(
			"VerifySignature: hmac.Write failed (n=%d len=%d err=%v) — impossible",
			n, len(body), err,
		))
	}
	expected := prefix + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(signatureHeader)) {
		return errors.New("verify signature: HMAC mismatch — body or secret is wrong")
	}
	return nil
}
