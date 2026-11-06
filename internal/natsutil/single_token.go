package natsutil

import (
	"fmt"
	"unicode"
)

// SubjectTokenLenMax bounds a single-token setting such as a JetStream
// domain, which becomes a token in every cross-domain API subject.
const SubjectTokenLenMax = 64

// ValidateSubjectToken accepts "" or a single NATS subject token of at
// most SubjectTokenLenMax bytes; key names the setting in the error. A
// dot would split $JS.<domain>.API into extra tokens and a wildcard would
// match other domains' APIs. Callers that need a value reject "" first.
func ValidateSubjectToken(key, value string) error {
	if key == "" {
		panic("natsutil.ValidateSubjectToken: key is empty")
	}
	if len(value) > SubjectTokenLenMax {
		return fmt.Errorf("%s is %d bytes; max %d", key, len(value), SubjectTokenLenMax)
	}
	for _, r := range value {
		reason := ""
		switch {
		case r == '.':
			reason = "contains a dot"
		case r == '*' || r == '>':
			reason = "contains a wildcard"
		case unicode.IsSpace(r) || unicode.IsControl(r):
			reason = "contains whitespace or a control character"
		}
		if reason != "" {
			return fmt.Errorf("%s must be one NATS token; %q %s", key, value, reason)
		}
	}
	return nil
}
