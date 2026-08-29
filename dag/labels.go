// dag/labels.go

// Labels let external triggers stamp arbitrary key/value metadata on a
// WorkflowRun at start time -- an event source starts a run *about*
// something (an order ID, a tenant, a region) and needs to find or bulk
// cancel those runs later without standing up a shadow lookup table. The
// bounds here keep labels cheap to carry on every snapshot and cheap to
// scan when filtering.
package dag

import (
	"fmt"
	"regexp"
)

// LabelsCountMax bounds how many labels a single run may carry.
const LabelsCountMax = 16

// LabelKeyLengthMax bounds the length of a single label key.
const LabelKeyLengthMax = 64

// LabelValueLengthMax bounds the length of a single label value.
const LabelValueLengthMax = 256

// labelKeyPattern restricts label keys to a safe, greppable charset:
// lowercase letters, digits, underscore, dot, and hyphen. Compiled once
// at package init so ValidateLabels stays allocation-cheap per call.
var labelKeyPattern = regexp.MustCompile(`^[a-z0-9_.-]+$`)

// ValidateLabels reports whether labels is a legal set of run labels. A
// nil or empty map is valid -- labels are optional. On the first
// violation it returns a descriptive error naming the offending key (or
// the label count, when the count itself is the violation).
func ValidateLabels(labels map[string]string) error {
	if len(labels) == 0 {
		return nil
	}
	if len(labels) > LabelsCountMax {
		return fmt.Errorf(
			"labels: at most %d labels allowed, got %d",
			LabelsCountMax, len(labels),
		)
	}
	for key, value := range labels {
		if err := validateLabel(key, value); err != nil {
			return err
		}
	}
	return nil
}

// validateLabel checks a single key/value pair against the charset and
// length bounds, in that order (charset first, since a key that fails
// the charset check is the more actionable thing to report).
func validateLabel(key, value string) error {
	if !labelKeyPattern.MatchString(key) {
		return fmt.Errorf(
			"labels: key %q must match %s",
			key, labelKeyPattern.String(),
		)
	}
	if len(key) > LabelKeyLengthMax {
		return fmt.Errorf(
			"labels: key %q exceeds max length %d",
			key, LabelKeyLengthMax,
		)
	}
	if len(value) > LabelValueLengthMax {
		return fmt.Errorf(
			"labels: value for key %q exceeds max length %d",
			key, LabelValueLengthMax,
		)
	}
	return nil
}
