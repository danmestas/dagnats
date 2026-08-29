package protocol

// Methodology: red-green TDD for the Annotations contract (#630). The
// engine parses nothing from TaskResolution.Data -- this test only proves
// that Annotations round-trips through the json.RawMessage Data field
// unchanged, since that field is the only place a forge integration can
// rely on finding it.

import (
	"encoding/json"
	"testing"
)

func TestAnnotationsRoundTripThroughTaskResolutionData(t *testing.T) {
	original := Annotations{
		Annotations: []Annotation{
			{
				Path:     "main.go",
				Line:     42,
				Column:   7,
				Severity: AnnotationSeverityError,
				Message:  "undefined variable",
			},
			{
				Path:     "util.go",
				Line:     10,
				Severity: AnnotationSeverityWarning,
				Message:  "unused import",
			},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal Annotations: %v", err)
	}

	resolution := TaskResolution{
		Action: "complete",
		Data:   json.RawMessage(data),
	}

	resolutionBytes, err := json.Marshal(resolution)
	if err != nil {
		t.Fatalf("marshal TaskResolution: %v", err)
	}

	var decodedResolution TaskResolution
	if err := json.Unmarshal(resolutionBytes, &decodedResolution); err != nil {
		t.Fatalf("unmarshal TaskResolution: %v", err)
	}

	var decoded Annotations
	if err := json.Unmarshal(
		decodedResolution.Data, &decoded,
	); err != nil {
		t.Fatalf("unmarshal Annotations from Data: %v", err)
	}

	if len(decoded.Annotations) != len(original.Annotations) {
		t.Fatalf("annotation count = %d, want %d",
			len(decoded.Annotations), len(original.Annotations))
	}
	if decoded.Annotations[0] != original.Annotations[0] {
		t.Fatalf("annotation[0] = %+v, want %+v",
			decoded.Annotations[0], original.Annotations[0])
	}
	if decoded.Annotations[1].Column != 0 {
		t.Fatalf("annotation[1].Column = %d, want 0 (omitted)",
			decoded.Annotations[1].Column)
	}
}

func TestAnnotationSeverityConstants(t *testing.T) {
	if AnnotationSeverityError != "error" {
		t.Fatalf("AnnotationSeverityError = %q, want %q",
			AnnotationSeverityError, "error")
	}
	if AnnotationSeverityWarning != "warning" {
		t.Fatalf("AnnotationSeverityWarning = %q, want %q",
			AnnotationSeverityWarning, "warning")
	}
	if AnnotationSeverityNotice != "notice" {
		t.Fatalf("AnnotationSeverityNotice = %q, want %q",
			AnnotationSeverityNotice, "notice")
	}
	if AnnotationsMax != 1000 {
		t.Fatalf("AnnotationsMax = %d, want 1000", AnnotationsMax)
	}
}
