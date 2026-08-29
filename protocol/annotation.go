package protocol

// Annotation and Annotations define the blessed (but optional) shape a
// worker may put into TaskResolution.Data (#630) so a forge integration
// (GitHub, GitLab, etc.) can pin failure/warning markers onto a diff
// view. This is a paper contract: the engine reads TaskResolution.Data
// as an opaque json.RawMessage and never parses it for engine-level
// decisions. Workers that emit some other shape -- or nothing at all --
// lose nothing; only a forge-integration consumer that specifically
// understands this shape benefits from it.
//
// A worker that wants to use it marshals an Annotations value and sets
// it as TaskResolution.Data directly (Data is already json.RawMessage,
// so no wrapping is required).

// Annotation pins one finding onto a specific file/line, mirroring the
// shape most forge check-run APIs (GitHub Checks, GitLab CI, etc.)
// expect for inline diff annotations.
type Annotation struct {
	Path     string `json:"path"`
	Line     int    `json:"line"`
	Column   int    `json:"column,omitempty"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

// Annotations is the top-level shape a worker places into
// TaskResolution.Data when it wants to surface per-line findings.
type Annotations struct {
	Annotations []Annotation `json:"annotations"`
}

// Annotation severities. These are the only three levels forges
// commonly distinguish; consumers should treat an unrecognized value as
// AnnotationSeverityNotice rather than rejecting the annotation.
const (
	AnnotationSeverityError   = "error"
	AnnotationSeverityWarning = "warning"
	AnnotationSeverityNotice  = "notice"
)

// AnnotationsMax is the documented ceiling on Annotations.Annotations
// that consumers (forge integrations) may rely on when sizing their own
// buffers or API batch calls. The engine does not enforce this bound --
// it never parses Data -- so a worker that emits more is not rejected;
// it is simply outside the contract a well-behaved consumer promises to
// honor.
const AnnotationsMax = 1000
