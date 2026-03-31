// ui/embed.go
// Embeds HTML templates and static assets into the binary so the
// dashboard requires zero external files at runtime.
package ui

import "embed"

//go:embed templates/*.html templates/partials/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS
