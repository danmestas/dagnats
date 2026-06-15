// chart_height_invariant_test.go pins the coupling between the uPlot
// render height in metrics.js and the .console-chart-canvas min-height
// in app.css. uPlot draws a two-row time axis at the bottom of the
// canvas; if the container's min-height is smaller than the uPlot
// height the bottom axis row clips, and if the axis band is too tight
// the verbose date row crowds the canvas edge. These constants must
// move together, so the test reads both embedded assets and asserts
// they agree (and clear the 240px floor that gives the time axis room).
//
// Methodology: read the embedded assets directly (no server needed);
// extract each numeric constant by regex; assert equality + floor.
// Positive space: both values present and equal. Negative space: a
// drift between them (a smaller min-height than render height) fails.
package console

import (
	"io/fs"
	"regexp"
	"strconv"
	"testing"
)

const chartHeightFloor = 240

func TestChartHeight_canvasMinHeightMatchesUPlotRenderHeight(t *testing.T) {
	cssBody, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	jsBody, err := fs.ReadFile(assetsFS, "assets/sources/metrics.js")
	if err != nil {
		t.Fatalf("read metrics.js: %v", err)
	}

	cssRe := regexp.MustCompile(
		`\.console-chart-canvas\s*\{[^}]*?min-height:\s*(\d+)px`)
	cssMatch := cssRe.FindSubmatch(cssBody)
	if cssMatch == nil {
		t.Fatal("app.css: .console-chart-canvas min-height not found")
	}
	cssHeight, err := strconv.Atoi(string(cssMatch[1]))
	if err != nil {
		t.Fatalf("parse css min-height: %v", err)
	}

	// The uPlot height lives in buildOptions: `height: NNN,`.
	jsRe := regexp.MustCompile(`height:\s*(\d+),`)
	jsMatch := jsRe.FindSubmatch(jsBody)
	if jsMatch == nil {
		t.Fatal("metrics.js: uPlot height not found")
	}
	jsHeight, err := strconv.Atoi(string(jsMatch[1]))
	if err != nil {
		t.Fatalf("parse js height: %v", err)
	}

	if cssHeight != jsHeight {
		t.Errorf("canvas min-height (%dpx) must equal uPlot height (%d) "+
			"or the bottom time axis clips", cssHeight, jsHeight)
	}
	if cssHeight < chartHeightFloor {
		t.Errorf("chart height %d below the %dpx floor that gives the "+
			"two-row time axis room", cssHeight, chartHeightFloor)
	}
}
