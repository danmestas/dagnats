package console

import (
	"math"
	"strconv"
	"strings"
)

// sparkMaxPoints bounds the number of points rendered into a single
// Datatype expression so the string stays short enough to be readable
// inline. The tail (most recent points) is kept when a series is longer.
const sparkMaxPoints = 48

// SparkExpr formats a numeric series as a Datatype line-chart
// expression ("{l:v1,v2,...}", values 0-100) for the .console-spark
// font sparkline. Returns "" for a nil/empty series or one that is
// entirely zero, so the template renders nothing rather than a flat
// baseline. Values are min-max normalized across the series to 0-100;
// a constant non-zero series renders as a mid line (50).
func SparkExpr(series []float64) string {
	if len(series) == 0 {
		return ""
	}
	if len(series) > sparkMaxPoints {
		series = series[len(series)-sparkMaxPoints:]
	}
	minValue, maxValue := series[0], series[0]
	for _, v := range series {
		if v < minValue {
			minValue = v
		}
		if v > maxValue {
			maxValue = v
		}
	}
	if maxValue <= 0 {
		return ""
	}
	ints := make([]string, len(series))
	for i, v := range series {
		ints[i] = strconv.Itoa(normalizeSpark(v, minValue, maxValue))
	}
	return "{l:" + strings.Join(ints, ",") + "}"
}

// normalizeSpark maps v into [0,100] via min-max scaling. A constant
// non-zero series (max == min) collapses to a 50 mid line.
func normalizeSpark(v, minValue, maxValue float64) int {
	if maxValue == minValue {
		return 50
	}
	scaled := int(math.Round((v - minValue) / (maxValue - minValue) * 100))
	if scaled < 0 {
		return 0
	}
	if scaled > 100 {
		return 100
	}
	return scaled
}
