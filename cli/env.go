// cli/env.go
// Environment variable helpers with deprecation fallback.
package cli

import (
	"fmt"
	"os"
)

// GetEnvWithFallback checks newName first, then oldName with a
// deprecation warning on stderr, then returns defaultVal.
func GetEnvWithFallback(
	newName, oldName, defaultVal string,
) string {
	if newName == "" {
		panic("GetEnvWithFallback: newName must not be empty")
	}
	if oldName == "" {
		panic("GetEnvWithFallback: oldName must not be empty")
	}
	if val := os.Getenv(newName); val != "" {
		return val
	}
	if val := os.Getenv(oldName); val != "" {
		fmt.Fprintf(os.Stderr,
			"Warning: %s is deprecated, use %s instead\n",
			oldName, newName)
		return val
	}
	return defaultVal
}
