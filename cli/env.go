// cli/env.go
// Environment variable helpers with deprecation fallback.
package cli

import (
	"fmt"
	"os"
	"strconv"
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

// JetStreamReplicasFromEnv reads DAGNATS_NATS_JETSTREAM_REPLICAS, the
// variable `dagnats serve` reads for nats_jetstream_replicas, for the
// split binaries (dagnats-engine, dagnats-api). Unset means 0: SetupAll
// then derives the factor from the cluster it is connected to. Values
// outside {0, 1, 3, 5} are an error, matching serve's validation.
func JetStreamReplicasFromEnv() (int, error) {
	const name = "DAGNATS_NATS_JETSTREAM_REPLICAS"
	raw := os.Getenv(name)
	if raw == "" {
		return 0, nil
	}
	replicas, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s=%q: not an integer", name, raw)
	}
	switch replicas {
	case 0, 1, 3, 5:
		return replicas, nil
	}
	return 0, fmt.Errorf("%s=%d: must be 0, 1, 3 or 5", name, replicas)
}
