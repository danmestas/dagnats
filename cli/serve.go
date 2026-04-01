package cli

import (
	"fmt"
	"os"

	"github.com/danmestas/dagnats/server"
)

func runServeCmd(args []string) {
	cfg := server.ConfigFromEnv()
	srv := server.New(cfg)
	if err := srv.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
