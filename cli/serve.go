package cli

import (
	"fmt"
	"os"

	"github.com/danmestas/dagnats/server"
)

func runServeCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats serve")
		fmt.Println("Starts the embedded NATS server with DagNats engine.")
		return
	}

	cfg := server.ConfigFromEnv()
	srv := server.New(cfg)
	if err := srv.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
