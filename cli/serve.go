package cli

import (
	"fmt"
	"os"

	"github.com/danmestas/dagnats/server"
)

func runServeCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats serve")
		fmt.Println("Starts embedded NATS server with" +
			" DagNats engine and API.")
		fmt.Println()
		fmt.Println("Config: dagnats.yaml" +
			" (optional, in current directory)")
		fmt.Println("Env:    DAGNATS_DATA_DIR," +
			" DAGNATS_HTTP_ADDR, DAGNATS_NATS_PORT")
		fmt.Println()
		fmt.Println("Run 'dagnats config show'" +
			" to see effective configuration.")
		return
	}

	cfg := server.ConfigFromEnv()
	srv := server.New(cfg)
	if err := srv.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
