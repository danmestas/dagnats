package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mark3labs/mcp-go/server"
)

func main() {
	dataDir := flag.String(
		"data-dir",
		"./telemetry-data",
		"path to Parquet data directory",
	)
	flag.Parse()

	db, err := OpenDB(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	srv := NewMCPServer(db)
	if err := server.ServeStdio(srv); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
