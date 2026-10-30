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
		"Parquet data directory: a local path or an s3://bucket/prefix",
	)
	s3Endpoint := flag.String(
		"s3-endpoint",
		"",
		"S3-compatible endpoint for an s3:// data dir (R2, Garage, Tigris); "+
			"http:// turns TLS off. Credentials from AWS_ACCESS_KEY_ID, "+
			"AWS_SECRET_ACCESS_KEY, AWS_REGION. Unset: the AWS credential chain",
	)
	flag.Parse()

	var opts []Option
	if *s3Endpoint != "" {
		opts = append(opts, WithS3Endpoint(*s3Endpoint))
	}
	db, err := OpenDB(*dataDir, opts...)
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
