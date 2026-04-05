// examples/map-step/main.go
// Demonstrates the Map step for parallel fan-out. A "fetch-urls" step
// returns a list of image URLs, the "resize-image" Map step processes
// each URL in parallel, and "build-gallery" collects all results.
//
// Run alongside `dagnats serve`:
//
//	Terminal 1: dagnats serve
//	Terminal 2: go run ./examples/map-step/
//	Terminal 3: dagnats workflow register examples/map-step/workflow.json
//	            dagnats run start image-pipeline '["img1.png","img2.png","img3.png"]'
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	w := worker.NewWorker(nc, nil)

	// Step 1: Return a list of image URLs.
	// The Map step expects its input to be a JSON array.
	worker.HandleTyped(w, "fetch-urls",
		func(
			ctx worker.TaskContext, input json.RawMessage,
		) ([]string, error) {
			urls := []string{
				"img/hero.png",
				"img/logo.png",
				"img/banner.png",
			}
			fmt.Printf("[fetch-urls] returning %d URLs\n",
				len(urls))
			return urls, nil
		},
	)

	// Step 2: Process ONE image (called once per array element).
	// The Map step fans out — this handler runs in parallel for
	// each URL in the array from step 1.
	worker.HandleTyped(w, "resize-image",
		func(
			ctx worker.TaskContext, imageURL string,
		) (string, error) {
			resized := strings.Replace(
				imageURL, ".png", "_thumb.png", 1,
			)
			fmt.Printf("[resize] %s → %s\n", imageURL, resized)
			return resized, nil
		},
	)

	// Step 3: Collect all resized URLs into a gallery.
	// Input is a JSON array of all Map step outputs.
	worker.HandleTyped(w, "build-gallery",
		func(
			ctx worker.TaskContext, thumbnails []string,
		) (string, error) {
			fmt.Printf("[gallery] building from %d thumbnails\n",
				len(thumbnails))
			return fmt.Sprintf(
				"gallery with %d images", len(thumbnails),
			), nil
		},
	)

	fmt.Println("Map-step worker ready. Waiting for tasks...")
	w.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}
