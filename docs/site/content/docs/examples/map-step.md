---
title: Map Step
weight: 3
---

A three-step image pipeline that fans out over an array of URLs in parallel, demonstrating the Map step for fan-out/fan-in execution.

## Workflow Definition

The `resize` step has `type: "map"`, which tells the engine to split its input array into individual elements, process each in parallel, and collect the results back into an array for the next step.

```json
{
  "name": "image-pipeline",
  "version": "1.0",
  "steps": [
    {
      // Step 1: produce a JSON array of image URLs.
      "id": "fetch-urls",
      "task": "fetch-urls",
      "type": "normal"
    },
    {
      // Step 2: Map step -- runs once per element in the array.
      // The engine fans out automatically.
      "id": "resize",
      "task": "resize-image",
      "type": "map",
      "depends_on": ["fetch-urls"]
    },
    {
      // Step 3: receives all Map outputs collected into an array.
      "id": "build-gallery",
      "task": "build-gallery",
      "type": "normal",
      "depends_on": ["resize"]
    }
  ]
}
```

## Worker Implementation

Each handler is straightforward. The Map step handler processes a single element -- the engine handles the fan-out and fan-in. The handler does not need to know it is running inside a Map step.

```go
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
	// The output must be a JSON array for the Map step to split.
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
	// The engine fans out -- this handler receives a single string,
	// not the full array. Multiple instances run in parallel.
	worker.HandleTyped(w, "resize-image",
		func(
			ctx worker.TaskContext, imageURL string,
		) (string, error) {
			resized := strings.Replace(
				imageURL, ".png", "_thumb.png", 1,
			)
			fmt.Printf("[resize] %s -> %s\n", imageURL, resized)
			return resized, nil
		},
	)

	// Step 3: Collect all resized URLs into a gallery.
	// The engine fans in -- input is an array of all Map outputs.
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
```

## Running the Example

1. Start the DagNats server:
   ```bash
   dagnats serve
   ```

2. In a second terminal, start the worker:
   ```bash
   go run ./examples/map-step/
   ```

3. In a third terminal, register and run:
   ```bash
   dagnats workflow register examples/map-step/workflow.json
   dagnats run start image-pipeline '["img1.png","img2.png","img3.png"]'
   ```

4. Watch the parallel fan-out:
   ```
   [fetch-urls] returning 3 URLs
   [resize] img/hero.png -> img/hero_thumb.png
   [resize] img/logo.png -> img/logo_thumb.png
   [resize] img/banner.png -> img/banner_thumb.png
   [gallery] building from 3 thumbnails
   ```

## What's Happening

1. The engine dispatches `fetch-urls`, which returns a JSON array of three URLs.
2. The engine sees `resize` is a `map` step. It splits the array into three individual tasks and dispatches them in parallel.
3. Each `resize-image` handler receives a single URL string, processes it, and returns the result. These run concurrently across available workers.
4. Once all three map tasks complete, the engine collects their outputs into a JSON array and passes it to `build-gallery`.
5. The `build-gallery` handler receives `["img/hero_thumb.png", "img/logo_thumb.png", "img/banner_thumb.png"]` and produces the final result.

Key concepts demonstrated:
- **Map step fan-out** -- the engine splits arrays automatically. Handlers process single elements.
- **Fan-in collection** -- downstream steps receive the collected array of all Map outputs.
- **Parallel execution** -- Map tasks run concurrently, scaling with the number of available workers.
- **Handler simplicity** -- the `resize-image` handler has no idea it is part of a Map step. It just processes one item.

## Related

- [Map Steps](/docs/step-types/map-steps) -- step type reference and configuration options
