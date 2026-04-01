package server

import (
	"fmt"
	"io"
)

const banner = `
  ╺━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╸

   ██████╗  █████╗  ██████╗
   ██╔══██╗██╔══██╗██╔════╝
   ██║  ██║███████║██║  ███╗
   ██║  ██║██╔══██║██║   ██║
   ██████╔╝██║  ██║╚██████╔╝
   ╚═════╝ ╚═╝  ╚═╝ ╚═════╝
              ███╗   ██╗ █████╗ ████████╗███████╗
              ████╗  ██║██╔══██╗╚══██╔══╝██╔════╝
              ██╔██╗ ██║███████║   ██║   ███████╗
              ██║╚██╗██║██╔══██║   ██║   ╚════██║
              ██║ ╚████║██║  ██║   ██║   ███████║
              ╚═╝  ╚═══╝╚═╝  ╚═╝   ╚═╝   ╚══════╝

  ╺━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╸
`

// printBanner writes the startup banner with connection details.
func printBanner(w io.Writer, httpAddr, natsURL string) {
	if w == nil {
		panic("printBanner: w must not be nil")
	}
	if httpAddr == "" {
		panic("printBanner: httpAddr must not be empty")
	}

	fmt.Fprint(w, banner)
	fmt.Fprintf(w, "   HTTP: %s\n", httpAddr)
	fmt.Fprintf(w, "   NATS: %s\n\n", natsURL)
}
