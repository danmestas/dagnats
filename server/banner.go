package server

import (
	"fmt"
	"io"
)

// Gruvbox palette — 24-bit ANSI escape sequences.
const (
	grvFg    = "\033[38;2;235;219;178m" // #ebdbb2
	grvAqua  = "\033[38;2;142;192;124m" // #8ec07c
	grvGray  = "\033[38;2;146;131;116m" // #928374
	grvOr    = "\033[38;2;254;128;25m"  // #fe8019
	ansiB    = "\033[1m"
	ansiR    = "\033[0m"
)

//nolint:lll
const logo = "" +
	grvFg + ansiB +
	"   ██████╗  █████╗  ██████╗ ███╗   ██╗ █████╗ ████████╗███████╗\n" +
	"   ██╔══██╗██╔══██╗██╔════╝ ████╗  ██║██╔══██╗╚══██╔══╝██╔════╝\n" +
	"   ██║  ██║███████║██║  ███╗██╔██╗ ██║███████║   ██║   ███████╗\n" +
	"   ██║  ██║██╔══██║██║   ██║██║╚██╗██║██╔══██║   ██║   ╚════██║\n" +
	"   ██████╔╝██║  ██║╚██████╔╝██║ ╚████║██║  ██║   ██║   ███████║\n" +
	"   ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═╝   ╚═╝   ╚══════╝" +
	ansiR + "\n"

var rule = grvGray +
	"   ─────────────────────────────────────────────────────────────" +
	ansiR

// printBanner writes the startup banner with connection details.
func printBanner(w io.Writer, httpAddr, natsURL string) {
	if w == nil {
		panic("printBanner: w must not be nil")
	}
	if httpAddr == "" {
		panic("printBanner: httpAddr must not be empty")
	}

	fmt.Fprintf(w, "\n%s\n\n%s\n%s\n\n",
		rule, logo, rule)
	displayAddr := httpAddr
	if len(displayAddr) > 0 && displayAddr[0] == ':' {
		displayAddr = "localhost" + displayAddr
	}
	fmt.Fprintf(w, "   %shttp%s  %shttp://%s%s\n",
		grvGray, ansiR, grvAqua, displayAddr, ansiR)
	fmt.Fprintf(w, "   %snats%s  %s%s%s\n\n",
		grvGray, ansiR, grvAqua, natsURL, ansiR)
}

// printStep writes a styled progress line.
func printStep(w io.Writer, msg string) {
	if w == nil {
		panic("printStep: w must not be nil")
	}
	if msg == "" {
		panic("printStep: msg must not be empty")
	}

	fmt.Fprintf(w, "   %s%s•%s %s%s%s\n",
		grvOr, ansiB, ansiR, grvGray, msg, ansiR)
}
