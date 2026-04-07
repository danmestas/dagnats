package server

import (
	"fmt"
	"io"
)

// Gruvbox palette — 24-bit ANSI escape sequences.
const (
	grvAqua = "\033[38;2;142;192;124m" // #8ec07c
	grvGray = "\033[38;2;146;131;116m" // #928374
	grvOr   = "\033[38;2;254;128;25m"  // #fe8019
	ansiB   = "\033[1m"
	ansiR   = "\033[0m"
)

// banner3-D figlet font, each letter colored from gruvbox palette.
// Letter columns: D[0:12] A[12:23] G[23:35] N[35:47] A[47:58] T[58:69] S[69:]
//
//nolint:lll
var logo = ansiB + logoLine(0) + logoLine(1) + logoLine(2) + logoLine(3) +
	logoLine(4) + logoLine(5) + logoLine(6) + logoLine(7) + ansiR

// logoColors applies an orange-to-aqua gradient across the 7 letters.
var logoColors = [7]string{
	"\033[38;2;254;128;25m",  // D — orange #fe8019
	"\033[38;2;234;146;39m",  // A — warm amber
	"\033[38;2;214;164;53m",  // G — golden
	"\033[38;2;194;178;72m",  // N — olive
	"\033[38;2;174;188;90m",  // A — yellow-green
	"\033[38;2;158;190;107m", // T — sage
	"\033[38;2;142;192;124m", // S — aqua #8ec07c
}

// logoCuts defines the column boundaries for each of the 7 letters.
var logoCuts = [8]int{0, 12, 23, 35, 47, 58, 69, -1}

// logoLines holds the raw (uncolored) banner text.
var logoLines = [8]string{
	"'########:::::'###:::::'######:::'##::: ##::::'###::::'########::'######::",
	" ##.... ##:::'## ##:::'##... ##:: ###:: ##:::'## ##:::... ##..::'##... ##:",
	" ##:::: ##::'##:. ##:: ##:::..::: ####: ##::'##:. ##::::: ##:::: ##:::..:",
	" ##:::: ##:'##:::. ##: ##::'####: ## ## ##:'##:::. ##:::: ##::::. ######:",
	" ##:::: ##: #########: ##::: ##:: ##. ####: #########:::: ##:::::..... ##",
	" ##:::: ##: ##.... ##: ##::: ##:: ##:. ###: ##.... ##:::: ##::::'##::: ##",
	" ########:: ##:::: ##:. ######::: ##::. ##: ##:::: ##:::: ##::::. ######:",
	"........:::..:::::..:::......::::..::::..::..:::::..:::::..::::::......::",
}

func logoLine(row int) string {
	if row < 0 || row >= len(logoLines) {
		panic("logoLine: row out of range")
	}
	line := logoLines[row]
	result := "   "
	for i := 0; i < len(logoColors); i++ {
		start := logoCuts[i]
		end := logoCuts[i+1]
		if end < 0 || end > len(line) {
			end = len(line)
		}
		if start >= len(line) {
			break
		}
		result += logoColors[i] + line[start:end]
	}
	result += ansiR + "\n"
	return result
}

var rule = grvGray +
	"    ──────────────────────────────────────────────────────────────" +
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
