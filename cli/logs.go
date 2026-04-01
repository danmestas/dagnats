// cli/logs.go
// Tails the NATS telemetry log stream in real time. Subscribes to
// telemetry.logs subjects on the TELEMETRY JetStream stream and prints
// each LogRecord in a human-readable format with optional filtering.
package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/danmestas/dagnats/observe/simple"
	"github.com/nats-io/nats.go"
)

// runLogsCmd tails telemetry logs from JetStream with optional filters.
// Blocks until interrupted by SIGINT or SIGTERM.
func runLogsCmd(args []string) {
	var levelFilter, serviceFilter string
	for _, arg := range args {
		if strings.HasPrefix(arg, "--level=") {
			levelFilter = strings.TrimPrefix(arg, "--level=")
		}
		if strings.HasPrefix(arg, "--service=") {
			serviceFilter = strings.TrimPrefix(arg, "--service=")
		}
	}

	_, nc := connectService()
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream: %v\n", err)
		os.Exit(1)
	}

	subject := buildLogSubject(serviceFilter, levelFilter)
	msgCh := make(chan *nats.Msg, 256)
	sub, err := js.ChanSubscribe(subject, msgCh, nats.DeliverNew())
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe %s: %v\n", subject, err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	fmt.Fprintf(os.Stderr, "Tailing logs on %s ...\n", subject)

	for {
		select {
		case msg := <-msgCh:
			var rec simple.LogRecord
			if err := json.Unmarshal(msg.Data, &rec); err != nil {
				continue
			}
			fmt.Println(formatLogLine(rec))
		case <-sigCh:
			if err := sub.Unsubscribe(); err != nil {
				fmt.Fprintf(os.Stderr, "unsubscribe: %v\n", err)
			}
			return
		}
	}
}

// buildLogSubject constructs the NATS subject filter for log
// subscriptions based on optional service and level filters.
func buildLogSubject(service, level string) string {
	if service == "" && level == "" {
		return "telemetry.logs.>"
	}
	if service != "" && level == "" {
		return "telemetry.logs." + service + ".>"
	}
	if service == "" && level != "" {
		return "telemetry.logs.*." + level
	}
	return "telemetry.logs." + service + "." + level
}

// formatLogLine renders a LogRecord as a single human-readable line.
// Format: HH:MM:SS LEVEL   SERVICE  message [key=val ...]
func formatLogLine(rec simple.LogRecord) string {
	if rec.Message == "" {
		panic("formatLogLine: Message must not be empty")
	}
	if rec.Level == "" {
		panic("formatLogLine: Level must not be empty")
	}

	var b strings.Builder
	b.WriteString(rec.Timestamp.Format("15:04:05"))
	b.WriteByte(' ')
	b.WriteString(colorLevel(strings.ToUpper(rec.Level)))
	b.WriteByte(' ')
	b.WriteString(rec.Service)
	b.WriteString("  ")
	b.WriteString(rec.Message)

	fields := formatFields(rec.Fields)
	if rec.Error != "" {
		fields = append(fields, colorRed("error="+rec.Error))
	}
	if len(fields) > 0 {
		b.WriteString(" [")
		b.WriteString(strings.Join(fields, " "))
		b.WriteByte(']')
	}
	return b.String()
}

// formatFields sorts field key=value pairs alphabetically.
// Returns nil when the map is empty to avoid unnecessary allocation.
func formatFields(fields map[string]any) []string {
	if len(fields) == 0 {
		return nil
	}

	const maxFields = 1000
	if len(fields) > maxFields {
		panic("formatFields: fields exceeds max bound")
	}

	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%v", k, fields[k]))
	}
	return pairs
}

// colorLevel pads the level string to 7 characters and applies
// ANSI color based on severity. Respects NO_COLOR env var.
func colorLevel(level string) string {
	padded := fmt.Sprintf("%-7s", level)
	if os.Getenv("NO_COLOR") != "" {
		return padded
	}
	switch level {
	case "ERROR":
		return "\033[31m" + padded + "\033[0m"
	case "WARN":
		return "\033[33m" + padded + "\033[0m"
	case "INFO":
		return "\033[32m" + padded + "\033[0m"
	case "DEBUG":
		return "\033[90m" + padded + "\033[0m"
	default:
		return padded
	}
}

// colorRed wraps text in red ANSI escape codes. Respects NO_COLOR.
func colorRed(s string) string {
	if os.Getenv("NO_COLOR") != "" {
		return s
	}
	return "\033[31m" + s + "\033[0m"
}
