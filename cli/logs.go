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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/danmestas/dagnats/internal/observe/simple"
	"github.com/nats-io/nats.go"
)

// runLogsCmd tails telemetry logs from JetStream with optional filters.
// Blocks until interrupted by SIGINT or SIGTERM.
func runLogsCmd(args []string) {
	if args == nil {
		panic("runLogsCmd: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("runLogsCmd: args exceeds max bound")
	}

	var levelFilter, serviceFilter string
	tailCount := parseTailFlag(args)
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

	jsLegacy, err := nc.JetStream()
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream: %v\n", err)
		os.Exit(1)
	}

	subject := buildLogSubject(serviceFilter, levelFilter)

	if tailCount > 0 {
		runLogsTail(jsLegacy, subject, tailCount)
		return
	}

	msgCh := make(chan *nats.Msg, 256)
	sub, err := jsLegacy.ChanSubscribe(
		subject, msgCh, nats.DeliverNew(),
	)
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
				fmt.Fprintf(os.Stderr,
					"logs: unmarshal: %v\n", err)
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
	if len(service) > 200 {
		panic("buildLogSubject: service name unreasonably long")
	}
	if len(level) > 20 {
		panic("buildLogSubject: level string unreasonably long")
	}
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
		fields = append(fields, ColorRed("error="+rec.Error))
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
	if fields == nil {
		return nil
	}
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

// tailCountMax is the upper bound for --tail to prevent unbounded
// memory usage when collecting historical log messages.
const tailCountMax = 10000

// parseTailFlag extracts the --tail=N value from args. Returns 0
// when the flag is absent. Exits with an error for invalid values.
func parseTailFlag(args []string) int {
	if args == nil {
		panic("parseTailFlag: args must not be nil")
	}
	if len(args) > 100 {
		panic("parseTailFlag: args exceeds max bound")
	}

	for _, arg := range args {
		if !strings.HasPrefix(arg, "--tail=") {
			continue
		}
		val := strings.TrimPrefix(arg, "--tail=")
		n, err := strconv.Atoi(val)
		if err != nil || n <= 0 {
			fmt.Fprintln(os.Stderr,
				"Error: --tail must be a positive integer")
			os.Exit(1)
		}
		if n > tailCountMax {
			fmt.Fprintf(os.Stderr,
				"Error: --tail exceeds maximum (%d)\n",
				tailCountMax)
			os.Exit(1)
		}
		return n
	}
	return 0
}

// runLogsTail fetches the last count log messages from the stream
// and prints them. Subscribes with DeliverAll, drains into a ring
// buffer, then exits without blocking for SIGINT.
func runLogsTail(
	js nats.JetStreamContext, subject string, count int,
) {
	if js == nil {
		panic("runLogsTail: js must not be nil")
	}
	if count <= 0 || count > tailCountMax {
		panic("runLogsTail: count out of bounds")
	}

	sub, err := js.SubscribeSync(subject,
		nats.DeliverAll(), nats.AckNone())
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe %s: %v\n",
			subject, err)
		os.Exit(1)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			fmt.Fprintf(os.Stderr, "unsubscribe: %v\n", err)
		}
	}()

	buf := collectTailMessages(sub, count)
	for _, rec := range buf {
		fmt.Println(formatLogLine(rec))
	}
}

// collectTailMessages reads messages from sub into a ring buffer
// of capacity count. Stops when no message arrives within 1 second.
func collectTailMessages(
	sub *nats.Subscription, count int,
) []simple.LogRecord {
	if sub == nil {
		panic("collectTailMessages: sub must not be nil")
	}
	if count <= 0 || count > tailCountMax {
		panic("collectTailMessages: count out of bounds")
	}

	const drainTimeout = time.Second
	buf := make([]simple.LogRecord, 0, count)

	for i := 0; i < tailCountMax; i++ {
		msg, err := sub.NextMsg(drainTimeout)
		if err != nil {
			break
		}
		var rec simple.LogRecord
		if err := json.Unmarshal(msg.Data, &rec); err != nil {
			continue
		}
		if len(buf) >= count {
			buf = buf[1:]
		}
		buf = append(buf, rec)
	}
	return buf
}

// colorLevel pads the level string to 7 characters and applies
// Gruvbox color based on severity. Uses shared color utilities.
func colorLevel(level string) string {
	if level == "" {
		panic("colorLevel: level must not be empty")
	}
	if len(level) > 20 {
		panic("colorLevel: level string unreasonably long")
	}
	padded := fmt.Sprintf("%-7s", level)
	switch level {
	case "ERROR":
		return ColorRed(padded)
	case "WARN":
		return ColorYellow(padded)
	case "INFO":
		return ColorGreen(padded)
	case "DEBUG":
		return ColorGray(padded)
	default:
		return padded
	}
}
