// cli/logs.go
// Tails the NATS telemetry log stream in real time. Subscribes to
// telemetry.logs subjects on the TELEMETRY JetStream stream and prints
// each LogRecord in a human-readable format with optional filtering.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
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

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream: %v\n", err)
		os.Exit(1)
	}

	subject := buildLogSubject(serviceFilter, levelFilter)

	if tailCount > 0 {
		runLogsTail(js, subject, tailCount)
		return
	}

	runLogsFollow(js, subject)
}

// runLogsFollow streams new log messages using an ordered consumer.
// Blocks until interrupted by SIGINT or SIGTERM.
func runLogsFollow(js jetstream.JetStream, subject string) {
	if js == nil {
		panic("runLogsFollow: js must not be nil")
	}
	if subject == "" {
		panic("runLogsFollow: subject must not be empty")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cons, err := js.OrderedConsumer(
		ctx, "TELEMETRY",
		jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{subject},
			DeliverPolicy:  jetstream.DeliverNewPolicy,
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe %s: %v\n",
			subject, err)
		os.Exit(1)
	}

	iter, err := cons.Messages()
	if err != nil {
		fmt.Fprintf(os.Stderr, "messages %s: %v\n",
			subject, err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	fmt.Fprintf(os.Stderr, "Tailing logs on %s ...\n", subject)

	go func() {
		<-sigCh
		iter.Stop()
	}()

	const maxMessages = 1000000
	for i := 0; i < maxMessages; i++ {
		msg, err := iter.Next()
		if err != nil {
			return
		}
		var rec LogRecord
		if err := json.Unmarshal(msg.Data(), &rec); err != nil {
			fmt.Fprintf(os.Stderr,
				"logs: unmarshal: %v\n", err)
			continue
		}
		fmt.Println(formatLogLine(rec))
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
// Format: HH:MM:SS SEVERITY SERVICE  body [key=val ...]
func formatLogLine(rec LogRecord) string {
	if rec.Body == "" {
		panic("formatLogLine: Body must not be empty")
	}
	if rec.Severity == "" {
		panic("formatLogLine: Severity must not be empty")
	}

	ts, err := time.Parse(time.RFC3339Nano, rec.Timestamp)
	if err != nil {
		ts = time.Now().UTC()
	}

	var b strings.Builder
	b.WriteString(ts.Format("15:04:05"))
	b.WriteByte(' ')
	b.WriteString(colorLevel(strings.ToUpper(rec.Severity)))
	b.WriteByte(' ')
	b.WriteString(rec.ServiceName)
	b.WriteString("  ")
	b.WriteString(rec.Body)

	fields := formatAttributes(rec.Attributes)
	if len(fields) > 0 {
		b.WriteString(" [")
		b.WriteString(strings.Join(fields, " "))
		b.WriteByte(']')
	}
	return b.String()
}

// formatAttributes sorts attribute key=value pairs alphabetically.
// Returns nil when the map is empty to avoid unnecessary allocation.
func formatAttributes(attrs map[string]string) []string {
	if attrs == nil {
		return nil
	}
	if len(attrs) == 0 {
		return nil
	}

	const maxAttrs = 1000
	if len(attrs) > maxAttrs {
		panic("formatAttributes: attributes exceeds max bound")
	}

	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, k+"="+attrs[k])
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
// and prints them. Uses an ordered consumer with DeliverAll, drains
// into a ring buffer, then exits without blocking for SIGINT.
func runLogsTail(
	js jetstream.JetStream, subject string, count int,
) {
	if js == nil {
		panic("runLogsTail: js must not be nil")
	}
	if count <= 0 || count > tailCountMax {
		panic("runLogsTail: count out of bounds")
	}

	ctx := context.Background()
	cons, err := js.OrderedConsumer(
		ctx, "TELEMETRY",
		jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{subject},
			DeliverPolicy:  jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe %s: %v\n",
			subject, err)
		os.Exit(1)
	}

	buf := collectTailMessages(cons, count)
	for _, rec := range buf {
		fmt.Println(formatLogLine(rec))
	}
}

// collectTailMessages reads messages from consumer into a ring
// buffer of capacity count. Stops when no message arrives within
// the fetch timeout.
func collectTailMessages(
	cons jetstream.Consumer, count int,
) []LogRecord {
	if cons == nil {
		panic("collectTailMessages: cons must not be nil")
	}
	if count <= 0 || count > tailCountMax {
		panic("collectTailMessages: count out of bounds")
	}

	buf := make([]LogRecord, 0, count)

	for i := 0; i < tailCountMax; i++ {
		msg, err := cons.Next(
			jetstream.FetchMaxWait(time.Second),
		)
		if err != nil {
			break
		}
		var rec LogRecord
		if err := json.Unmarshal(msg.Data(), &rec); err != nil {
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
