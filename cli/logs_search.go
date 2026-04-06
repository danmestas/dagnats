// cli/logs_search.go
// Replays historical logs from the NATS TELEMETRY stream with time-based
// filtering. Creates an ordered consumer with DeliverByStartTime and
// iterates log records up to a bounded limit and scan timeout.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// searchResultMax is the absolute upper bound on search results to
// prevent unbounded memory usage.
const searchResultMax = 1000

// searchScanTimeout is the maximum wall-clock time spent scanning
// the stream before giving up.
const searchScanTimeout = 30 * time.Second

// searchFlags holds parsed flags for the logs search command.
type searchFlags struct {
	level   string
	service string
	search  string
	since   time.Duration
	limit   int
	jsonOut bool
}

// runLogsSearchCmd replays historical logs matching the given filters.
// Bounded by both result limit and scan timeout.
func runLogsSearchCmd(args []string) {
	if args == nil {
		panic("runLogsSearchCmd: args must not be nil")
	}
	const maxArgs = 100
	if len(args) > maxArgs {
		panic("runLogsSearchCmd: args exceeds max bound")
	}

	flags := parseSearchFlags(args)

	_, nc := connectService()
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream: %v\n", err)
		os.Exit(1)
	}

	subject := buildLogSubject(flags.service, flags.level)
	startTime := time.Now().Add(-flags.since)

	records := fetchSearchResults(
		js, subject, startTime, flags.limit, flags.search,
	)
	printLogSearchResults(os.Stdout, records, flags.jsonOut)
}

// parseSearchFlags extracts search-specific flags from args.
// Defaults: since=30m, limit=100, json=false.
func parseSearchFlags(args []string) searchFlags {
	if args == nil {
		panic("parseSearchFlags: args must not be nil")
	}

	flags := searchFlags{
		since: 30 * time.Minute,
		limit: 100,
	}

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--level="):
			flags.level = strings.TrimPrefix(arg, "--level=")
		case strings.HasPrefix(arg, "--service="):
			flags.service = strings.TrimPrefix(
				arg, "--service=",
			)
		case strings.HasPrefix(arg, "--search="):
			flags.search = strings.TrimPrefix(
				arg, "--search=",
			)
		case strings.HasPrefix(arg, "--since="):
			flags.since = parseSinceDuration(
				strings.TrimPrefix(arg, "--since="),
			)
		case strings.HasPrefix(arg, "--limit="):
			flags.limit = parseSearchLimit(
				strings.TrimPrefix(arg, "--limit="),
			)
		case arg == "--json":
			flags.jsonOut = true
		}
	}
	return flags
}

// parseSinceDuration parses a duration string for the --since flag.
// Exits with an error for invalid values.
func parseSinceDuration(val string) time.Duration {
	if val == "" {
		panic("parseSinceDuration: val must not be empty")
	}

	d, err := time.ParseDuration(val)
	if err != nil || d <= 0 {
		fmt.Fprintf(os.Stderr,
			"Error: --since must be a positive duration "+
				"(e.g. 30m, 1h, 24h)\n")
		os.Exit(1)
	}
	return d
}

// parseSearchLimit parses and validates the --limit flag value.
// Exits with an error for invalid or out-of-range values.
func parseSearchLimit(val string) int {
	if val == "" {
		panic("parseSearchLimit: val must not be empty")
	}

	n, err := strconv.Atoi(val)
	if err != nil || n <= 0 {
		fmt.Fprintln(os.Stderr,
			"Error: --limit must be a positive integer")
		os.Exit(1)
	}
	if n > searchResultMax {
		fmt.Fprintf(os.Stderr,
			"Error: --limit exceeds maximum (%d)\n",
			searchResultMax)
		os.Exit(1)
	}
	return n
}

// fetchSearchResults creates an ordered consumer starting at
// startTime and collects up to limit matching records. Stops
// when the limit is reached, the scan times out, or the stream
// is exhausted.
func fetchSearchResults(
	js jetstream.JetStream,
	subject string,
	startTime time.Time,
	limit int,
	searchText string,
) []LogRecord {
	if js == nil {
		panic("fetchSearchResults: js must not be nil")
	}
	if limit <= 0 || limit > searchResultMax {
		panic("fetchSearchResults: limit out of bounds")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), searchScanTimeout,
	)
	defer cancel()

	cons, err := js.OrderedConsumer(
		ctx, "TELEMETRY",
		jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{subject},
			DeliverPolicy:  jetstream.DeliverByStartTimePolicy,
			OptStartTime:   &startTime,
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe %s: %v\n",
			subject, err)
		os.Exit(1)
	}

	return collectSearchRecords(cons, limit, searchText)
}

// collectSearchRecords iterates the consumer and collects records
// matching the optional text filter. Bounded by limit and fetch
// timeout.
func collectSearchRecords(
	cons jetstream.Consumer,
	limit int,
	searchText string,
) []LogRecord {
	if cons == nil {
		panic("collectSearchRecords: cons must not be nil")
	}
	if limit <= 0 || limit > searchResultMax {
		panic("collectSearchRecords: limit out of bounds")
	}

	results := make([]LogRecord, 0, limit)
	lowerSearch := strings.ToLower(searchText)

	for i := 0; i < searchResultMax; i++ {
		if len(results) >= limit {
			break
		}
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
		if matchesSearch(rec, lowerSearch) {
			results = append(results, rec)
		}
	}
	return results
}

// matchesSearch returns true when the record body contains the
// search text (case-insensitive). Returns true for empty search.
func matchesSearch(rec LogRecord, lowerSearch string) bool {
	if lowerSearch == "" {
		return true
	}
	return strings.Contains(
		strings.ToLower(rec.Body), lowerSearch,
	)
}

// printLogSearchResults outputs search results in either human or
// JSON format. Prints a count summary to stderr.
func printLogSearchResults(
	w io.Writer, records []LogRecord, jsonOut bool,
) {
	if w == nil {
		panic("printLogSearchResults: writer must not be nil")
	}
	if records == nil {
		panic("printLogSearchResults: records must not be nil")
	}

	if jsonOut {
		if err := FormatJSON(w, records); err != nil {
			fmt.Fprintf(os.Stderr, "json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	for _, rec := range records {
		fmt.Fprintln(w, formatLogLine(rec))
	}
	fmt.Fprintf(os.Stderr, "Found %d log(s)\n", len(records))
}
