// cli/dlq.go
// Commands for managing dead-letter queue: list, replay.
// Dead letters are stored in NATS stream DEAD_LETTERS with subjects dead.>.
package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/nats-io/nats.go"
)

// runDLQCmd dispatches DLQ subcommands.
func runDLQCmd(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: dagnats dlq <list|replay>")
		return
	}
	switch args[0] {
	case "list":
		runDLQListCmd(args[1:])
	case "replay":
		runDLQReplayCmd(args[1:])
	default:
		fmt.Printf("unknown dlq subcommand: %s\n", args[0])
	}
}

// runDLQListCmd lists dead-letter messages from the DEAD_LETTERS stream.
func runDLQListCmd(args []string) {
	nc, js := connectNATS()
	defer nc.Close()

	// Create ephemeral consumer to fetch all messages
	sub, err := js.SubscribeSync("dead.>",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe to dead letters: %v\n", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	// Fetch up to 50 messages with 2s timeout
	messages := make([]*nats.Msg, 0, 50)
	deadline := time.Now().Add(2 * time.Second)
	for i := 0; i < 50; i++ {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			break
		}
		msg, err := sub.NextMsg(timeout)
		if err != nil {
			break
		}
		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		fmt.Println("No dead letters found.")
		return
	}

	// Print table header
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SEQ\tSUBJECT\tRUN_ID\tSTEP_ID\tTASK\tERROR\tTIMESTAMP")

	for _, msg := range messages {
		meta, _ := msg.Metadata()
		var payload map[string]interface{}
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			continue
		}

		runID := getStringField(payload, "run_id")
		stepID := getStringField(payload, "step_id")
		task := getStringField(payload, "task")
		errMsg := getStringField(payload, "error")

		timestamp := ""
		if meta != nil {
			timestamp = meta.Timestamp.Format("2006-01-02 15:04:05")
		}

		seq := uint64(0)
		if meta != nil {
			seq = meta.Sequence.Stream
		}

		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			seq, msg.Subject, runID, stepID, task, errMsg, timestamp)
	}

	w.Flush()
}

// runDLQReplayCmd replays a dead-letter message to its task queue.
func runDLQReplayCmd(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats dlq replay <sequence-number>")
		os.Exit(1)
	}

	seqNum, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid sequence number: %v\n", err)
		os.Exit(1)
	}
	if seqNum == 0 {
		panic("runDLQReplayCmd: sequence number must be > 0")
	}

	nc, js := connectNATS()
	defer nc.Close()

	// Fetch message by sequence number from DEAD_LETTERS stream
	msg, err := fetchMessageBySequence(js, "DEAD_LETTERS", seqNum)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch dead letter: %v\n", err)
		os.Exit(1)
	}

	// Parse payload to extract task name and run ID
	var payload map[string]interface{}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		fmt.Fprintf(os.Stderr, "parse dead letter payload: %v\n", err)
		os.Exit(1)
	}

	task := getStringField(payload, "task")
	runID := getStringField(payload, "run_id")

	if task == "" || runID == "" {
		fmt.Fprintln(os.Stderr,
			"error: dead letter missing task or run_id fields")
		os.Exit(1)
	}

	// Republish to task queue with new message ID for dedup
	subject := fmt.Sprintf("task.%s.%s", task, runID)
	msgID := fmt.Sprintf("replay-%d", time.Now().UnixNano())

	pubMsg := &nats.Msg{
		Subject: subject,
		Data:    msg.Data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	_, err = js.PublishMsg(pubMsg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "republish message: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Replayed dead letter %d to %s\n", seqNum, subject)
}

// connectNATS establishes connection to NATS and returns JetStream context.
func connectNATS() (*nats.Conn, nats.JetStreamContext) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to NATS: %v\n", err)
		os.Exit(1)
	}

	js, err := nc.JetStream()
	if err != nil {
		fmt.Fprintf(os.Stderr, "get JetStream context: %v\n", err)
		os.Exit(1)
	}

	return nc, js
}

// fetchMessageBySequence retrieves a message from a stream by sequence number.
func fetchMessageBySequence(
	js nats.JetStreamContext, stream string, seq uint64,
) (*nats.Msg, error) {
	if stream == "" {
		panic("fetchMessageBySequence: stream must not be empty")
	}
	if seq == 0 {
		panic("fetchMessageBySequence: seq must be > 0")
	}

	// Create ephemeral consumer starting at the sequence number
	sub, err := js.SubscribeSync("dead.>",
		nats.AckExplicit(),
		nats.StartSequence(seq))
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		return nil, fmt.Errorf("message not found at sequence %d", seq)
	}

	return msg, nil
}

// getStringField safely extracts a string field from a JSON map.
func getStringField(m map[string]interface{}, key string) string {
	if m == nil {
		panic("getStringField: map must not be nil")
	}
	if key == "" {
		panic("getStringField: key must not be empty")
	}

	val, ok := m[key]
	if !ok {
		return ""
	}
	str, ok := val.(string)
	if !ok {
		return ""
	}
	return str
}
