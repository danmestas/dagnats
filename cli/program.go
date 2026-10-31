// cli/program.go
// `dagnats program join-key create|list|revoke`: manage the join keys
// programs register with (#885), through the admin-gated
// /v1/programs/join-keys REST routes. Reads DAGNATS_HTTP_ADDR for the
// server and DAGNATS_BRIDGE_TOKEN for the admin bearer.
package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/danmestas/dagnats/internal/joinkey"
)

const programUsage = `Usage: dagnats program join-key <command>
Commands:
  create --label L [--names GLOB] [--uses N] [--expires DURATION]
             mint a join key; the key is printed once, here only
  list [--json]
             list join keys (never their secrets)
  revoke <id>
             revoke a join key at once`

// adminRequestTimeout bounds one admin REST call.
const adminRequestTimeout = 10 * time.Second

// adminResponseBytesMax caps a response body read; a full listing of
// joinkey.KeysCountMax keys is well under it.
const adminResponseBytesMax = 4 << 20

func runProgramCmd(args []string) {
	if len(args) < 2 || args[0] != "join-key" || HasHelpFlag(args) {
		fmt.Println(programUsage)
		if len(args) == 0 || HasHelpFlag(args) {
			return
		}
		os.Exit(1)
	}
	client := newAdminClient()
	var err error
	switch args[1] {
	case "create":
		err = joinKeyCreate(os.Stdout, client, args[2:])
	case "list":
		err = joinKeyList(os.Stdout, client, args[2:])
	case "revoke":
		err = joinKeyRevoke(os.Stdout, client, args[2:])
	default:
		err = fmt.Errorf("unknown join-key subcommand: %s", args[1])
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "program join-key: %v\n", err)
		os.Exit(1)
	}
}

func joinKeyCreate(w io.Writer, client adminClient, args []string) error {
	fs := flag.NewFlagSet("join-key create", flag.ContinueOnError)
	label := fs.String("label", "", "what the key is for (required)")
	names := fs.String("names", "", "glob of program names it may register, e.g. artifact*")
	uses := fs.Int("uses", 0, "registrations it allows (0: unbounded)")
	expires := fs.String("expires", "", "lifetime, e.g. 24h or 7d (default: never)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *label == "" {
		return errors.New("--label is required")
	}
	body := map[string]any{"label": *label, "name_glob": *names, "uses_max": *uses}
	if *expires != "" {
		lifetime, err := parseExpiry(*expires)
		if err != nil {
			return err
		}
		body["expires_at"] = time.Now().UTC().Add(lifetime)
	}
	var created struct {
		joinkey.JoinKey
		Key string `json:"key"`
	}
	if err := client.do(http.MethodPost, "", body, &created); err != nil {
		return err
	}
	fmt.Fprintf(w, "Join key %s created.\n\n  %s\n\n", created.ID, created.Key)
	fmt.Fprintln(w, "Store it now: it is not shown again.")
	return nil
}

func joinKeyList(w io.Writer, client adminClient, args []string) error {
	var listed struct {
		JoinKeys []joinkey.JoinKey `json:"join_keys"`
	}
	if err := client.do(http.MethodGet, "", nil, &listed); err != nil {
		return err
	}
	if HasJSONFlag(args) {
		return FormatJSON(w, listed.JoinKeys)
	}
	if len(listed.JoinKeys) == 0 {
		fmt.Fprintln(w, "No join keys.")
		return nil
	}
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "ID\tLABEL\tNAMES\tUSES\tEXPIRES\tSTATUS")
	now := time.Now()
	for _, key := range listed.JoinKeys {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n", key.ID, key.Label,
			orDefault(key.NameGlob, "*"), usesText(key), expiresText(key),
			joinKeyStatus(key, now))
	}
	return table.Flush()
}

func joinKeyRevoke(w io.Writer, client adminClient, args []string) error {
	if len(args) != 1 || args[0] == "" {
		return errors.New("usage: dagnats program join-key revoke <id>")
	}
	if err := client.do(http.MethodDelete, "/"+args[0], nil, nil); err != nil {
		return err
	}
	fmt.Fprintf(w, "Join key %s revoked.\n", args[0])
	return nil
}

// parseExpiry reads a positive Go duration, or a whole number of days
// written "Nd", which time.ParseDuration does not accept.
func parseExpiry(text string) (time.Duration, error) {
	if days, ok := strings.CutSuffix(text, "d"); ok {
		count, err := strconv.Atoi(days)
		if err != nil || count <= 0 {
			return 0, fmt.Errorf("--expires %q: days must be a positive whole number", text)
		}
		return time.Duration(count) * 24 * time.Hour, nil
	}
	lifetime, err := time.ParseDuration(text)
	if err != nil || lifetime <= 0 {
		return 0, fmt.Errorf("--expires %q: want a positive duration like 24h or 7d", text)
	}
	return lifetime, nil
}

func usesText(key joinkey.JoinKey) string {
	if key.UsesMax == 0 {
		return "unbounded"
	}
	return fmt.Sprintf("%d/%d left", key.UsesLeft, key.UsesMax)
}

func expiresText(key joinkey.JoinKey) string {
	if key.ExpiresAt == nil {
		return "never"
	}
	return key.ExpiresAt.Format(time.RFC3339)
}

func joinKeyStatus(key joinkey.JoinKey, now time.Time) string {
	switch {
	case key.RevokedAt != nil:
		return "revoked"
	case key.ExpiresAt != nil && !now.Before(*key.ExpiresAt):
		return "expired"
	case key.UsesMax > 0 && key.UsesLeft <= 0:
		return "exhausted"
	}
	return "active"
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// adminClient calls the admin-gated /v1/programs/join-keys routes.
type adminClient struct {
	baseURL string
	token   string
}

func newAdminClient() adminClient {
	return adminClient{baseURL: httpBaseURL(), token: os.Getenv("DAGNATS_BRIDGE_TOKEN")}
}

// do sends body (JSON, when non-nil) to the join-keys route plus
// suffix and decodes a 2xx response into out (when non-nil). A non-2xx
// status becomes an error carrying the status and the server's text.
func (c adminClient) do(method, suffix string, body, out any) error {
	if c.baseURL == "" {
		panic("adminClient.do: baseURL must not be empty")
	}
	if method == "" {
		panic("adminClient.do: method must not be empty")
	}
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		payload = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, c.baseURL+"/v1/programs/join-keys"+suffix, payload)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := (&http.Client{Timeout: adminRequestTimeout}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, adminResponseBytesMax))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}
