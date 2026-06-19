package githubapp

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dagnatsext"
)

// Event is a normalized view of a GitHub webhook event. It strips the raw JSON
// down to the fields that dagnats-ci needs for routing, checkout, and status
// reporting. Keeping this struct narrow avoids coupling the rest of the add-on
// to GitHub's sprawling API surface.
type Event struct {
	Kind           string `json:"event"`
	Action         string `json:"action"`
	Owner          string `json:"owner"`
	Repo           string `json:"repo"`
	HeadSHA        string `json:"head_sha"`
	BaseRef        string `json:"base_ref"`
	PR             int    `json:"pr"`
	InstallationID int64  `json:"installation_id"`
	CloneURL       string `json:"clone_url"`
}

// pushPayload mirrors the GitHub push webhook JSON shape for the fields we use.
// Nested structs are unexported because the only public surface is Event.
type pushPayload struct {
	Ref   string `json:"ref"`
	After string `json:"after"`
	Repo  struct {
		Name     string `json:"name"`
		CloneURL string `json:"clone_url"`
		Owner    struct {
			Login string `json:"login"`
		} `json:"owner"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

// prPayload mirrors the GitHub pull_request webhook JSON for the fields we use.
type prPayload struct {
	Action      string `json:"action"`
	PullRequest struct {
		Number int `json:"number"`
		Head   struct {
			SHA string `json:"sha"`
		} `json:"head"`
		Base struct {
			Ref string `json:"ref"`
		} `json:"base"`
	} `json:"pull_request"`
	Repo struct {
		Name     string `json:"name"`
		CloneURL string `json:"clone_url"`
		Owner    struct {
			Login string `json:"login"`
		} `json:"owner"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

// ParseEvent decodes a GitHub webhook body into a normalized Event.
// githubEventType is the value of the X-GitHub-Event header.
// Returns an error for unsupported event types or malformed JSON.
func ParseEvent(githubEventType string, body []byte) (Event, error) {
	switch githubEventType {
	case "push":
		return parsePush(body)
	case "pull_request":
		return parsePullRequest(body)
	default:
		return Event{}, fmt.Errorf(
			"parse event: unsupported event type %q (supported: push, pull_request)",
			githubEventType,
		)
	}
}

// parsePush decodes a push webhook payload into a normalized Event.
func parsePush(body []byte) (Event, error) {
	var p pushPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return Event{}, fmt.Errorf("parse push event: %w", err)
	}
	return Event{
		Kind:           "push",
		Owner:          p.Repo.Owner.Login,
		Repo:           p.Repo.Name,
		HeadSHA:        p.After,
		BaseRef:        strings.TrimPrefix(p.Ref, "refs/heads/"),
		CloneURL:       p.Repo.CloneURL,
		InstallationID: p.Installation.ID,
	}, nil
}

// parsePullRequest decodes a pull_request webhook payload into a normalized Event.
func parsePullRequest(body []byte) (Event, error) {
	var p prPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return Event{}, fmt.Errorf("parse pull_request event: %w", err)
	}
	return Event{
		Kind:           "pull_request",
		Action:         p.Action,
		Owner:          p.Repo.Owner.Login,
		Repo:           p.Repo.Name,
		HeadSHA:        p.PullRequest.Head.SHA,
		BaseRef:        p.PullRequest.Base.Ref,
		PR:             p.PullRequest.Number,
		CloneURL:       p.Repo.CloneURL,
		InstallationID: p.Installation.ID,
	}, nil
}

// ToEnvelope converts a normalized Event into a dagnatsext.TriggerEnvelope
// suitable for starting a DagNats run. Owner and Repo must both be non-empty;
// an empty value would produce a malformed Source identifier that cannot be
// used for routing or reporting.
func ToEnvelope(e Event) (dagnatsext.TriggerEnvelope, error) {
	if e.Owner == "" || e.Repo == "" {
		return dagnatsext.TriggerEnvelope{}, fmt.Errorf(
			"to envelope: owner and repo must both be non-empty (got owner=%q repo=%q)",
			e.Owner, e.Repo,
		)
	}
	data, err := json.Marshal(e)
	if err != nil {
		// json.Marshal on a struct with only basic types should never fail;
		// the panic surfaces a regression if the Event type gains an unmarshalable field.
		panic(fmt.Sprintf("ToEnvelope: json.Marshal(Event) failed: %v", err))
	}
	return dagnatsext.TriggerEnvelope{
		Trigger:   "github",
		Source:    fmt.Sprintf("github:%s/%s", e.Owner, e.Repo),
		Timestamp: time.Now().UTC(),
		Data:      data,
	}, nil
}
