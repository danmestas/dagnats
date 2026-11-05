package natsutil

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// PeerRetireOfflineMin is how long a retired peer must have been offline,
// continuously as one meta leader saw it, before that leader removes it.
// Long enough that a restart or a brief partition never costs a node its
// membership; short enough that a node switched off for good is gone,
// and its replicas re-placed, within minutes.
const PeerRetireOfflineMin = 2 * time.Minute

// peerRemoveSubject removes a server from the JetStream meta group. Only
// the system account may send it and only the meta leader answers.
const peerRemoveSubject = "$JS.API.SERVER.REMOVE"

// peerIDLength and peerIDDigits are nats-server's server hash (getHash in
// its events.go): the first 8 bytes of the name's SHA-256, each mapped
// onto 62 digits. The meta group names its peers by this hash.
const (
	peerIDLength = 8
	peerIDDigits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

// PeerID is the meta-group peer ID of the server named name.
func PeerID(name string) string {
	if name == "" {
		panic("PeerID: name must not be empty")
	}
	sum := sha256.Sum256([]byte(name))
	id := make([]byte, peerIDLength)
	for i := range id {
		id[i] = peerIDDigits[int(sum[i]%byte(len(peerIDDigits)))]
	}
	if len(id) != peerIDLength {
		panic("PeerID: hash has the wrong length")
	}
	return string(id)
}

// PeerRetirer removes servers that are leaving the cluster for good from
// the JetStream meta group, once each has been offline for the offline
// window. On removal the meta leader re-places that server's stream and
// consumer replicas onto the remaining servers. Without the removal a
// switched-off node stays a voter: every replica on it is lost
// redundancy, and the meta group's quorum still counts it. Safe for one
// goroutine; it acts only on the meta leader, so the cluster has one
// actor.
type PeerRetirer struct {
	system     *nats.Conn // in the system account, which alone may remove a peer
	peers      []string
	offlineMin time.Duration

	mu           sync.Mutex
	offlineSince map[string]time.Time // peer -> first seen offline, this leadership
	gone         map[string]bool      // peer -> already out of the meta group, logged
}

// NewPeerRetirer returns a retirer for the named servers. system must be
// a connection in the system account. offlineMin is the offline window,
// PeerRetireOfflineMin outside tests.
func NewPeerRetirer(system *nats.Conn, peers []string, offlineMin time.Duration) *PeerRetirer {
	if system == nil {
		panic("NewPeerRetirer: system must not be nil")
	}
	if offlineMin < 0 {
		panic("NewPeerRetirer: offlineMin must not be negative")
	}
	return &PeerRetirer{
		system: system, peers: slices.Clone(peers), offlineMin: offlineMin,
		offlineSince: map[string]time.Time{}, gone: map[string]bool{},
	}
}

// Run calls Tick on the rebalancer's schedule until ctx ends, reading
// meta each time.
func (r *PeerRetirer) Run(ctx context.Context, meta func() MetaState) {
	if ctx == nil || meta == nil {
		panic("PeerRetirer.Run: ctx and meta must be set")
	}
	runMetaTicks(ctx, meta, func(state MetaState) { r.Tick(ctx, state) })
}

// Tick sends at most one removal and returns the server it was sent
// for, or "". A removal's answer is logged: INFO when accepted, WARN
// when refused or unanswered; a refused one is retried a window later.
func (r *PeerRetirer) Tick(ctx context.Context, meta MetaState) string {
	if ctx == nil {
		panic("PeerRetirer.Tick: ctx must not be nil")
	}
	name, ok := r.plan(meta, time.Now())
	if !ok {
		return ""
	}
	id := PeerID(name)
	body, err := json.Marshal(map[string]string{"peer_id": id})
	if err != nil {
		panic("PeerRetirer.Tick: a peer ID always encodes")
	}
	if err := jsAPIRequest(ctx, r.system, peerRemoveSubject, body); err != nil {
		slog.Warn("natsutil: retired peer not removed from the meta group",
			"peer", name, "peer_id", id, "error", err)
		return name
	}
	slog.Info("natsutil: removed a retired peer from the JetStream meta group; "+
		"its replicas are re-placed on the remaining servers", "peer", name, "peer_id", id)
	return name
}

// plan picks the listed peer to remove at now, if any, and updates what
// this node has seen. Only the meta leader plans, and a spell as follower
// forgets what it saw, so the offline window is always one leader's
// continuous view.
func (r *PeerRetirer) plan(meta MetaState, now time.Time) (string, bool) {
	if r.offlineSince == nil || r.gone == nil {
		panic("PeerRetirer.plan: built without NewPeerRetirer")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !meta.Leader {
		clear(r.offlineSince)
		return "", false
	}
	chosen := ""
	for _, name := range r.peers {
		peer, present := findMetaPeer(meta.Peers, name)
		if !present {
			r.markGone(name)
			continue
		}
		delete(r.gone, name)
		if !peer.Offline {
			delete(r.offlineSince, name)
			continue
		}
		since, seen := r.offlineSince[name]
		if !seen {
			r.offlineSince[name] = now
			since = now
		}
		if chosen == "" && now.Sub(since) >= r.offlineMin {
			chosen = name
			// The removal takes a moment to leave the meta group; a full
			// window passes before it is sent again.
			r.offlineSince[name] = now
		}
	}
	return chosen, chosen != ""
}

// markGone records that name is out of the meta group, logging it the
// first time. Callers hold r.mu.
func (r *PeerRetirer) markGone(name string) {
	if name == "" {
		panic("PeerRetirer.markGone: name must not be empty")
	}
	delete(r.offlineSince, name)
	if r.gone[name] {
		return
	}
	r.gone[name] = true
	slog.Info("natsutil: retired peer is not in the JetStream meta group",
		"peer", name, "peer_id", PeerID(name))
}

// findMetaPeer finds name among peers by name or, for a server the
// leader has not heard from since its own restart, by peer ID.
func findMetaPeer(peers []MetaPeer, name string) (MetaPeer, bool) {
	if name == "" {
		panic("findMetaPeer: name must not be empty")
	}
	id := PeerID(name)
	for _, peer := range peers {
		if peer.Name == name || peer.ID == id {
			return peer, true
		}
	}
	return MetaPeer{}, false
}
