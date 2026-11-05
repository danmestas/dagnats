package server

import (
	"context"
	"log/slog"
	"os"
	"strings"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/danmestas/dagnats/internal/natsutil"
)

// startMetaReconcilers runs the preferred-leader rebalancer when
// preferred_leaders is set and the peer retirer when retire_peers is
// set. Every node runs them, and each tick acts only while this node is
// the JetStream meta leader, so the cluster has one actor however
// leadership moves. Both share one connection as the system user, dialed
// in process, which alone may move the meta leader or remove a peer;
// without it the rebalancer still moves streams and nothing is retired.
func (s *Server) startMetaReconcilers() {
	if s.nc == nil || s.ns == nil {
		panic("startMetaReconcilers: server not connected")
	}
	if len(s.cfg.PreferredLeaders) == 0 && len(s.cfg.RetirePeers) == 0 {
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	system := s.dialSystem()
	s.metaReconcileStop = func() {
		stop()
		if system != nil {
			system.Close()
		}
	}
	meta := func() natsutil.MetaState { return metaStateOf(s.ns) }
	if len(s.cfg.PreferredLeaders) > 0 {
		rebalancer := natsutil.NewLeaderRebalancer(s.nc, system, s.cfg.PreferredLeaders)
		go rebalancer.Run(ctx, meta)
		slog.Info("stream leader rebalancer on", "preferred_leaders", s.cfg.PreferredLeaders)
		printStep(os.Stderr, "preferring stream leaders on "+
			strings.Join(s.cfg.PreferredLeaders, ", "))
	}
	s.startPeerRetirer(ctx, system, meta)
}

// startPeerRetirer runs the peer retirer when retire_peers is set and the
// system user connected.
func (s *Server) startPeerRetirer(
	ctx context.Context, system *nats.Conn, meta func() natsutil.MetaState,
) {
	if ctx == nil || meta == nil {
		panic("startPeerRetirer: ctx and meta must be set")
	}
	if len(s.cfg.RetirePeers) == 0 {
		return
	}
	if system == nil {
		slog.Warn("retire_peers off: no system user connection",
			"retire_peers", s.cfg.RetirePeers)
		return
	}
	retirer := natsutil.NewPeerRetirer(system, s.cfg.RetirePeers, natsutil.PeerRetireOfflineMin)
	go retirer.Run(ctx, meta)
	slog.Info("peer retirer on", "retire_peers", s.cfg.RetirePeers,
		"offline_min", natsutil.PeerRetireOfflineMin)
	printStep(os.Stderr, "retiring peers "+strings.Join(s.cfg.RetirePeers, ", "))
}

// dialSystem connects in process as systemUser, or returns nil with a
// warning: the meta leader is then neither moved nor asked to retire a
// peer.
func (s *Server) dialSystem() *nats.Conn {
	value, ok := liveOptions.Load(s.ns)
	if !ok {
		slog.Warn("system user off: server options unknown")
		return nil
	}
	system, err := nats.Connect("", nats.InProcessServer(s.ns),
		nats.UserInfo(systemUser, systemPassword(value.(*natsserver.Options))),
		nats.Name("dagnats meta reconcilers"))
	if err != nil {
		slog.Warn("system user did not connect: meta leader moves and peer retirement off",
			"error", err)
		return nil
	}
	return system
}

// metaStateOf is ns's view of the JetStream meta group: whether it leads
// and, when it does, the other members as it sees them. A follower's
// view of the others is not reliable, and a follower does not act.
func metaStateOf(ns *natsserver.Server) natsutil.MetaState {
	if ns == nil {
		panic("metaStateOf: ns must not be nil")
	}
	if !ns.JetStreamIsLeader() {
		return natsutil.MetaState{}
	}
	state := natsutil.MetaState{Leader: true, Self: ns.Name()}
	info, err := ns.Jsz(&natsserver.JSzOptions{})
	if err != nil || info == nil || info.Meta == nil {
		state.PeersUnread = true
		return state
	}
	for _, peer := range info.Meta.Replicas {
		if peer != nil {
			state.Peers = append(state.Peers, natsutil.MetaPeer{
				Name: peer.Name, ID: peer.Peer, Current: peer.Current, Offline: peer.Offline,
			})
		}
	}
	return state
}
