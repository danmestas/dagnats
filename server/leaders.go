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

// startLeaderRebalancer runs the preferred-leader rebalancer when
// preferred_leaders is set. Every node runs the loop, and each tick acts
// only while this node is the JetStream meta leader, so the cluster has
// one actor however leadership moves. The meta leader itself is moved
// through the system user, dialed in process; without that connection
// the rebalancer still moves streams.
func (s *Server) startLeaderRebalancer() {
	if s.nc == nil || s.ns == nil {
		panic("startLeaderRebalancer: server not connected")
	}
	if len(s.cfg.PreferredLeaders) == 0 {
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	system := s.dialSystem()
	s.leaderRebalanceStop = func() {
		stop()
		if system != nil {
			system.Close()
		}
	}
	rebalancer := natsutil.NewLeaderRebalancer(s.nc, system, s.cfg.PreferredLeaders)
	go rebalancer.Run(ctx, func() natsutil.MetaState { return metaStateOf(s.ns) })
	slog.Info("stream leader rebalancer on", "preferred_leaders", s.cfg.PreferredLeaders)
	printStep(os.Stderr, "preferring stream leaders on "+
		strings.Join(s.cfg.PreferredLeaders, ", "))
}

// dialSystem connects in process as systemUser, or returns nil with a
// warning: the rebalancer then leaves the meta leader alone.
func (s *Server) dialSystem() *nats.Conn {
	value, ok := liveOptions.Load(s.ns)
	if !ok {
		slog.Warn("meta leader rebalance off: server options unknown")
		return nil
	}
	system, err := nats.Connect("", nats.InProcessServer(s.ns),
		nats.UserInfo(systemUser, systemPassword(value.(*natsserver.Options))),
		nats.Name("dagnats leader rebalancer"))
	if err != nil {
		slog.Warn("meta leader rebalance off: system user did not connect", "error", err)
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
		return state
	}
	for _, peer := range info.Meta.Replicas {
		if peer != nil {
			state.Peers = append(state.Peers, natsutil.MetaPeer{
				Name: peer.Name, Current: peer.Current, Offline: peer.Offline,
			})
		}
	}
	return state
}
