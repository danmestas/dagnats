package server

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/danmestas/dagnats/internal/natsutil"
)

// startLeaderRebalancer runs the preferred-leader rebalancer when
// preferred_leaders is set. Every node runs the loop, and each tick acts
// only while this node is the JetStream meta leader, so the cluster has
// one actor however leadership moves.
func (s *Server) startLeaderRebalancer() {
	if s.nc == nil || s.ns == nil {
		panic("startLeaderRebalancer: server not connected")
	}
	if len(s.cfg.PreferredLeaders) == 0 {
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	s.leaderRebalanceStop = stop
	rebalancer := natsutil.NewLeaderRebalancer(s.nc, s.cfg.PreferredLeaders)
	go rebalancer.Run(ctx, s.ns.JetStreamIsLeader)
	slog.Info("stream leader rebalancer on", "preferred_leaders", s.cfg.PreferredLeaders)
	printStep(os.Stderr, "preferring stream leaders on "+
		strings.Join(s.cfg.PreferredLeaders, ", "))
}
