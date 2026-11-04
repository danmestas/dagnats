package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/api"
)

// replicaOnlyMessage answers every HTTP route a replica-only node does
// not serve, and is logged at startup.
const replicaOnlyMessage = "replica-only node: engine off"

// startReplicaOnly finishes startup for engine: false, once NATS,
// JetStream and telemetry are up. It starts only what a voting replica
// needs: the cluster report behind /health/cluster, and the program
// users, so a program dialing this node can still log in. No
// orchestrator, control plane (the api.* micro service), program
// registration, bridge, triggers, embedded workers or dagnats.yaml
// watcher: each consumes shared work, serves a queue group or writes
// shared declarations, and a request this node does not serve is
// answered by another node's queue-group member instead. Run then
// starts the leader rebalancer and disk monitor as on any node.
func (s *Server) startReplicaOnly() error {
	if !s.cfg.ReplicaOnly || s.nc == nil || s.ns == nil {
		panic("startReplicaOnly: a connected replica-only server is required")
	}
	slog.Info(replicaOnlyMessage)
	printStep(os.Stderr, replicaOnlyMessage)
	s.clusterReporter = api.NewClusterReporter(
		s.nc, s.cfg.NATSClusterRoutes, clusterMembershipFrom(s.ns),
	)
	s.clusterReporter.SetReplicaOnly()
	if s.cfg.Region != "" {
		s.clusterReporter.SetRegion(s.cfg.Region)
	}
	js, err := jetstream.New(s.nc)
	if err != nil {
		return s.abortReplicaOnly(fmt.Errorf("jetstream init: %w", err))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.startProgramUsers(ctx, js); err != nil {
		return s.abortReplicaOnly(err)
	}
	printStep(os.Stderr, "program users ready")
	return nil
}

func (s *Server) abortReplicaOnly(err error) error {
	if err == nil {
		panic("abortReplicaOnly: err must not be nil")
	}
	s.telShutdown(context.Background())
	s.nc.Close()
	s.ns.Shutdown()
	return err
}

// replicaOnlyMux serves health and readiness, and answers every other
// route 503 with replicaOnlyMessage: the REST API, console and bridge
// run on the engine nodes, and a half-working copy here would mislead.
func (s *Server) replicaOnlyMux() *http.ServeMux {
	if s.clusterReporter == nil {
		panic("replicaOnlyMux: cluster reporter not built")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.Handle("/health/cluster", api.NewClusterHealthHandler(s.clusterReporter))
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, replicaOnlyMessage+"; use an engine node", http.StatusServiceUnavailable)
	})
	return mux
}
