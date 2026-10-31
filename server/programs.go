package server

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/danmestas/dagnats/internal/buildinfo"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/programs"
	"github.com/nats-io/nats.go/jetstream"
)

// startPrograms serves program registration (#885). The auth mode is
// the server's own: with no client login configured, anonymous NATS
// clients are root, so a program may register without a join key;
// with one, registration is refused until programs get NKey users.
func (s *Server) startPrograms(js jetstream.JetStream) error {
	if js == nil || s.joinKeyStore == nil {
		panic("startPrograms: js and the join key store must be ready")
	}
	if s.cfg.MaxStoreBytes <= 0 {
		panic("startPrograms: MaxStoreBytes is resolved before startup")
	}
	version := s.cfg.Build
	if version == "" {
		version = buildinfo.Version
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registrar, err := programs.NewRegistrar(ctx, js, s.joinKeyStore, programs.Config{
		Open:          s.cfg.NATSUser == "",
		MaxStoreBytes: s.cfg.MaxStoreBytes,
		Replicas: natsutil.DeriveReplicas(
			s.cfg.NATSClusterRoutes, s.cfg.NATSJetStreamReplicas,
		),
		ServerVersion: version,
	})
	if err != nil {
		return fmt.Errorf("programs registrar: %w", err)
	}
	s.programsService, err = programs.Serve(s.nc, registrar)
	if err != nil {
		return fmt.Errorf("programs service: %w", err)
	}
	printStep(os.Stderr, "program registration ready")
	return nil
}
