package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nkeys"
	"log/slog"
	"os"
	"runtime/debug"
	"time"

	"github.com/danmestas/dagnats/internal/buildinfo"
	"github.com/danmestas/dagnats/internal/kvmirror"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/programs"
	"github.com/nats-io/nats.go/jetstream"
)

// startPrograms serves program registration (#885) and keeps this
// node's NATS users in step with the programs bucket. With no client
// login configured (open mode), anonymous NATS clients are root and a
// program may register without a join key; with one, anonymous is the
// join user and a join key is required.
//
// Every node watches the bucket and reloads its own server, because a
// NATS user exists only on the server that holds it; the registering
// node also reloads itself before replying, so the program's first dial
// to it succeeds. Another node catches up through its watch, and
// program.Run retries its first dial to cover that gap.
func (s *Server) startPrograms(js jetstream.JetStream) error {
	if js == nil || s.joinKeyStore == nil {
		panic("startPrograms: js and the join key store must be ready")
	}
	if s.cfg.MaxStoreBytes <= 0 {
		panic("startPrograms: MaxStoreBytes is resolved before startup")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.startProgramUsers(ctx, js); err != nil {
		return err
	}
	version := s.cfg.Build
	if version == "" {
		version = buildinfo.Version
	}
	registrar, err := programs.NewRegistrar(ctx, js, s.joinKeyStore, programs.Config{
		Open:          s.cfg.NATSUser == "",
		MaxStoreBytes: s.cfg.MaxStoreBytes,
		Replicas: natsutil.DeriveReplicas(
			s.cfg.NATSClusterRoutes, s.cfg.NATSJetStreamReplicas,
		),
		ServerVersion: version,
		Bind: func(record programs.Record) error {
			return s.programUsers.put(record.Manifest.Name, record, false)
		},
	})
	if err != nil {
		s.stopProgramUsers()
		return fmt.Errorf("programs registrar: %w", err)
	}
	s.programsService, err = programs.Serve(s.nc, registrar)
	if err != nil {
		s.stopProgramUsers()
		return fmt.Errorf("programs service: %w", err)
	}
	// Unconfirmed registrations expire and refund their join-key use.
	expiryCtx, stopExpiry := context.WithCancel(context.Background())
	s.programsExpiryStop = stopExpiry
	go registrar.RunExpiry(expiryCtx)
	printStep(os.Stderr, "program registration ready")
	return nil
}

// startProgramUsers loads every registered program into this node's
// NATS users, then watches the bucket for the rest of the server's life.
func (s *Server) startProgramUsers(ctx context.Context, js jetstream.JetStream) error {
	if s.ns == nil {
		panic("startProgramUsers: NATS server must be running")
	}
	kv, stream, err := kvmirror.Bind(ctx, js, programs.BucketName)
	if err != nil {
		return fmt.Errorf("programs bucket: %w", err)
	}
	s.programUsers = newUserSet(s.ns)
	if err := kvmirror.Load(ctx, kv, stream, s.applyProgramEntry); err != nil {
		return fmt.Errorf("load program users: %w", err)
	}
	watchCtx, cancel := context.WithCancel(context.Background())
	s.programUsersStop = cancel
	s.programUsersDone = make(chan struct{})
	go func() {
		defer close(s.programUsersDone)
		kvmirror.Watch(watchCtx, kv, nil, s.applyProgramEntry)
	}()
	return nil
}

// stopProgramUsers stops the watch and waits for it. Safe to call when
// it never started.
func (s *Server) stopProgramUsers() {
	if s.programUsersStop == nil {
		return
	}
	s.programUsersStop()
	<-s.programUsersDone
	s.programUsersStop = nil
}

// applyProgramEntry folds one programs-bucket entry into this node's
// NATS users. A failed reload is logged: the next change retries the
// whole set, and an unreadable record must not stop the watch.
func (s *Server) applyProgramEntry(entry jetstream.KeyValueEntry) {
	if entry == nil {
		panic("applyProgramEntry: entry must not be nil")
	}
	// The bucket is shared state; nothing in it may stop this node's user
	// set, or every node would crash-loop on the same record (#901).
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("programs: applying a record panicked; skipped it",
				"program", entry.Key(), "panic", recovered, "stack", string(debug.Stack()))
		}
	}()
	op := entry.Operation()
	deleted := op == jetstream.KeyValueDelete || op == jetstream.KeyValuePurge
	var record programs.Record
	if !deleted {
		if err := json.Unmarshal(entry.Value(), &record); err != nil {
			slog.Warn("programs: unreadable record", "program", entry.Key(), "error", err)
			return
		}
		if reason := malformedRecord(entry.Key(), record); reason != "" {
			slog.Warn("programs: malformed record skipped", "program", entry.Key(),
				"reason", reason)
			return
		}
	}
	if err := s.programUsers.put(entry.Key(), record, deleted); err != nil {
		slog.Error("programs: NATS users not reloaded", "program", entry.Key(), "error", err)
	}
}

// malformedRecord names what makes a programs record unusable as a NATS
// user, or "": a record must name a valid program, under its own key,
// with an NKey user public key.
func malformedRecord(key string, record programs.Record) string {
	switch {
	case program.ValidateName(record.Manifest.Name) != nil:
		return "no valid program name"
	case record.Manifest.Name != key:
		return "program name is not the record's key"
	case !nkeys.IsValidPublicUserKey(record.PublicKey):
		return "public key is not an NKey user key"
	case program.Validate(record.Manifest) != nil:
		return "manifest is invalid"
	}
	return ""
}
