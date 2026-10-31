// Package program is the contract between dagnats and a program: a
// separately released binary that runs tasks, workflows and services on
// a dagnats it shares with other programs.
//
// A program declares a Manifest. Its Name is the prefix root of
// everything it owns: task types and workflows "name.*", subjects
// "name.>", streams "NAME_*", buckets "name_*". Validate checks a
// manifest without touching NATS; PermissionsFor derives the NATS
// permission set dagnats binds to the program's NKey user, which is
// exactly the crossings the worker SDK and a micro service make plus the
// program's own resources and nothing wider.
//
// Isolation rests on two rules the package enforces:
//
//   - A program dials with nats.CustomInboxPrefix(InboxPrefix(name)), and
//     its permissions grant subscribe on "_INBOX.<name>.>" only. Every
//     pull fetch, KV watch and JetStream API reply lands on that inbox,
//     so no program can read another's task bodies or replies. Attach
//     panics on a connection dialed without the prefix.
//   - The program generates its own NKey (LoadOrCreateKey). The seed is
//     written once, mode 0600, and never leaves the process; only the
//     public key is registered.
//
// Attach is the one path that calls Program.Start. It takes the Grant
// the caller obtained (from registration, the in-process host, or a
// literal in tests), builds a Host, starts the worker that serves the
// handlers Start registered, and returns a stop func.
//
// Permissions is a plain struct. Converting it to nats-server options is
// the server's job, so a program never links nats-server.
package program
