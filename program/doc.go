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
// A program never creates or reconfigures a stream or bucket: a stream
// config can capture, mirror or republish subjects outside the
// program's grant. The host creates the manifest's streams and buckets
// with Provision over its own connection, and the program gets reads,
// consumers, and key writes on them.
//
// Attach is the one path that calls Program.Start. It takes the Grant
// the caller obtained (from registration, the in-process host, or a
// literal in tests), builds a Host, starts the worker that serves the
// handlers Start registered, and returns a stop func.
//
// Permissions is a plain struct. Converting it to nats-server options is
// the server's job, so a program never links nats-server.
//
// Limits of user permissions inside one account, accepted for
// cooperating programs:
//
//   - A consumer's deliver subject is in the request body, which subject
//     permissions cannot see, and nats-server does not check it against
//     the creator's permissions. Any program can therefore push its own
//     stream contents, or a shared bucket's, to any subject with interest.
//     KV watches need push consumers, so this cannot be closed without
//     accounts or a server-side check.
//   - Consumer delete on shared buckets and next/ack on STICKY_TASKS are
//     gated only by consumer names, which are random NUIDs that a program
//     cannot list.
//   - Validate cannot see other programs, so a protocol such as "beta.x"
//     declared by program alpha would be granted under beta's prefix.
//     Registration must check protocol ownership both ways: a protocol
//     under a registered program's prefix, and a program registering a
//     name some protocol already sits under.
package program
