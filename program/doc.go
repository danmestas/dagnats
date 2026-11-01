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
// Programs are trusted: these permissions are guardrails against
// mistakes, not an isolation boundary. A program can still write
// arbitrary data into the account through JetStream reply reflection
// (see Known limitations), so run only trusted programs until
// per-program NATS accounts land (dagnats#909). The guardrails rest on
// two rules the package enforces:
//
//   - A program dials with nats.CustomInboxPrefix(InboxPrefix(name)), and
//     its permissions grant subscribe on "_INBOX.<name>.>" only. Every
//     pull fetch, KV watch and JetStream API reply lands on that inbox,
//     so a program does not read another's task bodies or replies. Attach
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
// A dagnats node starts serving before it has loaded its program users:
// they come from the programs bucket, which needs JetStream, which
// starts with the node. So a node that restarts refuses every program's
// key for a while (about 22 seconds on a 3-node mesh). Run's connection
// keeps retrying through that, at first dial and on reconnect, for up to
// Options.AuthRetryWindow (AuthRetryWindowDefault, two minutes), and
// closes only if the refusals outlast it.
//
// # Known limitations / threat model
//
// Programs on a dagnats are trusted: today they are all the operator's
// own, the same stance as the coarse-shared engine buckets. Within that
// model these gaps are accepted, and they are closed before untrusted
// (third-party) programs join a dagnats, the same trigger as making
// nats_auth: required the default.
//
//   - JetStream reply reflection. nats-server publishes a JetStream API
//     response, a direct get's message and a pull delivery to whatever
//     reply subject the request names, as the account, without checking
//     it against the requester's permissions. So a program can write
//     bytes it chose into any stream or bucket of the account: store them
//     in its own bucket, then direct-get them with a reply of
//     "$KV.<bucket>.<key>". Every node therefore derives a program's
//     permissions from its stored manifest and never from a stored
//     permission set. Closed by per-program NATS accounts, dagnats#909;
//     TestResidual_JetStreamReplyWritesForeignBuckets pins the gap and
//     fails once it is closed.
//   - Push-consumer delivery. A consumer's deliver subject travels in the
//     JetStream request body, which subject permissions never see, and
//     nats-server does not check it against the creating user's
//     permissions. So a program can deliver its own stream's messages,
//     or a shared bucket's, onto any subject that has interest,
//     including another program's task or service subjects. KV watches
//     need push consumers, so denying consumer creates is not an option.
//     User permissions inside one account cannot close this; one NATS
//     account per program, or a server-side check of the deliver
//     subject, will. Tracked as dagnats#862;
//     TestAttack_PushConsumerCannotDeliverIntoForeignSubjects is the proof,
//     skipped until then.
//   - Inbox forgery. Micro services reply by publishing to the caller's
//     inbox, so every program may publish on "_INBOX.>". Inbox names are
//     not secret: one process's inboxes share a prefix and are numbered
//     in sequence, so a single reply subject seen from program beta (on a
//     shared protocol, say) lets program alpha write forged replies into
//     beta's next inboxes. Reading stays confined to the program's own
//     "_INBOX.<name>.>". Tracked as dagnats#862 and closed by the same fix
//     as push-consumer delivery: accounts or a server-side check.
//   - Name-gated grants. Consumer delete on shared buckets and next, ack
//     and create on STICKY_TASKS are gated only by consumer names: random
//     NUIDs, and for sticky consumers 128 bits from crypto/rand, which a
//     program can neither list nor read from any bucket, reply or
//     advisory.
//   - Protocol ownership. Validate cannot see other programs, so a
//     protocol such as "beta.x" declared by program alpha would be
//     granted under beta's prefix. Registration must check ownership both
//     ways: a protocol under a registered program's prefix, and a program
//     registering a name some protocol already sits under.
package program
