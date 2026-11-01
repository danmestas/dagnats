package program

import (
	"strings"

	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/danmestas/dagnats/protocol"
)

// SubjectPermission is one direction's allow and deny subject lists,
// with NATS wildcard semantics. A subject is permitted when an Allow
// entry matches it and no Deny entry does.
type SubjectPermission struct {
	Allow []string
	Deny  []string
}

// Permissions is a plain permission set for one program's NATS user.
// The server converts it to nats-server's own type, so a program never
// links nats-server.
//
// There is deliberately no response permission (allow_responses): the
// server grants it for any delivered message with a reply subject,
// whoever sent it, so a program could publish to itself with a forged
// reply and "respond" anywhere. Replies go out under Publish "_INBOX.>"
// instead. A program still reads only its own "_INBOX.<name>.>", but it
// can write into another's inboxes: see "Known limitations / threat
// model" in the package doc (#862).
type Permissions struct {
	Publish   SubjectPermission
	Subscribe SubjectPermission
}

// readOnlyBuckets are shared buckets the SDK only reads: triggers
// (WatchTriggers' catch-up). Writing one would make the trigger service
// fire another program's workflow.
var readOnlyBuckets = []string{"triggers"}

// scopedBuckets are shared buckets a program reads in full but writes
// only under its own "<name>." keys: trigger_types (RegisterTriggerType)
// and the worker, service and status directories, which a relayed
// worker keys as "<name>.<id>", so one program cannot replace or delete
// another's entries.
var scopedBuckets = []string{"trigger_types", "workers", "services", "worker_status"}

// engineEventSubjects are the shared subject roots a worker publishes on
// directly, each only under its program's own namespace
// "<root>.p.<name>.>": task output streaming and build logs. Readers
// accept the namespace of the program that owns the step, so a program
// cannot pass its output off as another's. Step events, journal records
// and the buckets above go through the engine relay (worker.
// WithEngineRelay). No engine stream is readable directly: the journal
// and history hold other programs' model outputs and tool results.
var engineEventSubjects = []string{"stream", "logs"}

// PermissionsFor derives the NATS permission set for a manifest: every
// crossing the worker SDK and a micro service make, the program's own
// subjects, streams and buckets, its protocols, and subscribe on
// "_INBOX.<name>.>" and nothing wider. Pure. Panics on a manifest that
// fails Validate: permissions for an invalid manifest are meaningless.
//
// Residual: next, ack and create on a STICKY_TASKS consumer are granted
// for any consumer name, so a program holding a sibling's name could pull,
// ack or reconfigure it. The names are unguessable and never disclosed
// (stickyPublish), and TestPermissions_StickyConsumerNamesNotDisclosed
// pins the disclosure surfaces.
func PermissionsFor(m Manifest) Permissions {
	if err := Validate(m); err != nil {
		panic("program.PermissionsFor: " + err.Error())
	}
	var pub, sub []string
	pub = append(pub, "$JS.API.INFO")
	pub = append(pub, taskQueuePublish(m.TaskTypes)...)
	pub = append(pub, stickyPublish(m.TaskTypes)...)
	for _, root := range engineEventSubjects {
		pub = append(pub, root+".p."+m.Name+".>")
	}
	pub = append(pub, "api.p."+m.Name+".>")
	for _, b := range readOnlyBuckets {
		pub = append(pub, readBucketPublish(b)...)
	}
	for _, b := range scopedBuckets {
		pub = append(pub, readBucketPublish(b)...)
		pub = append(pub, "$KV."+b+"."+m.Name+".>")
	}
	for _, s := range m.Streams {
		pub = append(pub, ownStreamPublish(s.Name)...)
	}
	for _, b := range m.Buckets {
		pub = append(pub, ownStreamPublish("KV_"+b.Name)...)
		pub = append(pub, "$KV."+b.Name+".>")
	}
	for _, b := range m.Reads {
		pub = append(pub, readBucketPublish(b)...)
	}
	own := append([]string{m.Name + ".>"}, protocolSubjects(m.Protocols)...)
	pub = append(pub, own...)
	pub = append(pub, "_INBOX.>")
	sub = append(sub, own...)
	sub = append(sub, microSubscribe(m.Name)...)
	sub = append(sub, protocol.CancelSubjectPrefix+"*", InboxPrefix(m.Name)+".>")
	if len(pub) == 0 || len(sub) == 0 {
		panic("program.PermissionsFor: empty permission list")
	}
	return Permissions{
		Publish:   SubjectPermission{Allow: pub},
		Subscribe: SubjectPermission{Allow: sub},
	}
}

// taskQueuePublish grants the worker SDK's TASK_QUEUES calls for each
// declared task type, by exact durable name and filter: create (the
// filtered form only; nats.go sends unfiltered and multi-filter creates
// without the filter token), info, delete, next and ack. Listing lets
// the SDK clean orphan ephemerals and detect cross-process collisions.
func taskQueuePublish(taskTypes []string) []string {
	out := []string{
		"$JS.API.STREAM.INFO.TASK_QUEUES",
		"$JS.API.CONSUMER.LIST.TASK_QUEUES",
	}
	for _, t := range taskTypes {
		durable := consumername.NameFor(t, "")
		filter := consumername.FilterFor(t, "")
		out = append(out,
			"$JS.API.CONSUMER.CREATE.TASK_QUEUES."+durable+"."+filter,
			"$JS.API.CONSUMER.INFO.TASK_QUEUES."+durable,
			"$JS.API.CONSUMER.DELETE.TASK_QUEUES."+durable,
			"$JS.API.CONSUMER.MSG.NEXT.TASK_QUEUES."+durable,
			"$JS.ACK.TASK_QUEUES."+durable+".>",
		)
	}
	if len(out) != 2+5*len(taskTypes) {
		panic("program.taskQueuePublish: entry count drifted")
	}
	return out
}

// stickyPublish grants the SDK's optional worker-affinity consumer on
// STICKY_TASKS. Its durable name is per Worker and made at run time, and
// a NATS wildcard matches only a whole token, so next, ack and the
// create's name token are wildcards: the name is the capability. It
// carries 128 bits from crypto/rand (worker.stickyConsumerConfig) and is
// never disclosed: no CONSUMER.LIST or NAMES on STICKY_TASKS, stream info
// carries only a consumer count, no bucket or reply holds one, and no
// "$JS.EVENT" advisory is subscribable. The create filter
// "sticky.<type>.<worker id>.>" is pinned to the declared types, so no
// program can make a consumer that reads another's sticky tasks.
func stickyPublish(taskTypes []string) []string {
	if len(taskTypes) == 0 {
		return nil
	}
	out := []string{
		"$JS.API.STREAM.INFO.STICKY_TASKS",
		"$JS.API.CONSUMER.MSG.NEXT.STICKY_TASKS.*",
		"$JS.ACK.STICKY_TASKS.>",
	}
	for _, t := range taskTypes {
		out = append(out, "$JS.API.CONSUMER.CREATE.STICKY_TASKS.*.sticky."+t+".*.>")
	}
	return out
}

// readBucketPublish grants a read-only view of another program's
// bucket: open, get, watch (consumer create, info, delete, flow
// control) and nothing under "$KV.<bucket>.>", so no put, delete or
// purge. Deliveries arrive on the program's own inbox.
//
// Residual risk, accepted for a trusted reader (package threat model):
// consumer delete is granted because nats.go's watcher Stop deletes its
// consumer, and a denial makes every Stop block ~5s and error. Watcher
// names are random, so the grant cannot be scoped to the reader's own:
// a reader can delete the owner's watch consumer, which nats.go's
// ordered consumer recreates (a resubscribe, not a broken watch).
// Consumer create cannot tell ephemeral from durable (durability is in
// the request body), so a reader could also pin a durable consumer on
// the owner's stream, bounded by the stream's consumer limit.
func readBucketPublish(bucket string) []string {
	if bucket == "" {
		panic("program.readBucketPublish: bucket must not be empty")
	}
	if strings.ContainsAny(bucket, ".*>") {
		panic("program.readBucketPublish: bucket must be one literal token")
	}
	stream := "KV_" + bucket
	return []string{
		"$JS.API.STREAM.INFO." + stream,
		"$JS.API.DIRECT.GET." + stream + ".>",
		"$JS.API.CONSUMER.CREATE." + stream + ".>",
		"$JS.API.CONSUMER.INFO." + stream + ".*",
		"$JS.API.CONSUMER.DELETE." + stream + ".*",
		"$JS.FC." + stream + ".>",
	}
}

// ownStreamPublish grants reads and consumers on one of the program's
// own streams (a bucket's is "KV_<bucket>"): info, message get, direct
// get, and consumer create, info, list, next, ack and delete. No stream
// create, update, purge or delete: a stream config can capture, mirror
// or republish foreign subjects, so the host provisions (Provision).
func ownStreamPublish(stream string) []string {
	if stream == "" {
		panic("program.ownStreamPublish: stream must not be empty")
	}
	out := []string{
		"$JS.API.STREAM.INFO." + stream,
		"$JS.API.STREAM.MSG.GET." + stream,
		"$JS.API.DIRECT.GET." + stream,
		"$JS.API.DIRECT.GET." + stream + ".>",
		"$JS.API.CONSUMER.CREATE." + stream + ".>",
		"$JS.API.CONSUMER.DURABLE.CREATE." + stream + ".*",
		"$JS.API.CONSUMER.INFO." + stream + ".*",
		"$JS.API.CONSUMER.DELETE." + stream + ".*",
		"$JS.API.CONSUMER.LIST." + stream,
		"$JS.API.CONSUMER.NAMES." + stream,
		"$JS.API.CONSUMER.MSG.NEXT." + stream + ".*",
		"$JS.ACK." + stream + ".>",
		"$JS.FC." + stream + ".>",
	}
	if strings.ContainsAny(stream, ".*>") {
		panic("program.ownStreamPublish: stream must be one literal token")
	}
	return out
}

func protocolSubjects(protocols []string) []string {
	out := make([]string, 0, 2*len(protocols))
	for _, p := range protocols {
		out = append(out, p, p+".>")
	}
	if len(out) != 2*len(protocols) {
		panic("program.protocolSubjects: entry count drifted")
	}
	return out
}

// microSubscribe grants the micro framework's discovery subjects for a
// service named after the program: "$SRV.<verb>", "$SRV.<verb>.<name>"
// and "$SRV.<verb>.<name>.<id>".
func microSubscribe(name string) []string {
	if name == "" {
		panic("program.microSubscribe: name must not be empty")
	}
	var out []string
	for _, verb := range []string{"PING", "INFO", "STATS"} {
		root := "$SRV." + verb
		out = append(out, root, root+"."+name, root+"."+name+".*")
	}
	if len(out) != 9 {
		panic("program.microSubscribe: entry count drifted")
	}
	return out
}
