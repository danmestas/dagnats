package program

import (
	"strings"

	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/danmestas/dagnats/internal/natsutil"
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

// sharedBuckets are the engine buckets every program reads and writes
// through the worker SDK. Coarse-shared in v1: a program can read a
// sibling's keys (design §2.3, accepted).
var sharedBuckets = []string{
	"workers", "services", "triggers", "trigger_types", "checkpoints",
	"signals", "workflow_runs", "worker_status", natsutil.SubmissionsBucket,
	natsutil.LeasesBucket,
}

// engineEventSubjects are the shared subjects a worker reports on: step
// events, task output streaming, build logs and the agent journal.
var engineEventSubjects = []string{"history.>", "stream.>", "logs.>", "journal.>"}

// engineReadStreams are the shared streams the SDK reads a tail from:
// a journal step claims by reading its journal's last record and the
// run's last history event (journal/journal.go, journal/check.go).
var engineReadStreams = []string{natsutil.AgentJournalStream, "WORKFLOW_HISTORY"}

// PermissionsFor derives the NATS permission set for a manifest: every
// crossing the worker SDK and a micro service make, the program's own
// subjects, streams and buckets, its protocols, and subscribe on
// "_INBOX.<name>.>" and nothing wider. Pure. Panics on a manifest that
// fails Validate: permissions for an invalid manifest are meaningless.
func PermissionsFor(m Manifest) Permissions {
	if err := Validate(m); err != nil {
		panic("program.PermissionsFor: " + err.Error())
	}
	var pub, sub []string
	pub = append(pub, "$JS.API.INFO")
	pub = append(pub, taskQueuePublish(m.TaskTypes)...)
	pub = append(pub, stickyPublish(m.TaskTypes)...)
	pub = append(pub, engineEventSubjects...)
	for _, s := range engineReadStreams {
		pub = append(pub, "$JS.API.STREAM.INFO."+s, "$JS.API.STREAM.MSG.GET."+s)
	}
	pub = append(pub, "api.p."+m.Name+".>")
	for _, b := range sharedBuckets {
		pub = append(pub, sharedBucketPublish(b)...)
	}
	for _, s := range m.Streams {
		pub = append(pub, ownStreamPublish(s.Name)...)
	}
	for _, b := range m.Buckets {
		pub = append(pub, ownStreamPublish("KV_"+b.Name)...)
		pub = append(pub, "$KV."+b.Name+".>")
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
// STICKY_TASKS. It is ephemeral with a generated (NUID) name, so the
// name token of next and ack is a wildcard: a program that learned a
// sibling's sticky consumer name could pull from it. Names are not
// listable (no CONSUMER.LIST or NAMES on STICKY_TASKS), which is the
// whole gate. The create filter "sticky.<type>.<worker id>.>" is pinned
// to the declared type.
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

// sharedBucketPublish grants open, get, watch and put on one engine
// bucket, but not create, update, purge or delete of the bucket.
// Consumer delete is granted because nats.go deletes a KV watcher's
// ephemeral consumer on Stop; denying it would make every Stop wait out
// the JetStream timeout. The exposure is gated by the consumer name: a
// generated NUID, which a program cannot list (no CONSUMER.LIST, NAMES
// or INFO on these streams), so it can delete only watchers it created.
func sharedBucketPublish(bucket string) []string {
	if bucket == "" {
		panic("program.sharedBucketPublish: bucket must not be empty")
	}
	stream := "KV_" + bucket
	out := []string{
		"$JS.API.STREAM.INFO." + stream,
		"$JS.API.DIRECT.GET." + stream + ".>",
		"$JS.API.CONSUMER.CREATE." + stream + ".>",
		"$JS.API.CONSUMER.DELETE." + stream + ".*",
		"$JS.FC." + stream + ".>",
		"$KV." + bucket + ".>",
	}
	if strings.ContainsAny(bucket, ".*>") {
		panic("program.sharedBucketPublish: bucket must be one literal token")
	}
	return out
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
