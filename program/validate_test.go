// program/validate_test.go
// Tests for Validate, the pure manifest check registration, Use and a
// program's own tests share.
// Methodology: table-driven. One valid manifest is the baseline; each
// row breaks exactly one rule and names the text the error must carry,
// so a rule that silently stops firing fails its row.
package program_test

import (
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/program"
)

func validManifest() program.Manifest {
	return program.Manifest{
		Name:      "toy",
		Version:   "1.0.0",
		Dagnats:   ">=0.0.35 <0.1.0",
		TaskTypes: []string{"toy.echo", "toy.send.email", "toy.wait"},
		Workflows: []string{"toy.echo"},
		Streams: []program.StreamSpec{{
			Name: "TOY_EVENTS", Subjects: []string{"toy.events.>"},
			MaxBytesFraction: 0.1, MaxBytesFloor: 1 << 20, MaxAge: time.Hour,
		}},
		Buckets: []program.BucketSpec{{
			Name: "toy_state", History: 1, MaxBytes: 1 << 20,
		}},
		Protocols: []string{"toyproto.ping"},
		Console:   &program.Link{Label: "Toy", URL: "http://127.0.0.1:9000/"},
	}
}

func TestValidate_BridgeTokenNeedsATaskTypeToScopeTo(t *testing.T) {
	m := validManifest()
	m.BridgeToken = true
	if err := program.Validate(m); err != nil {
		t.Fatalf("Validate(bridge token with task types) = %v, want nil", err)
	}
	m.TaskTypes = nil
	err := program.Validate(m)
	if err == nil || !strings.Contains(err.Error(), "bridge token needs a task type") {
		t.Fatalf("Validate(bridge token, no task types) = %v, want the bridge token error", err)
	}
}

func TestValidate_AcceptsValidManifest(t *testing.T) {
	m := validManifest()
	// Positive: the baseline passes.
	if err := program.Validate(m); err != nil {
		t.Fatalf("Validate(valid) = %v, want nil", err)
	}
	// Negative: an empty manifest does not.
	if err := program.Validate(program.Manifest{}); err == nil {
		t.Fatal("Validate(empty) = nil, want error")
	}
}

func TestValidate_RejectsUnprefixedAndProtocolUnderPrefix(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*program.Manifest)
		want   string
	}{
		{"name uppercase", func(m *program.Manifest) { m.Name = "Toy" }, "name"},
		{"name too short", func(m *program.Manifest) { m.Name = "t" }, "name"},
		{"name underscore", func(m *program.Manifest) { m.Name = "toy_x" }, "name"},
		{"name engine subject root", func(m *program.Manifest) { m.Name = "task" }, "reserved"},
		{"name engine stream prefix", func(m *program.Manifest) { m.Name = "workflow" }, "reserved"},
		{"name engine bucket", func(m *program.Manifest) { m.Name = "workers" }, "reserved"},
		{"name engine bucket prefix", func(m *program.Manifest) { m.Name = "trigger" }, "reserved"},
		{"task unprefixed", func(m *program.Manifest) { m.TaskTypes = []string{"echo"} }, "toy."},
		{"task foreign prefix", func(m *program.Manifest) {
			m.TaskTypes = []string{"notify.send"}
		}, "toy."},
		{"task bare prefix", func(m *program.Manifest) { m.TaskTypes = []string{"toy."} }, "task type"},
		{"task wildcard", func(m *program.Manifest) { m.TaskTypes = []string{"toy.*"} }, "task type"},
		{"task duplicate durable", func(m *program.Manifest) {
			m.TaskTypes = []string{"toy.a-b", "toy.a.b"}
		}, "consumer"},
		{"workflow unprefixed", func(m *program.Manifest) { m.Workflows = []string{"echo"} }, "toy."},
		{"stream unprefixed", func(m *program.Manifest) { m.Streams[0].Name = "EVENTS" }, "TOY_"},
		{"stream lowercase", func(m *program.Manifest) { m.Streams[0].Name = "TOY_events" }, "TOY_"},
		{"stream subject foreign", func(m *program.Manifest) {
			m.Streams[0].Subjects = []string{"history.>"}
		}, "subject"},
		{"stream subject capture inbox", func(m *program.Manifest) {
			m.Streams[0].Subjects = []string{"_INBOX.beta.>"}
		}, "subject"},
		{"stream no subjects", func(m *program.Manifest) { m.Streams[0].Subjects = nil }, "subject"},
		{"stream no size", func(m *program.Manifest) {
			m.Streams[0].MaxBytesFraction, m.Streams[0].MaxBytesFloor = 0, 0
		}, "size"},
		{"stream fraction above one", func(m *program.Manifest) {
			m.Streams[0].MaxBytesFraction = 1.5
		}, "fraction"},
		{"bucket unprefixed", func(m *program.Manifest) { m.Buckets[0].Name = "state" }, "toy_"},
		{"bucket no size", func(m *program.Manifest) { m.Buckets[0].MaxBytes = 0 }, "max_bytes"},
		{"read own prefix", func(m *program.Manifest) {
			m.Reads = []string{"toy_state"}
		}, "own prefix"},
		{"read engine bucket", func(m *program.Manifest) {
			m.Reads = []string{"workflow_runs"}
		}, "engine bucket"},
		{"read engine bucket no underscore", func(m *program.Manifest) {
			m.Reads = []string{"workers"}
		}, "engine bucket"},
		{"read no owner prefix", func(m *program.Manifest) { m.Reads = []string{"registry"} }, "read"},
		{"read empty rest", func(m *program.Manifest) { m.Reads = []string{"tools_"} }, "read"},
		{"read wildcard", func(m *program.Manifest) { m.Reads = []string{"tools_>"} }, "read"},
		{"read uppercase", func(m *program.Manifest) { m.Reads = []string{"tools_Reg"} }, "read"},
		{"read reserved owner", func(m *program.Manifest) {
			m.Reads = []string{"history_x"}
		}, "read"},
		{"read duplicate", func(m *program.Manifest) {
			m.Reads = []string{"tools_registry", "tools_registry"}
		}, "twice"},
		{"read too many", func(m *program.Manifest) {
			for i := 0; i < 33; i++ {
				m.Reads = append(m.Reads, "tools_b"+strings.Repeat("x", i))
			}
		}, "reads"},
		{"domain read empty domain", func(m *program.Manifest) {
			m.Reads = []string{":tools_registry"}
		}, "domain"},
		{"domain read wildcard domain", func(m *program.Manifest) {
			m.Reads = []string{"*:tools_registry"}
		}, "domain"},
		{"domain read dotted domain", func(m *program.Manifest) {
			m.Reads = []string{"e.u:tools_registry"}
		}, "domain"},
		{"domain read long domain", func(m *program.Manifest) {
			m.Reads = []string{strings.Repeat("d", 65) + ":tools_registry"}
		}, "domain"},
		{"domain read engine bucket", func(m *program.Manifest) {
			m.Reads = []string{"eu:workflow_runs"}
		}, "engine bucket"},
		{"domain read no owner prefix", func(m *program.Manifest) {
			m.Reads = []string{"eu:registry"}
		}, "read"},
		{"domain read second colon", func(m *program.Manifest) {
			m.Reads = []string{"eu:us:tools_registry"}
		}, "read"},
		{"domain read duplicate", func(m *program.Manifest) {
			m.Reads = []string{"eu:tools_registry", "eu:tools_registry"}
		}, "twice"},
		{"protocol under own prefix", func(m *program.Manifest) {
			m.Protocols = []string{"toy.tool"}
		}, "prefix"},
		{"protocol engine root", func(m *program.Manifest) {
			m.Protocols = []string{"history.tap"}
		}, "reserved"},
		{"protocol system subject", func(m *program.Manifest) {
			m.Protocols = []string{"$SRV.toy"}
		}, "protocol"},
		{"protocol wildcard", func(m *program.Manifest) {
			m.Protocols = []string{"tool.>"}
		}, "protocol"},
		{"console without url", func(m *program.Manifest) { m.Console.URL = "" }, "console"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := validManifest()
			tc.mutate(&m)
			err := program.Validate(m)
			// Negative: the broken rule is refused.
			if err == nil {
				t.Fatalf("Validate = nil, want error mentioning %q", tc.want)
			}
			// Positive: the error names what was wrong.
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestValidate_AcceptsReadsOfOtherProgramsBuckets(t *testing.T) {
	m := validManifest()
	m.Reads = []string{"tools_registry", "tools_roles", "other2_a_b"}
	// Positive: well-formed foreign bucket names pass.
	if err := program.Validate(m); err != nil {
		t.Fatalf("Validate(reads) = %v, want nil", err)
	}
	// Negative: one bad entry among good ones still fails.
	m.Reads = append(m.Reads, "toy_state")
	if err := program.Validate(m); err == nil {
		t.Fatal("Validate(reads with own bucket) = nil, want error")
	}
}

// TestValidate_AcceptsDomainQualifiedReads: a read may name the JetStream
// domain its bucket lives in (#949). Bare and qualified entries for one
// bucket are distinct grants, and the program's own prefix is allowed in
// another domain, where its local full access does not reach.
func TestValidate_AcceptsDomainQualifiedReads(t *testing.T) {
	m := validManifest()
	m.Reads = []string{"eu:chat_cards", "chat_cards", "us-west_2:tools_registry", "eu:toy_state"}
	// Positive: domain-qualified foreign buckets pass.
	if err := program.Validate(m); err != nil {
		t.Fatalf("Validate(domain reads) = %v, want nil", err)
	}
	// Negative: the program's own bucket unqualified is still refused.
	m.Reads = append(m.Reads, "toy_state")
	if err := program.Validate(m); err == nil {
		t.Fatal("Validate(reads with own bucket) = nil, want error")
	}
}

func TestSplitRead(t *testing.T) {
	cases := []struct{ read, domain, bucket string }{
		{"chat_cards", "", "chat_cards"},
		{"eu:chat_cards", "eu", "chat_cards"},
	}
	for _, tc := range cases {
		domain, bucket := program.SplitRead(tc.read)
		// Positive and negative: domain is "" exactly for a bare read.
		if domain != tc.domain || bucket != tc.bucket {
			t.Errorf("SplitRead(%q) = %q, %q; want %q, %q",
				tc.read, domain, bucket, tc.domain, tc.bucket)
		}
	}
}
