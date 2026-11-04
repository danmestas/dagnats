// program/discovers_test.go
// Tests for Manifest.Discovers (#931): micro discovery of named services.
// Methodology: three layers. Validate's rules on the names; the publish
// grants PermissionsFor derives, checked with NATS wildcard rules; and a
// real server where the program's NKey user, with exactly those
// permissions, asks a micro service named agents for its info and is
// refused another service's. Positive and negative space throughout.
package program_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go/micro"
)

func TestValidate_Discovers(t *testing.T) {
	m := validManifest()
	m.Discovers = []string{"agents", "Tools_v2", "a-b"}
	// Positive: micro service names pass.
	if err := program.Validate(m); err != nil {
		t.Fatalf("Validate(discovers) = %v, want nil", err)
	}
	tooMany := make([]string, 17)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("svc%d", i)
	}
	for _, c := range []struct {
		name      string
		discovers []string
		want      string
	}{
		{"wildcard", []string{"*"}, "micro service name"},
		{"full wildcard", []string{">"}, "micro service name"},
		{"dotted", []string{"agents.x"}, "micro service name"},
		{"empty", []string{""}, "micro service name"},
		{"too long", []string{strings.Repeat("a", 65)}, "micro service name"},
		{"duplicate", []string{"agents", "agents"}, "listed twice"},
		{"too many", tooMany, "discovers, max 16"},
	} {
		m.Discovers = c.discovers
		// Negative: each bad list is refused, saying why.
		if err := program.Validate(m); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Validate = %v, want an error containing %q", c.name, err, c.want)
		}
	}
}

func TestPermissions_DiscoversGrantsNamedDiscoveryOnly(t *testing.T) {
	m := validManifest()
	before := program.PermissionsFor(m)
	// Negative: without the entry, no discovery of another service.
	if allowedBy(before.Publish, "$SRV.INFO.agents") {
		t.Fatal("$SRV.INFO.agents granted without Discovers")
	}
	m.Discovers = []string{"agents"}
	perms := program.PermissionsFor(m)
	for _, subject := range []string{
		"$SRV.PING.agents", "$SRV.INFO.agents", "$SRV.STATS.agents",
		"$SRV.PING.agents.id1", "$SRV.INFO.agents.id1", "$SRV.STATS.agents.id1",
	} {
		// Positive: micro's named and per-instance control subjects.
		if !allowedBy(perms.Publish, subject) {
			t.Errorf("publish %q not granted", subject)
		}
	}
	for _, subject := range []string{
		"$SRV.INFO", "$SRV.PING", "$SRV.INFO.other", "$SRV.INFO.agents.id1.x",
	} {
		// Negative: not every service, not another one, nothing deeper.
		if allowedBy(perms.Publish, subject) {
			t.Errorf("publish %q granted, want denied", subject)
		}
	}
}

func TestPermissions_DiscoversEnforced(t *testing.T) {
	m := validManifest()
	m.Discovers = []string{"agents"}
	f := startFixture(t, m)
	service, err := micro.AddService(f.admin, micro.Config{
		Name: "agents", Version: "0.3.0", Description: "a discoverable agent",
	})
	if err != nil {
		t.Fatalf("AddService: %v", err)
	}
	t.Cleanup(func() { _ = service.Stop() })
	nc := f.dialProgram(t, m.Name)
	reply, err := nc.Request("$SRV.INFO.agents", nil, 2*time.Second)
	// Positive: the program discovers the declared service.
	if err != nil || !strings.Contains(string(reply.Data), `"name":"agents"`) {
		t.Fatalf("$SRV.INFO.agents = %v, %v; want the agents service info", reply, err)
	}
	if _, err := nc.Request("$SRV.INFO.other", nil, 500*time.Millisecond); err == nil {
		t.Fatal("$SRV.INFO.other answered, want refused")
	}
	// Negative: another service's discovery is a permissions violation.
	if !waitFor(func() bool { return f.violations.contains("$SRV.INFO.other") }) {
		t.Fatal("no violation logged for $SRV.INFO.other")
	}
}
