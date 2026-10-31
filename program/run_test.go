// run_test.go
// OptionsFromEnv reads a program's dagnats settings from its own
// prefixed env and fills the defaults. Run itself is tested end to end
// against the registration endpoint in internal/programs.
// Methodology: t.Setenv only, no NATS.
package program_test

import (
	"testing"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
)

func TestOptionsFromEnv(t *testing.T) {
	t.Setenv("NOTIFY_DAGNATS_URL", "nats://mini:4222")
	t.Setenv("NOTIFY_SEED", "/var/lib/notify/seed")
	t.Setenv("NOTIFY_JOIN_KEY", "djk_a_b")
	got := program.OptionsFromEnv("notify")
	want := program.Options{
		URL: "nats://mini:4222", SeedPath: "/var/lib/notify/seed", JoinKey: "djk_a_b",
	}
	if got != want {
		t.Fatalf("OptionsFromEnv = %+v, want %+v", got, want)
	}
	defaults := program.OptionsFromEnv("lode")
	if defaults.URL != nats.DefaultURL || defaults.SeedPath != "lode.seed" ||
		defaults.JoinKey != "" {
		t.Fatalf("defaults = %+v", defaults)
	}
}
