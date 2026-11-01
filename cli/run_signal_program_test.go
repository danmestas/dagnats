// run_signal_program_test.go
// `dagnats run signal --program <name>` names the signal in that
// program's namespace, the only signals a program's step can wait on
// (#885): approved becomes notify.approved, an already prefixed name is
// left alone, and a bad program name is refused.
// Methodology: pure argument handling, no NATS.
package cli

import "testing"

func TestProgramSignalName(t *testing.T) {
	cases := []struct {
		args       []string
		wantName   string
		wantRest   []string
		wantErrors bool
	}{
		{[]string{"r1", "approved", "{}"}, "approved", []string{"r1", "approved", "{}"}, false},
		{[]string{"--program", "notify", "r1", "approved", "{}"}, "notify.approved",
			[]string{"r1", "approved", "{}"}, false},
		{[]string{"r1", "--program=notify", "notify.approved", "{}"}, "notify.approved",
			[]string{"r1", "notify.approved", "{}"}, false},
		{[]string{"--program", "Bad", "r1", "approved", "{}"}, "", nil, true},
		{[]string{"r1", "approved", "{}", "--program"}, "", nil, true},
	}
	for _, tc := range cases {
		program, rest, err := takeProgramFlag(tc.args)
		if tc.wantErrors {
			if err == nil {
				t.Errorf("%v: no error", tc.args)
			}
			continue
		}
		if err != nil || len(rest) != len(tc.wantRest) {
			t.Fatalf("%v: rest %v, err %v", tc.args, rest, err)
		}
		if got := programSignalName(program, rest[1]); got != tc.wantName {
			t.Errorf("%v: name %q, want %q", tc.args, got, tc.wantName)
		}
	}
}
