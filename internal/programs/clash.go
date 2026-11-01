package programs

import (
	"strings"

	"github.com/danmestas/dagnats/program"
)

// checkProtocols refuses a manifest whose protocols reach into another
// program's subjects, in either direction. PermissionsFor grants a
// protocol root p publish and subscribe on p and p.>, so:
//
//   - a declared protocol equal to, or under, a registered program's
//     name would hand this program that program's subjects;
//   - a new name equal to, or a prefix of, a protocol another program
//     already declared would put this program's own subjects in that
//     program's grant.
//
// Protocols two programs share, and names that only share bytes
// (notify and notifyx), do not clash.
func checkProtocols(m program.Manifest, records []Record) error {
	if m.Name == "" {
		panic("programs.checkProtocols: name was validated")
	}
	for _, record := range records {
		owner := record.Manifest.Name
		if owner == m.Name {
			continue
		}
		for _, protocol := range m.Protocols {
			if underSubject(protocol, owner) {
				return refused(program.RefusedProtocolClash,
					"protocol %q is under program %q's subjects", protocol, owner)
			}
		}
		for _, protocol := range record.Manifest.Protocols {
			if underSubject(protocol, m.Name) {
				return refused(program.RefusedProtocolClash,
					"name %q is under protocol %q, which another program declared",
					m.Name, protocol)
			}
		}
	}
	return nil
}

// underSubject reports whether subject is root or a token path below
// it.
func underSubject(subject, root string) bool {
	if root == "" {
		panic("programs.underSubject: root must not be empty")
	}
	return subject == root || strings.HasPrefix(subject, root+".")
}
