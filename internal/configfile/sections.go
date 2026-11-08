package configfile

import (
	"reflect"
	"strings"
)

// Sections are the top-level keys of dagnats.yaml this package owns
// (workflows, triggers, policy), read from ConfigFile's yaml tags so
// they cannot drift from it. The server's flat key parser reads the same
// file and skips these sections instead of warning about their lines.
func Sections() []string {
	root := reflect.TypeOf(ConfigFile{})
	if root.Kind() != reflect.Struct {
		panic("configfile.Sections: ConfigFile must be a struct")
	}
	names := make([]string, 0, root.NumField())
	for i := range root.NumField() {
		name, _, _ := strings.Cut(root.Field(i).Tag.Get("yaml"), ",")
		if name == "" || name == "-" {
			panic("configfile.Sections: every ConfigFile field needs a yaml name")
		}
		names = append(names, name)
	}
	return names
}
