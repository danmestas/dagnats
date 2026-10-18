// internal/durationjson/durationjson.go
// Duration fields decode from either integer nanoseconds (what
// encoding/json writes for time.Duration, so every stored definition)
// or a Go duration string such as "30s" (what people write in workflow
// JSON). Marshalling is untouched, so stored bytes keep their shape.
//
// Ousterhout note: one function owns the rewrite; each type with a
// duration field gets a three-line UnmarshalJSON that names nothing but
// itself. It lives here, not in dag, so trigger types share it without
// widening dag's public API. The field list comes from reflection, so
// adding a duration field to a struct that already calls Unmarshal
// needs no other change.
package durationjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

var durationType = reflect.TypeOf(time.Duration(0))

// durationKeysByType caches durationKeys per struct type; decoding sits
// on the engine's hot path (definitions are re-read from KV per event).
var durationKeysByType sync.Map

// Unmarshal decodes data into target, a pointer to a struct
// type without its own UnmarshalJSON (callers pass a local alias type
// to avoid recursing), after rewriting any string value of one of the
// struct's time.Duration fields to integer nanoseconds.
func Unmarshal(data []byte, target any) error {
	if target == nil {
		panic("durationjson.Unmarshal: target must not be nil")
	}
	value := reflect.ValueOf(target)
	if value.Kind() != reflect.Pointer || value.Elem().Kind() != reflect.Struct {
		panic("durationjson.Unmarshal: target must point to a struct")
	}
	keys := durationKeys(value.Elem().Type())
	if hasQuotedValue(data, keys) {
		rewritten, err := rewriteDurationStrings(data, keys)
		if err != nil {
			return err
		}
		data = rewritten
	}
	return json.Unmarshal(data, target)
}

// rewriteDurationStrings replaces top-level string values of keys with
// their nanosecond counts. Matching is case-insensitive, like
// encoding/json's own field matching.
func rewriteDurationStrings(data []byte, keys map[string]struct{}) ([]byte, error) {
	if len(keys) == 0 {
		panic("rewriteDurationStrings: keys must not be empty")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		// Not an object: let the struct decode report it.
		return data, nil
	}
	changed := false
	for name, raw := range fields {
		if _, ok := keys[strings.ToLower(name)]; !ok {
			continue
		}
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 || raw[0] != '"' {
			continue
		}
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		duration, err := time.ParseDuration(text)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid duration %q: %w", name, text, err)
		}
		fields[name] = json.RawMessage(strconv.FormatInt(int64(duration), 10))
		changed = true
	}
	if !changed {
		return data, nil
	}
	return json.Marshal(fields)
}

// hasQuotedValue is a cheap pre-check: true when some key in keys is
// followed by a string value anywhere in data. Nested objects can make
// it a false positive, never a false negative, so a miss skips the
// map decode that rewriteDurationStrings needs.
func hasQuotedValue(data []byte, keys map[string]struct{}) bool {
	if keys == nil {
		panic("hasQuotedValue: keys must not be nil")
	}
	if len(keys) == 0 || bytes.IndexByte(data, '"') < 0 {
		return false
	}
	lower := bytes.ToLower(data)
	for key := range keys {
		needle := []byte(`"` + key + `"`)
		rest := lower
		for found := bytes.Index(rest, needle); found >= 0; found = bytes.Index(rest, needle) {
			rest = rest[found+len(needle):]
			after := bytes.TrimLeft(rest, " \t\r\n")
			if len(after) == 0 || after[0] != ':' {
				continue
			}
			after = bytes.TrimLeft(after[1:], " \t\r\n")
			if len(after) > 0 && after[0] == '"' {
				return true
			}
		}
	}
	return false
}

// durationKeys returns the lowercased JSON names of typ's direct
// time.Duration fields.
func durationKeys(typ reflect.Type) map[string]struct{} {
	if typ.Kind() != reflect.Struct {
		panic("durationKeys: typ must be a struct")
	}
	if cached, ok := durationKeysByType.Load(typ); ok {
		return cached.(map[string]struct{})
	}
	keys := make(map[string]struct{})
	for index := 0; index < typ.NumField(); index++ {
		field := typ.Field(index)
		if field.Type != durationType || !field.IsExported() {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		keys[strings.ToLower(name)] = struct{}{}
	}
	if len(keys) == 0 {
		panic("durationKeys: " + typ.Name() + " has no duration fields")
	}
	durationKeysByType.Store(typ, keys)
	return keys
}
