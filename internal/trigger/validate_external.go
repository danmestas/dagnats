// trigger/validate_external.go
// Schema lookup + jsonschema validation for External triggers
// (parent #273 Phase 2.2). Pulled out of validate.go so the main
// Validate body stays under the 70-LoC function budget. Per-call
// fetch from the trigger_types KV bucket — no cache layer in this
// PR; cache lives as a future optimisation with its own invalidation
// surface (issue #318 audit).
package trigger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// validateExternal validates def.External, fetching the registered
// TriggerTypeDef from the KV handle (when present) and compiling its
// ConfigSchema. Returns a redirect error when no KV handle is
// available, matching the Validate-vs-ValidateWithKV contract.
func validateExternal(def TriggerDef, kvh *kvHandle) error {
	if def.External == nil {
		panic("validateExternal: External must not be nil")
	}
	if def.ID == "" {
		panic("validateExternal: def.ID must not be empty")
	}
	if def.External.Kind == "" {
		return fmt.Errorf(
			"trigger %q: external.kind must not be empty", def.ID)
	}
	if kvh == nil || kvh.kv == nil {
		return fmt.Errorf(
			"trigger %q: External requires a KV handle; "+
				"use ValidateWithKV", def.ID)
	}
	tdef, err := fetchTriggerType(kvh, def.External.Kind)
	if err != nil {
		return fmt.Errorf("trigger %q: %w", def.ID, err)
	}
	if err := validateAgainstSchema(
		tdef.ConfigSchema, def.External.Config,
	); err != nil {
		return fmt.Errorf(
			"trigger %q: external.config: %w", def.ID, err)
	}
	return nil
}

// fetchTriggerType reads the TriggerTypeDef registered under kind
// from the trigger_types KV bucket. A jetstream.ErrKeyNotFound is
// translated to the canonical "no trigger type registered: <kind>"
// error so callers can match on the message.
func fetchTriggerType(
	kvh *kvHandle, kind string,
) (TriggerTypeDef, error) {
	if kvh == nil || kvh.kv == nil {
		panic("fetchTriggerType: kvh must carry a KV handle")
	}
	if kind == "" {
		panic("fetchTriggerType: kind must not be empty")
	}
	entry, err := kvh.kv.Get(kvh.ctx, kind)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return TriggerTypeDef{}, fmt.Errorf(
				"no trigger type registered: %s", kind)
		}
		return TriggerTypeDef{}, fmt.Errorf(
			"trigger_types KV lookup for %q: %w", kind, err)
	}
	var tdef TriggerTypeDef
	if err := json.Unmarshal(entry.Value(), &tdef); err != nil {
		return TriggerTypeDef{}, fmt.Errorf(
			"trigger_types KV decode for %q: %w", kind, err)
	}
	if len(tdef.ConfigSchema) == 0 {
		return TriggerTypeDef{}, fmt.Errorf(
			"trigger type %q has empty config_schema", kind)
	}
	return tdef, nil
}

// validateAgainstSchema compiles schemaJSON via santhosh-tekuri/
// jsonschema/v5 and validates configJSON against it. Empty config
// is treated as `{}` so a schema-with-defaults still applies.
func validateAgainstSchema(
	schemaJSON json.RawMessage, configJSON json.RawMessage,
) error {
	if len(schemaJSON) == 0 {
		panic("validateAgainstSchema: schemaJSON must not be empty")
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(
		"external.json", bytes.NewReader(schemaJSON),
	); err != nil {
		return fmt.Errorf("schema parse: %w", err)
	}
	schema, err := compiler.Compile("external.json")
	if err != nil {
		return fmt.Errorf("schema compile: %w", err)
	}
	cfgBytes := configJSON
	if len(cfgBytes) == 0 {
		cfgBytes = json.RawMessage(`{}`)
	}
	var doc any
	if err := json.Unmarshal(cfgBytes, &doc); err != nil {
		return fmt.Errorf("config not valid JSON: %w", err)
	}
	if err := schema.Validate(doc); err != nil {
		return err
	}
	return nil
}
