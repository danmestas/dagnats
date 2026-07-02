// cli/demo_schemas.go
// Domain-shaped JSON Schemas for the rich keep-alive demo workflows.
//
// These are attached to each dag.WorkflowDef by decorateDemoDef so the
// console Functions detail page renders a populated Contract section instead
// of "No schema registered". Each schema mirrors the payload
// buildWorkflowInput produces (input) and the per-step output the noop
// worker emits (output) for that workflow, so the documented contract
// matches the run IO an operator inspects on the run-detail page.
//
// They live as standalone valid-JSON string constants (verified by
// TestRichWorkflowDefsCarrySchemas) rather than reflection-derived shapes so
// the descriptions, formats, and required lists read like a hand-authored
// contract.
package cli

const schemaNoopInput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "DemoNoopInput",
  "type": "object",
  "properties": {
    "outcome": {
      "type": "string",
      "enum": ["completed", "failed", "cancelled"],
      "description": "Terminal state the demo run is driven to"
    }
  }
}`

const schemaNoopOutput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "DemoNoopOutput",
  "type": "object",
  "properties": {
    "noop": {"type": "string"},
    "processed": {"type": "boolean"}
  }
}`

const schemaImagePipelineInput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "ImagePipelineInput",
  "type": "object",
  "required": ["album", "source_urls"],
  "properties": {
    "album": {"type": "string", "description": "Destination album slug"},
    "source_urls": {
      "type": "array",
      "items": {"type": "string", "format": "uri"},
      "description": "Image URLs to fetch and process"
    },
    "max_dimension_px": {
      "type": "integer",
      "minimum": 1,
      "description": "Longest-edge resize target in pixels"
    }
  }
}`

const schemaImagePipelineOutput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "ImagePipelineOutput",
  "type": "object",
  "properties": {
    "gallery_url": {"type": "string", "format": "uri"},
    "thumbnails": {"type": "integer"},
    "bytes": {"type": "integer"}
  }
}`

const schemaRetryErrorsInput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "RetryErrorsInput",
  "type": "object",
  "properties": {
    "outcome": {
      "type": "string",
      "enum": ["completed", "failed", "cancelled"],
      "description": "Terminal state the flaky attempt is driven to"
    }
  }
}`

const schemaRetryErrorsOutput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "RetryErrorsOutput",
  "type": "object",
  "properties": {
    "attempt": {"type": "integer"},
    "status": {"type": "string"},
    "latency_ms": {"type": "integer"}
  }
}`

const schemaAgentLoopInput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "AgentLoopInput",
  "type": "object",
  "required": ["goal"],
  "properties": {
    "goal": {"type": "string", "description": "Objective the agent pursues"},
    "model": {"type": "string", "description": "LLM model identifier"},
    "max_iterations": {
      "type": "integer",
      "minimum": 1,
      "description": "Upper bound on plan/act/observe cycles"
    }
  }
}`

const schemaAgentLoopOutput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "AgentLoopOutput",
  "type": "object",
  "properties": {
    "summary": {"type": "string"},
    "tokens": {"type": "integer"}
  }
}`

const schemaETLInput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "ETLNightlyInput",
  "type": "object",
  "required": ["table", "date"],
  "properties": {
    "table": {"type": "string", "description": "Fully-qualified target table"},
    "date": {"type": "string", "format": "date", "description": "Partition date"},
    "batch_size": {
      "type": "integer",
      "minimum": 1,
      "description": "Rows processed per load batch"
    }
  }
}`

const schemaETLOutput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "ETLNightlyOutput",
  "type": "object",
  "properties": {
    "rows_loaded": {"type": "integer"},
    "table": {"type": "string"}
  }
}`

const schemaNotifyInput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "NotifyFanoutInput",
  "type": "object",
  "required": ["template", "recipients"],
  "properties": {
    "template": {"type": "string", "description": "Notification template name"},
    "recipients": {
      "type": "array",
      "items": {"type": "string"},
      "description": "Email addresses and chat channels to notify"
    }
  }
}`

const schemaNotifyOutput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "NotifyFanoutOutput",
  "type": "object",
  "description": "Merged result of the parallel email and slack branches",
  "properties": {
    "provider": {"type": "string"},
    "accepted": {"type": "integer"},
    "rejected": {"type": "integer"},
    "channel": {"type": "string"},
    "ok": {"type": "boolean"}
  }
}`

const schemaSupervisorInput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "AgentSupervisorInput",
  "type": "object",
  "required": ["goal"],
  "properties": {
    "goal": {"type": "string", "description": "Fleet objective to fan out"},
    "model": {"type": "string", "description": "LLM model identifier"}
  }
}`

const schemaSupervisorOutput = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "AgentSupervisorOutput",
  "type": "object",
  "properties": {
    "agents_spawned": {
      "type": "integer",
      "description": "Child agent runs launched under this supervisor"
    }
  }
}`
