---
title: ci
weight: 10
---

```
import "github.com/danmestas/dagnats/ci"
```

Parses `.dagnats/ci.yml` CI specs and compiles them into `dag.WorkflowDef`
instances. Promoted to the module root in #633 so the `dagnats-ci` add-on
(a separate Go module) can depend on it -- see the
[CI Module Reference](../../ci-module) for the full spec format, the
diagnostic shape, and the mounted `/v1/ci/{compile,validate}`
control-plane endpoints that also use this package.

## Key Types

| Type | Description |
|------|-------------|
| `Spec` | Parsed form of a ci.yml file: `On` triggers, `Defaults`, `Checks`, `Deploy` |
| `Check` | One CI check step: Dagger `Call`, `Needs` dependencies, `Timeout` |
| `DeployStep` | Optional deploy stage: `Call`, `Needs`, `Approval`, `Branches`, `Timeout` |
| `Diagnostic` | One parse or compile problem: `Line`, `Column`, `Field`, `Message` |

## Key Functions

| Function | Description |
|----------|--------------|
| `Parse(spec)` | Decodes YAML bytes into a `Spec`, accumulating a `Diagnostic` per field that fails to decode (with source `Line`/`Column`) |
| `Compile(name, spec)` | Compiles a `Spec` into a `dag.WorkflowDef`, accumulating diagnostics instead of failing fast |
| `CompileYAML(name, bytes)` | `Parse` + `Compile` for callers holding raw ci.yml bytes |

`DiagnosticsMax` (100) bounds how many diagnostics a single `Parse`/`Compile`
call accumulates before appending a terminal "too many errors" sentinel.

## Usage

```go
def, diags := ci.CompileYAML("ci:myrepo", ciYMLBytes)
if len(diags) > 0 {
    for _, d := range diags {
        log.Printf("%d:%d %s: %s", d.Line, d.Column, d.Field, d.Message)
    }
    return
}
if err := dag.Validate(def); err != nil {
    log.Fatal(err) // unreachable: Compile already validated def
}
```
