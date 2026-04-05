---
title: Go SDK
weight: 5
---

API reference for the public Go packages in the DagNats module (`github.com/danmestas/dagnats`).

Each package section includes a hand-written overview and auto-generated API documentation from source.

| Package | Description |
|---------|-------------|
| [dag](dag/) | Pure DAG logic: workflow definitions, validation, state machine |
| [worker](worker/) | Task execution framework: worker lifecycle, handlers, task context |
| [protocol](protocol/) | Wire types: events, task payloads, resolutions |
| [observe](observe/) | Telemetry interfaces: logging, tracing, metrics, error reporting |
| [actor](actor/) | Actor runtime with supervision trees |
| [server](server/) | Embedded NATS server with programmatic startup |
| [bridge](bridge/) | HTTP-to-NATS gateway for non-Go workers |
| [httpclient](httpclient/) | Go HTTP reference client for the bridge |
| [dagnatstest](dagnatstest/) | Test helpers for one-call test setup |
