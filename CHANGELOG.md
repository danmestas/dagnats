# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Apache-2.0 LICENSE.
- Auto-sync of landing-page version from latest git tag.

## [0.1.1] - 2026-04-10

### Fixed

- Workflow run input now correctly forwards to root steps.

## [0.1.0] - 2026-04-08

Initial tagged release of `dagnats`, a workflow orchestration engine combining
DAG-based task graphs with NATS-backed coordination. Single-binary deployable
with embedded NATS server, actor runtime, and webhook/cron triggers.

### Added

- DAG-based workflow definition and validation engine.
- Embedded NATS server (no external broker required).
- Actor runtime with checkpoint/heartbeat semantics for crash recovery.
- Worker, server, CLI, sidecar, bridge, and SDK packages.
- Webhook and cron trigger sources.
- Lazy orchestrator subsystem initialization.
