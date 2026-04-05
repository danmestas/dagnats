---
title: DagNats
layout: hextra-home
---

{{< hextra/hero-badge >}}
  <span>Open Source</span>
{{< /hextra/hero-badge >}}

<div class="hx:mt-6 hx:mb-6">
{{< hextra/hero-headline >}}
  DAG-based Workflow Engine<br class="sm:hx:block hx:hidden" />Built on NATS
{{< /hextra/hero-headline >}}
</div>

{{< hextra/hero-subtitle >}}
  Orchestrate durable workflows and autonomous LLM coding pipelines&nbsp;<br class="sm:hx:block hx:hidden" />with a single binary and zero external dependencies.
{{< /hextra/hero-subtitle >}}

<div class="hx:mb-12">
{{< hextra/hero-button text="Get Started" link="docs/get-started/quickstart" >}}
{{< hextra/hero-button text="GitHub" link="https://github.com/danmestas/dagnats" style="alt" >}}
</div>

{{< hextra/feature-grid >}}
  {{< hextra/feature-card
    title="DAG Orchestration"
    subtitle="Define workflows as directed acyclic graphs. Automatic dependency resolution, conditional branching, and dynamic DAG generation."
  >}}
  {{< hextra/feature-card
    title="Single Binary"
    subtitle="dagnats serve starts everything: embedded NATS, engine, API, triggers. No Postgres, no Redis, no Kafka."
  >}}
  {{< hextra/feature-card
    title="Built for LLM Agents"
    subtitle="Agent loops, checkpoints, signals, and approval gates designed for autonomous coding pipelines."
  >}}
  {{< hextra/feature-card
    title="NATS Native"
    subtitle="JetStream for durable messaging, KV for state, pub/sub for signals. One infrastructure dependency."
  >}}
  {{< hextra/feature-card
    title="Durable Execution"
    subtitle="Event-sourced history stream. Replay from any point. Survive restarts without losing state."
  >}}
  {{< hextra/feature-card
    title="Deep Worker SDK"
    subtitle="Typed handlers, checkpoints, signals, streaming. Workers see a clean TaskContext, never retry logic."
  >}}
{{< /hextra/feature-grid >}}
