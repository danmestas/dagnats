// Shared types + mock data for the dagnats console redesign prototype.
// Visual prototype only — no backend. All data is illustrative.

export type ViewKey = 'dashboard' | 'workflows' | 'workflowDetail' | 'runs' | 'runDetail' | 'triggers' | 'triggersEmpty' | 'functions' | 'functionDetail' | 'workers' | 'workerDetail' | 'triggerDetail' | 'health' | 'services' | 'serviceDetail' | 'connections' | 'consumers' | 'streams' | 'streamDetail' | 'kv' | 'dlq' | 'dlqDetail' | 'logs' | 'traces' | 'traceDetail' | 'metrics' | 'concurrency' | 'audit' | 'config';
export interface NavItem {
  id: string;
  label: string;
  icon: string;
  badge?: string;
  view: ViewKey;
  group: string;
}
export const NAV: NavItem[] = [
// Inventory — what exists
{
  id: 'workflows',
  label: 'Workflows',
  icon: '⌗',
  badge: '12',
  view: 'workflows',
  group: 'Inventory'
}, {
  id: 'functions',
  label: 'Functions',
  icon: '𝑓',
  badge: '19',
  view: 'functions',
  group: 'Inventory'
}, {
  id: 'workers',
  label: 'Workers',
  icon: '⚙',
  badge: '4',
  view: 'workers',
  group: 'Inventory'
}, {
  id: 'triggers',
  label: 'Triggers',
  icon: '⏱',
  badge: '5',
  view: 'triggers',
  group: 'Inventory'
},
// Activity — what's happening / what broke
{
  id: 'dashboard',
  label: 'Dashboard',
  icon: '▤',
  view: 'dashboard',
  group: 'Activity'
}, {
  id: 'runs',
  label: 'Runs',
  icon: '▸',
  badge: '240',
  view: 'runs',
  group: 'Activity'
}, {
  id: 'dlq',
  label: 'DLQ',
  icon: '☠',
  badge: '2',
  view: 'dlq',
  group: 'Activity'
}, {
  id: 'logs',
  label: 'Logs',
  icon: '≣',
  view: 'logs',
  group: 'Activity'
}, {
  id: 'traces',
  label: 'Traces',
  icon: '⋔',
  view: 'traces',
  group: 'Activity'
}, {
  id: 'metrics',
  label: 'Metrics',
  icon: '◧',
  view: 'metrics',
  group: 'Activity'
},
// System — how it's wired (plumbing)
{
  id: 'health',
  label: 'Server',
  icon: '◎',
  view: 'health',
  group: 'System'
}, {
  id: 'services',
  label: 'Services',
  icon: '❖',
  badge: '6',
  view: 'services',
  group: 'System'
}, {
  id: 'connections',
  label: 'Connections',
  icon: '🔌',
  badge: '8',
  view: 'connections',
  group: 'System'
}, {
  id: 'streams',
  label: 'Streams',
  icon: '≋',
  badge: '8',
  view: 'streams',
  group: 'System'
}, {
  id: 'consumers',
  label: 'Consumers',
  icon: '⇄',
  badge: '6',
  view: 'consumers',
  group: 'System'
}, {
  id: 'kv',
  label: 'KV',
  icon: '🗝',
  badge: '18',
  view: 'kv',
  group: 'System'
}, {
  id: 'concurrency',
  label: 'Concurrency',
  icon: '◫',
  view: 'concurrency',
  group: 'System'
}, {
  id: 'audit',
  label: 'Audit log',
  icon: '✦',
  view: 'audit',
  group: 'System'
}, {
  id: 'config',
  label: 'Config',
  icon: '◷',
  view: 'config',
  group: 'System'
}];

// nav id that should light up for a given view (detail views map back to their list)
export const VIEW_TO_NAV: Record<ViewKey, string> = {
  dashboard: 'dashboard',
  workflows: 'workflows',
  workflowDetail: 'workflows',
  runs: 'runs',
  runDetail: 'runs',
  triggers: 'triggers',
  triggersEmpty: 'triggers',
  triggerDetail: 'triggers',
  functions: 'functions',
  functionDetail: 'functions',
  workers: 'workers',
  workerDetail: 'workers',
  health: 'health',
  services: 'services',
  serviceDetail: 'services',
  connections: 'connections',
  consumers: 'consumers',
  streams: 'streams',
  streamDetail: 'streams',
  kv: 'kv',
  dlq: 'dlq',
  dlqDetail: 'dlq',
  logs: 'logs',
  traces: 'traces',
  traceDetail: 'traces',
  metrics: 'metrics',
  concurrency: 'concurrency',
  audit: 'audit',
  config: 'config'
};
export type Tone = '' | 'dn-ok' | 'dn-warn' | 'dn-danger' | 'dn-info' | 'dn-teal' | 'dn-amber';
export const TILES: Record<ViewKey, [string, string, Tone][]> = {
  dashboard: [['2', 'failed runs · 1h', 'dn-danger'], ['2', 'DLQ depth', 'dn-danger'], ['1', 'in-flight', 'dn-teal'], ['99.1%', 'success · 24h', 'dn-ok']],
  workflows: [['12', 'workflows', 'dn-teal'], ['8', 'active', 'dn-ok'], ['4', 'draft', 'dn-info'], ['240', 'runs · 24h', '']],
  workflowDetail: [],
  runs: [['240', 'in last 1h', 'dn-info'], ['1', 'running', 'dn-teal'], ['2', 'failed', 'dn-danger'], ['1.2s', 'p50 duration', '']],
  runDetail: [],
  triggers: [['5', 'triggers', 'dn-teal'], ['3', 'active', 'dn-ok'], ['2', 'disabled', 'dn-warn'], ['18', 'fires · 24h', 'dn-info']],
  triggersEmpty: [],
  triggerDetail: [],
  functions: [['19', 'functions', 'dn-teal'], ['4', 'workers', 'dn-ok'], ['3', 'groups', 'dn-info'], ['0.4%', 'fail rate 24h', 'dn-warn']],
  functionDetail: [],
  workers: [['4', 'workers', 'dn-teal'], ['3', 'groups', 'dn-info'], ['5/5', 'online', 'dn-ok'], ['0', 'stale', '']],
  workerDetail: [],
  health: [['14d 3h', 'uptime', 'dn-ok'], ['0', 'slow consumers', 'dn-ok'], ['1.9 / 10 GiB', 'JS storage', 'dn-info'], ['0', 'API errors', 'dn-ok']],
  consumers: [['6', 'consumers', 'dn-teal'], ['1', 'lag alarm', 'dn-danger'], ['4', 'task pending', 'dn-warn'], ['0', 'stalled', 'dn-ok']],
  services: [['6', 'services', 'dn-teal'], ['7', 'instances', 'dn-info'], ['1', 'live $SRV endpoint', 'dn-ok'], ['0', 'errors · 1h', 'dn-ok']],
  serviceDetail: [],
  connections: [['8', 'connections', 'dn-teal'], ['41', 'total · since boot', 'dn-info'], ['0', 'slow consumers', 'dn-ok'], ['128', 'subscriptions', '']],
  streamDetail: [],
  streams: [['8', 'streams', 'dn-teal'], ['2.1k', 'messages', 'dn-info'], ['9', 'consumers', 'dn-ok'], ['6.0 MB', 'on disk', '']],
  kv: [['18', 'buckets', 'dn-teal'], ['7', 'roles', 'dn-info'], ['8', 'TTL-bounded', ''], ['2', 'watched', 'dn-teal']],
  dlq: [['2', 'entries', 'dn-danger'], ['1', 'redrive-eligible', 'dn-warn'], ['1', 'expired', ''], ['DEAD_LETTERS', 'stream', 'dn-info']],
  dlqDetail: [],
  logs: [['4.2k', 'lines · 1h', 'dn-info'], ['38', 'warnings', 'dn-warn'], ['6', 'errors', 'dn-danger'], ['live', 'tail', 'dn-teal']],
  traces: [['142', 'traces · 1h', 'dn-info'], ['1', 'errored', 'dn-danger'], ['7', 'spans · avg', 'dn-teal'], ['2.4s', 'p50 root', '']],
  traceDetail: [],
  metrics: [['142/m', 'runs/min', 'dn-teal'], ['12', 'runs active', 'dn-info'], ['2', 'DLQ depth', 'dn-warn'], ['4.2×', 'snapshot p99/p50', 'dn-danger']],
  audit: [['28', 'events · 24h', 'dn-info'], ['2', 'denied', 'dn-warn'], ['1', 'failed', 'dn-danger'], ['25', 'succeeded', 'dn-ok']],
  concurrency: [['3', 'runs blocked', 'dn-warn'], ['2', 'locks held', 'dn-info'], ['1', 'rate-limited', 'dn-amber'], ['1', 'worker-starved', 'dn-danger']],
  config: []
};
export const TITLES: Record<ViewKey, [string, string]> = {
  dashboard: ['Dashboard', 'Is anything on fire?'],
  workflows: ['Workflows', 'Every workflow definition registered with the engine'],
  workflowDetail: ['Workflow', 'Definition · steps · recent runs'],
  runs: ['Runs', 'Live operational view · SSE'],
  runDetail: ['Run', 'Events · IO · timeline'],
  triggers: ['Triggers', 'What fires your workflows'],
  triggersEmpty: ['Triggers', '0 of 0 — nothing configured yet'],
  triggerDetail: ['Trigger', 'Config · fire history · actions'],
  functions: ['Functions', 'Every registered task type across live workers'],
  functionDetail: ['Function', 'Contract · providers · invocations — and a live quick-test'],
  workers: ['Workers', 'Connected workers and the task types they own'],
  workerDetail: ['Worker', 'Pull · execute · ack — what this worker is running'],
  health: ['Server', 'Embedded NATS · JetStream health & capacity'],
  services: ['Services', 'Live process roster · micro discovery ($SRV)'],
  serviceDetail: ['Service', 'Discovery · endpoints · $SRV stats'],
  connections: ['Connections', 'Connected NATS clients · /connz'],
  consumers: ['Consumers', 'Durable consumers — delivery & lag per consumer'],
  streamDetail: ['Stream', 'Config · state · consumers'],
  streams: ['Streams', 'JetStream streams backing the engine'],
  kv: ['KV', 'JetStream buckets by role — the state substrate'],
  dlq: ['Dead-letter queue', 'Messages that exhausted their delivery budget'],
  dlqDetail: ['DLQ entry', 'Failed message · headers · payload · error'],
  logs: ['Logs', 'Structured log tail across all components'],
  traces: ['Traces', 'OTLP spans per run · W3C traceparent'],
  traceDetail: ['Trace', 'Span tree · waterfall · attributes'],
  metrics: ['Metrics', 'OTel Meter series, aggregated in-console'],
  concurrency: ['Admission control', 'Slots · locks · rate limits · debounce — why work waits'],
  audit: ['Audit log', 'Operator mutations — success, denied & failed · console_audit KV (90d)'],
  config: ['Configuration', 'Deployment posture · effective config · engine invariants']
};
export type RunStatus = 'ok' | 'run' | 'fail';
export const STATUS_TXT: Record<RunStatus, string> = {
  ok: 'completed',
  run: 'running',
  fail: 'failed'
};

// ---- Runs ----
export interface RunRow {
  id: string;
  workflow: string;
  status: RunStatus;
  trigger: string;
  started: string;
  duration: string;
}
export const RUNS: RunRow[] = [{
  id: 'a662ac7e',
  workflow: 'cron-trigger',
  status: 'run',
  trigger: 'cron',
  started: '17:20:01',
  duration: '1m52s'
}, {
  id: '6689ab0b',
  workflow: 'retry-errors',
  status: 'fail',
  trigger: 'manual',
  started: '17:19:31',
  duration: '0.4s'
}, {
  id: '8b8485fb',
  workflow: 'retry-errors',
  status: 'fail',
  trigger: 'manual',
  started: '17:19:31',
  duration: '0.4s'
}, {
  id: '8aaf9b3a',
  workflow: 'image-pipeline',
  status: 'ok',
  trigger: 'manual',
  started: '17:19:31',
  duration: '2.1s'
}, {
  id: 'f87980ac',
  workflow: 'image-pipeline',
  status: 'ok',
  trigger: 'manual',
  started: '17:19:31',
  duration: '2.4s'
}, {
  id: 'b5b0c77a',
  workflow: 'hello-world',
  status: 'ok',
  trigger: 'manual',
  started: '17:19:31',
  duration: '0.3s'
}, {
  id: '0809c77e',
  workflow: 'hello-world',
  status: 'ok',
  trigger: 'manual',
  started: '17:19:31',
  duration: '0.3s'
}, {
  id: 'd9644b3d',
  workflow: 'hello-world',
  status: 'ok',
  trigger: 'manual',
  started: '17:19:31',
  duration: '0.3s'
}];

// ---- Functions ----
export const FNS: [string, string, string, string, string, string][] = [['greet', 'hello-world ×2', '0', '312', '0.2s', '0.0%'], ['uppercase', 'hello-world ×2', '0', '312', '0.1s', '0.0%'], ['fetch-urls', 'image-pipeline', '1', '48', '1.1s', '2.1%'], ['build-gallery', 'image-pipeline', '0', '48', '0.9s', '0.0%'], ['fetch', 'retry-errors', '0', '16', '0.3s', '100%'], ['tick', '— (no worker)', '3', '—', '—', '—']];

// ---- Function detail (per-function drill-down) ----
// A function is a registered task type (service::name). Workers register the
// functions they can serve; a function with zero providers is "no worker" —
// tasks queue but nothing pulls them until a worker registers.
export interface FnProvider {
  worker: string;
  status: 'online' | 'stale';
  inflight: string;
}
export interface FnInvocation {
  time: string;
  runId: string;
  caller: string;
  status: RunStatus;
  duration: string;
}
export interface FunctionDetail {
  service: string;
  name: string;
  description: string;
  // invocations 1h, pending, fail rate 24h, avg duration
  stats: [string, string, string, string];
  inputSchema: string | null;
  outputSchema: string | null;
  providers: FnProvider[];
  invocations: FnInvocation[];
  // prefilled InvokeModal payload + the mock output a successful invoke returns
  sampleInput: string;
  sampleOutput: string | null;
}
export const FUNCTION_DETAILS: Record<string, FunctionDetail> = {
  greet: {
    service: 'hello-world',
    name: 'greet',
    description: 'Render a greeting for a given name and locale.',
    stats: ['312', '0', '0.0%', '0.2s'],
    inputSchema: `{
  "type": "object",
  "required": ["name"],
  "properties": {
    "name":   { "type": "string" },
    "locale": { "type": "string", "default": "en" }
  }
}`,
    outputSchema: `{
  "type": "object",
  "properties": { "greeting": { "type": "string" } }
}`,
    providers: [{
      worker: 'wkr-3a1f',
      status: 'online',
      inflight: '1'
    }, {
      worker: 'wkr-9b22',
      status: 'online',
      inflight: '0'
    }],
    invocations: [{
      time: '17:20:01',
      runId: 'a662ac7e',
      caller: 'manual',
      status: 'ok',
      duration: '0.2s'
    }, {
      time: '17:18:44',
      runId: '0809c77e',
      caller: 'hello-world',
      status: 'ok',
      duration: '0.2s'
    }, {
      time: '17:17:12',
      runId: 'd9644b3d',
      caller: 'hello-world',
      status: 'ok',
      duration: '0.1s'
    }],
    sampleInput: `{
  "name": "Ada",
  "locale": "en"
}`,
    sampleOutput: `{
  "greeting": "Hello, Ada!"
}`
  },
  uppercase: {
    service: 'hello-world',
    name: 'uppercase',
    description: 'Uppercase a string. Pure, idempotent transform.',
    stats: ['312', '0', '0.0%', '0.1s'],
    inputSchema: `{
  "type": "object",
  "required": ["text"],
  "properties": { "text": { "type": "string" } }
}`,
    outputSchema: `{
  "type": "object",
  "properties": { "text": { "type": "string" } }
}`,
    providers: [{
      worker: 'wkr-3a1f',
      status: 'online',
      inflight: '1'
    }, {
      worker: 'wkr-9b22',
      status: 'online',
      inflight: '0'
    }],
    invocations: [{
      time: '17:19:58',
      runId: '8aaf9b3a',
      caller: 'hello-world',
      status: 'ok',
      duration: '0.1s'
    }, {
      time: '17:18:30',
      runId: 'b5b0c77a',
      caller: 'manual',
      status: 'ok',
      duration: '0.1s'
    }, {
      time: '17:16:02',
      runId: 'f87980ac',
      caller: 'hello-world',
      status: 'ok',
      duration: '0.1s'
    }],
    sampleInput: `{
  "text": "make me loud"
}`,
    sampleOutput: `{
  "text": "MAKE ME LOUD"
}`
  },
  'fetch-urls': {
    service: 'image-pipeline',
    name: 'fetch-urls',
    description: 'List source URLs from a bucket prefix for the pipeline fanout.',
    stats: ['48', '1', '2.1%', '1.1s'],
    inputSchema: `{
  "type": "object",
  "required": ["source"],
  "properties": {
    "source":          { "type": "string" },
    "max_concurrency": { "type": "integer", "default": 12 }
  }
}`,
    outputSchema: `{
  "type": "object",
  "properties": {
    "urls":  { "type": "array", "items": { "type": "string" } },
    "count": { "type": "integer" }
  }
}`,
    providers: [{
      worker: 'wkr-7c04',
      status: 'online',
      inflight: '1'
    }],
    invocations: [{
      time: '17:19:31',
      runId: '8aaf9b3a',
      caller: 'image-pipeline',
      status: 'ok',
      duration: '0.4s'
    }, {
      time: '17:15:02',
      runId: 'f87980ac',
      caller: 'trg-photos-in',
      status: 'ok',
      duration: '1.1s'
    }, {
      time: '17:09:48',
      runId: 'c19f44de',
      caller: 'image-pipeline',
      status: 'fail',
      duration: '2.0s'
    }],
    sampleInput: `{
  "source": "s3://photos/incoming/",
  "max_concurrency": 12
}`,
    sampleOutput: `{
  "urls": [
    "s3://photos/incoming/001.jpg",
    "s3://photos/incoming/002.jpg"
  ],
  "count": 12
}`
  },
  'build-gallery': {
    service: 'image-pipeline',
    name: 'build-gallery',
    description: 'Assemble fetched images into a static gallery artifact.',
    stats: ['48', '0', '0.0%', '0.9s'],
    inputSchema: `{
  "type": "object",
  "required": ["run_id", "images"],
  "properties": {
    "run_id": { "type": "string" },
    "images": { "type": "integer" }
  }
}`,
    outputSchema: `{
  "type": "object",
  "properties": { "gallery_url": { "type": "string" } }
}`,
    providers: [{
      worker: 'wkr-7c04',
      status: 'online',
      inflight: '0'
    }],
    invocations: [{
      time: '17:19:33',
      runId: '8aaf9b3a',
      caller: 'image-pipeline',
      status: 'ok',
      duration: '0.9s'
    }, {
      time: '17:15:05',
      runId: 'f87980ac',
      caller: 'image-pipeline',
      status: 'ok',
      duration: '0.8s'
    }],
    sampleInput: `{
  "run_id": "8aaf9b3a",
  "images": 12
}`,
    sampleOutput: `{
  "gallery_url": "s3://photos/galleries/8aaf9b3a.html"
}`
  },
  fetch: {
    service: 'retry-errors',
    name: 'fetch',
    description: 'Fetch a URL. Demonstrates retry/NAK behavior — currently failing every delivery.',
    stats: ['16', '0', '100%', '0.3s'],
    inputSchema: `{
  "type": "object",
  "required": ["url"],
  "properties": {
    "url":     { "type": "string" },
    "retries": { "type": "integer", "default": 0 }
  }
}`,
    outputSchema: `{
  "type": "object",
  "properties": {
    "status": { "type": "integer" },
    "bytes":  { "type": "integer" }
  }
}`,
    providers: [{
      worker: 'wkr-1d8e',
      status: 'online',
      inflight: '0'
    }],
    invocations: [{
      time: '17:19:31',
      runId: '6689ab0b',
      caller: 'manual',
      status: 'fail',
      duration: '0.4s'
    }, {
      time: '17:19:31',
      runId: '8b8485fb',
      caller: 'manual',
      status: 'fail',
      duration: '0.4s'
    }, {
      time: '17:12:18',
      runId: '99cab1a0',
      caller: 'retry-errors',
      status: 'fail',
      duration: '0.3s'
    }],
    sampleInput: `{
  "url": "https://flaky.example/api",
  "retries": 0
}`,
    sampleOutput: `{
  "status": 200,
  "bytes": 1841
}`
  },
  tick: {
    service: 'cron-trigger',
    name: 'tick',
    description: 'Periodic heartbeat step fired by the cron-trigger workflow.',
    stats: ['—', '3', '—', '—'],
    inputSchema: null,
    outputSchema: null,
    providers: [],
    invocations: [{
      time: '17:20:00',
      runId: 'a662ac7e',
      caller: 'trg-cron-nightly',
      status: 'run',
      duration: '—'
    }],
    sampleInput: `{
  "cursor": "2026-06-10T17:20"
}`,
    sampleOutput: null
  }
};
// Per-function 1h invocation rate trend for the detail header (Datatype line).
export const FUNCTION_DETAIL_TRENDS: Record<string, string> = {
  greet: '{l:55,60,52,68,60,72,66,78,70,82}',
  uppercase: '{l:50,58,54,62,58,66,60,70,64,72}',
  'fetch-urls': '{l:30,40,35,45,42,38,48,44,52,50}',
  'build-gallery': '{l:28,36,32,42,38,46,42,50,46,52}',
  fetch: '{l:20,15,28,18,35,22,40,30,25,32}',
  tick: '{l:0,0,0,0,0,0,0,0,0,0}'
};

// ---- Workflows ----
export interface WorkflowRow {
  name: string;
  steps: number;
  last: RunStatus;
  runs24h: string;
  avg: string;
  trigger: string;
  state: 'active' | 'draft';
}
export const WORKFLOWS: WorkflowRow[] = [{
  name: 'hello-world',
  steps: 2,
  last: 'ok',
  runs24h: '94',
  avg: '0.3s',
  trigger: 'manual',
  state: 'active'
}, {
  name: 'image-pipeline',
  steps: 4,
  last: 'ok',
  runs24h: '48',
  avg: '2.2s',
  trigger: 'subject',
  state: 'active'
}, {
  name: 'retry-errors',
  steps: 1,
  last: 'fail',
  runs24h: '16',
  avg: '0.4s',
  trigger: 'manual',
  state: 'active'
}, {
  name: 'sub-workflow',
  steps: 3,
  last: 'ok',
  runs24h: '22',
  avg: '1.8s',
  trigger: 'manual',
  state: 'active'
}, {
  name: 'signals',
  steps: 3,
  last: 'ok',
  runs24h: '12',
  avg: '4.1s',
  trigger: 'webhook',
  state: 'active'
}, {
  name: 'agent-loop',
  steps: 5,
  last: 'ok',
  runs24h: '8',
  avg: '9.6s',
  trigger: 'manual',
  state: 'active'
}, {
  name: 'http-echo',
  steps: 1,
  last: 'ok',
  runs24h: '31',
  avg: '0.1s',
  trigger: 'http',
  state: 'active'
}, {
  name: 'cron-trigger',
  steps: 1,
  last: 'run',
  runs24h: '9',
  avg: '1.9s',
  trigger: 'cron',
  state: 'active'
}, {
  name: 'nightly-report',
  steps: 4,
  last: 'ok',
  runs24h: '0',
  avg: '—',
  trigger: 'cron',
  state: 'draft'
}, {
  name: 'reindex-search',
  steps: 6,
  last: 'ok',
  runs24h: '0',
  avg: '—',
  trigger: 'manual',
  state: 'draft'
}, {
  name: 'archive-runs',
  steps: 2,
  last: 'ok',
  runs24h: '0',
  avg: '—',
  trigger: 'cron',
  state: 'draft'
}, {
  name: 'fanout-notify',
  steps: 3,
  last: 'ok',
  runs24h: '0',
  avg: '—',
  trigger: 'subject',
  state: 'draft'
}];
export type StepType = 'normal' | 'agent' | 'sub_workflow' | 'map' | 'sleep';
export interface StepDef {
  name: string;
  type: StepType;
  dependsOn: string[];
}
// step definition for the detail view (image-pipeline-ish)
export const WORKFLOW_STEPS: StepDef[] = [{
  name: 'fetch-urls',
  type: 'normal',
  dependsOn: []
}, {
  name: 'fetch',
  type: 'map',
  dependsOn: ['fetch-urls']
}, {
  name: 'enrich',
  type: 'agent',
  dependsOn: ['fetch']
}, {
  name: 'cooldown',
  type: 'sleep',
  dependsOn: ['enrich']
}, {
  name: 'build-gallery',
  type: 'sub_workflow',
  dependsOn: ['cooldown']
}];

// ---- Run detail events ----
export const RUN_EVENTS: [string, string, string, string][] = [['17:19:31.002', 'run.started', '—', 'workflow=image-pipeline trigger=manual'], ['17:19:31.118', 'step.scheduled', 'fetch-urls', 'queue=image-pipeline'], ['17:19:31.231', 'step.started', 'fetch-urls', 'worker=wkr-3a1f'], ['17:19:31.642', 'step.completed', 'fetch-urls', 'urls=12 duration=411ms'], ['17:19:31.701', 'step.scheduled', 'fetch', 'fanout=12'], ['17:19:33.004', 'step.completed', 'fetch', '12/12 ok'], ['17:19:33.120', 'step.started', 'build-gallery', 'worker=wkr-3a1f'], ['17:19:33.488', 'run.completed', '—', 'duration=2.1s status=completed']];
export const RUN_INPUT = `{
  "source": "s3://photos/incoming/",
  "max_concurrency": 12,
  "thumbnail": { "w": 320, "h": 240 }
}`;
export const RUN_OUTPUT = `{
  "gallery_url": "s3://photos/galleries/8aaf9b3a.html",
  "images": 12,
  "bytes": 4831204,
  "duration_ms": 2103
}`;
// run timeline: [name, offsetPct, widthPct, fail]
export const RUN_TIMELINE: [string, number, number, boolean][] = [['fetch-urls', 0, 20, false], ['fetch', 22, 62, false], ['build-gallery', 86, 14, false]];

// ---- Triggers ----
export type TriggerType = 'cron' | 'subject' | 'webhook' | 'http';
export interface TriggerRow {
  id: string;
  workflow: string;
  type: TriggerType;
  config: string;
  enabled: boolean;
}
export const TRIGGERS: TriggerRow[] = [{
  id: 'trg-cron-nightly',
  workflow: 'cron-trigger',
  type: 'cron',
  config: '*/5 * * * *',
  enabled: true
}, {
  id: 'trg-photos-in',
  workflow: 'image-pipeline',
  type: 'subject',
  config: 'photos.incoming.*',
  enabled: true
}, {
  id: 'trg-echo',
  workflow: 'http-echo',
  type: 'http',
  config: 'POST /api/echo',
  enabled: true
}, {
  id: 'trg-gh-hook',
  workflow: 'signals',
  type: 'webhook',
  config: '/hooks/github',
  enabled: false
}, {
  id: 'trg-report',
  workflow: 'nightly-report',
  type: 'cron',
  config: '0 6 * * *',
  enabled: false
}];

// ---- Workers ----
export interface WorkerRow {
  id: string;
  group: string;
  status: 'online' | 'stale';
  taskTypes: string;
  lastSeen: string;
  host: string;
}
export const WORKERS: WorkerRow[] = [{
  id: 'wkr-3a1f',
  group: 'hello-world',
  status: 'online',
  taskTypes: 'greet, uppercase',
  lastSeen: '3s ago',
  host: 'node-a'
}, {
  id: 'wkr-9b22',
  group: 'hello-world',
  status: 'online',
  taskTypes: 'greet, uppercase',
  lastSeen: '2s ago',
  host: 'node-a'
}, {
  id: 'wkr-7c04',
  group: 'image-pipeline',
  status: 'online',
  taskTypes: 'fetch-urls, fetch, build-gallery',
  lastSeen: '5s ago',
  host: 'node-b'
}, {
  id: 'wkr-1d8e',
  group: 'retry-errors',
  status: 'online',
  taskTypes: 'fetch',
  lastSeen: '2s ago',
  host: 'node-b'
}];

// ---- Streams ----
// dagnats creates exactly these 8 JetStream streams (natsutil/conn.go). KV
// buckets are NOT streams — they live in KvView. Each row carries the real
// stream config: retention policy, storage backend, sequence range, and the
// dedup window / max-age that govern its messages.
export type StreamRetention = 'limits' | 'work' | 'interest';
export type StreamStorage = 'file' | 'memory';
export interface StreamRow {
  name: string;
  subjects: string;
  retention: StreamRetention;
  storage: StreamStorage;
  messages: string;
  bytes: string;
  firstSeq: number;
  lastSeq: number;
  deleted: number;
  consumers: number;
  // dedup window / max-age / atomic-publish note (the load-bearing semantics)
  dedupOrAge: string;
}
export const STREAMS: StreamRow[] = [{
  name: 'WORKFLOW_HISTORY',
  subjects: 'history.>',
  retention: 'limits',
  storage: 'file',
  messages: '842',
  bytes: '1.2 MB',
  firstSeq: 1,
  lastSeq: 842,
  deleted: 0,
  consumers: 1,
  dedupOrAge: 'dedup 5s'
}, {
  name: 'TASK_QUEUES',
  subjects: 'task.>',
  retention: 'work',
  storage: 'file',
  messages: '3',
  bytes: '925 B',
  firstSeq: 4012,
  lastSeq: 4014,
  deleted: 0,
  consumers: 4,
  dedupOrAge: 'atomic-publish'
}, {
  name: 'EVENTS',
  subjects: 'event.>',
  retention: 'limits',
  storage: 'file',
  messages: '1,284',
  bytes: '3.1 MB',
  firstSeq: 1,
  lastSeq: 1284,
  deleted: 0,
  consumers: 1,
  dedupOrAge: '—'
}, {
  name: 'DEAD_LETTERS',
  subjects: 'dead.>',
  retention: 'limits',
  storage: 'file',
  messages: '2',
  bytes: '1.6 KB',
  firstSeq: 1,
  lastSeq: 2,
  deleted: 0,
  consumers: 0,
  dedupOrAge: 'dedup 24h'
}, {
  name: 'SLEEP_TIMERS',
  subjects: 'sleep.>, scheduled.>',
  retention: 'limits',
  storage: 'file',
  messages: '9',
  bytes: '12 KB',
  firstSeq: 1,
  lastSeq: 9,
  deleted: 0,
  consumers: 2,
  dedupOrAge: '—'
}, {
  name: 'STICKY_TASKS',
  subjects: 'sticky.>',
  retention: 'limits',
  storage: 'memory',
  messages: '0',
  bytes: '0 B',
  firstSeq: 0,
  lastSeq: 0,
  deleted: 0,
  consumers: 0,
  dedupOrAge: 'maxAge 30m'
}, {
  name: 'TELEMETRY',
  subjects: 'telemetry.>',
  retention: 'limits',
  storage: 'file',
  messages: '6,910',
  bytes: '18 MB',
  firstSeq: 1,
  lastSeq: 6910,
  deleted: 0,
  consumers: 1,
  dedupOrAge: '7d / 1 GiB · dedup 5s'
}, {
  name: 'TRIGGER_HISTORY',
  subjects: 'trigger.fire.>',
  retention: 'limits',
  storage: 'file',
  messages: '128',
  bytes: '64 KB',
  firstSeq: 1,
  lastSeq: 128,
  deleted: 0,
  consumers: 0,
  dedupOrAge: 'maxAge 30d'
}];

// Per-stream detail for the streamDetail drill-down. Carries the row config
// plus the consumers bound to the stream (cross-linked to CONSUMERS below).
export interface StreamConsumerRef {
  name: string;
  filter: string;
  pending: number;
  ackPending: number;
  redelivered: number;
}
export interface StreamDetail {
  row: StreamRow;
  consumers: StreamConsumerRef[];
}
export const STREAM_DETAILS: Record<string, StreamDetail> = {
  WORKFLOW_HISTORY: {
    row: STREAMS[0],
    consumers: [{
      name: 'orchestrator',
      filter: 'history.>',
      pending: 0,
      ackPending: 0,
      redelivered: 0
    }]
  },
  TASK_QUEUES: {
    row: STREAMS[1],
    consumers: [{
      name: 'wkr-image-pipeline',
      filter: 'task.image-pipeline.>',
      pending: 4,
      ackPending: 0,
      redelivered: 0
    }, {
      name: 'wkr-hello-world',
      filter: 'task.hello-world.>',
      pending: 0,
      ackPending: 2,
      redelivered: 0
    }, {
      name: 'wkr-retry-errors',
      filter: 'task.retry-errors.>',
      pending: 0,
      ackPending: 0,
      redelivered: 12
    }, {
      name: 'wkr-cron',
      filter: 'task.cron-trigger.>',
      pending: 0,
      ackPending: 1,
      redelivered: 0
    }]
  },
  EVENTS: {
    row: STREAMS[2],
    consumers: [{
      name: 'event-correlator',
      filter: 'event.>',
      pending: 0,
      ackPending: 0,
      redelivered: 0
    }]
  },
  DEAD_LETTERS: {
    row: STREAMS[3],
    consumers: []
  },
  SLEEP_TIMERS: {
    row: STREAMS[4],
    consumers: [{
      name: 'sleep-timer',
      filter: 'sleep.>',
      pending: 0,
      ackPending: 0,
      redelivered: 0
    }, {
      name: 'scheduled-run-timer',
      filter: 'scheduled.>',
      pending: 1,
      ackPending: 0,
      redelivered: 0
    }]
  },
  STICKY_TASKS: {
    row: STREAMS[5],
    consumers: []
  },
  TELEMETRY: {
    row: STREAMS[6],
    consumers: [{
      name: 'console_metrics_aggregator',
      filter: 'telemetry.>',
      pending: 0,
      ackPending: 0,
      redelivered: 0
    }]
  },
  TRIGGER_HISTORY: {
    row: STREAMS[7],
    consumers: []
  }
};

// ---- Consumers ----
// dagnats runs named durable consumers (the engine's nervous system) plus
// dynamic per-worker task consumers. lag = deliveredSeq − ackFloorSeq; on an
// explicit-ack work consumer, lag with zero waiting pulls means no worker is
// pulling — the headline health signal.
export type AckPolicy = 'explicit' | 'none';
export interface ConsumerRow {
  name: string;
  stream: string;
  filter: string;
  ackPolicy: AckPolicy;
  numPending: number;
  numAckPending: number;
  numWaiting: number;
  numRedelivered: number;
  deliveredSeq: number;
  ackFloorSeq: number;
  lag: number;
  ackWait: string;
  maxDeliver: string;
  tone: Tone;
}
export const CONSUMERS: ConsumerRow[] = [{
  name: 'orchestrator',
  stream: 'WORKFLOW_HISTORY',
  filter: 'history.>',
  ackPolicy: 'explicit',
  numPending: 0,
  numAckPending: 0,
  numWaiting: 1,
  numRedelivered: 0,
  deliveredSeq: 842,
  ackFloorSeq: 842,
  lag: 0,
  ackWait: '30s',
  maxDeliver: '—',
  tone: 'dn-ok'
}, {
  name: 'event-correlator',
  stream: 'EVENTS',
  filter: 'event.>',
  ackPolicy: 'explicit',
  numPending: 0,
  numAckPending: 0,
  numWaiting: 1,
  numRedelivered: 0,
  deliveredSeq: 1284,
  ackFloorSeq: 1284,
  lag: 0,
  ackWait: '30s',
  maxDeliver: '—',
  tone: 'dn-ok'
}, {
  name: 'sleep-timer',
  stream: 'SLEEP_TIMERS',
  filter: 'sleep.>',
  ackPolicy: 'explicit',
  numPending: 0,
  numAckPending: 0,
  numWaiting: 1,
  numRedelivered: 0,
  deliveredSeq: 9,
  ackFloorSeq: 9,
  lag: 0,
  ackWait: '30s',
  maxDeliver: '-1',
  tone: 'dn-ok'
}, {
  name: 'scheduled-run-timer',
  stream: 'SLEEP_TIMERS',
  filter: 'scheduled.>',
  ackPolicy: 'explicit',
  numPending: 1,
  numAckPending: 0,
  numWaiting: 1,
  numRedelivered: 0,
  deliveredSeq: 9,
  ackFloorSeq: 8,
  lag: 1,
  ackWait: '30s',
  maxDeliver: '-1',
  tone: 'dn-ok'
}, {
  name: 'console_metrics_aggregator',
  stream: 'TELEMETRY',
  filter: 'telemetry.>',
  ackPolicy: 'none',
  numPending: 0,
  numAckPending: 0,
  numWaiting: 0,
  numRedelivered: 0,
  deliveredSeq: 6910,
  ackFloorSeq: 6910,
  lag: 0,
  ackWait: '—',
  maxDeliver: '—',
  tone: 'dn-ok'
}, {
  name: 'wkr-image-pipeline',
  stream: 'TASK_QUEUES',
  filter: 'task.image-pipeline.>',
  ackPolicy: 'explicit',
  numPending: 4,
  numAckPending: 0,
  numWaiting: 0,
  numRedelivered: 0,
  deliveredSeq: 96,
  ackFloorSeq: 92,
  lag: 4,
  ackWait: '60s',
  maxDeliver: '-1',
  tone: 'dn-danger'
}, {
  name: 'wkr-hello-world',
  stream: 'TASK_QUEUES',
  filter: 'task.hello-world.>',
  ackPolicy: 'explicit',
  numPending: 0,
  numAckPending: 2,
  numWaiting: 2,
  numRedelivered: 0,
  deliveredSeq: 312,
  ackFloorSeq: 312,
  lag: 0,
  ackWait: '30s',
  maxDeliver: '-1',
  tone: 'dn-ok'
}, {
  name: 'wkr-retry-errors',
  stream: 'TASK_QUEUES',
  filter: 'task.retry-errors.>',
  ackPolicy: 'explicit',
  numPending: 0,
  numAckPending: 0,
  numWaiting: 1,
  numRedelivered: 12,
  deliveredSeq: 16,
  ackFloorSeq: 16,
  lag: 0,
  ackWait: '30s',
  maxDeliver: '-1',
  tone: 'dn-warn'
}, {
  name: 'wkr-cron',
  stream: 'TASK_QUEUES',
  filter: 'task.cron-trigger.>',
  ackPolicy: 'explicit',
  numPending: 0,
  numAckPending: 1,
  numWaiting: 0,
  numRedelivered: 0,
  deliveredSeq: 47,
  ackFloorSeq: 47,
  lag: 0,
  ackWait: '30s',
  maxDeliver: '-1',
  tone: 'dn-ok'
}];

// Per-consumer lag trend (Datatype line). Healthy consumers stay flat-low;
// the alarm consumer rises to its current backlog.
export const CONSUMER_LAG_SPARK: Record<string, string> = {
  orchestrator: '{l:0,0,1,0,0,1,0,0}',
  'event-correlator': '{l:0,1,0,0,1,0,0,0}',
  'sleep-timer': '{l:0,0,0,1,0,0,0,0}',
  'scheduled-run-timer': '{l:0,0,1,0,1,0,1,1}',
  console_metrics_aggregator: '{l:0,0,0,0,0,0,0,0}',
  'wkr-image-pipeline': '{l:0,1,1,2,2,3,4,4}',
  'wkr-hello-world': '{l:0,0,1,0,0,1,0,0}',
  'wkr-retry-errors': '{l:1,0,1,1,0,1,0,0}',
  'wkr-cron': '{l:0,0,0,1,0,0,0,0}'
};

// ---- Server health ----
// From the embedded NATS monitor port :8222 — /varz, /jsz, /healthz. Field
// names mirror the real monitoring endpoints so the view reads as live.
export interface ServerHealth {
  identity: {
    version: string;
    healthz: string;
    uptime: string;
    store: string;
    storeDir: string;
    listen: string;
    monitor: string;
  };
  traffic: {
    connections: number;
    totalConnections: number;
    subscriptions: number;
    inMsgs: string;
    outMsgs: string;
    inBytes: string;
    outBytes: string;
    slowConsumers: number;
  };
  host: {
    mem: string;
    cpu: string;
    cores: number;
  };
  jetstream: {
    storageUsed: string;
    storageMax: string;
    storagePct: number;
    memoryUsed: string;
    streams: number;
    consumers: number;
    messages: string;
    bytes: string;
    apiTotal: string;
    apiErrors: number;
  };
  slowConsumerStats: {
    clients: number;
    routes: number;
    leafs: number;
  };
}
export const SERVER_HEALTH: ServerHealth = {
  identity: {
    version: 'nats-server 2.12.1',
    healthz: 'ok',
    uptime: '14d 3h 22m',
    store: 'file',
    storeDir: '/var/lib/dagnats/jetstream',
    listen: '127.0.0.1:4222',
    monitor: '127.0.0.1:8222'
  },
  traffic: {
    connections: 7,
    totalConnections: 41,
    subscriptions: 128,
    inMsgs: '2.4M',
    outMsgs: '2.9M',
    inBytes: '1.1 GiB',
    outBytes: '1.4 GiB',
    slowConsumers: 0
  },
  host: {
    mem: '182 MB',
    cpu: '3.4%',
    cores: 8
  },
  jetstream: {
    storageUsed: '1.9 GiB',
    storageMax: '10 GiB',
    storagePct: 19,
    memoryUsed: '2 MB',
    streams: 8,
    consumers: 9,
    messages: '9,283',
    bytes: '24 MB',
    apiTotal: '48,201',
    apiErrors: 0
  },
  slowConsumerStats: {
    clients: 0,
    routes: 0,
    leafs: 0
  }
};
// connections trend (Datatype line) + JetStream storage pie expr (0-100).
export const SERVER_CONNECTIONS_SPARK = '{l:5,6,6,7,7,6,7,7}';
export const SERVER_STORAGE_PIE = '{p:19}';

// ---- KV ----
// A role-grouped catalog of the ~18 JetStream KV buckets dagnats creates
// (natsutil/conn.go). Every bucket is History:1 — there is no revision history —
// and dagnats exposes no external KV read API, so this is a catalog, not an
// inspector. The load-bearing fact per bucket is its TTL (the behavior
// contract). The richest buckets cross-link to where they are best inspected.
export type Churn = 'high' | 'med' | 'low';
export interface CatalogBucket {
  name: string;
  ttl: string;
  ttlMeaning: string;
  history: number;
  churn: Churn;
  purpose: string;
  crosslink?: string;
}
export interface CatalogRole {
  role: string;
  note?: string;
  buckets: CatalogBucket[];
}
export const KV_CATALOG: CatalogRole[] = [{
  role: 'Definitions',
  buckets: [{
    name: 'workflow_defs',
    ttl: '—',
    ttlMeaning: 'permanent',
    history: 1,
    churn: 'low',
    purpose: 'Registered workflow DAGs'
  }, {
    name: 'trigger_types',
    ttl: '—',
    ttlMeaning: 'permanent',
    history: 1,
    churn: 'low',
    purpose: 'External trigger type schemas (watched)'
  }, {
    name: 'triggers',
    ttl: '—',
    ttlMeaning: 'permanent',
    history: 1,
    churn: 'low',
    purpose: 'Active trigger definitions (watched)'
  }]
}, {
  role: 'Runtime state',
  buckets: [{
    name: 'workflow_runs',
    ttl: '—',
    ttlMeaning: 'permanent until archived',
    history: 1,
    churn: 'low',
    purpose: 'Run state snapshots'
  }, {
    name: 'checkpoints',
    ttl: '—',
    ttlMeaning: 'permanent',
    history: 1,
    churn: 'med',
    purpose: 'Step checkpoints for resume'
  }, {
    name: 'event_waiters',
    ttl: '—',
    ttlMeaning: 'cleared on match',
    history: 1,
    churn: 'high',
    purpose: 'Wait-for-event correlation index (watched)'
  }, {
    name: 'signals',
    ttl: '—',
    ttlMeaning: 'permanent',
    history: 1,
    churn: 'med',
    purpose: 'External signal state'
  }]
}, {
  role: 'Liveness',
  note: 'TTL = the heartbeat/staleness contract.',
  buckets: [{
    name: 'workers',
    ttl: '60s',
    ttlMeaning: 'worker heartbeat — key expires → worker goes stale',
    history: 1,
    churn: 'high',
    purpose: 'Worker registry',
    crosslink: 'Services'
  }, {
    name: 'worker_status',
    ttl: '120s',
    ttlMeaning: 'per-worker counter snapshots expire',
    history: 1,
    churn: 'high',
    purpose: 'Task counters per worker',
    crosslink: 'Workers'
  }, {
    name: 'services',
    ttl: '—',
    ttlMeaning: 'permanent',
    history: 1,
    churn: 'low',
    purpose: 'Service roster (ADR-017)',
    crosslink: 'Services'
  }]
}, {
  role: 'Idempotency / dedup',
  buckets: [{
    name: 'idempotency_keys',
    ttl: '24h',
    ttlMeaning: 'request replay window',
    history: 1,
    churn: 'med',
    purpose: 'Generic idempotency'
  }, {
    name: 'http_idempotency',
    ttl: '1h',
    ttlMeaning: 'HTTP trigger dedup window',
    history: 1,
    churn: 'med',
    purpose: 'HTTP trigger replay guard'
  }, {
    name: 'singleton_locks',
    ttl: '—',
    ttlMeaning: 'held until released',
    history: 1,
    churn: 'med',
    purpose: 'Singleton workflow locks',
    crosslink: 'Concurrency'
  }]
}, {
  role: 'Admission control',
  note: 'Inspect these as Concurrency, not raw KV.',
  buckets: [{
    name: 'concurrency_tasks',
    ttl: '—',
    ttlMeaning: 'live counters',
    history: 1,
    churn: 'med',
    purpose: 'Per-task-type slot counters',
    crosslink: 'Concurrency'
  }, {
    name: 'rate_limits',
    ttl: '—',
    ttlMeaning: 'token buckets',
    history: 1,
    churn: 'med',
    purpose: 'Token-bucket state',
    crosslink: 'Concurrency'
  }, {
    name: 'debounce_state',
    ttl: '14d',
    ttlMeaning: 'stale timer cleanup',
    history: 1,
    churn: 'low',
    purpose: 'Trigger debounce windows',
    crosslink: 'Concurrency'
  }]
}, {
  role: 'Scheduling / affinity',
  buckets: [{
    name: 'scheduled_runs',
    ttl: '—',
    ttlMeaning: 'until fired',
    history: 1,
    churn: 'low',
    purpose: 'Pending scheduled runs'
  }, {
    name: 'sticky_bindings',
    ttl: '25h',
    ttlMeaning: 'affinity lifetime',
    history: 1,
    churn: 'low',
    purpose: 'Sticky worker assignment'
  }]
}, {
  role: 'Approvals',
  buckets: [{
    name: 'approval_tokens',
    ttl: '168h',
    ttlMeaning: 'approval expiry (7d)',
    history: 1,
    churn: 'low',
    purpose: 'Human-approval tokens'
  }]
}];
export const KV_CATALOG_NOTE = "Catalog, not an inspector. Every bucket is History:1 (no revision history) and dagnats exposes no external KV read API today — values are written/read internally. What's load-bearing here is the TTL: it is the behavior contract (e.g. workers 60s = liveness). The richest buckets (locks, rate limits, counters, waiters) are best inspected in Concurrency / Services.";

// ---- DLQ ----
export interface DlqRow {
  id: string;
  workflow: string;
  step: string;
  error: string;
  age: string;
  eligible: boolean;
}
export const DLQ: DlqRow[] = [{
  id: 'dl-6689ab0b',
  workflow: 'retry-errors',
  step: 'fetch',
  error: 'dial tcp 10.0.0.4:443: connect: connection refused',
  age: '6m',
  eligible: true
}, {
  id: 'dl-8b8485fb',
  workflow: 'retry-errors',
  step: 'fetch',
  error: 'context deadline exceeded (Client.Timeout)',
  age: '21h',
  eligible: false
}];
export const DLQ_HEADERS = `Nats-Msg-Id: 8b8485fb-fetch-3
Nats-Stream: TASK_QUEUES
Nats-Delivered: 5
Dn-Workflow: retry-errors
Dn-Step: fetch
Dn-Run-Id: 8b8485fb`;
export const DLQ_PAYLOAD = `{
  "run_id": "8b8485fb",
  "step": "fetch",
  "args": { "url": "https://flaky.example/api", "retries": 0 }
}`;
export const DLQ_STACK = `connection refused
  worker/exec.go:142  runTask
  worker/exec.go:88   (*Runner).dispatch
  net/http transport.go:1607  roundTrip`;

// ---- Logs ----
// TELEMETRY JetStream stream, subjects telemetry.logs.{service}.{severity}.
// Each line carries the W3C trace id (clickable → span tree) plus structured
// attributes (run_id / step_id / task) rendered as chips.
export type Severity = 'debug' | 'info' | 'warn' | 'error';
export type LogService = 'engine' | 'worker' | 'api' | 'trigger-svc';
export interface LogRow {
  ts: string;
  sev: Severity;
  service: LogService;
  trace: string;
  span: string;
  message: string;
  // structured attributes surfaced as chips
  runId?: string;
  stepId?: string;
  task?: string;
}
export const LOGS: LogRow[] = [{
  ts: '17:20:01.882',
  sev: 'error',
  service: 'worker',
  trace: '4bf92f3577b3',
  span: 'd5e1a7c0',
  message: 'task fetch failed after 5 deliveries → DLQ',
  runId: 'a662ac7e',
  task: 'fetch'
}, {
  ts: '17:20:01.640',
  sev: 'warn',
  service: 'engine',
  trace: '00f067aa0ba9',
  span: 'b7ad6b71',
  message: 'step fetch NAK with delay=2s (attempt 4/5)',
  runId: 'a662ac7e',
  stepId: 'fetch'
}, {
  ts: '17:20:01.231',
  sev: 'info',
  service: 'engine',
  trace: 'a662ac7eddee',
  span: 'a662ac7e',
  message: 'run a662ac7e started · workflow=cron-trigger',
  runId: 'a662ac7e'
}, {
  ts: '17:20:00.998',
  sev: 'info',
  service: 'trigger-svc',
  trace: 'c3f1b2009aa1',
  span: 'c3f1b200',
  message: 'cron trigger fired · */5 * * * *'
}, {
  ts: '17:20:00.512',
  sev: 'debug',
  service: 'worker',
  trace: '7c04aa18bb02',
  span: '7c04aa18',
  message: 'heartbeat → workers KV (group=image-pipeline)',
  task: 'wkr-3a1f'
}, {
  ts: '17:19:59.330',
  sev: 'info',
  service: 'api',
  trace: '8aaf9b3a1100',
  span: '8aaf9b3a',
  message: 'registerWorkflow ok · image-pipeline (3 steps)'
}, {
  ts: '17:19:58.114',
  sev: 'info',
  service: 'engine',
  trace: '8aaf9b3a1100',
  span: 'f12c9a04',
  message: 'run 8aaf9b3a completed · duration=2.1s',
  runId: '8aaf9b3a'
}, {
  ts: '17:19:57.001',
  sev: 'debug',
  service: 'trigger-svc',
  trace: 'c3f1b2009aa1',
  span: 'c3f1b200',
  message: 'evaluated 5 triggers in 1.2ms'
}];

// per-service line counts for the footer (engine/worker/api/trigger-svc)
export const LOG_SOURCES: [string, number][] = [['engine', 2841], ['worker', 1024], ['trigger-svc', 312], ['api', 188]];

// ---- Metrics ----
// Real engine/metrics.go series. Counters/gauges/histogram exported via OTel
// Meter → NATS telemetry.metrics.* (delta) → in-console aggregator → SSE 4 Hz;
// also scraped at GET /metrics (Prometheus). Sparks are Datatype expressions.
export interface MetricSeries {
  // OTel instrument name as registered in engine/metrics.go
  metric: string;
  // workflow/step labels, illustrative
  labels: string;
  value: string;
  spark: string;
  color: string;
}
export const METRIC_SERIES: MetricSeries[] = [{
  metric: 'workflow.runs.completed',
  labels: 'workflow=*',
  value: '142',
  spark: '{l:30,45,40,55,60,52,70,65,80,90}',
  color: 'var(--dn-teal)'
}, {
  metric: 'workflow.runs.active',
  labels: 'workflow=*',
  value: '12',
  spark: '{l:8,10,9,14,12,11,15,12,13,12}',
  color: 'var(--dn-teal)'
}, {
  metric: 'workflow.runs.failed',
  labels: 'workflow=retry-errors',
  value: '4',
  spark: '{b:0,1,0,0,1,0,2,0,1,3}',
  color: '#ff8a93'
}, {
  metric: 'dlq.depth',
  labels: 'stream=DEAD_LETTERS',
  value: '2',
  spark: '{l:0,0,1,1,1,2,2,2,2,2}',
  color: 'var(--dn-amber)'
}];

// step.enqueue counter trend (its own card)
export const STEP_ENQUEUE_SPARK = '{l:20,40,30,50,45,60,55,70,65,75}';
// task.concurrency.acquired counter. There is NO task.concurrency.rejected
// counter — rejections are only meaningful per-gate (singleton Reject mode,
// rate-limit throttles), surfaced in the Admission control view.
export const CONCURRENCY_ACQUIRED = 312;
// snapshot.save.duration_ms histogram percentiles (labels: workflow, step)
export const SNAPSHOT_LATENCY = {
  p50: '0.8',
  p95: '2.1',
  p99: '3.4'
};

// ---- Audit log ----
// Real audit subsystem (ADR-014): every state-changing console action is
// written to the console_audit JetStream KV bucket (90-day TTL). The console
// records not just successes but `denied` attempts (mutation blocked while
// CONSOLE_READ_ONLY is on) and `failed` ones. The action vocabulary is exactly
// the seven mutating endpoints the console exposes — nothing else.
export type AuditTargetKind = 'dlq' | 'trigger' | 'workflow' | 'run';
export type AuditOutcome = 'success' | 'denied' | 'failed';
export interface AuditEvent {
  ts: string;
  actor: string;
  action: string;
  target: string;
  targetKind: AuditTargetKind;
  outcome: AuditOutcome;
  // JSON-ish event payload (AuditEvent.Data)
  data: string;
}
export const AUDIT: AuditEvent[] = [{
  ts: '17:18:42',
  actor: 'dan',
  action: 'dlq.retry',
  target: 'dl-6689ab0b',
  targetKind: 'dlq',
  outcome: 'success',
  data: '{"seq":6689,"stream":"DEAD_LETTERS","redrive_to":"TASK_QUEUES"}'
}, {
  ts: '17:12:09',
  actor: 'dan',
  action: 'trigger.disable',
  target: 'trg-gh-hook',
  targetKind: 'trigger',
  outcome: 'success',
  data: '{"trigger_id":"trg-gh-hook","was_enabled":true}'
}, {
  ts: '17:05:51',
  actor: 'maya',
  action: 'dlq.retry',
  target: 'dl-8b8485fb',
  targetKind: 'dlq',
  outcome: 'denied',
  data: '{"reason":"console_read_only"}'
}, {
  ts: '16:58:31',
  actor: 'ci-bot',
  action: 'workflow.run',
  target: 'image-pipeline',
  targetKind: 'workflow',
  outcome: 'success',
  data: '{"workflow":"image-pipeline","run_id":"8aaf9b3a","trigger":"manual"}'
}, {
  ts: '16:40:02',
  actor: 'maya',
  action: 'workflow.run',
  target: 'retry-errors',
  targetKind: 'workflow',
  outcome: 'failed',
  data: '{"workflow":"retry-errors","run_id":"6689ab0b","error":"step fetch failed after 5 deliveries"}'
}, {
  ts: '15:22:17',
  actor: 'dan',
  action: 'trigger.fire.manual',
  target: 'trg-echo',
  targetKind: 'trigger',
  outcome: 'success',
  data: '{"trigger_id":"trg-echo","run_id":"f87980ac"}'
}, {
  ts: '14:05:55',
  actor: 'dan',
  action: 'dlq.discard',
  target: 'dl-99cab1',
  targetKind: 'dlq',
  outcome: 'success',
  data: '{"seq":99,"stream":"DEAD_LETTERS"}'
}, {
  ts: '13:48:10',
  actor: 'ci-bot',
  action: 'trigger.enable',
  target: 'trg-report',
  targetKind: 'trigger',
  outcome: 'denied',
  data: '{"reason":"console_read_only"}'
}, {
  ts: '13:11:08',
  actor: 'dan',
  action: 'trigger.fire.manual',
  target: 'trg-cron-nightly',
  targetKind: 'trigger',
  outcome: 'success',
  data: '{"trigger_id":"trg-cron-nightly","run_id":"a662ac7e"}'
}];

// The complete, bounded set of state-changing operations the console exposes.
export const AUDIT_ACTION_SET = ['dlq.retry', 'dlq.discard', 'dlq.undo-discard', 'trigger.enable', 'trigger.disable', 'trigger.fire.manual', 'workflow.run'] as const;

// Gated write actions added by the ADR-022 gating ladder. Tier 1 is graceful /
// reversible (gated by read-only off); tier 2 is destructive / bounded (gated
// by read-only off AND CONSOLE_ALLOW_DESTRUCTIVE, plus dry-run + typed confirm).
export const AUDIT_GATED_ACTIONS = ['worker.drain', 'worker.resume', 'worker.decommission', 'conn.drain', 'server.lameduck', 'stream.backup', 'stream.purge', 'kv.purge'] as const;
export const AUDIT_META = {
  bucket: 'console_audit',
  ttl: '90 days',
  authMode: 'forward-auth',
  authDetail: 'actor resolved from X-Forwarded-User / X-Forwarded-Email',
  actionSet: [...AUDIT_ACTION_SET, ...AUDIT_GATED_ACTIONS] as readonly string[]
};
export const AUDIT_ACTORS = ['dan', 'maya', 'ci-bot'];

// ---- Dashboard panels ----
export const RECENT_FAILURES: [string, string, string][] = [['6689ab0b', 'retry-errors', 'dial tcp 10.0.0.4:443: connection refused'], ['8b8485fb', 'retry-errors', 'context deadline exceeded (Client.Timeout)']];
export const RECENT_ACTIONS: [string, string, string][] = [['17:18:42', 'dan', 'dlq.retry → dl-6689ab0b'], ['17:12:09', 'dan', 'trigger.disable → trg-gh-hook'], ['16:58:31', 'ci-bot', 'workflow.run → image-pipeline']];

// ---- Admission control ----
// Every gate that decides whether a task runs now, waits, or is shed. dagnats
// has four real gates: a per-task-type slot pool (ConcurrencyManager), singleton
// locks (engine/admission.go), token-bucket rate limits (engine/ratelimit.go),
// and trigger debounce (trigger/subject.go). Sleep timers are durable waits,
// NOT an admission gate — they are shown de-emphasized for contrast.

// Per-task-type CAS slot pool (ConcurrencyManager, concurrency_tasks KV).
// limit null = unlimited (rendered "∞").
export interface SlotLimiter {
  taskType: string;
  inUse: number;
  limit: number | null;
  waiting: number;
  tone: Tone;
}
export const SLOT_LIMITERS: SlotLimiter[] = [{
  taskType: 'image-pipeline::fetch-urls',
  inUse: 1,
  limit: 10,
  waiting: 0,
  tone: 'dn-ok'
}, {
  taskType: 'retry-errors::fetch',
  inUse: 2,
  limit: 2,
  waiting: 1,
  tone: 'dn-warn'
}, {
  taskType: 'hello-world::greet',
  inUse: 2,
  limit: null,
  waiting: 0,
  tone: 'dn-ok'
}, {
  taskType: 'agent-loop::enrich',
  inUse: 1,
  limit: 3,
  waiting: 0,
  tone: 'dn-ok'
}];

// Per-workflow + dotpath-keyed singleton locks (engine/admission.go,
// singleton_locks KV). Modes: Cancel / Queue / Reject.
export interface SingletonLock {
  key: string;
  scope: 'workflow' | 'keyed';
  heldBy: string;
  mode: 'Cancel' | 'Queue' | 'Reject';
  held: string;
  queued: number;
  rejected: number;
  tone: Tone;
}
export const SINGLETON_LOCKS: SingletonLock[] = [{
  key: 'nightly-report',
  scope: 'workflow',
  heldBy: '4f1abc02',
  mode: 'Queue',
  held: '2m10s',
  queued: 1,
  rejected: 0,
  tone: 'dn-info'
}, {
  key: 'reindex-search:tenant-acme',
  scope: 'keyed',
  heldBy: 'c19f44de',
  mode: 'Reject',
  held: '14s',
  queued: 0,
  rejected: 2,
  tone: 'dn-warn'
}, {
  key: 'archive-runs',
  scope: 'workflow',
  heldBy: '—',
  mode: 'Cancel',
  held: '—',
  queued: 0,
  rejected: 0,
  tone: 'dn-ok'
}];

// Token-bucket rate limits (engine/ratelimit.go). Returns retryAfter → a
// SleepTimer reschedules the task. tokens 0 = currently throttling.
export interface RateLimiter {
  key: string;
  tokens: number;
  limit: number;
  period: string;
  retryAfter: string;
  tone: Tone;
}
export const RATE_LIMITERS: RateLimiter[] = [{
  key: 'image-pipeline::fetch',
  tokens: 0,
  limit: 20,
  period: '/min',
  retryAfter: '3.2s',
  tone: 'dn-amber'
}, {
  key: 'fanout-notify::send',
  tokens: 84,
  limit: 100,
  period: '/min',
  retryAfter: '—',
  tone: 'dn-ok'
}];

// Per-trigger debounce windows (trigger/subject.go, debounce_state KV 14d).
export interface Debouncer {
  trigger: string;
  subject: string;
  windowMs: number;
  absorbed: number;
  firesIn: string;
  tone: Tone;
}
export const DEBOUNCERS: Debouncer[] = [{
  trigger: 'trg-photos-in',
  subject: 'photos.incoming.*',
  windowMs: 5000,
  absorbed: 3,
  firesIn: '1.8s',
  tone: 'dn-teal'
}];

// Runs currently waiting on a gate. gate names the PRECISE gate; gateKind
// colors the cell. The 'sleep' row is a durable wait, not an admission gate.
export type GateKind = 'slot' | 'lock' | 'rate' | 'sleep';
export interface BlockedRow {
  id: string;
  workflow: string;
  step: string;
  gate: string;
  gateKind: GateKind;
  waiting: string;
  tone: Tone;
}
export const BLOCKED: BlockedRow[] = [{
  id: 'b1c0',
  workflow: 'retry-errors',
  step: 'fetch',
  gate: 'global slot · retry-errors::fetch (2/2 full)',
  gateKind: 'slot',
  waiting: '1.1s',
  tone: 'dn-warn'
}, {
  id: 'a662ac7e',
  workflow: 'nightly-report',
  step: '—',
  gate: 'singleton lock · nightly-report (Queue, held by 4f1abc02)',
  gateKind: 'lock',
  waiting: '2.4s',
  tone: 'dn-info'
}, {
  id: '7be0a210',
  workflow: 'image-pipeline',
  step: 'fetch',
  gate: 'rate limit · image-pipeline::fetch (retry 3.2s)',
  gateKind: 'rate',
  waiting: '3.2s',
  tone: 'dn-amber'
}, {
  id: '8aaf9b3a',
  workflow: 'image-pipeline',
  step: 'cooldown',
  gate: 'sleep timer 0.6s — durable wait, not an admission gate',
  gateKind: 'sleep',
  waiting: '0.6s',
  tone: ''
}];

// The teaching moment: free slots + pending work is a worker-starvation
// problem, never an admission problem.
export const WORKER_STARVATION_NOTE = 'image-pipeline::fetch-urls — slot pool 1/10 (9 free), yet task.image-pipeline.> has 4 pending with 0 waiting pulls. The gate is open: this is worker-starvation, not admission control. The fetch-urls worker (wkr-7c04) is idle 2m14s → see Connections / Workers. Free slots + pending work is never a concurrency problem.';

// MaxSteps is defined but not a live runtime admission gate — the honesty fix.
export const MAXSTEPS_NOTE = 'MaxSteps is validated at definition time but not enforced at runtime — it is not a live admission gate, so it does not appear here.';

// ---- Worker detail ----
// Per-worker drill-down. The pull/execute/ack model is made visible: a worker
// fetches tasks for its registered functions, executes them, and acks before
// AckWait elapses — otherwise the task redelivers to another worker.
export interface WorkerDetail {
  id: string;
  group: string;
  host: string;
  uptime: string;
  lastHeartbeat: string;
  processed1h: string;
  inflight: string;
  redelivered: string;
  dedupHits: string;
}
// fn rows: [service::name, pending, inflight, processed1h, avg, failPct]
export interface WorkerFnRow {
  name: string;
  pending: string;
  inflight: string;
  processed1h: string;
  avg: string;
  failPct: string;
}
// in-flight task rows: [runId, fn, started, ackWaitRemaining]
export interface WorkerTaskRow {
  runId: string;
  fn: string;
  started: string;
  ackWaitRemaining: string;
}
export interface WorkerDetailBundle {
  detail: WorkerDetail;
  fns: WorkerFnRow[];
  tasks: WorkerTaskRow[];
}
export const WORKER_DETAILS: Record<string, WorkerDetailBundle> = {
  'wkr-3a1f': {
    detail: {
      id: 'wkr-3a1f',
      group: 'hello-world',
      host: '10.0.1.14',
      uptime: '4h12m',
      lastHeartbeat: '2s ago',
      processed1h: '312',
      inflight: '2',
      redelivered: '4',
      dedupHits: '7'
    },
    fns: [{
      name: 'hello-world::greet',
      pending: '0',
      inflight: '1',
      processed1h: '163',
      avg: '0.2s',
      failPct: '0.0%'
    }, {
      name: 'hello-world::uppercase',
      pending: '0',
      inflight: '1',
      processed1h: '149',
      avg: '0.1s',
      failPct: '0.0%'
    }],
    tasks: [{
      runId: 'a662ac7e',
      fn: 'hello-world::greet',
      started: '17:20:01',
      ackWaitRemaining: '28s'
    }, {
      runId: '0809c77e',
      fn: 'hello-world::uppercase',
      started: '17:20:02',
      ackWaitRemaining: '24s'
    }]
  }
};
export const WORKER_FN_TRENDS: Record<string, string> = {
  'hello-world::greet': '{l:55,60,52,68,60,72,66,78,70,82}',
  'hello-world::uppercase': '{l:50,58,54,62,58,66,60,70,64,72}',
  'image-pipeline::fetch-urls': '{l:30,40,35,45,42,38,48,44,52,50}',
  'image-pipeline::fetch': '{l:28,36,32,42,38,46,42,50,46,52}',
  'image-pipeline::build-gallery': '{l:25,30,28,35,32,38,34,40,36,42}',
  'retry-errors::fetch': '{l:20,15,28,18,35,22,40,30,25,32}'
};
// Fallback bundle generator data for workers without an explicit detail entry.
export const WORKER_GROUP_FNS: Record<string, WorkerFnRow[]> = {
  'image-pipeline': [{
    name: 'image-pipeline::fetch-urls',
    pending: '0',
    inflight: '1',
    processed1h: '48',
    avg: '1.1s',
    failPct: '2.1%'
  }, {
    name: 'image-pipeline::fetch',
    pending: '1',
    inflight: '1',
    processed1h: '48',
    avg: '0.9s',
    failPct: '0.0%'
  }, {
    name: 'image-pipeline::build-gallery',
    pending: '0',
    inflight: '0',
    processed1h: '48',
    avg: '0.9s',
    failPct: '0.0%'
  }],
  'retry-errors': [{
    name: 'retry-errors::fetch',
    pending: '0',
    inflight: '0',
    processed1h: '16',
    avg: '0.3s',
    failPct: '100%'
  }],
  'hello-world': [{
    name: 'hello-world::greet',
    pending: '0',
    inflight: '1',
    processed1h: '163',
    avg: '0.2s',
    failPct: '0.0%'
  }, {
    name: 'hello-world::uppercase',
    pending: '0',
    inflight: '1',
    processed1h: '149',
    avg: '0.1s',
    failPct: '0.0%'
  }]
};

// ---- Trigger fire history (per-trigger drill-down) ----
export interface FireRow {
  time: string;
  runId: string;
  result: RunStatus;
}
export const TRIGGER_FIRES: Record<string, FireRow[]> = {
  'trg-cron-nightly': [{
    time: '17:20:00',
    runId: 'a662ac7e',
    result: 'run'
  }, {
    time: '17:15:00',
    runId: '4f1abc02',
    result: 'ok'
  }, {
    time: '17:10:00',
    runId: 'c19f44de',
    result: 'ok'
  }]
};
// Default fire history when a trigger has no explicit history.
export const TRIGGER_FIRES_DEFAULT: FireRow[] = [{
  time: '17:18:42',
  runId: '8aaf9b3a',
  result: 'ok'
}, {
  time: '16:58:31',
  runId: 'f87980ac',
  result: 'ok'
}, {
  time: '15:22:17',
  runId: '6689ab0b',
  result: 'fail'
}];
// Trigger types that can be fired on demand (subject triggers fire from NATS
// traffic, so a manual "Fire now" doesn't apply to them).
export const FIREABLE_TYPES: TriggerType[] = ['cron', 'webhook', 'http'];
// Workflow names available as trigger targets in the add/edit modal.
export const TRIGGER_TARGET_WORKFLOWS: string[] = WORKFLOWS.map(w => w.name);

// ---- Traces ----
// OTLP spans land on telemetry.spans.{service}.{runID}. Each run becomes one
// trace (W3C traceparent); `dagnats trace <run-id>` renders this tree in the
// CLI today — this view gives it a web home.
export type TraceStatus = 'ok' | 'err';
export interface TraceRow {
  id: string;
  rootOp: string;
  service: LogService;
  spans: number;
  duration: string;
  status: TraceStatus;
  started: string;
  runId: string;
}
export const TRACES: TraceRow[] = [{
  id: 'a662ac7eddee',
  rootOp: 'startRun image-pipeline',
  service: 'engine',
  spans: 7,
  duration: '2.41s',
  status: 'ok',
  started: '17:20:01',
  runId: 'a662ac7e'
}, {
  id: '8aaf9b3a1100',
  rootOp: 'startRun image-pipeline',
  service: 'engine',
  spans: 7,
  duration: '2.10s',
  status: 'ok',
  started: '17:19:58',
  runId: '8aaf9b3a'
}, {
  id: 'f87980ac22bd',
  rootOp: 'startRun image-pipeline',
  service: 'engine',
  spans: 7,
  duration: '2.40s',
  status: 'ok',
  started: '17:15:02',
  runId: 'f87980ac'
}, {
  id: '6689ab0b9c1e',
  rootOp: 'startRun retry-errors',
  service: 'engine',
  spans: 4,
  duration: '0.41s',
  status: 'err',
  started: '17:19:31',
  runId: '6689ab0b'
}, {
  id: 'c3f1b2009aa1',
  rootOp: 'reconcile cron-trigger',
  service: 'trigger-svc',
  spans: 3,
  duration: '0.05s',
  status: 'ok',
  started: '17:20:00',
  runId: 'a662ac7e'
}, {
  id: 'b5b0c77a3f02',
  rootOp: 'startRun hello-world',
  service: 'engine',
  spans: 5,
  duration: '0.31s',
  status: 'ok',
  started: '17:18:30',
  runId: 'b5b0c77a'
}];

// A span in the tree/waterfall. `depth` indents the name; offsetPct/widthPct
// position the gantt bar; barClass picks one of the four track colors.
export interface SpanRow {
  id: string;
  name: string;
  depth: number;
  offsetPct: number;
  widthPct: number;
  barClass: '' | 'b2' | 'b3' | 'b4';
  duration: string;
  status: TraceStatus;
  spanId: string;
  parentSpanId: string;
  workflowId: string;
  stepId: string;
  taskId: string;
  runId: string;
}
// Spans for trace a662ac7eddee (image-pipeline run). Mirrors the mockup tree:
// startRun → reconcile → step:fetch-urls → task.dispatch → worker exec →
// step:build-gallery → task.dispatch.
export const TRACE_SPANS: SpanRow[] = [{
  id: 's0',
  name: 'startRun',
  depth: 0,
  offsetPct: 0,
  widthPct: 100,
  barClass: '',
  duration: '2.41s',
  status: 'ok',
  spanId: 'a662ac7e00000001',
  parentSpanId: '—',
  workflowId: 'image-pipeline',
  stepId: '—',
  taskId: '—',
  runId: 'a662ac7e'
}, {
  id: 's1',
  name: 'reconcile',
  depth: 1,
  offsetPct: 0.5,
  widthPct: 1,
  barClass: 'b2',
  duration: '12ms',
  status: 'ok',
  spanId: 'a662ac7e00000002',
  parentSpanId: 'a662ac7e00000001',
  workflowId: 'image-pipeline',
  stepId: '—',
  taskId: '—',
  runId: 'a662ac7e'
}, {
  id: 's2',
  name: 'step:fetch-urls',
  depth: 1,
  offsetPct: 2,
  widthPct: 46,
  barClass: 'b3',
  duration: '1.10s',
  status: 'ok',
  spanId: 'b7ad6b7169203331',
  parentSpanId: '00f067aa0ba902b7',
  workflowId: 'image-pipeline',
  stepId: 'fetch-urls',
  taskId: 'image-pipeline::fetch-urls',
  runId: 'a662ac7e'
}, {
  id: 's3',
  name: 'task.dispatch fetch-urls',
  depth: 2,
  offsetPct: 3,
  widthPct: 8,
  barClass: 'b4',
  duration: '0.19s',
  status: 'ok',
  spanId: 'b7ad6b7169204410',
  parentSpanId: 'b7ad6b7169203331',
  workflowId: 'image-pipeline',
  stepId: 'fetch-urls',
  taskId: 'image-pipeline::fetch-urls',
  runId: 'a662ac7e'
}, {
  id: 's4',
  name: 'worker exec · wkr-3a1f',
  depth: 3,
  offsetPct: 4,
  widthPct: 6,
  barClass: '',
  duration: '0.16s',
  status: 'ok',
  spanId: 'b7ad6b7169205501',
  parentSpanId: 'b7ad6b7169204410',
  workflowId: 'image-pipeline',
  stepId: 'fetch-urls',
  taskId: 'image-pipeline::fetch-urls',
  runId: 'a662ac7e'
}, {
  id: 's5',
  name: 'step:build-gallery',
  depth: 1,
  offsetPct: 50,
  widthPct: 38,
  barClass: 'b3',
  duration: '0.90s',
  status: 'ok',
  spanId: 'c91d4f8820116602',
  parentSpanId: '00f067aa0ba902b7',
  workflowId: 'image-pipeline',
  stepId: 'build-gallery',
  taskId: 'image-pipeline::build-gallery',
  runId: 'a662ac7e'
}, {
  id: 's6',
  name: 'task.dispatch build-gallery',
  depth: 2,
  offsetPct: 51,
  widthPct: 34,
  barClass: 'b4',
  duration: '0.82s',
  status: 'ok',
  spanId: 'c91d4f8820117703',
  parentSpanId: 'c91d4f8820116602',
  workflowId: 'image-pipeline',
  stepId: 'build-gallery',
  taskId: 'image-pipeline::build-gallery',
  runId: 'a662ac7e'
}];

// ---- Services ----
// The live process roster. Health derives from heartbeat TTL on the
// workers (60s TTL) / worker_status (120s TTL) / services (ADR-017) KV buckets —
// this part is live TODAY. Per-endpoint $SRV.STATS (SERVICE_ENDPOINTS below) is
// the nats-micro adoption upgrade path, except the one real micro endpoint
// trigger-svc serves today: _REGISTRY.trigger_types.ack.
export type ServiceKind = 'engine' | 'api' | 'trigger' | 'worker-group';
export interface ServiceRow {
  name: string;
  kind: ServiceKind;
  version: string;
  commit: string;
  instances: number;
  group: string;
  status: 'online' | 'stale';
  lastSeen: string;
  note?: string;
}
export const SERVICES: ServiceRow[] = [{
  name: 'engine',
  kind: 'engine',
  version: 'v0.1.0',
  commit: '5824fe3',
  instances: 1,
  group: 'core',
  status: 'online',
  lastSeen: '1s',
  note: '—'
}, {
  name: 'api',
  kind: 'api',
  version: 'v0.1.0',
  commit: '5824fe3',
  instances: 1,
  group: 'core',
  status: 'online',
  lastSeen: '1s',
  note: '—'
}, {
  name: 'trigger-svc',
  kind: 'trigger',
  version: 'v0.1.0',
  commit: '5824fe3',
  instances: 1,
  group: 'core',
  status: 'online',
  lastSeen: '2s',
  note: '5 triggers'
}, {
  name: 'hello-world',
  kind: 'worker-group',
  version: 'v0.1.0',
  commit: '5824fe3',
  instances: 2,
  group: 'workers',
  status: 'online',
  lastSeen: '2s',
  note: 'greet, uppercase'
}, {
  name: 'image-pipeline',
  kind: 'worker-group',
  version: 'v0.1.0',
  commit: '5824fe3',
  instances: 1,
  group: 'workers',
  status: 'online',
  lastSeen: '5s',
  note: 'fetch-urls, fetch, build-gallery'
}, {
  name: 'retry-errors',
  kind: 'worker-group',
  version: 'v0.1.0',
  commit: '5824fe3',
  instances: 1,
  group: 'workers',
  status: 'online',
  lastSeen: '2s',
  note: 'fetch'
}];

// ---- Service endpoints ($SRV detail) ----
// Per-endpoint stats keyed by service name. `live:true` means the stats are
// real micro discovery TODAY (only _REGISTRY.trigger_types.ack qualifies);
// `live:false` rows are the illustrative shape $SRV.STATS would return once the
// service adopts the nats micro framework — marked clearly in the detail view.
export interface EndpointRow {
  subject: string;
  numRequests: number;
  numErrors: number;
  avgLatency: string;
  lastError: string;
  live: boolean;
}
export const SERVICE_ENDPOINTS: Record<string, {
  endpoints: EndpointRow[];
  live: boolean;
}> = {
  'trigger-svc': {
    live: true,
    endpoints: [{
      subject: '_REGISTRY.trigger_types.ack',
      numRequests: 312,
      numErrors: 0,
      avgLatency: '0.8ms',
      lastError: '—',
      live: true
    }]
  },
  engine: {
    live: false,
    endpoints: [{
      subject: 'startRun',
      numRequests: 240,
      numErrors: 2,
      avgLatency: '4.1ms',
      lastError: '—',
      live: false
    }, {
      subject: 'reconcile',
      numRequests: 1284,
      numErrors: 0,
      avgLatency: '0.6ms',
      lastError: '—',
      live: false
    }]
  },
  api: {
    live: false,
    endpoints: [{
      subject: 'registerWorkflow',
      numRequests: 96,
      numErrors: 0,
      avgLatency: '2.2ms',
      lastError: '—',
      live: false
    }, {
      subject: 'invokeFunction',
      numRequests: 18,
      numErrors: 1,
      avgLatency: '1.1ms',
      lastError: 'schema validation failed',
      live: false
    }]
  },
  'hello-world': {
    live: false,
    endpoints: [{
      subject: 'greet',
      numRequests: 163,
      numErrors: 0,
      avgLatency: '0.2s',
      lastError: '—',
      live: false
    }, {
      subject: 'uppercase',
      numRequests: 149,
      numErrors: 0,
      avgLatency: '0.1s',
      lastError: '—',
      live: false
    }]
  },
  'image-pipeline': {
    live: false,
    endpoints: [{
      subject: 'fetch-urls',
      numRequests: 48,
      numErrors: 1,
      avgLatency: '1.1s',
      lastError: 'dial tcp: connection refused',
      live: false
    }, {
      subject: 'fetch',
      numRequests: 48,
      numErrors: 0,
      avgLatency: '0.9s',
      lastError: '—',
      live: false
    }, {
      subject: 'build-gallery',
      numRequests: 48,
      numErrors: 0,
      avgLatency: '0.9s',
      lastError: '—',
      live: false
    }]
  },
  'retry-errors': {
    live: false,
    endpoints: [{
      subject: 'fetch',
      numRequests: 16,
      numErrors: 16,
      avgLatency: '0.3s',
      lastError: 'context deadline exceeded',
      live: false
    }]
  }
};

// ---- Connections (/connz) ----
// Every client connected to the embedded NATS server. pending_bytes is the
// slow-consumer signal (0 everywhere here — healthy); idle is time since last
// activity. The signal to surface is wkr-7c04's high idle (2m14s), which is the
// upstream of the image-pipeline task backlog seen in Consumers — distinct from
// a slow consumer (which would show non-zero pending_bytes).
export type ConnKind = 'engine' | 'api' | 'trigger' | 'worker' | 'cli';
export interface ConnRow {
  cid: number;
  name: string;
  kind: ConnKind;
  lang: string;
  version: string;
  rtt: string;
  uptime: string;
  idle: string;
  subs: number;
  pendingBytes: string;
  inMsgs: string;
  outMsgs: string;
  tone: Tone;
}
export const CONN_PENDING_NOTE = 'pending_bytes is the slow-consumer signal — it is 0 on every connection here, so there are 0 slow consumers. idle is time since last activity; wkr-7c04 standing idle at 2m14s is the signal worth watching.';
export const CONNECTIONS: ConnRow[] = [{
  cid: 1,
  name: 'engine',
  kind: 'engine',
  lang: 'go',
  version: 'nats.go 1.37',
  rtt: '42µs',
  uptime: '14d',
  idle: '0s',
  subs: 18,
  pendingBytes: '0 B',
  inMsgs: '1.2M',
  outMsgs: '1.4M',
  tone: 'dn-ok'
}, {
  cid: 2,
  name: 'api',
  kind: 'api',
  lang: 'go',
  version: 'nats.go 1.37',
  rtt: '55µs',
  uptime: '14d',
  idle: '0s',
  subs: 6,
  pendingBytes: '0 B',
  inMsgs: '210k',
  outMsgs: '142k',
  tone: 'dn-ok'
}, {
  cid: 3,
  name: 'trigger-svc',
  kind: 'trigger',
  lang: 'go',
  version: 'nats.go 1.37',
  rtt: '61µs',
  uptime: '14d',
  idle: '1s',
  subs: 4,
  pendingBytes: '0 B',
  inMsgs: '88k',
  outMsgs: '96k',
  tone: 'dn-ok'
}, {
  cid: 7,
  name: 'wkr-3a1f',
  kind: 'worker',
  lang: 'go',
  version: 'nats.go 1.37',
  rtt: '88µs',
  uptime: '4h12m',
  idle: '2s',
  subs: 3,
  pendingBytes: '0 B',
  inMsgs: '14k',
  outMsgs: '13k',
  tone: 'dn-ok'
}, {
  cid: 8,
  name: 'wkr-9b22',
  kind: 'worker',
  lang: 'go',
  version: 'nats.go 1.37',
  rtt: '91µs',
  uptime: '4h10m',
  idle: '1s',
  subs: 3,
  pendingBytes: '0 B',
  inMsgs: '13k',
  outMsgs: '12k',
  tone: 'dn-ok'
}, {
  cid: 11,
  name: 'wkr-7c04',
  kind: 'worker',
  lang: 'go',
  version: 'nats.go 1.37',
  rtt: '210µs',
  uptime: '4h08m',
  idle: '2m14s',
  subs: 3,
  pendingBytes: '0 B',
  inMsgs: '4.8k',
  outMsgs: '4.8k',
  tone: 'dn-warn'
}, {
  cid: 14,
  name: 'wkr-1d8e',
  kind: 'worker',
  lang: 'go',
  version: 'nats.go 1.37',
  rtt: '77µs',
  uptime: '4h11m',
  idle: '2s',
  subs: 3,
  pendingBytes: '0 B',
  inMsgs: '1.6k',
  outMsgs: '1.6k',
  tone: 'dn-ok'
}, {
  cid: 22,
  name: 'cli',
  kind: 'cli',
  lang: 'go',
  version: 'nats.go 1.37',
  rtt: '120µs',
  uptime: '8s',
  idle: '0s',
  subs: 1,
  pendingBytes: '0 B',
  inMsgs: '12',
  outMsgs: '4',
  tone: 'dn-ok'
}];

// ---- Config: engine invariants ----
// The operational constants that govern the deployment are compile-time
// constants, NOT config. They live across Consumers (AckWait), KV (TTLs) and
// Streams (dedup windows) — surfaced here as the deployment's fixed contract.
export interface EngineInvariant {
  name: string;
  value: string;
  governs: string;
  source: 'hardcoded';
}
export const ENGINE_INVARIANTS: EngineInvariant[] = [{
  name: 'AckWait',
  value: '30s',
  governs: 'consumer ack timeout before redelivery',
  source: 'hardcoded'
}, {
  name: 'MaxDeliver',
  value: '-1 (unlimited)',
  governs: 'engine retries via NakWithDelay, not consumer redelivery cap',
  source: 'hardcoded'
}, {
  name: 'WORKFLOW_HISTORY dedup',
  value: '5s',
  governs: 'duplicate-publish window on history.>',
  source: 'hardcoded'
}, {
  name: 'DEAD_LETTERS dedup',
  value: '24h',
  governs: 'dead-letter dedup window',
  source: 'hardcoded'
}, {
  name: 'TELEMETRY retention',
  value: '7 days / 1 GiB',
  governs: 'telemetry stream age/size cap',
  source: 'hardcoded'
}, {
  name: 'TELEMETRY dedup',
  value: '5s',
  governs: 'telemetry duplicate window',
  source: 'hardcoded'
}, {
  name: 'workers KV TTL',
  value: '60s',
  governs: 'worker heartbeat liveness — key expiry = stale',
  source: 'hardcoded'
}, {
  name: 'worker_status KV TTL',
  value: '120s',
  governs: 'per-worker counter snapshot expiry',
  source: 'hardcoded'
}, {
  name: 'idempotency_keys TTL',
  value: '24h',
  governs: 'generic idempotency replay window',
  source: 'hardcoded'
}, {
  name: 'http_idempotency TTL',
  value: '1h',
  governs: 'HTTP trigger dedup window',
  source: 'hardcoded'
}, {
  name: 'approval_tokens TTL',
  value: '168h (7d)',
  governs: 'human-approval token expiry',
  source: 'hardcoded'
}, {
  name: 'sticky_bindings TTL',
  value: '25h',
  governs: 'worker affinity lifetime',
  source: 'hardcoded'
}, {
  name: 'debounce_state TTL',
  value: '14d',
  governs: 'trigger debounce timer cleanup',
  source: 'hardcoded'
}];
export const ENGINE_INVARIANTS_NOTE = 'Compile-time constants, not config. Referenced across Consumers (AckWait), KV (TTLs) and Streams (dedup) — surfaced here as the deployment\'s fixed contract. Graduating any to runtime config is a roadmap item.';

// ---- Config: access posture ----
// The static counterpart to the (dynamic) Audit log. Same identity model.
export interface AccessPosture {
  authMode: string;
  authModes: string[];
  authNote: string;
  readOnly: boolean;
  readOnlyEnv: string;
  mutatingEndpoints: readonly string[];
  note: string;
}
export const ACCESS_POSTURE: AccessPosture = {
  authMode: 'forward-auth',
  authModes: ['loopback', 'forward-auth', 'basic', 'disabled'],
  authNote: 'actor from X-Forwarded-User',
  readOnly: false,
  readOnlyEnv: 'CONSOLE_READ_ONLY',
  mutatingEndpoints: AUDIT_ACTION_SET,
  note: 'Same identity model that attributes the Audit log. When read-only is on, all seven mutating endpoints return 405 and emit an audit event with outcome=denied.'
};

// ---- Config: effective config (north-star, backend-pending) ----
export type ConfigSource = 'flag' | 'env' | 'file' | 'default';
export interface EffectiveConfigRow {
  key: string;
  value: string;
  source: ConfigSource;
  origin: string;
}
export const EFFECTIVE_CONFIG: EffectiveConfigRow[] = [{
  key: 'HTTPAddr',
  value: '127.0.0.1:8080',
  source: 'default',
  origin: '—'
}, {
  key: 'NATSPort',
  value: '4222',
  source: 'default',
  origin: '—'
}, {
  key: 'MonitorPort',
  value: '8222',
  source: 'env',
  origin: 'DAGNATS_MONITOR_PORT'
}, {
  key: 'DataDir',
  value: '~/Library/Application Support/dagnats',
  source: 'default',
  origin: '—'
}, {
  key: 'MaxStoreBytes',
  value: '10 GiB',
  source: 'file',
  origin: 'dagnats.yaml'
}, {
  key: 'NATSJetStreamReplicas',
  value: '1',
  source: 'default',
  origin: '—'
}, {
  key: 'OTLPEndpoint',
  value: 'otlp://localhost:4317',
  source: 'env',
  origin: 'OTEL_EXPORTER_OTLP_ENDPOINT'
}, {
  key: 'ConfigFilePath',
  value: './dagnats.yaml',
  source: 'flag',
  origin: '--config'
}, {
  key: 'CONSOLE_READ_ONLY',
  value: 'false',
  source: 'env',
  origin: 'CONSOLE_READ_ONLY'
}];
export const CONFIG_PRECEDENCE = 'flag > env > dagnats.yaml > default';
export const EFFECTIVE_CONFIG_NOTE = 'Preview — requires GET /console/api/config/effective (not yet wired). Source attribution shows where each resolved value came from.';