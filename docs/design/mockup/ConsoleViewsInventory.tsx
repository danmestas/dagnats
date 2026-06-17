import { useState } from 'react';
import { FNS, WORKERS, STREAMS, KV_CATALOG, KV_CATALOG_NOTE, WORKER_DETAILS, WORKER_GROUP_FNS, WORKER_FN_TRENDS, FUNCTION_DETAILS, FUNCTION_DETAIL_TRENDS, STATUS_TXT, type WorkerFnRow, type FunctionDetail, type RunStatus, type ViewKey, type Churn } from './consoleData';
import type { StreamRow } from './consoleData';
import { Spark } from './Spark';
import { ConfirmModal, DangerButton } from './ConsoleViewsSystem';

// service::name label for a function id, using its detail entry when present.
function fnLabel(id: string): string {
  const d = FUNCTION_DETAILS[id];
  return d ? `${d.service}::${d.name}` : id;
}

// Per-function 1h invocation trend (Datatype line). `tick` has no worker, so
// it renders a flat empty trend rather than a misleading line.
const FN_TRENDS: Record<string, string> = {
  greet: '{l:55,60,52,68,60,72,66,78,70,82}',
  uppercase: '{l:50,58,54,62,58,66,60,70,64,72}',
  'fetch-urls': '{l:30,40,35,45,42,38,48,44,52,50}',
  'build-gallery': '{l:28,36,32,42,38,46,42,50,46,52}',
  fetch: '{l:20,15,28,18,35,22,40,30,25,32}',
  tick: '{l:0,0,0,0,0,0,0,0,0,0}'
};

// ---------------- Function invoke modal ----------------
// THE actionable: a quick-test that dispatches a real task to a live worker.
// Reuses the .dn-overlay / .dn-modal pattern from the trigger modal. Invoke
// state is local: idle → running (~700ms) → result (success or no-worker error).
type InvokeState = 'idle' | 'running' | 'done';
function FunctionInvokeModal({
  id,
  onClose
}: {
  id: string;
  onClose: () => void;
}) {
  const detail = FUNCTION_DETAILS[id];
  const label = detail ? `${detail.service}::${detail.name}` : id;
  const hasWorker = (detail?.providers.length ?? 0) > 0;
  const [payload, setPayload] = useState(detail?.sampleInput ?? '{}');
  const [state, setState] = useState<InvokeState>('idle');
  function invoke() {
    setState('running');
    // Simulate a dispatched task round-trip to a worker.
    window.setTimeout(() => setState('done'), 700);
  }
  return <div className="dn-overlay" onClick={onClose}>
      <div className="dn-modal" onClick={e => e.stopPropagation()}>
        <div className="dn-modalhead">
          Invoke <span className="dn-mono" style={{
          color: 'var(--dn-blue)'
        }}>{label}</span>
        </div>
        <div className="dn-modalbody">
          <label className="dn-field">
            <span className="dn-flabel">Payload (JSON)</span>
            <textarea className="dn-input dn-mono" rows={7} style={{
            resize: 'vertical',
            lineHeight: 1.5
          }} value={payload} onChange={e => setPayload(e.target.value)} />
          </label>
          <div className="dn-robanner" style={{
          margin: 0
        }}>
            Invoking dispatches a real task to a live worker — side effects apply.
            Disabled in read-only mode.
          </div>
          {state === 'running' && <div className="dn-fnresult dn-running">
              <span className="dn-spin">◐</span> running…
            </div>}
          {state === 'done' && hasWorker && <div className="dn-fnresult dn-okres">
              <div className="dn-rline">
                <span className="dn-ok-mark">✓</span> completed in 0.4s · handled by{' '}
                <span className="dn-mono">wkr-{detail!.providers[0].worker.slice(4)}</span>
              </div>
              <pre className="dn-pre" style={{
            marginTop: 10
          }}>{detail!.sampleOutput}</pre>
            </div>}
          {state === 'done' && !hasWorker && <div className="dn-fnresult dn-errres">
              <div className="dn-rline">
                <span className="dn-err-mark">✗</span> no worker available — task queued,
                AckWait pending.
              </div>
            </div>}
        </div>
        <div className="dn-modalfoot">
          <button className="dn-btn" onClick={onClose}>Cancel</button>
          <button className="dn-btn dn-primary" disabled={state === 'running'} onClick={invoke}>
            {state === 'done' ? 'Invoke again' : 'Invoke'}
          </button>
        </div>
      </div>
    </div>;
}

// ---------------- Functions ----------------
export function FunctionsView({
  onOpen,
  readOnly
}: {
  onOpen: (id: string) => void;
  readOnly: boolean;
}) {
  const [invoking, setInvoking] = useState<string | null>(null);
  return <>
      <div className="dn-card">
      <div className="dn-chead">
        Registered functions across all workers
        <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
          aggregated from workers KV
        </span>
      </div>
      <table>
        <thead>
          <tr>
            <th>service::name</th>
            <th>Owner workers</th>
            <th>Pending</th>
            <th>Rate 1h</th>
            <th>Avg</th>
            <th>Fail %</th>
            <th style={{
              textAlign: 'right'
            }}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {FNS.map(f => <tr key={f[0]} onClick={() => onOpen(f[0])}>
              <td className="dn-idcell">{fnLabel(f[0])}</td>
              <td className="dn-dim">{f[1]}</td>
              <td className="dn-mono">{f[2]}</td>
              <td className="dn-mono">
                <span style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 8
              }}>
                  {f[3]}
                  {FN_TRENDS[f[0]] && <Spark expr={FN_TRENDS[f[0]]} label={`${f[0]} invocation trend, last hour`} color={f[0] === 'tick' ? 'var(--dn-faint)' : 'var(--dn-teal)'} size={18} />}
                </span>
              </td>
              <td className="dn-mono">{f[4]}</td>
              <td className="dn-mono" style={f[5] === '100%' ? {
              color: '#ff8a93'
            } : {
              color: 'var(--dn-muted)'
            }}>

                {f[5]}
              </td>
              <td>
                <div className="dn-rowact" onClick={e => e.stopPropagation()}>
                  <button className="dn-btn dn-sm" disabled={readOnly} onClick={() => setInvoking(f[0])}>
                    Invoke
                  </button>
                  <span className="dn-chev">›</span>
                </div>
              </td>
            </tr>)}
        </tbody>
      </table>
    </div>
      {invoking && <FunctionInvokeModal id={invoking} onClose={() => setInvoking(null)} />}
    </>;
}

// ---------------- Function detail ----------------
// Drill from the Functions list. Makes a function actionable: see its contract,
// who serves it, recent invocations — and quick-test it via Invoke. The `tick`
// function has no provider, so it renders a broken "no worker" state.
function Pill({
  status
}: {
  status: RunStatus;
}) {
  return <span className={`dn-pill dn-${status}`}>{STATUS_TXT[status]}</span>;
}
export function FunctionDetailView({
  id,
  onBack,
  onOpenWorker,
  onNavigate,
  readOnly
}: {
  id: string;
  onBack: () => void;
  onOpenWorker: (workerId: string) => void;
  onNavigate: (view: ViewKey) => void;
  readOnly: boolean;
}) {
  const detail: FunctionDetail = FUNCTION_DETAILS[id] ?? FUNCTION_DETAILS.greet;
  const label = `${detail.service}::${detail.name}`;
  const hasWorker = detail.providers.length > 0;
  const [invokeOpen, setInvokeOpen] = useState(false);
  const [inv1h, pending, fail24h, avg] = detail.stats;
  const failTone = fail24h !== '—' && fail24h !== '0.0%';
  const statTiles: [string, string, string][] = [[inv1h, 'invocations · 1h', 'dn-teal'], [pending, 'pending', pending !== '0' && pending !== '—' ? 'dn-warn' : 'dn-info'], [fail24h, 'fail rate · 24h', failTone ? 'dn-danger' : ''], [avg, 'avg duration', '']];
  return <>
      <button className="dn-back" onClick={onBack}>
        <span className="dn-bk">‹</span> Functions
      </button>
      <div className="dn-detailhead">
        <span className="dn-monoid">{label}</span>
        <span className={`dn-pill ${hasWorker ? 'dn-ok' : 'dn-fail'}`}>
          {hasWorker ? 'healthy' : 'no worker'}
        </span>
        <span className="dn-meta">
          <span>{detail.description}</span>
        </span>
        <div className="dn-acts">
          <button className="dn-btn dn-primary" disabled={readOnly} onClick={() => setInvokeOpen(true)}>
            ▸ Invoke
          </button>
        </div>
      </div>

      <div className="dn-tiles" style={{
      marginBottom: 18
    }}>
        {statTiles.map(([n, l, tone], i) => <div key={i} className={`dn-tile ${tone}`}>
            <div className="dn-n">{n}</div>
            <div className="dn-l">{l}</div>
          </div>)}
        <div className="dn-tile" style={{
        minWidth: 150
      }}>
          <Spark expr={FUNCTION_DETAIL_TRENDS[id] ?? '{l:0,0,0,0,0,0,0,0,0,0}'} label={`${label} invocation rate, last hour`} color={hasWorker ? 'var(--dn-teal)' : 'var(--dn-faint)'} size={26} />
          <div className="dn-l">rate · 1h</div>
        </div>
      </div>

      <div className="dn-sectionh">Contract</div>
      <div className="dn-grid2 dn-top">
        <div className="dn-card">
          <div className="dn-chead">Input schema</div>
          <div style={{
          padding: 14
        }}>
            {detail.inputSchema ? <pre className="dn-pre">{detail.inputSchema}</pre> : <span className="dn-dim">No schema registered</span>}
          </div>
        </div>
        <div className="dn-card">
          <div className="dn-chead">Output schema</div>
          <div style={{
          padding: 14
        }}>
            {detail.outputSchema ? <pre className="dn-pre">{detail.outputSchema}</pre> : <span className="dn-dim">No schema registered</span>}
          </div>
        </div>
      </div>

      <div className="dn-sectionh">Providers</div>
      <div className="dn-card">
        {hasWorker ? <table>
            <thead>
              <tr>
                <th>Worker id</th>
                <th>Status</th>
                <th>In-flight</th>
                <th style={{
              textAlign: 'right'
            }}></th>
              </tr>
            </thead>
            <tbody>
              {detail.providers.map(p => <tr key={p.worker} onClick={() => onOpenWorker(p.worker)}>
                  <td className="dn-idcell">{p.worker}</td>
                  <td>
                    <span className={`dn-dot ${p.status === 'online' ? 'dn-on' : 'dn-stale'}`} />
                    {p.status}
                  </td>
                  <td className="dn-mono">{p.inflight}</td>
                  <td>
                    <div className="dn-rowact">
                      <span className="dn-chev">›</span>
                    </div>
                  </td>
                </tr>)}
            </tbody>
          </table> : <div className="dn-robanner" style={{
        margin: 14
      }}>
            No worker is serving this function — deploy one (see Workers). Tasks for{' '}
            <span className="dn-mono">{label}</span> queue and wait for a provider.
          </div>}
      </div>

      <div className="dn-sectionh">Recent invocations</div>
      <div className="dn-card">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Run id</th>
              <th>Caller</th>
              <th>Status</th>
              <th>Duration</th>
            </tr>
          </thead>
          <tbody>
            {detail.invocations.map((iv, i) => <tr key={i}>
                <td className="dn-dim dn-mono">{iv.time}</td>
                <td className="dn-idcell">{iv.runId}</td>
                <td className="dn-mono dn-dim">{iv.caller}</td>
                <td>
                  <Pill status={iv.status} />
                </td>
                <td className="dn-mono">{iv.duration}</td>
              </tr>)}
          </tbody>
        </table>
      </div>

      <div className="dn-acts" style={{
      marginTop: 16
    }}>
        <button className="dn-btn" onClick={() => onNavigate('runs')}>View runs →</button>
        <button className="dn-btn" onClick={() => onNavigate('dlq')}>View DLQ entries →</button>
      </div>

      {invokeOpen && <FunctionInvokeModal id={id} onClose={() => setInvokeOpen(false)} />}
    </>;
}

// ---------------- Provision worker modal ----------------
// A worker is a PROCESS, not a record. Provisioning is real but gated on a
// supervisor (container/k8s/process supervisor); this modal teaches that —
// it is a DEPLOY form, and the Provision button stays disabled in the demo
// because no supervisor is wired up.
function ProvisionModal({
  onClose
}: {
  onClose: () => void;
}) {
  const [image, setImage] = useState('ghcr.io/acme/dagnats-worker:latest');
  const [functions, setFunctions] = useState('billing::charge, billing::refund');
  const [replicas, setReplicas] = useState(1);
  const [namespace, setNamespace] = useState('default');
  return <div className="dn-overlay" onClick={onClose}>
      <div className="dn-modal" onClick={e => e.stopPropagation()}>
        <div className="dn-modalhead">Provision worker</div>
        <div className="dn-modalbody">
          <div className="dn-robanner" style={{
          margin: 0
        }}>
            Workers are processes, not records — provisioning requires a supervisor
            (container / k8s / process supervisor).
          </div>
          <label className="dn-field">
            <span className="dn-flabel">Image / binary</span>
            <input className="dn-input" placeholder="ghcr.io/acme/worker:latest" value={image} onChange={e => setImage(e.target.value)} />
          </label>
          <label className="dn-field">
            <span className="dn-flabel">Functions to serve</span>
            <input className="dn-input" placeholder="billing::charge, billing::refund" value={functions} onChange={e => setFunctions(e.target.value)} />
          </label>
          <label className="dn-field">
            <span className="dn-flabel">Replicas</span>
            <input className="dn-input" type="number" min={1} value={replicas} onChange={e => setReplicas(Math.max(1, Number(e.target.value) || 1))} />
          </label>
          <label className="dn-field">
            <span className="dn-flabel">Namespace</span>
            <input className="dn-input" placeholder="default" value={namespace} onChange={e => setNamespace(e.target.value)} />
          </label>
        </div>
        <div className="dn-modalfoot">
          <span className="dn-dim" style={{
          marginRight: 'auto',
          fontSize: 11
        }}>
            No supervisor configured (demo)
          </span>
          <button className="dn-btn" onClick={onClose}>Cancel</button>
          <button className="dn-btn" disabled>Provision</button>
        </div>
      </div>
    </div>;
}

// ---------------- Workers ----------------
// A worker is a PROCESS, not a record. So unlike Triggers (full CRUD), this
// surface is "observe + rotate" — no casual Add; provisioning is a deliberate,
// supervisor-gated DEPLOY action.
export function WorkersView({
  onOpen,
  readOnly
}: {
  onOpen: (id: string) => void;
  readOnly: boolean;
}) {
  const [provisionOpen, setProvisionOpen] = useState(false);
  return <>
      <div className="dn-card">
      <div className="dn-chead">
        Connected workers
        <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
          heartbeats via workers KV
        </span>
        <button className="dn-btn" style={{
          marginLeft: 10
        }} disabled={readOnly} onClick={() => setProvisionOpen(true)}>
          Provision worker
        </button>
      </div>
      <div className="dn-dim" style={{
        padding: '0 16px 11px',
        fontSize: 12,
        lineHeight: 1.5
      }}>
        Workers self-register when they start — observe and drain them here. To add
        one, deploy a process or provision via a supervisor.
      </div>
      <table>
        <thead>
          <tr>
            <th>Worker id</th>
            <th>Group</th>
            <th>Status</th>
            <th>Task types</th>
            <th>Last seen</th>
            <th>Host</th>
            <th style={{
              textAlign: 'right'
            }}></th>
          </tr>
        </thead>
        <tbody>
          {WORKERS.map(w => <tr key={w.id} onClick={() => onOpen(w.id)}>
              <td className="dn-idcell">{w.id}</td>
              <td className="dn-mono dn-dim">{w.group}</td>
              <td>
                <span className={`dn-dot ${w.status === 'online' ? 'dn-on' : 'dn-stale'}`} />
                {w.status}
              </td>
              <td className="dn-mono dn-dim">{w.taskTypes}</td>
              <td className="dn-dim">{w.lastSeen}</td>
              <td className="dn-mono dn-dim">{w.host}</td>
              <td>
                <div className="dn-rowact">
                  <span className="dn-chev">›</span>
                </div>
              </td>
            </tr>)}
        </tbody>
      </table>
    </div>
      {provisionOpen && <ProvisionModal onClose={() => setProvisionOpen(false)} />}
    </>;
}

// ---------------- Worker detail ----------------
// A worker pulls tasks for its registered functions, executes them, and acks
// before AckWait elapses; on crash the in-flight task redelivers elsewhere.
type LifeStatus = 'online' | 'draining' | 'offline';
export function WorkerDetailView({
  id,
  onBack,
  readOnly,
  allowDestructive
}: {
  id: string;
  onBack: () => void;
  readOnly: boolean;
  allowDestructive: boolean;
}) {
  const row = WORKERS.find(w => w.id === id) ?? WORKERS[0];
  const bundle = WORKER_DETAILS[id];
  const detail = bundle?.detail ?? {
    id: row.id,
    group: row.group,
    host: '10.0.1.20',
    uptime: '2h05m',
    lastHeartbeat: row.lastSeen,
    processed1h: '128',
    inflight: '1',
    redelivered: '2',
    dedupHits: '3'
  };
  const fns: WorkerFnRow[] = bundle?.fns ?? WORKER_GROUP_FNS[row.group] ?? [];
  const tasks = bundle?.tasks ?? [{
    runId: 'c19f44de',
    fn: `${row.group}::${row.taskTypes.split(',')[0].trim()}`,
    started: '17:20:03',
    ackWaitRemaining: '26s'
  }];

  // A worker is a process: we observe it and rotate it (drain / decommission).
  // `status` and `inflightRemaining` are local so the lifecycle controls below
  // visibly change the header pill and wind the in-flight tasks down.
  const [status, setStatus] = useState<LifeStatus>('online');
  const [inflightRemaining, setInflightRemaining] = useState(tasks.length);
  // Which gated-action confirm modal is open.
  const [modal, setModal] = useState<'drain' | 'resume' | 'decommission' | null>(null);
  function drain() {
    setStatus('draining');
    // Graceful: stop accepting new tasks and let in-flight tasks finish. We
    // step the visible count down one task at a time to show the wind-down.
    let remaining = tasks.length;
    const tick = window.setInterval(() => {
      remaining -= 1;
      setInflightRemaining(Math.max(0, remaining));
      if (remaining <= 0) window.clearInterval(tick);
    }, 1100);
  }
  function undrain() {
    setStatus('online');
    setInflightRemaining(tasks.length);
  }
  function decommission() {
    setStatus('offline');
    setInflightRemaining(0);
  }
  if (status === 'offline') {
    return <>
        <button className="dn-back" onClick={onBack}>
          <span className="dn-bk">‹</span> Workers
        </button>
        <div className="dn-empty">
          <div className="dn-big">DECOMMISSIONED</div>
          <p>
            <span className="dn-monoid">{detail.id}</span> process exited. Any in-flight
            tasks redeliver to another worker after AckWait. It will reappear here only
            if a new process registers under this id.
          </p>
        </div>
      </>;
  }
  const dot = status === 'draining' ? 'dn-stale' : 'dn-on';
  const inflightDisplay = status === 'online' ? detail.inflight : String(inflightRemaining);
  const statTiles: [string, string, string][] = [[detail.processed1h, 'processed · 1h', 'dn-teal'], [inflightDisplay, 'in-flight', status === 'draining' ? 'dn-warn' : 'dn-info'], [detail.redelivered, 'redelivered (nak/timeout)', 'dn-warn'], [detail.dedupHits, 'dedup hits', '']];
  return <>
      <button className="dn-back" onClick={onBack}>
        <span className="dn-bk">‹</span> Workers
      </button>
      <div className="dn-detailhead">
        <span className="dn-monoid">{detail.id}</span>
        <span className="dn-type">{detail.group}</span>
        <span className="dn-meta">
          <span>
            <span className={`dn-dot ${dot}`} />{status}
          </span>
          <span>·</span>
          <span className="dn-mono">{detail.host}</span>
          <span>·</span>
          <span>last heartbeat {detail.lastHeartbeat}</span>
          <span>·</span>
          <span>uptime <span className="dn-mono">{detail.uptime}</span></span>
        </span>
        <div className="dn-acts">
          {status === 'online' && <DangerButton label="Drain" tier={1} readOnly={readOnly} allowDestructive={allowDestructive} onClick={() => setModal('drain')} />}
          {status === 'draining' && <DangerButton label="Resume" tier={1} readOnly={readOnly} allowDestructive={allowDestructive} onClick={() => setModal('resume')} />}
          <DangerButton label="Decommission" tier={2} readOnly={readOnly} allowDestructive={allowDestructive} onClick={() => setModal('decommission')} />
        </div>
      </div>

      {modal === 'drain' && <ConfirmModal title={`Drain worker ${detail.id}`} tone="warn" confirmLabel="Drain worker" preview={`${detail.id} stops pulling new tasks.\n  ${tasks.length} in-flight task${tasks.length === 1 ? '' : 's'} finish, then it idles.\n  Reversible via Resume.`} bodyNote="Graceful drain: in-flight work completes, no new tasks are pulled." onConfirm={drain} onCancel={() => setModal(null)} />}

      {modal === 'resume' && <ConfirmModal title={`Resume worker ${detail.id}`} tone="warn" confirmLabel="Resume worker" preview={`${detail.id} resumes pulling tasks for its registered functions.\n  Returns to online.`} bodyNote="Resume: the worker rejoins the pull loop for its functions." onConfirm={undrain} onCancel={() => setModal(null)} />}

      {modal === 'decommission' && <ConfirmModal title={`Decommission worker ${detail.id}`} tone="danger" confirmLabel="Decommission" requireTyped={detail.id} preview={`deregister ${detail.id}\n  its task types reassign to the group\n  re-provision to undo`} bodyNote="The process exits; in-flight tasks redeliver elsewhere after AckWait." onConfirm={decommission} onCancel={() => setModal(null)} />}

      {status === 'draining' && <div className="dn-robanner">
          Draining — finishing {inflightRemaining} in-flight, not accepting new tasks.
          Resume to return this worker to online.
        </div>}

      <div className="dn-tiles" style={{
      marginBottom: 18
    }}>
        {statTiles.map(([n, l, tone], i) => <div key={i} className={`dn-tile ${tone}`}>
            <div className="dn-n">{n}</div>
            <div className="dn-l">{l}</div>
          </div>)}
      </div>

      <div className="dn-sectionh">Registered functions</div>
      <div className="dn-card">
        <table>
          <thead>
            <tr>
              <th>Function</th>
              <th>Pending</th>
              <th>In-flight</th>
              <th>Processed 1h</th>
              <th>Avg</th>
              <th>Fail %</th>
            </tr>
          </thead>
          <tbody>
            {fns.map(f => <tr key={f.name}>
                <td className="dn-idcell">{f.name}</td>
                <td className="dn-mono">{f.pending}</td>
                <td className="dn-mono">{f.inflight}</td>
                <td className="dn-mono">
                  <span style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 8
              }}>
                    {f.processed1h}
                    {WORKER_FN_TRENDS[f.name] && <Spark expr={WORKER_FN_TRENDS[f.name]} label={`${f.name} rate trend, last hour`} color={f.failPct === '100%' ? '#ff8a93' : 'var(--dn-teal)'} size={18} />}
                  </span>
                </td>
                <td className="dn-mono dn-dim">{f.avg}</td>
                <td className="dn-mono" style={f.failPct === '100%' ? {
              color: '#ff8a93'
            } : {
              color: 'var(--dn-muted)'
            }}>
                  {f.failPct}
                </td>
              </tr>)}
          </tbody>
        </table>
      </div>

      <div className="dn-sectionh">In-flight tasks</div>
      <div className="dn-card">
        <table>
          <thead>
            <tr>
              <th>Run id</th>
              <th>Function</th>
              <th>Started</th>
              <th>AckWait remaining</th>
              {status === 'draining' && <th>State</th>}
            </tr>
          </thead>
          <tbody>
            {tasks.slice(0, status === 'online' ? tasks.length : inflightRemaining).map(t => <tr key={t.runId}>
                <td className="dn-idcell">{t.runId}</td>
                <td className="dn-mono dn-dim">{t.fn}</td>
                <td className="dn-mono dn-dim">{t.started}</td>
                <td className="dn-mono" style={{
              color: 'var(--dn-amber)'
            }}>{t.ackWaitRemaining}</td>
                {status === 'draining' && <td>
                    <span className="dn-type dn-t-map">draining</span>
                  </td>}
              </tr>)}
            {status === 'draining' && inflightRemaining === 0 && <tr>
                <td colSpan={5} className="dn-dim" style={{
              textAlign: 'center',
              padding: '14px 0'
            }}>
                  Drained — all in-flight tasks finished, not accepting new tasks.
                </td>
              </tr>}
          </tbody>
        </table>
      </div>

      <div className="dn-sectionh" style={{
      marginTop: 18
    }}>How this worker runs tasks</div>
      <div className="dn-card" style={{
      padding: '11px 16px',
      color: 'var(--dn-muted)',
      fontSize: 12,
      lineHeight: 1.6
    }}>
        Pull-based: this worker fetches tasks for its registered functions; on
        crash, in-flight tasks redeliver to another worker after AckWait.
      </div>
    </>;
}

// ---------------- Streams ----------------
// The 8 JetStream streams that back the engine. Retention ('work' vs 'limits')
// and storage ('memory' vs 'file') are load-bearing semantics — TASK_QUEUES is
// a work queue, STICKY_TASKS lives in memory — so those pills are highlighted.
function RetentionPill({
  retention
}: {
  retention: StreamRow['retention'];
}) {
  return <span className={`dn-type${retention === 'work' ? ' dn-t-agent' : ''}`}>{retention}</span>;
}
function StoragePill({
  storage
}: {
  storage: StreamRow['storage'];
}) {
  return <span className={`dn-type${storage === 'memory' ? ' dn-t-map' : ''}`}>{storage}</span>;
}
export function StreamsView({
  onOpen
}: {
  onOpen?: (name: string) => void;
}) {
  return <div className="dn-card">
      <div className="dn-chead">
        JetStream streams
        <span className="dn-dim" style={{
        marginLeft: 'auto',
        fontWeight: 400
      }}>
          ● live · click a stream → config, state &amp; consumers
        </span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Stream</th>
            <th>Subjects</th>
            <th>Retention</th>
            <th>Storage</th>
            <th>Messages</th>
            <th>Bytes</th>
            <th>Seq</th>
            <th>Deleted</th>
            <th>Consumers</th>
            <th>Policy</th>
          </tr>
        </thead>
        <tbody>
          {STREAMS.map(s => <tr key={s.name} onClick={() => onOpen?.(s.name)}>
              <td className="dn-idcell">{s.name}</td>
              <td className="dn-mono dn-dim">{s.subjects}</td>
              <td>
                <RetentionPill retention={s.retention} />
              </td>
              <td>
                <StoragePill storage={s.storage} />
              </td>
              <td className="dn-mono">{s.messages}</td>
              <td className="dn-mono dn-dim">{s.bytes}</td>
              <td className="dn-mono dn-dim">{s.firstSeq}–{s.lastSeq}</td>
              <td className="dn-mono" style={s.deleted > 0 ? {
            color: '#ff8a93'
          } : {
            color: 'var(--dn-muted)'
          }}>
                {s.deleted}
              </td>
              <td className="dn-mono">{s.consumers}</td>
              <td className="dn-mono dn-dim">{s.dedupOrAge}</td>
            </tr>)}
        </tbody>
      </table>
    </div>;
}

// ---------------- KV catalog ----------------
// A role-grouped catalog of the JetStream KV buckets, not a value inspector:
// every bucket is History:1 and there is no external read API. The TTL is the
// load-bearing fact (the behavior contract), so Liveness TTLs are emphasized.
function churnPill(churn: Churn): string {
  return churn === 'high' ? 'dn-t-map' : 'dn-t-sleep';
}
function TtlCell({
  ttl,
  prominent
}: {
  ttl: string;
  prominent?: boolean;
}) {
  if (ttl === '—') {
    return <span className="dn-type dn-t-sleep">—</span>;
  }
  return <span className={`dn-type ${prominent ? 'dn-t-agent' : 'dn-t-map'}`} style={prominent ? {
    fontWeight: 700
  } : undefined}>{ttl}</span>;
}
// Engine-internal KV buckets are off-limits for purge — the orchestrator owns
// their lifecycle. Operator-managed buckets (idempotency/dedup/debounce/sticky)
// are purgeable under the tier-2 gate.
const ENGINE_OWNED_BUCKETS = new Set(['workflow_runs', 'checkpoints', 'concurrency_tasks', 'singleton_locks', 'event_waiters', 'rate_limits', 'workflow_defs', 'console_audit']);
export function KvView({
  readOnly,
  allowDestructive
}: {
  readOnly: boolean;
  allowDestructive: boolean;
}) {
  // Illustrative key counts per bucket for the dry-run preview.
  const keyCounts: Record<string, number> = {
    idempotency_keys: 1842,
    http_idempotency: 96,
    debounce_state: 12,
    sticky_bindings: 34
  };
  const [purgeBucket, setPurgeBucket] = useState<string | null>(null);
  return <>
      <div className="dn-srcline">{KV_CATALOG_NOTE}</div>
      {KV_CATALOG.map(group => {
      const prominent = group.role === 'Liveness';
      return <div key={group.role} style={{
        marginBottom: 18
      }}>
            <div className="dn-sectionh">
              {group.role}
              {group.note && <span className="dn-dim" style={{
            marginLeft: 10,
            textTransform: 'none',
            letterSpacing: 0,
            fontWeight: 400
          }}>
                  {group.note}
                </span>}
            </div>
            <div className="dn-card">
              <table>
                <thead>
                  <tr>
                    <th>Bucket</th>
                    <th>TTL</th>
                    <th>History</th>
                    <th>Churn</th>
                    <th>Purpose</th>
                    <th style={{
                  textAlign: 'right'
                }}>Inspect in</th>
                    <th style={{
                  textAlign: 'right'
                }}>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {group.buckets.map(b => <tr key={b.name}>
                      <td className="dn-idcell">{b.name}</td>
                      <td title={b.ttlMeaning}>
                        <TtlCell ttl={b.ttl} prominent={prominent} />
                      </td>
                      <td className="dn-mono dn-dim">{b.history}</td>
                      <td>
                        <span className={`dn-type ${churnPill(b.churn)}`}>{b.churn}</span>
                      </td>
                      <td className="dn-dim">{b.purpose}</td>
                      <td style={{
                  textAlign: 'right'
                }}>
                        {b.crosslink ? <span className="dn-type dn-t-sub">→ {b.crosslink}</span> : <span className="dn-dim">—</span>}
                      </td>
                      <td>
                        <div className="dn-rowact">
                          <DangerButton label="Purge…" tier={2} readOnly={readOnly} allowDestructive={allowDestructive} engineOwned={ENGINE_OWNED_BUCKETS.has(b.name)} onClick={() => setPurgeBucket(b.name)} />
                        </div>
                      </td>
                    </tr>)}
                </tbody>
              </table>
            </div>
          </div>;
    })}
      {purgeBucket && <ConfirmModal title={`Purge bucket ${purgeBucket}`} tone="danger" confirmLabel="Purge bucket" requireTyped={purgeBucket} preview={`${keyCounts[purgeBucket] ?? 0} keys purged\n  cannot be undone`} bodyNote="An automatic backup runs first." onConfirm={() => {}} onCancel={() => setPurgeBucket(null)} />}
    </>;
}