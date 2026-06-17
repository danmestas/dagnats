import { useState, type Dispatch, type SetStateAction } from 'react';
import { RUNS, STATUS_TXT, WORKFLOWS, WORKFLOW_STEPS, RUN_EVENTS, RUN_INPUT, RUN_OUTPUT, RUN_TIMELINE, RECENT_FAILURES, RECENT_ACTIONS, TRIGGER_FIRES, TRIGGER_FIRES_DEFAULT, FIREABLE_TYPES, TRIGGER_TARGET_WORKFLOWS, type RunStatus, type StepType, type TriggerRow, type TriggerType, type FireRow } from './consoleData';
import { Spark } from './Spark';

// Per-workflow 24h invocation trends (Datatype line expressions). Failed-heavy
// workflows render reddish; idle drafts render a flat low line.
const WORKFLOW_TRENDS: Record<string, {
  expr: string;
  color: string;
}> = {
  'hello-world': {
    expr: '{l:60,72,68,80,75,88,82,94,90,96}',
    color: 'var(--dn-teal)'
  },
  'image-pipeline': {
    expr: '{l:30,42,38,50,45,55,52,60,58,64}',
    color: 'var(--dn-teal)'
  },
  'retry-errors': {
    expr: '{l:20,35,18,45,25,60,40,75,55,80}',
    color: '#ff8a93'
  },
  'sub-workflow': {
    expr: '{l:40,38,45,42,50,48,55,52,58,55}',
    color: 'var(--dn-teal)'
  },
  'signals': {
    expr: '{l:25,30,22,35,28,40,32,45,38,42}',
    color: 'var(--dn-teal)'
  },
  'agent-loop': {
    expr: '{l:10,18,14,22,30,26,35,28,40,34}',
    color: 'var(--dn-teal)'
  },
  'http-echo': {
    expr: '{l:55,60,58,65,62,68,64,70,66,72}',
    color: 'var(--dn-teal)'
  },
  'cron-trigger': {
    expr: '{l:20,20,40,20,20,40,20,20,40,20}',
    color: 'var(--dn-blue)'
  }
};
const STEP_CLASS: Record<StepType, string> = {
  normal: '',
  agent: 'dn-t-agent',
  sub_workflow: 'dn-t-sub',
  map: 'dn-t-map',
  sleep: 'dn-t-sleep'
};
const STEP_LABEL: Record<StepType, string> = {
  normal: 'normal',
  agent: 'agent',
  sub_workflow: 'sub_workflow',
  map: 'map',
  sleep: 'sleep'
};
function Pill({
  status
}: {
  status: RunStatus;
}) {
  return <span className={`dn-pill dn-${status}`}>{STATUS_TXT[status]}</span>;
}
function Sparkcard({
  label,
  value,
  sparkExpr,
  sparkLabel,
  sparkColor,
  delta,
  up
}: {
  label: string;
  value: string;
  sparkExpr: string;
  sparkLabel: string;
  sparkColor?: string;
  delta: string;
  up: boolean;
}) {
  return <div className="dn-sparkcard">
      <div className="dn-sl">{label}</div>
      <div className="dn-sv-big">
        {value}
        <span className={`dn-sdelta ${up ? 'dn-up' : 'dn-down'}`}>{delta}</span>
      </div>
      <div style={{
      marginTop: 8
    }}>
        <Spark expr={sparkExpr} label={sparkLabel} color={sparkColor} size={30} />
      </div>
    </div>;
}

// ---------------- Dashboard ----------------
export function DashboardView() {
  return <>
      <div className="dn-card" style={{
      marginBottom: 14,
      padding: '9px 16px',
      color: 'var(--dn-muted)',
      fontSize: 12,
      lineHeight: 1.5
    }}>
        <b style={{
        color: 'var(--dn-text)'
      }}>Workers</b> register{' '}
        <b style={{
        color: 'var(--dn-text)'
      }}>Functions</b> ·{' '}
        <b style={{
        color: 'var(--dn-text)'
      }}>Triggers</b> fire{' '}
        <b style={{
        color: 'var(--dn-text)'
      }}>Workflows</b> · a{' '}
        <b style={{
        color: 'var(--dn-text)'
      }}>Workflow</b> is a DAG of steps that each call a{' '}
        <b style={{
        color: 'var(--dn-text)'
      }}>Function</b> · every firing is a{' '}
        <b style={{
        color: 'var(--dn-text)'
      }}>Run</b> you can watch live and replay.
      </div>
      <div className="dn-tiles" style={{
      marginBottom: 14
    }}>
        <Sparkcard label="throughput" value="142/s" sparkExpr="{l:30,45,40,55,60,52,70,65,80,90}" sparkLabel="throughput trend, rising over the last hour" sparkColor="var(--dn-teal)" delta="▲ 6%" up />
        <Sparkcard label="p50 latency" value="1.2s" sparkExpr="{l:60,55,58,50,45,48,40,42,38,35}" sparkLabel="p50 latency trend, falling over the last hour" sparkColor="var(--dn-blue)" delta="▲ 0.1s" up={false} />
        <Sparkcard label="error rate" value="0.4%" sparkExpr="{b:5,8,4,6,10,30,55,20,8,5}" sparkLabel="error rate, a spike mid-window then recovered" sparkColor="#ff8a93" delta="▼ 0.2%" up />
      </div>
      <div className="dn-grid2 dn-top">
        <div className="dn-card">
          <div className="dn-chead">Recent failures</div>
          <table>
            <thead>
              <tr>
                <th>Run id</th>
                <th>Workflow</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody>
              {RECENT_FAILURES.map(f => <tr key={f[0]}>
                  <td className="dn-idcell">{f[0]}</td>
                  <td>{f[1]}</td>
                  <td>
                    <span className="dn-errsnip" title={f[2]}>
                      {f[2]}
                    </span>
                  </td>
                </tr>)}
            </tbody>
          </table>
        </div>
        <div className="dn-card">
          <div className="dn-chead">Recent operator actions</div>
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Actor</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {RECENT_ACTIONS.map(a => <tr key={a[0]}>
                  <td className="dn-dim dn-mono">{a[0]}</td>
                  <td className="dn-mono">{a[1]}</td>
                  <td className="dn-mono dn-dim">{a[2]}</td>
                </tr>)}
            </tbody>
          </table>
        </div>
      </div>
    </>;
}

// ---------------- Workflows list ----------------
export function WorkflowsView({
  onOpen
}: {
  onOpen: (name: string) => void;
}) {
  return <div className="dn-card">
      <div className="dn-chead">
        Workflow definitions
        <span className="dn-dim" style={{
        marginLeft: 'auto',
        fontWeight: 400
      }}>
          aggregated from WORKFLOW_HISTORY
        </span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Steps</th>
            <th>Last run</th>
            <th>Runs 24h</th>
            <th>24h trend</th>
            <th>Avg</th>
            <th>Trigger</th>
            <th style={{
            textAlign: 'right'
          }}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {WORKFLOWS.map(w => <tr key={w.name} onClick={() => onOpen(w.name)}>
              <td className="dn-idcell">{w.name}</td>
              <td className="dn-mono">{w.steps}</td>
              <td>
                <Pill status={w.last} />
              </td>
              <td className="dn-mono">{w.runs24h}</td>
              <td>
                {WORKFLOW_TRENDS[w.name] ? <Spark expr={WORKFLOW_TRENDS[w.name].expr} label={`${w.name} 24h run trend`} color={WORKFLOW_TRENDS[w.name].color} size={20} /> : <span className="dn-dim">—</span>}
              </td>
              <td className="dn-mono dn-dim">{w.avg}</td>
              <td>
                <span className="dn-type">{w.trigger}</span>
              </td>
              <td>
                <div className="dn-rowact">
                  <button className="dn-btn dn-sm" onClick={e => e.stopPropagation()}>
                    Run
                  </button>
                  <span className="dn-chev">›</span>
                </div>
              </td>
            </tr>)}
        </tbody>
      </table>
    </div>;
}

// ---------------- Workflow detail ----------------
export function WorkflowDetailView({
  name,
  onBack,
  onOpenRun
}: {
  name: string;
  onBack: () => void;
  onOpenRun: (id: string) => void;
}) {
  const recent = RUNS.filter(r => r.workflow === name).slice(0, 4);
  const shown = recent.length > 0 ? recent : RUNS.slice(0, 4);
  return <>
      <button className="dn-back" onClick={onBack}>
        <span className="dn-bk">‹</span> Workflows
      </button>
      <div className="dn-detailhead">
        <span className="dn-monoid">{name}</span>
        <span className="dn-meta">
          <span>{WORKFLOW_STEPS.length} steps</span>
          <span>·</span>
          <span>DAG</span>
        </span>
        <div className="dn-acts">
          <button className="dn-btn dn-primary">▸ Run workflow</button>
          <button className="dn-btn">Edit definition</button>
        </div>
      </div>
      <div className="dn-sectionh">Steps</div>
      <div className="dn-card">
        <div className="dn-steps">
          {WORKFLOW_STEPS.map((s, i) => <div key={s.name}>
              <div className="dn-step">
                <span className="dn-snum">{i + 1}</span>
                <span className="dn-sname">{s.name}</span>
                <span className={`dn-type ${STEP_CLASS[s.type]}`}>{STEP_LABEL[s.type]}</span>
                <span className="dn-sdep">
                  {s.dependsOn.length === 0 ? 'entry' : <>
                      depends_on <code>{s.dependsOn.join(', ')}</code>
                    </>}
                </span>
              </div>
              {i < WORKFLOW_STEPS.length - 1 && <div className="dn-edge" />}
            </div>)}
        </div>
      </div>
      <div className="dn-sectionh">Recent runs</div>
      <div className="dn-card">
        <table>
          <thead>
            <tr>
              <th>Run id</th>
              <th>Status</th>
              <th>Trigger</th>
              <th>Started</th>
              <th>Duration</th>
            </tr>
          </thead>
          <tbody>
            {shown.map(r => <tr key={r.id} onClick={() => onOpenRun(r.id)}>
                <td className="dn-idcell">{r.id}</td>
                <td>
                  <Pill status={r.status} />
                </td>
                <td className="dn-dim dn-mono">{r.trigger}</td>
                <td className="dn-dim dn-mono">{r.started}</td>
                <td className="dn-mono">{r.duration}</td>
              </tr>)}
          </tbody>
        </table>
      </div>
    </>;
}

// ---------------- Runs list ----------------
export function RunsView({
  onOpen
}: {
  onOpen: (id: string) => void;
}) {
  return <>
      <div className="dn-filterbar">
        <input className="dn-input" placeholder="workflow: any" style={{
        width: 160
      }} />
        <input className="dn-input" placeholder="status: any" style={{
        width: 140
      }} />
        <input className="dn-input" placeholder="paste a run id…" style={{
        flex: 1
      }} />
        <button className="dn-btn">Find</button>
      </div>
      <div className="dn-card">
        <div className="dn-chead">
          Recent runs · live
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            showing 1–8 of 240
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Run id</th>
              <th>Workflow</th>
              <th>Status</th>
              <th>Trigger</th>
              <th>Started</th>
              <th>Duration</th>
              <th style={{
              textAlign: 'right'
            }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {RUNS.map(r => <tr key={r.id} onClick={() => onOpen(r.id)}>
                <td className="dn-idcell">{r.id}</td>
                <td>{r.workflow}</td>
                <td>
                  <Pill status={r.status} />
                </td>
                <td>
                  <span className="dn-dim dn-mono">{r.trigger}</span>
                </td>
                <td className="dn-dim dn-mono">{r.started}</td>
                <td className="dn-mono">{r.duration}</td>
                <td>
                  <div className="dn-rowact">
                    <button className="dn-btn dn-sm" onClick={e => e.stopPropagation()}>
                      {r.trigger === 'cron' ? 'Fire now' : 'Run'}
                    </button>
                    <span className="dn-chev">›</span>
                  </div>
                </td>
              </tr>)}
          </tbody>
        </table>
      </div>
    </>;
}

// ---------------- Run detail ----------------
type RunTab = 'events' | 'io' | 'timeline';
export function RunDetailView({
  id,
  onBack,
  onOpenTrace
}: {
  id: string;
  onBack: () => void;
  onOpenTrace: (runId: string) => void;
}) {
  const [tab, setTab] = useState<RunTab>('events');
  const run = RUNS.find(r => r.id === id) ?? RUNS[3];
  return <>
      <button className="dn-back" onClick={onBack}>
        <span className="dn-bk">‹</span> Runs
      </button>
      <div className="dn-detailhead">
        <span className="dn-monoid">{run.id}</span>
        <Pill status={run.status} />
        <span className="dn-meta">
          <span>{run.workflow}</span>
          <span>·</span>
          <span className="dn-mono">{run.duration}</span>
          <span>·</span>
          <span className="dn-mono">{run.started}</span>
        </span>
        <div className="dn-acts">
          <button className="dn-btn dn-primary" onClick={() => onOpenTrace(run.id)}>View trace →</button>
          <button className="dn-btn">Signal</button>
          <button className="dn-btn">Cancel</button>
        </div>
      </div>
      <div className="dn-tabs">
        <div className={`dn-tab${tab === 'events' ? ' dn-active' : ''}`} onClick={() => setTab('events')}>
          Events
        </div>
        <div className={`dn-tab${tab === 'io' ? ' dn-active' : ''}`} onClick={() => setTab('io')}>
          IO
        </div>
        <div className={`dn-tab${tab === 'timeline' ? ' dn-active' : ''}`} onClick={() => setTab('timeline')}>
          Timeline
        </div>
      </div>

      {tab === 'events' && <div className="dn-card">
          <table>
            <thead>
              <tr>
                <th>Timestamp</th>
                <th>Type</th>
                <th>Step</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {RUN_EVENTS.map((e, i) => <tr key={i}>
                  <td className="dn-dim dn-mono">{e[0]}</td>
                  <td className="dn-mono">{e[1]}</td>
                  <td className="dn-mono dn-dim">{e[2]}</td>
                  <td className="dn-mono dn-dim">{e[3]}</td>
                </tr>)}
            </tbody>
          </table>
        </div>}

      {tab === 'io' && <div className="dn-grid2 dn-top">
          <div className="dn-card">
            <div className="dn-chead">Input</div>
            <div style={{
          padding: 14
        }}>
              <pre className="dn-pre">{RUN_INPUT}</pre>
            </div>
          </div>
          <div className="dn-card">
            <div className="dn-chead">Output</div>
            <div style={{
          padding: 14
        }}>
              <pre className="dn-pre">{RUN_OUTPUT}</pre>
            </div>
          </div>
        </div>}

      {tab === 'timeline' && <div className="dn-card">
          <div className="dn-chead">Step timeline · {run.duration} total</div>
          <div style={{
        padding: '14px 16px'
      }}>
            <div className="dn-tl">
              {RUN_TIMELINE.map(t => <div key={t[0]} className="dn-tlrow">
                  <span className="dn-tlname">{t[0]}</span>
                  <div className="dn-tltrack">
                    <div className={`dn-tlbar${t[3] ? ' dn-fail' : ''}`} style={{
                left: `${t[1]}%`,
                width: `${t[2]}%`
              }} />
                
                  </div>
                  <span className="dn-tldur">{Math.round(t[2] / 100 * 21) / 10}s</span>
                </div>)}
            </div>
          </div>
        </div>}
    </>;
}

// ---------------- Triggers ----------------
const TRIGGER_CLASS: Record<TriggerRow['type'], string> = {
  cron: 'dn-t-map',
  subject: 'dn-t-sub',
  webhook: 'dn-t-agent',
  http: ''
};
const TRIGGER_TYPES: TriggerType[] = ['cron', 'subject', 'webhook', 'http'];
// Per-type config placeholder shown in the modal's config field.
const CONFIG_PLACEHOLDER: Record<TriggerType, string> = {
  cron: '*/5 * * * *',
  subject: 'photos.incoming.*',
  webhook: '/hooks/...',
  http: 'POST /api/echo'
};
function defaultConfig(type: TriggerType): string {
  return type === 'http' ? 'POST /api/' : '';
}
type TriggerProps = {
  triggers: TriggerRow[];
  setTriggers: Dispatch<SetStateAction<TriggerRow[]>>;
  readOnly: boolean;
};

// --- Add/Edit trigger modal -----------------------------------------------
type ModalState = {
  open: boolean;
  editing: TriggerRow | null;
};
function TriggerModal({
  state,
  onClose,
  onSave
}: {
  state: ModalState;
  onClose: () => void;
  onSave: (t: TriggerRow, isNew: boolean) => void;
}) {
  const editing = state.editing;
  const [type, setType] = useState<TriggerType>(editing?.type ?? 'cron');
  const [workflow, setWorkflow] = useState(editing?.workflow ?? TRIGGER_TARGET_WORKFLOWS[0]);
  const [config, setConfig] = useState(editing?.config ?? defaultConfig(editing?.type ?? 'cron'));
  const [httpMethod, setHttpMethod] = useState('POST');
  const [enabled, setEnabled] = useState(editing?.enabled ?? true);
  function switchType(next: TriggerType) {
    setType(next);
    // Switching type resets the conditional config field to the new shape.
    setConfig(defaultConfig(next));
  }
  function save() {
    const id = editing?.id ?? `trg-${Math.random().toString(16).slice(2, 6)}`;
    const finalConfig = type === 'http' ? `${httpMethod} ${config || '/api/'}` : config || CONFIG_PLACEHOLDER[type];
    onSave({
      id,
      workflow,
      type,
      config: finalConfig,
      enabled
    }, editing === null);
  }
  return <div className="dn-overlay" onClick={onClose}>
      <div className="dn-modal" onClick={e => e.stopPropagation()}>
        <div className="dn-modalhead">{editing ? 'Edit trigger' : 'Add trigger'}</div>
        <div className="dn-modalbody">
          <label className="dn-field">
            <span className="dn-flabel">Type</span>
            <select className="dn-select" value={type} onChange={e => switchType(e.target.value as TriggerType)}>
              {TRIGGER_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </label>
          <label className="dn-field">
            <span className="dn-flabel">Target workflow</span>
            <select className="dn-select" value={workflow} onChange={e => setWorkflow(e.target.value)}>
              {TRIGGER_TARGET_WORKFLOWS.map(w => <option key={w} value={w}>{w}</option>)}
            </select>
          </label>
          {type === 'cron' && <label className="dn-field">
              <span className="dn-flabel">Cron expression</span>
              <input className="dn-input" placeholder="*/5 * * * *" value={config} onChange={e => setConfig(e.target.value)} />
            </label>}
          {type === 'subject' && <label className="dn-field">
              <span className="dn-flabel">NATS subject</span>
              <input className="dn-input" placeholder="photos.incoming.*" value={config} onChange={e => setConfig(e.target.value)} />
            </label>}
          {type === 'webhook' && <label className="dn-field">
              <span className="dn-flabel">Webhook path</span>
              <input className="dn-input" placeholder="/hooks/..." value={config} onChange={e => setConfig(e.target.value)} />
            </label>}
          {type === 'http' && <div className="dn-field">
              <span className="dn-flabel">HTTP endpoint</span>
              <div style={{
            display: 'flex',
            gap: 8
          }}>
                <select className="dn-select" style={{
              width: 110
            }} value={httpMethod} onChange={e => setHttpMethod(e.target.value)}>
                  {['POST', 'GET', 'PUT', 'DELETE'].map(m => <option key={m} value={m}>{m}</option>)}
                </select>
                <input className="dn-input" style={{
              flex: 1
            }} placeholder="/api/echo" value={config} onChange={e => setConfig(e.target.value)} />
              </div>
            </div>}
          <label className="dn-checkrow">
            <input type="checkbox" checked={enabled} onChange={e => setEnabled(e.target.checked)} />
            <span>Enabled</span>
          </label>
        </div>
        <div className="dn-modalfoot">
          <button className="dn-btn" onClick={onClose}>Cancel</button>
          <button className="dn-btn dn-primary" onClick={save}>Save</button>
        </div>
      </div>
    </div>;
}
export function TriggersView({
  triggers,
  setTriggers,
  readOnly,
  onOpen
}: TriggerProps & {
  onOpen: (id: string) => void;
}) {
  const [modal, setModal] = useState<ModalState>({
    open: false,
    editing: null
  });
  const [fired, setFired] = useState<Record<string, boolean>>({});
  function toggle(id: string) {
    setTriggers(prev => prev.map(t => t.id === id ? {
      ...t,
      enabled: !t.enabled
    } : t));
  }
  function fire(id: string) {
    setFired(prev => ({
      ...prev,
      [id]: true
    }));
    window.setTimeout(() => setFired(prev => ({
      ...prev,
      [id]: false
    })), 1400);
  }
  function save(t: TriggerRow, isNew: boolean) {
    setTriggers(prev => isNew ? [t, ...prev] : prev.map(p => p.id === t.id ? t : p));
    setModal({
      open: false,
      editing: null
    });
  }
  return <>
      <div className="dn-card">
        <div className="dn-chead">
          Configured triggers
          <span className="dn-dim" style={{
          fontWeight: 400
        }}>
            · evaluated by trigger-svc
          </span>
          <button className="dn-btn dn-primary" style={{
          marginLeft: 'auto'
        }} disabled={readOnly} onClick={() => setModal({
          open: true,
          editing: null
        })}>
            + Add trigger
          </button>
        </div>
        <table>
          <thead>
            <tr>
              <th>Trigger id</th>
              <th>Workflow</th>
              <th>Type</th>
              <th>Config</th>
              <th>Enabled</th>
              <th style={{
              textAlign: 'right'
            }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {triggers.map(t => <tr key={t.id} onClick={() => onOpen(t.id)}>
                <td className="dn-idcell">{t.id}</td>
                <td>{t.workflow}</td>
                <td>
                  <span className={`dn-type ${TRIGGER_CLASS[t.type]}`}>{t.type}</span>
                </td>
                <td className="dn-mono dn-dim">{t.config}</td>
                <td>
                  <span className={`dn-switch${t.enabled ? ' dn-on' : ''}`} onClick={e => {
                e.stopPropagation();
                if (!readOnly) toggle(t.id);
              }} role="switch" aria-checked={t.enabled}>
                    <span className="dn-knob" />
                  </span>
                </td>
                <td>
                  <div className="dn-rowact" onClick={e => e.stopPropagation()}>
                    {FIREABLE_TYPES.includes(t.type) && <button className="dn-btn dn-sm" disabled={readOnly} onClick={() => fire(t.id)}>
                        {fired[t.id] ? 'fired ✓' : 'Fire now'}
                      </button>}
                    <button className="dn-btn dn-sm" disabled={readOnly} onClick={() => setModal({
                  open: true,
                  editing: t
                })}>
                      Edit
                    </button>
                    <span className="dn-chev">›</span>
                  </div>
                </td>
              </tr>)}
          </tbody>
        </table>
      </div>
      {modal.open && <TriggerModal state={modal} onClose={() => setModal({
      open: false,
      editing: null
    })} onSave={save} />}
    </>;
}

// ---------------- Trigger detail ----------------
export function TriggerDetailView({
  id,
  triggers,
  setTriggers,
  readOnly,
  onBack
}: TriggerProps & {
  id: string;
  onBack: () => void;
}) {
  const [modal, setModal] = useState<ModalState>({
    open: false,
    editing: null
  });
  const [fired, setFired] = useState(false);
  const trigger = triggers.find(t => t.id === id);
  if (!trigger) {
    return <>
        <button className="dn-back" onClick={onBack}>
          <span className="dn-bk">‹</span> Triggers
        </button>
        <div className="dn-empty">
          <div className="dn-big">DELETED</div>
          <p>This trigger has been removed.</p>
        </div>
      </>;
  }
  const fires: FireRow[] = TRIGGER_FIRES[trigger.id] ?? TRIGGER_FIRES_DEFAULT;
  const fireable = FIREABLE_TYPES.includes(trigger.type);
  const configLabel: Record<TriggerType, string> = {
    cron: 'Cron expression',
    subject: 'NATS subject',
    webhook: 'Webhook path',
    http: 'HTTP endpoint'
  };
  function toggle() {
    setTriggers(prev => prev.map(t => t.id === trigger!.id ? {
      ...t,
      enabled: !t.enabled
    } : t));
  }
  function fire() {
    setFired(true);
    window.setTimeout(() => setFired(false), 1400);
  }
  function del() {
    // Mock confirm-then-remove. Removing from list state pops us back to the list.
    if (window.confirm(`Delete trigger ${trigger!.id}?`)) {
      setTriggers(prev => prev.filter(t => t.id !== trigger!.id));
      onBack();
    }
  }
  function save(t: TriggerRow) {
    setTriggers(prev => prev.map(p => p.id === t.id ? t : p));
    setModal({
      open: false,
      editing: null
    });
  }
  return <>
      <button className="dn-back" onClick={onBack}>
        <span className="dn-bk">‹</span> Triggers
      </button>
      <div className="dn-detailhead">
        <span className="dn-monoid">{trigger.id}</span>
        <span className={`dn-type ${TRIGGER_CLASS[trigger.type]}`}>{trigger.type}</span>
        <span className="dn-meta">
          <span>→ {trigger.workflow}</span>
          <span>·</span>
          <span style={{
          color: trigger.enabled ? 'var(--dn-green)' : 'var(--dn-muted)'
        }}>
            {trigger.enabled ? 'enabled' : 'disabled'}
          </span>
        </span>
        <div className="dn-acts">
          {fireable && <button className="dn-btn dn-primary" disabled={readOnly} onClick={fire}>
              {fired ? 'fired ✓' : '▸ Fire now'}
            </button>}
          <button className="dn-btn" disabled={readOnly} onClick={toggle}>
            {trigger.enabled ? 'Disable' : 'Enable'}
          </button>
          <button className="dn-btn" disabled={readOnly} onClick={() => setModal({
          open: true,
          editing: trigger
        })}>
            Edit
          </button>
          <button className="dn-btn dn-danger" disabled={readOnly} onClick={del}>
            Delete
          </button>
        </div>
      </div>

      <div className="dn-sectionh">Config</div>
      <div className="dn-card" style={{
      padding: '6px 16px'
    }}>
        <div className="dn-kv">
          <span className="dn-k">Type</span>
          <span className="dn-v">{trigger.type}</span>
        </div>
        <div className="dn-kv">
          <span className="dn-k">{configLabel[trigger.type]}</span>
          <span className="dn-v">{trigger.config}</span>
        </div>
        <div className="dn-kv">
          <span className="dn-k">Target workflow</span>
          <span className="dn-v">{trigger.workflow}</span>
        </div>
        <div className="dn-kv">
          <span className="dn-k">Enabled</span>
          <span className="dn-v">{trigger.enabled ? 'true' : 'false'}</span>
        </div>
      </div>

      <div className="dn-sectionh">Fire history</div>
      <div className="dn-card">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Run id</th>
              <th>Result</th>
            </tr>
          </thead>
          <tbody>
            {fires.map((f, i) => <tr key={i}>
                <td className="dn-dim dn-mono">{f.time}</td>
                <td className="dn-idcell">{f.runId}</td>
                <td>
                  <Pill status={f.result} />
                </td>
              </tr>)}
          </tbody>
        </table>
      </div>
      {modal.open && <TriggerModal state={modal} onClose={() => setModal({
      open: false,
      editing: null
    })} onSave={(t, _isNew) => save(t)} />}
    </>;
}