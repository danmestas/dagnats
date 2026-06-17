import { Fragment, useState } from 'react';
import { DLQ, DLQ_HEADERS, DLQ_PAYLOAD, DLQ_STACK, LOGS, LOG_SOURCES, AUDIT, AUDIT_META, AUDIT_ACTORS, ENGINE_INVARIANTS, ENGINE_INVARIANTS_NOTE, ACCESS_POSTURE, EFFECTIVE_CONFIG, CONFIG_PRECEDENCE, EFFECTIVE_CONFIG_NOTE, BLOCKED, SLOT_LIMITERS, SINGLETON_LOCKS, RATE_LIMITERS, DEBOUNCERS, WORKER_STARVATION_NOTE, MAXSTEPS_NOTE, METRIC_SERIES, STEP_ENQUEUE_SPARK, CONCURRENCY_ACQUIRED, SNAPSHOT_LATENCY, type Severity, type Tone, type GateKind, type AuditOutcome, type ConfigSource } from './consoleData';
import { Spark } from './Spark';

// ---------------- DLQ list ----------------
export function DlqView({
  onOpen
}: {
  onOpen: (id: string) => void;
}) {
  return <div className="dn-card">
      <div className="dn-chead">
        Dead-letter entries
        <span className="dn-dim" style={{
        marginLeft: 'auto',
        fontWeight: 400
      }}>
          stream DEAD_LETTERS
        </span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Msg id</th>
            <th>Workflow</th>
            <th>Step</th>
            <th>Error</th>
            <th>Age</th>
            <th style={{
            textAlign: 'right'
          }}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {DLQ.map(d => <tr key={d.id} onClick={() => onOpen(d.id)}>
              <td className="dn-idcell">{d.id}</td>
              <td>{d.workflow}</td>
              <td className="dn-mono dn-dim">{d.step}</td>
              <td>
                <span className="dn-errsnip" title={d.error}>
                  {d.error}
                </span>
              </td>
              <td className="dn-dim dn-mono">{d.age}</td>
              <td>
                <div className="dn-rowact">
                  <button className="dn-btn dn-sm" onClick={e => e.stopPropagation()} disabled={!d.eligible} style={d.eligible ? undefined : {
                opacity: 0.4,
                cursor: 'not-allowed'
              }}>
                  
                    Retry
                  </button>
                  <button className="dn-btn dn-sm" onClick={e => e.stopPropagation()}>
                    Discard
                  </button>
                  <span className="dn-chev">›</span>
                </div>
              </td>
            </tr>)}
        </tbody>
      </table>
    </div>;
}

// ---------------- DLQ detail ----------------
export function DlqDetailView({
  id,
  onBack
}: {
  id: string;
  onBack: () => void;
}) {
  const entry = DLQ.find(d => d.id === id) ?? DLQ[0];
  return <>
      <button className="dn-back" onClick={onBack}>
        <span className="dn-bk">‹</span> DLQ
      </button>
      <div className="dn-detailhead">
        <span className="dn-monoid">{entry.id}</span>
        <span className="dn-meta">
          <span>{entry.workflow}</span>
          <span>·</span>
          <span className="dn-mono">step {entry.step}</span>
          <span>·</span>
          <span>delivery 5/5</span>
        </span>
        <div className="dn-acts">
          <button className="dn-btn dn-primary" disabled={!entry.eligible} style={entry.eligible ? undefined : {
          opacity: 0.4
        }}>
            ↻ Retry
          </button>
          <button className="dn-btn">Discard</button>
          <button className="dn-btn">Soft-discard</button>
        </div>
      </div>
      <div className="dn-sectionh">Error</div>
      <pre className="dn-pre dn-err">{`${entry.error}\n\n${DLQ_STACK}`}</pre>
      <div className="dn-grid2 dn-top" style={{
      marginTop: 16
    }}>
        <div className="dn-card">
          <div className="dn-chead">Headers</div>
          <div style={{
          padding: 14
        }}>
            <pre className="dn-pre">{DLQ_HEADERS}</pre>
          </div>
        </div>
        <div className="dn-card">
          <div className="dn-chead">Payload</div>
          <div style={{
          padding: 14
        }}>
            <pre className="dn-pre">{DLQ_PAYLOAD}</pre>
          </div>
        </div>
      </div>
    </>;
}

// ---------------- Logs ----------------
const SEVERITIES: Severity[] = ['debug', 'info', 'warn', 'error'];
const SEV_LABEL: Record<Severity, string> = {
  debug: 'Debug',
  info: 'Info',
  warn: 'Warn',
  error: 'Error'
};
export function LogsView({
  onOpenTrace
}: {
  onOpenTrace: (traceId: string) => void;
}) {
  const [active, setActive] = useState<Record<Severity, boolean>>({
    debug: true,
    info: true,
    warn: true,
    error: true
  });
  const [query, setQuery] = useState('');
  function toggle(s: Severity) {
    setActive(prev => ({
      ...prev,
      [s]: !prev[s]
    }));
  }
  const q = query.trim().toLowerCase();
  const rows = LOGS.filter(l => active[l.sev]).filter(l => q === '' || l.trace.toLowerCase().includes(q) || l.service.toLowerCase().includes(q) || l.message.toLowerCase().includes(q));
  return <>
      <div className="dn-srcline">
        TELEMETRY stream · telemetry.logs.&#123;service&#125;.&#123;severity&#125; · 7-day / 1&nbsp;GB retention · showing last 500
      </div>
      <div className="dn-filterbar" style={{
      alignItems: 'center',
      flexWrap: 'wrap'
    }}>
        {SEVERITIES.map(s => <span key={s} className={`dn-chip${active[s] ? ` dn-on dn-sv-${s}` : ''}`} onClick={() => toggle(s)}>

            {SEV_LABEL[s]}
          </span>)}
        <input className="dn-input" placeholder="trace id / service / message" value={query} onChange={e => setQuery(e.target.value)} style={{
        flex: 1,
        minWidth: 200
      }} />
        <button className="dn-btn">Pause</button>
        <button className="dn-btn">Export</button>
      </div>
      <div className="dn-card">
        <div className="dn-chead">
          Live tail · SSE
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            click a trace id → span tree · {rows.length} of {LOGS.length}
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Severity</th>
              <th>Service</th>
              <th>Trace ID</th>
              <th>Message</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((l, i) => <tr key={i}>
                <td className="dn-dim dn-mono">{l.ts}</td>
                <td>
                  <span className={`dn-sev dn-sv-${l.sev}`}>{l.sev}</span>
                </td>
                <td className="dn-mono dn-dim">{l.service}</td>
                <td>
                  <span className="dn-traceid" onClick={() => onOpenTrace(l.trace)}>{l.trace}</span>
                </td>
                <td className="dn-mono">
                  {l.message}
                  {l.runId && <span className="dn-logchip">run {l.runId}</span>}
                  {l.stepId && <span className="dn-logchip">step {l.stepId}</span>}
                  {l.task && <span className="dn-logchip">{l.task}</span>}
                </td>
              </tr>)}
          </tbody>
        </table>
        <div className="dn-strip">
          <span className="dn-dim" style={{
          fontSize: 11,
          letterSpacing: '.06em',
          textTransform: 'uppercase'
        }}>
            Top sources
          </span>
          {LOG_SOURCES.map(([src, n]) => <span key={src} className="dn-src">
              {src} <b>{n.toLocaleString()}</b>
            </span>)}
        </div>
      </div>
    </>;
}

// ---------------- Metrics ----------------
// One card per real OTel series (engine/metrics.go). The metric name + its
// workflow/step labels are surfaced so the card maps back to the instrument.
function SeriesCard({
  metric,
  labels,
  value,
  spark,
  color
}: {
  metric: string;
  labels: string;
  value: string;
  spark: string;
  color: string;
}) {
  return <div className="dn-sparkcard">
      <div className="dn-sv-big" style={{
      color
    }}>{value}</div>
      <div className="dn-metricname">{metric}</div>
      <div className="dn-metriclabels">{labels}</div>
      <div style={{
      marginTop: 6
    }}>
        <Spark expr={spark} label={`${metric} trend over the last hour`} color={color} size={28} />
      </div>
    </div>;
}
export function MetricsView({
  onViewRuns
}: {
  onViewRuns: () => void;
}) {
  return <>
      <div className="dn-srcline">
        OTel Meter → NATS telemetry.metrics.&#123;service&#125;.&#123;name&#125; (delta) → in-console aggregator · SSE 4&nbsp;Hz · also GET /metrics (Prometheus)
      </div>
      <div className="dn-callout">
        <span className="dn-cicon">▲</span>
        <div style={{
        flex: 1
      }}>
          <div className="dn-ctitle">Anomaly · snapshot.save.duration_ms</div>
          <div className="dn-cbody dn-mono">
            p99 / p50 = 4.2× (threshold 3×) · p50 ≥ 1&nbsp;ms · spike at 17:19:31
          </div>
          <button className="dn-btn dn-amberbtn" style={{
          marginTop: 10
        }} onClick={onViewRuns}>
            View runs in this 90s window →
          </button>
        </div>
      </div>
      <div className="dn-tiles" style={{
      marginBottom: 14
    }}>
        {METRIC_SERIES.map(s => <SeriesCard key={s.metric} {...s} />)}
      </div>
      <div className="dn-tiles" style={{
      marginBottom: 14
    }}>
        <div className="dn-sparkcard">
          <div className="dn-sl">Concurrency</div>
          <div className="dn-metricname">task.concurrency.acquired</div>
          <div className="dn-pctrow">
            <span>
              <b style={{
              color: 'var(--dn-teal)'
            }}>{CONCURRENCY_ACQUIRED}</b>
              <div className="dn-pl">acquired</div>
            </span>
          </div>
        </div>
        <div className="dn-sparkcard">
          <div className="dn-sl">Snapshot save latency</div>
          <div className="dn-metricname">snapshot.save.duration_ms</div>
          <div className="dn-metriclabels">labels: workflow, step</div>
          <div className="dn-pctrow">
            <span>
              <b>{SNAPSHOT_LATENCY.p50}</b>
              <div className="dn-pl">p50 ms</div>
            </span>
            <span>
              <b style={{
              color: 'var(--dn-amber)'
            }}>{SNAPSHOT_LATENCY.p95}</b>
              <div className="dn-pl">p95 ms</div>
            </span>
            <span>
              <b style={{
              color: '#ff8a93'
            }}>{SNAPSHOT_LATENCY.p99}</b>
              <div className="dn-pl">p99 ms</div>
            </span>
          </div>
        </div>
        <div className="dn-sparkcard">
          <div className="dn-sl">Step enqueue</div>
          <div className="dn-metricname">step.enqueue</div>
          <div style={{
          marginTop: 10
        }}>
            <Spark expr={STEP_ENQUEUE_SPARK} label="step.enqueue rate trend" color="var(--dn-blue)" size={28} />
          </div>
        </div>
      </div>
      <button className="dn-btn">Prometheus · GET /metrics →</button>
    </>;
}

// ---------------- Audit log ----------------
// Outcome → pill modifier (denied is amber/warn, failed is danger).
const OUTCOME_PILL: Record<AuditOutcome, string> = {
  success: 'dn-ok',
  denied: 'dn-warn',
  failed: 'dn-fail'
};
export function AuditView({
  onOpenTarget
}: {
  onOpenTarget: (kind: string, id: string) => void;
}) {
  const [outcomeFilter, setOutcomeFilter] = useState<'all' | AuditOutcome>('all');
  const [actorFilter, setActorFilter] = useState<'all' | string>('all');
  const [selectedTs, setSelectedTs] = useState<string | null>(null);
  const deniedCount = AUDIT.filter(a => a.outcome === 'denied').length;
  const rows = AUDIT.filter(a => (outcomeFilter === 'all' || a.outcome === outcomeFilter) && (actorFilter === 'all' || a.actor === actorFilter));
  const outcomes: ('all' | AuditOutcome)[] = ['all', 'success', 'denied', 'failed'];
  return <>
      <div className="dn-srcline">
        Every state-changing console action, recorded to the <code>console_audit</code> KV bucket
        (90-day TTL). dagnats records not just successes but <b>denied</b> attempts (blocked by
        read-only mode) and <b>failed</b> ones.
      </div>

      <div className="dn-srcline dn-idbanner">
        Identity: <b>forward-auth</b> — {AUDIT_META.authDetail}. (Other modes: loopback→'loopback',
        basic→'console', disabled→console refuses to serve.)
      </div>

      <div className="dn-callout">
        <span className="dn-cicon">⚠</span>
        <div style={{
        flex: 1
      }}>
          <div className="dn-ctitle">{deniedCount} denied — mutations attempted while read-only</div>
          <div className="dn-cbody">
            {deniedCount} mutation{deniedCount === 1 ? ' was' : 's were'} attempted while the console
            was read-only (CONSOLE_READ_ONLY). The audit trail records blocked attempts, not just
            what succeeded.
          </div>
        </div>
      </div>

      <div className="dn-auditfilters">
        <span className="dn-flabel">Outcome</span>
        {outcomes.map(o => <button key={o} className={`dn-fchip${outcomeFilter === o ? ' dn-on' : ''}`} onClick={() => setOutcomeFilter(o)}>
            {o}
          </button>)}
        <span className="dn-flabel" style={{
        marginLeft: 18
      }}>Actor</span>
        {(['all', ...AUDIT_ACTORS] as const).map(a => <button key={a} className={`dn-fchip${actorFilter === a ? ' dn-on' : ''}`} onClick={() => setActorFilter(a)}>
            {a}
          </button>)}
      </div>

      <div className="dn-card">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Actor</th>
              <th>Action</th>
              <th>Target</th>
              <th>Outcome</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(a => <Fragment key={a.ts}>
                <tr onClick={() => setSelectedTs(selectedTs === a.ts ? null : a.ts)} style={{
              cursor: 'pointer'
            }}>
                  <td className="dn-dim dn-mono">{a.ts}</td>
                  <td className="dn-mono">{a.actor}</td>
                  <td>
                    <span className="dn-type">{a.action}</span>
                  </td>
                  <td onClick={e => {
                e.stopPropagation();
                onOpenTarget(a.targetKind, a.target);
              }}>
                    <span className="dn-targetchip">{a.target}</span>
                  </td>
                  <td>
                    <span className={`dn-pill ${OUTCOME_PILL[a.outcome]}`}>{a.outcome}</span>
                  </td>
                </tr>
                {selectedTs === a.ts && <tr className="dn-datarow">
                    <td colSpan={5}>
                      <pre className="dn-databox">{a.data}</pre>
                    </td>
                  </tr>}
              </Fragment>)}
          </tbody>
        </table>
      </div>

      <div className="dn-srcline" style={{
      marginTop: 14
    }}>
        These are the state-changing operations the console exposes (incl. the
        ADR-022 gated write actions):{' '}
        {AUDIT_META.actionSet.join(', ')}. Stored in console_audit KV — not WORKFLOW_HISTORY.
      </div>
    </>;
}

// ---------------- Admission control ----------------
// Maps a tone to its text color for inline cells (the dn-tile.dn-* rules only
// color tile numbers, so tables color text directly).
function toneColor(tone: Tone): string {
  switch (tone) {
    case 'dn-ok':
      return 'var(--dn-green)';
    case 'dn-warn':
      return 'var(--dn-amber)';
    case 'dn-amber':
      return 'var(--dn-amber)';
    case 'dn-danger':
      return '#ff8a93';
    case 'dn-info':
      return 'var(--dn-blue)';
    case 'dn-teal':
      return 'var(--dn-teal)';
    default:
      return 'var(--dn-muted)';
  }
}
// Maps a singleton-lock mode to a dn-type pill modifier.
function modePill(mode: 'Cancel' | 'Queue' | 'Reject'): string {
  if (mode === 'Queue') return 'dn-t-sub';
  if (mode === 'Reject') return 'dn-t-map';
  return 'dn-t-sleep';
}
// Maps a blocked-run gate kind to a dn-type pill modifier (sleep dimmed).
function gatePill(kind: GateKind): string {
  if (kind === 'slot') return 'dn-t-agent';
  if (kind === 'lock') return 'dn-t-sub';
  if (kind === 'rate') return 'dn-t-map';
  return 'dn-t-sleep';
}
export function ConcurrencyView() {
  return <>
      <div className="dn-srcline">
        Admission control is every gate that decides whether a task runs now, waits, or is shed.
        dagnats has four: a per-task-type slot pool, singleton locks, token-bucket rate limits,
        and trigger debounce.
      </div>

      <div className="dn-callout dn-danger">
        <span className="dn-cicon">▲</span>
        <div style={{
        flex: 1
      }}>
          <div className="dn-ctitle">Worker-starvation · image-pipeline::fetch-urls</div>
          <div className="dn-cbody">{WORKER_STARVATION_NOTE}</div>
          <span className="dn-dim dn-mono" style={{
          display: 'inline-block',
          marginTop: 10,
          fontSize: 11,
          textDecoration: 'underline dotted',
          textUnderlineOffset: 2
        }}>
            → Connections
          </span>
        </div>
      </div>

      <div className="dn-sectionh">Slot pool · ConcurrencyManager</div>
      <div className="dn-card">
        <div className="dn-chead">
          Per-task-type CAS pool
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            concurrency_tasks KV
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Task type</th>
              <th>In use / Limit</th>
              <th>Utilization</th>
              <th style={{
              textAlign: 'right'
            }}>Waiting</th>
            </tr>
          </thead>
          <tbody>
            {SLOT_LIMITERS.map(s => <tr key={s.taskType}>
                <td className="dn-idcell">{s.taskType}</td>
                <td className="dn-mono">
                  {s.inUse} / {s.limit === null ? '∞' : s.limit}
                </td>
                <td>
                  {s.limit === null ? <span className="dn-dim dn-mono" style={{
                fontSize: 11
              }}>unlimited</span> : <Spark expr={`{p:${Math.round(s.inUse / s.limit * 100)}}`} label={`${s.taskType} slot utilization`} color={toneColor(s.tone)} size={20} />}
                </td>
                <td className="dn-mono" style={{
              textAlign: 'right',
              color: s.waiting > 0 ? 'var(--dn-amber)' : 'var(--dn-muted)'
            }}>
                  {s.waiting}
                </td>
              </tr>)}
          </tbody>
        </table>
        <div className="dn-strip">
          <span className="dn-src">task.concurrency.acquired <b>{CONCURRENCY_ACQUIRED}</b></span>
        </div>
      </div>

      <div className="dn-sectionh">Singleton locks · engine/admission.go</div>
      <div className="dn-card">
        <div className="dn-chead">
          Per-workflow + keyed locks
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            singleton_locks KV · modes Cancel / Queue / Reject
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Lock key</th>
              <th>Scope</th>
              <th>Held by</th>
              <th>Mode</th>
              <th>Held</th>
              <th>Queued</th>
              <th style={{
              textAlign: 'right'
            }}>Rejected</th>
            </tr>
          </thead>
          <tbody>
            {SINGLETON_LOCKS.map(l => <tr key={l.key}>
                <td className="dn-idcell">{l.key}</td>
                <td>
                  <span className={`dn-type${l.scope === 'keyed' ? ' dn-t-agent' : ''}`}>{l.scope}</span>
                </td>
                <td className={l.heldBy === '—' ? 'dn-mono dn-dim' : 'dn-mono'} style={l.heldBy === '—' ? undefined : {
              color: 'var(--dn-blue)'
            }}>
                  {l.heldBy}
                </td>
                <td>
                  <span className={`dn-type ${modePill(l.mode)}`}>{l.mode}</span>
                </td>
                <td className="dn-mono dn-dim">{l.held}</td>
                <td className="dn-mono">{l.queued}</td>
                <td className="dn-mono" style={{
              textAlign: 'right',
              color: l.rejected > 0 ? '#ff8a93' : 'var(--dn-muted)'
            }}>
                  {l.rejected}
                </td>
              </tr>)}
          </tbody>
        </table>
      </div>

      <div className="dn-sectionh">Rate limits · engine/ratelimit.go</div>
      <div className="dn-card">
        <div className="dn-chead">
          Token buckets
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            tokens 0 = throttling · retryAfter → SleepTimer
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Limiter</th>
              <th>Tokens / Limit</th>
              <th>Period</th>
              <th style={{
              textAlign: 'right'
            }}>Retry after</th>
            </tr>
          </thead>
          <tbody>
            {RATE_LIMITERS.map(r => <tr key={r.key}>
                <td className="dn-idcell">{r.key}</td>
                <td className="dn-mono" style={{
              color: r.tokens === 0 ? 'var(--dn-amber)' : 'var(--dn-text)'
            }}>
                  {r.tokens} / {r.limit}
                </td>
                <td className="dn-mono dn-dim">{r.period}</td>
                <td className="dn-mono" style={{
              textAlign: 'right',
              color: r.retryAfter === '—' ? 'var(--dn-muted)' : 'var(--dn-amber)'
            }}>
                  {r.retryAfter}
                </td>
              </tr>)}
          </tbody>
        </table>
      </div>

      <div className="dn-sectionh">Debounce · trigger/subject.go</div>
      <div className="dn-card">
        <div className="dn-chead">
          Trigger debounce windows
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            trigger-scoped only · debounce_state KV (14d)
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Trigger</th>
              <th>Subject</th>
              <th>Window</th>
              <th>Absorbed</th>
              <th style={{
              textAlign: 'right'
            }}>Fires in</th>
            </tr>
          </thead>
          <tbody>
            {DEBOUNCERS.map(d => <tr key={d.trigger}>
                <td className="dn-idcell">{d.trigger}</td>
                <td className="dn-mono dn-dim">{d.subject}</td>
                <td className="dn-mono">{(d.windowMs / 1000).toFixed(1)}s</td>
                <td className="dn-mono">{d.absorbed}</td>
                <td className="dn-mono" style={{
              textAlign: 'right',
              color: 'var(--dn-teal)'
            }}>{d.firesIn}</td>
              </tr>)}
          </tbody>
        </table>
      </div>

      <div className="dn-sectionh">Blocked runs</div>
      <div className="dn-card">
        <div className="dn-chead">
          Waiting on a gate
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            {BLOCKED.length} runs · gate named precisely
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Run</th>
              <th>Workflow</th>
              <th>Step</th>
              <th>Gate</th>
              <th style={{
              textAlign: 'right'
            }}>Waiting</th>
            </tr>
          </thead>
          <tbody>
            {BLOCKED.map(b => <tr key={b.id} style={b.gateKind === 'sleep' ? {
            opacity: 0.5
          } : undefined}>
                <td className="dn-idcell">{b.id}</td>
                <td>{b.workflow}</td>
                <td className="dn-mono dn-dim">{b.step}</td>
                <td>
                  <span className={`dn-type ${gatePill(b.gateKind)}`}>{b.gate}</span>
                </td>
                <td className="dn-mono dn-dim" style={{
              textAlign: 'right'
            }}>
                  {b.waiting}
                </td>
              </tr>)}
          </tbody>
        </table>
      </div>

      <div className="dn-srcline" style={{
      marginTop: 14
    }}>
        {MAXSTEPS_NOTE}
      </div>
    </>;
}

// ---------------- Config ----------------
// Maps an effective-config source to its pill color modifier.
const SOURCE_PILL: Record<ConfigSource, string> = {
  flag: 'dn-info',
  env: 'dn-warn',
  file: 'dn-teal',
  default: 'dn-default'
};
export function ConfigView() {
  const ap = ACCESS_POSTURE;
  return <>
      <div className="dn-tiles" style={{
      marginBottom: 16
    }}>
        <div className="dn-tile dn-teal">
          <div className="dn-n">12</div>
          <div className="dn-l">Workflows</div>
        </div>
        <div className="dn-tile dn-info">
          <div className="dn-n">5</div>
          <div className="dn-l">Triggers</div>
        </div>
        <div className="dn-tile dn-ok">
          <div className="dn-n">4</div>
          <div className="dn-l">Workers</div>
        </div>
        <div className="dn-tile">
          <div className="dn-n">8</div>
          <div className="dn-l">Streams</div>
        </div>
        <div className="dn-tile">
          <div className="dn-n">18</div>
          <div className="dn-l">KV buckets</div>
        </div>
        <div className="dn-tile dn-danger">
          <div className="dn-n">2</div>
          <div className="dn-l">DLQ entries</div>
        </div>
      </div>

      {/* Access posture — static counterpart to the Audit log */}
      <div className="dn-card" style={{
      marginBottom: 16
    }}>
        <div className="dn-chead">
          Access posture
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            console/auth.go
          </span>
        </div>
        <div style={{
        padding: '12px 16px 16px'
      }}>
          <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          flexWrap: 'wrap'
        }}>
            <span className="dn-sectionh" style={{
            margin: 0
          }}>Auth mode</span>
            {ap.authModes.map(m => <span key={m} className={`dn-modepill${m === ap.authMode ? ' dn-on' : ''}`}>
                {m}
              </span>)}
            <span className="dn-dim dn-mono" style={{
            fontSize: 11
          }}>· {ap.authNote}</span>
          </div>
          <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: 10,
          marginTop: 14
        }}>
            <span className="dn-sectionh" style={{
            margin: 0
          }}>Read-only</span>
            <span className={`dn-pill ${ap.readOnly ? 'dn-warn' : 'dn-ok'}`}>
              {ap.readOnly ? 'on' : 'off'}
            </span>
            <span className="dn-dim dn-mono" style={{
            fontSize: 11
          }}>{ap.readOnlyEnv}</span>
            <span className="dn-targetchip" style={{
            marginLeft: 'auto',
            cursor: 'default'
          }}>→ Audit log</span>
          </div>
          <div className="dn-cbody" style={{
          marginTop: 14,
          fontSize: 12,
          color: 'var(--dn-muted)'
        }}>
            {ap.note}
          </div>
        </div>
      </div>

      <div className="dn-grid2">
        <div className="dn-card">
          <div className="dn-chead">Endpoints</div>
          <div style={{
          padding: '6px 16px 12px'
        }}>
            <div className="dn-kv">
              <span className="dn-k">NATS</span>
              <span className="dn-v">nats://127.0.0.1:4222</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">Console</span>
              <span className="dn-v">http://127.0.0.1:8080/console/</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">Monitor</span>
              <span className="dn-v">:8222</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">OTLP exporter</span>
              <span className="dn-v dn-dim">— not set —</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">HTTP bridge</span>
              <span className="dn-v">/hooks/*</span>
            </div>
          </div>
        </div>
        <div className="dn-card">
          <div className="dn-chead">Build info</div>
          <div style={{
          padding: '6px 16px 12px'
        }}>
            <div className="dn-kv">
              <span className="dn-k">dagnats</span>
              <span className="dn-v">v0.1.0</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">version</span>
              <span className="dn-v dn-dim">injected via -ldflags -X cli.Version (shown: v0.1.0)</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">commit</span>
              <span className="dn-v">5824fe3</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">built</span>
              <span className="dn-v">2026-06-10</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">NATS server</span>
              <span className="dn-v">v2.12.6</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">Go</span>
              <span className="dn-v">1.26.2</span>
            </div>
          </div>
        </div>
      </div>

      {/* Effective config — preview, backend not yet wired */}
      <div className="dn-card" style={{
      marginTop: 16
    }}>
        <div className="dn-chead">
          Effective config
          <span className="dn-mono dn-dim" style={{
          marginLeft: 12,
          fontWeight: 400,
          fontSize: 11
        }}>
            precedence: {CONFIG_PRECEDENCE}
          </span>
          <span className="dn-previewtag" style={{
          marginLeft: 'auto'
        }}>preview · not yet wired</span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Key</th>
              <th>Value</th>
              <th>Source</th>
              <th>Origin</th>
            </tr>
          </thead>
          <tbody>
            {EFFECTIVE_CONFIG.map(c => <tr key={c.key}>
                <td className="dn-mono">{c.key}</td>
                <td className="dn-mono dn-dim">{c.value}</td>
                <td>
                  <span className={`dn-pill ${SOURCE_PILL[c.source]}`}>{c.source}</span>
                </td>
                <td className="dn-mono dn-dim">{c.origin}</td>
              </tr>)}
          </tbody>
        </table>
        <div className="dn-srcline" style={{
        margin: '12px 16px 14px'
      }}>
          {EFFECTIVE_CONFIG_NOTE}
        </div>
      </div>

      {/* Engine invariants — the fixed contract */}
      <div className="dn-card" style={{
      marginTop: 16
    }}>
        <div className="dn-chead">
          Engine invariants
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            fixed contract · compile-time constants
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Constant</th>
              <th>Value</th>
              <th>Governs</th>
              <th>Source</th>
            </tr>
          </thead>
          <tbody>
            {ENGINE_INVARIANTS.map(inv => <tr key={inv.name}>
                <td className="dn-mono">{inv.name}</td>
                <td className="dn-mono" style={{
              color: 'var(--dn-teal)'
            }}>{inv.value}</td>
                <td className="dn-dim">{inv.governs}</td>
                <td>
                  <span className="dn-pill dn-default">{inv.source}</span>
                </td>
              </tr>)}
          </tbody>
        </table>
        <div className="dn-srcline" style={{
        margin: '12px 16px 14px'
      }}>
          {ENGINE_INVARIANTS_NOTE}
        </div>
      </div>

      <div className="dn-grid2" style={{
      marginTop: 16
    }}>
        <div className="dn-card">
          <div className="dn-chead">Worker groups</div>
          <table>
            <thead>
              <tr>
                <th>Group</th>
                <th>Members</th>
                <th>Last seen</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className="dn-mono">hello-world</td>
                <td className="dn-mono">2</td>
                <td className="dn-dim">3s ago</td>
              </tr>
              <tr>
                <td className="dn-mono">image-pipeline</td>
                <td className="dn-mono">1</td>
                <td className="dn-dim">5s ago</td>
              </tr>
              <tr>
                <td className="dn-mono">retry-errors</td>
                <td className="dn-mono">1</td>
                <td className="dn-dim">2s ago</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div style={{
      marginTop: 16,
      display: 'flex',
      gap: 10,
      alignItems: 'center'
    }}>
        <button className="dn-btn" disabled>⤓ Export config as YAML</button>
        <span className="dn-previewtag">planned · ADR-014</span>
      </div>
    </>;
}