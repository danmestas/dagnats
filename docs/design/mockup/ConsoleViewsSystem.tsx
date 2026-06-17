import { useState, type ReactNode } from 'react';
import { SERVER_HEALTH, SERVER_CONNECTIONS_SPARK, SERVER_STORAGE_PIE, CONSUMERS, CONSUMER_LAG_SPARK, STREAM_DETAILS, STREAMS, SERVICES, SERVICE_ENDPOINTS, CONNECTIONS, CONN_PENDING_NOTE, type ConsumerRow, type ServiceRow, type ConnRow } from './consoleData';
import { Spark } from './Spark';

// ================= ADR-022 gating-ladder primitives =================
// A reusable confirm/danger pattern for gated write actions. ConfirmModal
// renders a dry-run blast-radius preview, an audit note, and (for tier-2
// destructive actions) a typed-confirmation guard. DangerButton encodes the
// gating ladder: read-only blocks everything; tier-2 additionally requires the
// CONSOLE_ALLOW_DESTRUCTIVE flag; engine-owned resources are off-limits.

// ---------------- ConfirmModal ----------------
// Matches the .dn-overlay / .dn-modal pattern used by TriggerModal and the
// worker Provision modal. `requireTyped` gates Confirm until the user types the
// exact value (the resource name), the standard guard for irreversible purges.
export function ConfirmModal({
  title,
  tone,
  preview,
  bodyNote,
  requireTyped,
  confirmLabel,
  onConfirm,
  onCancel
}: {
  title: string;
  tone: 'warn' | 'danger';
  preview: ReactNode;
  bodyNote: string;
  requireTyped?: string;
  confirmLabel: string;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  const [typed, setTyped] = useState('');
  const typedOk = !requireTyped || typed === requireTyped;
  const confirmClass = tone === 'danger' ? 'dn-btn dn-danger' : 'dn-btn dn-amberbtn';
  function confirm() {
    if (!typedOk) return;
    onConfirm();
    onCancel();
  }
  return <div className="dn-overlay" onClick={onCancel}>
      <div className="dn-modal" onClick={e => e.stopPropagation()}>
        <div className="dn-modalhead">{title}</div>
        <div className="dn-modalbody">
          <div className="dn-sectionh" style={{
          margin: 0
        }}>Dry-run · blast radius</div>
          <pre className="dn-pre">{preview}</pre>
          <div className={`dn-robanner${tone === 'danger' ? ' dn-roban-danger' : ''}`} style={{
          margin: 0
        }}>
            {bodyNote} Recorded to console_audit.
          </div>
          {requireTyped && <label className="dn-field">
              <span className="dn-flabel">
                Type <span className="dn-mono">{requireTyped}</span> to confirm
              </span>
              <input className="dn-input" value={typed} placeholder={requireTyped} onChange={e => setTyped(e.target.value)} />
            </label>}
        </div>
        <div className="dn-modalfoot">
          <button className="dn-btn" onClick={onCancel}>Cancel</button>
          <button className={confirmClass} disabled={!typedOk} onClick={confirm}>
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>;
}

// ---------------- DangerButton ----------------
// The gating ladder as a button. Disabled with a reason-tooltip when the action
// is not permitted; the reason is the load-bearing affordance (operators learn
// WHY an action is blocked, not just that it is).
export function DangerButton({
  label,
  tier,
  readOnly,
  allowDestructive,
  engineOwned,
  onClick
}: {
  label: string;
  tier: 1 | 2;
  readOnly: boolean;
  allowDestructive: boolean;
  engineOwned?: boolean;
  onClick: () => void;
}) {
  let reason = '';
  if (readOnly) reason = 'console is read-only';else if (engineOwned) reason = 'engine-owned — managed by the orchestrator';else if (tier === 2 && !allowDestructive) reason = 'enable Destructive actions';
  const disabled = reason !== '';
  const cls = tier === 2 ? 'dn-btn dn-sm dn-danger' : 'dn-btn dn-sm dn-amberbtn';
  return <button className={cls} disabled={disabled} title={disabled ? reason : undefined} onClick={disabled ? undefined : onClick}>
      {label}
    </button>;
}
// ====================================================================

// ---------------- Server / health ----------------
// The live expansion of the rail footer: embedded NATS health from the monitor
// port :8222 (/varz, /jsz, /healthz). A HEALTHY hero, traffic + host cards, and
// a prominent JetStream capacity section (storage pie, api errors).
function StatCard({
  value,
  label,
  tone
}: {
  value: string;
  label: string;
  tone: '' | 'dn-danger' | 'dn-ok' | 'dn-info' | 'dn-teal' | 'dn-warn';
}) {
  return <div className={`dn-tile ${tone}`} style={{
    minWidth: 150
  }}>
      <div className="dn-n">{value}</div>
      <div className="dn-l">{label}</div>
    </div>;
}
export function ServerHealthView({
  readOnly,
  allowDestructive
}: {
  readOnly: boolean;
  allowDestructive: boolean;
}) {
  const h = SERVER_HEALTH;
  const slowTone = h.traffic.slowConsumers > 0 ? 'dn-danger' : 'dn-ok';
  const apiTone = h.jetstream.apiErrors > 0 ? 'dn-danger' : 'dn-ok';
  const [lameDuck, setLameDuck] = useState(false);
  return <>
      <div className="dn-srcline">
        Embedded NATS monitor · :8222 · /varz · /jsz · /healthz
      </div>

      <div className="dn-detailhead">
        <span className="dn-pill dn-ok" style={{
        fontSize: 13,
        padding: '4px 12px'
      }}>
          HEALTHY
        </span>
        <span className="dn-monoid">{h.identity.version}</span>
        <span className="dn-meta">
          <span>uptime <span className="dn-mono">{h.identity.uptime}</span></span>
          <span>·</span>
          <span>healthz <span className="dn-mono" style={{
            color: 'var(--dn-green)'
          }}>{h.identity.healthz}</span></span>
          <span>·</span>
          <span className="dn-mono">{h.identity.listen}</span>
        </span>
        <div className="dn-acts">
          <DangerButton label="Lame-duck mode" tier={1} readOnly={readOnly} allowDestructive={allowDestructive} onClick={() => setLameDuck(true)} />
        </div>
      </div>

      {lameDuck && <ConfirmModal title="Enter lame-duck mode" tone="warn" confirmLabel="Enter lame-duck" preview={`nats server signal ldm\n  server stops accepting new connections\n  drains gracefully; existing work completes\n  requires a restart to exit`} bodyNote="Enter lame-duck: the server stops accepting new connections and drains gracefully. Existing work completes. Requires restart to exit." onConfirm={() => {}} onCancel={() => setLameDuck(false)} />}

      <div className="dn-sectionh">Traffic</div>
      <div className="dn-tiles" style={{
      marginBottom: 4
    }}>
        <div className="dn-tile dn-teal" style={{
        minWidth: 150
      }}>
          <div className="dn-n">
            <span style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 8
          }}>
              {h.traffic.connections}
              <Spark expr={SERVER_CONNECTIONS_SPARK} label="connections trend, last interval" color="var(--dn-teal)" size={20} />
            </span>
          </div>
          <div className="dn-l">connections · {h.traffic.totalConnections} total</div>
        </div>
        <StatCard value={String(h.traffic.subscriptions)} label="subscriptions" tone="dn-info" />
        <StatCard value={`${h.traffic.inMsgs} / ${h.traffic.outMsgs}`} label="msgs in / out" tone="" />
        <StatCard value={`${h.traffic.inBytes} / ${h.traffic.outBytes}`} label="bytes in / out" tone="" />
        <StatCard value={String(h.traffic.slowConsumers)} label="slow consumers" tone={slowTone} />
      </div>

      <div className="dn-sectionh">Host</div>
      <div className="dn-tiles" style={{
      marginBottom: 4
    }}>
        <StatCard value={h.host.mem} label="memory" tone="dn-info" />
        <StatCard value={h.host.cpu} label="cpu" tone="" />
        <StatCard value={String(h.host.cores)} label="cores" tone="" />
      </div>

      <div className="dn-sectionh">JetStream capacity</div>
      <div className="dn-card" style={{
      marginBottom: 14
    }}>
        <div className="dn-chead">
          Storage &amp; API · /jsz
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            store {h.identity.store} · {h.identity.storeDir}
          </span>
        </div>
        <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: 28,
        padding: 18,
        flexWrap: 'wrap'
      }}>
          <div style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: 8,
          minWidth: 120
        }}>
            <Spark expr={SERVER_STORAGE_PIE} label={`JetStream storage ${h.jetstream.storagePct} percent used`} color="var(--dn-teal)" size={48} />
            <span className="dn-mono" style={{
            fontSize: 13
          }}>{h.jetstream.storageUsed} / {h.jetstream.storageMax}</span>
            <span className="dn-dim" style={{
            fontSize: 10.5,
            letterSpacing: '.06em',
            textTransform: 'uppercase'
          }}>
              storage ({h.jetstream.storagePct}%)
            </span>
          </div>
          <div className="dn-tiles" style={{
          flex: 1
        }}>
            <StatCard value={h.jetstream.memoryUsed} label="memory used" tone="" />
            <StatCard value={String(h.jetstream.streams)} label="streams" tone="dn-teal" />
            <StatCard value={String(h.jetstream.consumers)} label="consumers" tone="dn-info" />
            <StatCard value={h.jetstream.messages} label="messages" tone="" />
            <StatCard value={h.jetstream.apiTotal} label="api total" tone="" />
            <StatCard value={String(h.jetstream.apiErrors)} label="api errors" tone={apiTone} />
          </div>
        </div>
      </div>

      <div className="dn-srcline" style={{
      marginBottom: 0
    }}>
        Slow consumers — clients {h.slowConsumerStats.clients} · routes {h.slowConsumerStats.routes} · leafs {h.slowConsumerStats.leafs}.
        These figures come from the embedded NATS monitor port :8222 (/varz, /jsz, /healthz).
      </div>
    </>;
}

// ---------------- Consumers ----------------
// Every durable consumer NATS tracks for the engine. Lag (delivered − ack-floor)
// and pending with zero waiting pulls are the work-queue health signals: the
// alarm row (wkr-image-pipeline) has a backlog with no worker pulling.
function isWorkConsumer(c: ConsumerRow): boolean {
  return c.ackPolicy === 'explicit';
}
export function ConsumersView() {
  const alarm = CONSUMERS.find(c => c.tone === 'dn-danger');
  return <>
      <div className="dn-srcline">
        Every durable consumer NATS tracks for the engine. Lag (delivered −
        ack-floor) and pending with zero waiting pulls are the work-queue health
        signals.
      </div>

      {alarm && <div className="dn-callout">
          <span className="dn-cicon">⚠</span>
          <div style={{
        flex: 1
      }}>
            <div className="dn-ctitle">{alarm.filter} — {alarm.numPending} pending, {alarm.numWaiting} waiting pulls</div>
            <div className="dn-cbody dn-mono">
              Backlog with no worker consuming. Check Workers →
            </div>
          </div>
        </div>}

      <div className="dn-card">
        <div className="dn-chead">
          Durable consumers
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            {CONSUMERS.length} bound
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Consumer</th>
              <th>Stream</th>
              <th>Filter</th>
              <th>Ack</th>
              <th>Pending</th>
              <th>Ack-pending</th>
              <th>Waiting</th>
              <th>Redelivered</th>
              <th>Lag</th>
              <th>AckWait</th>
              <th>Trend</th>
            </tr>
          </thead>
          <tbody>
            {CONSUMERS.map(c => {
            const danger = c.tone === 'dn-danger';
            const ackNone = c.ackPolicy === 'none';
            const lagShown = danger || c.lag > 0 && isWorkConsumer(c);
            return <tr key={c.name}>
                  <td className="dn-idcell">
                    {c.name}
                    {danger && <span className="dn-logchip" style={{
                  color: '#ff8a93',
                  borderColor: '#4a2126',
                  background: '#2a121580'
                }}>
                        no workers pulling
                      </span>}
                  </td>
                  <td className="dn-mono dn-dim">{c.stream}</td>
                  <td className="dn-mono dn-dim">{c.filter}</td>
                  <td>
                    {ackNone ? <span className="dn-type dn-t-sleep">ack-none</span> : <span className="dn-type">{c.ackPolicy}</span>}
                  </td>
                  <td className="dn-mono" style={c.numPending > 0 ? {
                color: 'var(--dn-amber)'
              } : undefined}>
                    {c.numPending}
                  </td>
                  <td className="dn-mono dn-dim">{c.numAckPending}</td>
                  <td className="dn-mono dn-dim">{c.numWaiting}</td>
                  <td className="dn-mono" style={c.numRedelivered > 0 ? {
                color: 'var(--dn-amber)'
              } : {
                color: 'var(--dn-muted)'
              }}>
                    {c.numRedelivered}
                  </td>
                  <td className="dn-mono" style={lagShown ? {
                color: '#ff8a93',
                fontWeight: 600
              } : {
                color: 'var(--dn-faint)'
              }}>
                    {ackNone ? '—' : c.lag}
                  </td>
                  <td className="dn-mono dn-dim">{c.ackWait}</td>
                  <td className="dn-mono">
                    <Spark expr={CONSUMER_LAG_SPARK[c.name] ?? '{l:0,0,0,0,0,0,0,0}'} label={`${c.name} lag trend`} color={danger ? '#ff8a93' : 'var(--dn-teal)'} size={18} />
                  </td>
                </tr>;
          })}
          </tbody>
        </table>
      </div>
    </>;
}

// ---------------- Stream detail ----------------
// Drill from the Streams list. Config (subjects / retention / storage / dedup /
// max-age / replicas), a state block (messages / bytes / seq / deleted), and a
// Consumers-on-this-stream sub-table cross-linking the Consumers concept.
// Engine-owned streams are off-limits for purge — the orchestrator manages
// their lifecycle. The rest (TELEMETRY, TRIGGER_HISTORY, STICKY_TASKS,
// DEAD_LETTERS) are operator-purgeable under the tier-2 gate.
const ENGINE_OWNED_STREAMS = new Set(['WORKFLOW_HISTORY', 'EVENTS', 'TASK_QUEUES', 'console_audit']);
export function StreamDetailView({
  name,
  onBack,
  readOnly,
  allowDestructive
}: {
  name: string;
  onBack: () => void;
  readOnly: boolean;
  allowDestructive: boolean;
}) {
  const detail = STREAM_DETAILS[name] ?? STREAM_DETAILS[STREAMS[0].name];
  const s = detail.row;
  const engineOwned = ENGINE_OWNED_STREAMS.has(s.name);
  const [backupOpen, setBackupOpen] = useState(false);
  const [purgeOpen, setPurgeOpen] = useState(false);
  return <>
      <button className="dn-back" onClick={onBack}>
        <span className="dn-bk">‹</span> Streams
      </button>
      <div className="dn-detailhead">
        <span className="dn-monoid">{s.name}</span>
        <span className={`dn-pill ${s.storage === 'memory' ? 'dn-run' : 'dn-ok'}`}>
          {s.retention} · {s.storage}
        </span>
        <span className="dn-meta">
          <span className="dn-mono">{s.subjects}</span>
        </span>
        <div className="dn-acts">
          <DangerButton label="Backup (snapshot)" tier={1} readOnly={readOnly} allowDestructive={allowDestructive} onClick={() => setBackupOpen(true)} />
          <DangerButton label="Purge…" tier={2} readOnly={readOnly} allowDestructive={allowDestructive} engineOwned={engineOwned} onClick={() => setPurgeOpen(true)} />
        </div>
      </div>

      {backupOpen && <ConfirmModal title={`Backup ${s.name}`} tone="warn" confirmLabel="Snapshot to object store" preview={`Snapshot ${s.name} (${s.messages} msgs / ${s.bytes}) to the object store.\n  Read-only operation — no data change.\n  Est. ${s.bytes}.`} bodyNote="Snapshot is a read-only operation; no messages are removed." onConfirm={() => {}} onCancel={() => setBackupOpen(false)} />}

      {purgeOpen && <ConfirmModal title={`Purge ${s.name}`} tone="danger" confirmLabel="Purge stream" requireTyped={s.name} preview={`${s.messages} messages match (whole stream)\n  ${s.consumers} consumers affected\n  cannot be undone`} bodyNote="An automatic backup runs first. Wraps `dagnats clean`." onConfirm={() => {}} onCancel={() => setPurgeOpen(false)} />}

      <div className="dn-grid2 dn-top">
        <div className="dn-card">
          <div className="dn-chead">Config</div>
          <div style={{
          padding: '6px 16px 12px'
        }}>
            <div className="dn-kv">
              <span className="dn-k">Subjects</span>
              <span className="dn-v">{s.subjects}</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">Retention</span>
              <span className="dn-v">{s.retention}</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">Storage</span>
              <span className="dn-v">{s.storage}</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">Dedup / max-age</span>
              <span className="dn-v">{s.dedupOrAge}</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">Replicas</span>
              <span className="dn-v">1</span>
            </div>
          </div>
        </div>
        <div className="dn-card">
          <div className="dn-chead">State</div>
          <div style={{
          padding: '6px 16px 12px'
        }}>
            <div className="dn-kv">
              <span className="dn-k">Messages</span>
              <span className="dn-v">{s.messages}</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">Bytes</span>
              <span className="dn-v">{s.bytes}</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">First–last seq</span>
              <span className="dn-v">{s.firstSeq}–{s.lastSeq}</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">Deleted</span>
              <span className="dn-v" style={s.deleted > 0 ? {
              color: '#ff8a93'
            } : undefined}>{s.deleted}</span>
            </div>
            <div className="dn-kv">
              <span className="dn-k">Consumers</span>
              <span className="dn-v">{s.consumers}</span>
            </div>
          </div>
        </div>
      </div>

      <div className="dn-sectionh">Consumers on this stream</div>
      <div className="dn-card">
        {detail.consumers.length > 0 ? <table>
            <thead>
              <tr>
                <th>Consumer</th>
                <th>Filter</th>
                <th>Pending</th>
                <th>Ack-pending</th>
                <th>Redelivered</th>
              </tr>
            </thead>
            <tbody>
              {detail.consumers.map(c => <tr key={c.name}>
                  <td className="dn-idcell">{c.name}</td>
                  <td className="dn-mono dn-dim">{c.filter}</td>
                  <td className="dn-mono" style={c.pending > 0 ? {
              color: 'var(--dn-amber)'
            } : undefined}>{c.pending}</td>
                  <td className="dn-mono dn-dim">{c.ackPending}</td>
                  <td className="dn-mono" style={c.redelivered > 0 ? {
              color: 'var(--dn-amber)'
            } : {
              color: 'var(--dn-muted)'
            }}>{c.redelivered}</td>
                </tr>)}
            </tbody>
          </table> : <div className="dn-dim" style={{
        padding: '14px 16px',
        fontSize: 12.5,
        lineHeight: 1.6
      }}>
            No consumers — messages accumulate until a reader binds.
          </div>}
      </div>
    </>;
}

// ---------------- Services ----------------
// The live process roster. Health derives from heartbeat TTL on the
// workers/services KV buckets; per-endpoint stats come from $SRV discovery once
// services adopt the nats micro framework — except the one real micro endpoint
// trigger-svc serves today (_REGISTRY.trigger_types.ack).
function kindPill(kind: ServiceRow['kind']) {
  const core = kind !== 'worker-group';
  return <span className={`dn-type ${core ? 'dn-t-agent' : ''}`}>{kind}</span>;
}
export function ServicesView({
  onOpen
}: {
  onOpen: (name: string) => void;
}) {
  const instances = SERVICES.reduce((sum, s) => sum + s.instances, 0);
  return <>
      <div className="dn-srcline">
        The live process roster. Health derives from heartbeat TTL on the
        workers/services KV buckets; per-endpoint stats come from $SRV discovery
        once services adopt the nats micro framework.
      </div>

      <div className="dn-card">
        <div className="dn-chead">
          Service roster
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            {SERVICES.length} services · {instances} instances
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Service</th>
              <th>Kind</th>
              <th>Version</th>
              <th>Commit</th>
              <th>Instances</th>
              <th>Status</th>
              <th>Last seen</th>
              <th>Note</th>
            </tr>
          </thead>
          <tbody>
            {SERVICES.map(s => <tr key={s.name} onClick={() => onOpen(s.name)}>
                <td className="dn-idcell">{s.name}</td>
                <td>{kindPill(s.kind)}</td>
                <td className="dn-mono dn-dim">{s.version}</td>
                <td className="dn-mono dn-dim">{s.commit}</td>
                <td className="dn-mono">{s.instances}</td>
                <td>
                  <span className={`dn-pill ${s.status === 'online' ? 'dn-ok' : 'dn-fail'}`}>
                    {s.status}
                  </span>
                </td>
                <td className="dn-dim">{s.lastSeen}</td>
                <td className="dn-mono dn-dim">{s.note}</td>
              </tr>)}
          </tbody>
        </table>
      </div>

      <div className="dn-srcline" style={{
      marginTop: 12,
      marginBottom: 0
    }}>
        Roster + health: live from KV today. Per-endpoint $SRV.STATS: 1 live
        (<span className="dn-mono">_REGISTRY.trigger_types.ack</span>), rest
        available on micro adoption.
      </div>
    </>;
}

// ---------------- Service detail ----------------
// Roster fields header + an Endpoints sub-table from SERVICE_ENDPOINTS. When the
// section is `live:false` the stats are the $SRV-upgrade shape, not current — a
// clear tag says so. The one live row (trigger-svc) gets a "● live" teal chip.
export function ServiceDetailView({
  name,
  onBack
}: {
  name: string;
  onBack: () => void;
}) {
  const s = SERVICES.find(x => x.name === name) ?? SERVICES[0];
  const bundle = SERVICE_ENDPOINTS[s.name];
  const endpoints = bundle?.endpoints ?? [];
  const live = bundle?.live ?? false;
  return <>
      <button className="dn-back" onClick={onBack}>
        <span className="dn-bk">‹</span> Services
      </button>
      <div className="dn-detailhead">
        <span className="dn-monoid">{s.name}</span>
        <span className={`dn-pill ${s.status === 'online' ? 'dn-ok' : 'dn-fail'}`}>
          {s.status}
        </span>
        <span className="dn-meta">
          <span>{kindPill(s.kind)}</span>
          <span>·</span>
          <span className="dn-mono">{s.version}</span>
          <span>·</span>
          <span className="dn-mono">{s.commit}</span>
          <span>·</span>
          <span>{s.instances} {s.instances === 1 ? 'instance' : 'instances'}</span>
          <span>·</span>
          <span>last seen <span className="dn-mono">{s.lastSeen}</span></span>
        </span>
      </div>

      <div className="dn-sectionh">Endpoints · $SRV</div>
      {!live && endpoints.length > 0 && <div className="dn-callout">
          <span className="dn-cicon">⚠</span>
          <div style={{
        flex: 1
      }}>
            <div className="dn-ctitle">$SRV.STATS preview — requires nats micro adoption (not yet wired)</div>
            <div className="dn-cbody">
              dagnats does not yet run the full nats micro framework, so these
              per-endpoint stats are illustrative of the shape $SRV discovery
              would expose once <span className="dn-mono">{s.name}</span> adopts
              it. The roster + health above is live today.
            </div>
          </div>
        </div>}

      <div className="dn-card">
        {endpoints.length > 0 ? <table>
            <thead>
              <tr>
                <th>Subject</th>
                <th>Requests</th>
                <th>Errors</th>
                <th>Avg latency</th>
                <th>Last error</th>
              </tr>
            </thead>
            <tbody>
              {endpoints.map(e => <tr key={e.subject}>
                  <td className="dn-idcell">
                    {e.subject}
                    {e.live && <span className="dn-logchip" style={{
                color: 'var(--dn-teal)',
                borderColor: '#1d5b4f',
                background: '#10231580'
              }}>
                        ● live
                      </span>}
                  </td>
                  <td className="dn-mono">{e.numRequests}</td>
                  <td className="dn-mono" style={e.numErrors > 0 ? {
              color: '#ff8a93',
              fontWeight: 600
            } : {
              color: 'var(--dn-muted)'
            }}>
                    {e.numErrors}
                  </td>
                  <td className="dn-mono dn-dim">{e.avgLatency}</td>
                  <td className="dn-mono dn-dim">{e.lastError}</td>
                </tr>)}
            </tbody>
          </table> : <div className="dn-dim" style={{
        padding: '14px 16px',
        fontSize: 12.5,
        lineHeight: 1.6
      }}>
            No endpoints discovered — this service does not advertise $SRV yet.
          </div>}
      </div>
    </>;
}

// ---------------- Connections ----------------
// Every client connected to the embedded NATS server (/connz). pending_bytes is
// the slow-consumer signal (0 everywhere → 0 slow consumers); idle is time since
// last activity. wkr-7c04's high idle correlates with the image-pipeline task
// backlog seen in Consumers.
export function ConnectionsView({
  readOnly,
  allowDestructive
}: {
  readOnly: boolean;
  allowDestructive: boolean;
}) {
  const idleWarn = CONNECTIONS.find(c => c.tone === 'dn-warn');
  // Which connection's drain confirm is open (by cid).
  const [draining, setDraining] = useState<ConnRow | null>(null);
  // Core services (engine/api/trigger) auto-reconnect; note that in the preview.
  const isCore = (c: ConnRow) => c.kind === 'engine' || c.kind === 'api' || c.kind === 'trigger';
  return <>
      <div className="dn-srcline">
        Every client connected to the embedded NATS server (/connz). pending_bytes
        is the slow-consumer signal; idle is time since last activity.
      </div>

      {idleWarn && <div className="dn-callout">
          <span className="dn-cicon">⚠</span>
          <div style={{
        flex: 1
      }}>
            <div className="dn-ctitle">{idleWarn.name} — idle {idleWarn.idle}, pending_bytes {idleWarn.pendingBytes}</div>
            <div className="dn-cbody">
              High idle (not a slow consumer — pending_bytes is 0) means this
              worker has not pulled recently. This is the upstream of the
              <span className="dn-mono"> task.image-pipeline.&gt;</span> backlog
              with 0 waiting pulls seen in Consumers. 0 slow consumers across all
              connections.
            </div>
          </div>
        </div>}

      <div className="dn-card">
        <div className="dn-chead">
          Connected clients
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            {CONNECTIONS.length} connections · 0 slow consumers
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>CID</th>
              <th>Client</th>
              <th>Kind</th>
              <th>Lang / ver</th>
              <th>RTT</th>
              <th>Uptime</th>
              <th>Idle</th>
              <th>Subs</th>
              <th>Pending bytes</th>
              <th>In / out msgs</th>
              <th style={{
              textAlign: 'right'
            }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {CONNECTIONS.map(c => {
            const warn = c.tone === 'dn-warn';
            return <tr key={c.cid}>
                  <td className="dn-mono dn-dim">{c.cid}</td>
                  <td className="dn-idcell">{c.name}</td>
                  <td><span className="dn-type">{c.kind}</span></td>
                  <td className="dn-mono dn-dim">{c.lang} · {c.version}</td>
                  <td className="dn-mono dn-dim">{c.rtt}</td>
                  <td className="dn-mono dn-dim">{c.uptime}</td>
                  <td className="dn-mono" style={warn ? {
                color: 'var(--dn-amber)',
                fontWeight: 600
              } : {
                color: 'var(--dn-muted)'
              }}>
                    {c.idle}
                  </td>
                  <td className="dn-mono dn-dim">{c.subs}</td>
                  <td className="dn-mono dn-dim">{c.pendingBytes}</td>
                  <td className="dn-mono dn-dim">{c.inMsgs} / {c.outMsgs}</td>
                  <td>
                    <div className="dn-rowact">
                      <DangerButton label="Drain" tier={1} readOnly={readOnly} allowDestructive={allowDestructive} onClick={() => setDraining(c)} />
                    </div>
                  </td>
                </tr>;
          })}
          </tbody>
        </table>
      </div>

      <div className="dn-srcline" style={{
      marginTop: 12,
      marginBottom: 0
    }}>
        {CONN_PENDING_NOTE}
      </div>

      {draining && <ConfirmModal title={`Drain connection cid ${draining.cid}`} tone="warn" confirmLabel="Drain connection" requireTyped={undefined} preview={`Drain connection cid ${draining.cid} (${draining.name}):\n  subscriptions close, client reconnects.\n  ${draining.subs} in-flight settle first.` + (isCore(draining) ? `\n  Note: ${draining.kind} is a core service — it auto-reconnects.` : '')} bodyNote="Closes the client connection gracefully; the client is expected to reconnect." onConfirm={() => {}} onCancel={() => setDraining(null)} />}
    </>;
}