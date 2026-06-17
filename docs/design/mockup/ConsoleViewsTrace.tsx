import { useState } from 'react';
import { TRACES, TRACE_SPANS, type TraceStatus, type SpanRow } from './consoleData';

// Status pill shared by the list and detail header.
function TracePill({
  status
}: {
  status: TraceStatus;
}) {
  return <span className={`dn-pill ${status === 'ok' ? 'dn-ok' : 'dn-fail'}`}>{status === 'ok' ? 'ok' : 'error'}</span>;
}

// ---------------- Traces list ----------------
export function TracesView({
  onOpen
}: {
  onOpen: (id: string) => void;
}) {
  return <>
      <div className="dn-srcline">
        OTLP spans · telemetry.spans.&#123;service&#125;.&#123;runID&#125; · one trace per run (W3C traceparent)
      </div>
      <div className="dn-filterbar">
        <input className="dn-input" placeholder="service: any" style={{
        width: 160
      }} />
        <input className="dn-input" placeholder="status: any" style={{
        width: 140
      }} />
        <input className="dn-input" placeholder="since: 1h" style={{
        width: 140
      }} />
        <input className="dn-input" placeholder="trace id / root operation" style={{
        flex: 1
      }} />
        <button className="dn-btn">Find</button>
      </div>
      <div className="dn-card">
        <div className="dn-chead">
          Recent traces · live
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            click a trace → span tree
          </span>
        </div>
        <table>
          <thead>
            <tr>
              <th>Trace ID</th>
              <th>Root operation</th>
              <th>Service</th>
              <th>Spans</th>
              <th>Duration</th>
              <th>Status</th>
              <th>Started</th>
            </tr>
          </thead>
          <tbody>
            {TRACES.map(t => <tr key={t.id} onClick={() => onOpen(t.id)}>
                <td className="dn-idcell">{t.id}</td>
                <td className="dn-mono">{t.rootOp}</td>
                <td className="dn-mono dn-dim">{t.service}</td>
                <td className="dn-mono">{t.spans}</td>
                <td className="dn-mono">{t.duration}</td>
                <td>
                  <TracePill status={t.status} />
                </td>
                <td className="dn-dim dn-mono">{t.started}</td>
              </tr>)}
          </tbody>
        </table>
      </div>
    </>;
}

// ---------------- Trace detail (span tree / waterfall) ----------------
function KvRow({
  k,
  v,
  tone
}: {
  k: string;
  v: string;
  tone?: string;
}) {
  return <div className="dn-kv">
      <span className="dn-k">{k}</span>
      <span className="dn-v" style={tone ? {
      color: tone
    } : undefined}>{v}</span>
    </div>;
}
export function TraceDetailView({
  id,
  onBack
}: {
  id: string;
  onBack: () => void;
}) {
  const trace = TRACES.find(t => t.id === id) ?? TRACES[0];
  // The mock span tree is the image-pipeline shape; reuse it for any trace.
  const spans = TRACE_SPANS;
  const [selected, setSelected] = useState<string>('s2');
  const sel: SpanRow = spans.find(s => s.id === selected) ?? spans[0];
  return <>
      <button className="dn-back" onClick={onBack}>
        <span className="dn-bk">‹</span> Traces
      </button>
      <div className="dn-detailhead">
        <span className="dn-monoid">{trace.id}</span>
        <span className="dn-meta">
          <span>root: <span className="dn-mono">{trace.rootOp}</span></span>
          <span>·</span>
          <span className="dn-mono">total {trace.duration}</span>
        </span>
        <div className="dn-acts">
          <TracePill status={trace.status} />
        </div>
      </div>
      <div className="dn-card">
        <div className="dn-chead">
          Span tree · waterfall
          <span className="dn-dim" style={{
          marginLeft: 'auto',
          fontWeight: 400
        }}>
            click a span for attributes
          </span>
        </div>
        <div className="dn-waterfall">
          {spans.map(s => <div key={s.id} className={`dn-span${s.id === selected ? ' dn-sel' : ''}`} onClick={() => setSelected(s.id)}>
              <div className="dn-spanname" style={{
            paddingLeft: s.depth * 16
          }}>
                {s.depth === 0 ? '▸ ' : ''}{s.name}
              </div>
              <div className="dn-spantrack">
                <div className={`dn-spanbar${s.barClass ? ` dn-${s.barClass}` : ''}`} style={{
              left: `${s.offsetPct}%`,
              width: `${s.widthPct}%`
            }} />
              </div>
              <div className="dn-spandur">{s.duration}</div>
            </div>)}
        </div>
      </div>
      <div className="dn-card" style={{
      marginTop: 16
    }}>
        <div className="dn-chead">Span detail · {sel.name}</div>
        <div style={{
        padding: '6px 16px 12px'
      }}>
          <KvRow k="span_id" v={sel.spanId} />
          <KvRow k="parent_span_id" v={sel.parentSpanId} />
          <KvRow k="duration" v={sel.duration} />
          <KvRow k="status" v={sel.status === 'ok' ? 'ok' : 'error'} tone={sel.status === 'ok' ? 'var(--dn-green)' : '#ff8a93'} />
          <KvRow k="workflow_id" v={sel.workflowId} />
          <KvRow k="step_id" v={sel.stepId} />
          <KvRow k="task_id" v={sel.taskId} />
          <KvRow k="run_id" v={sel.runId} />
        </div>
      </div>
    </>;
}