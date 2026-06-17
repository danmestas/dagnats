import { useState } from 'react';
import { NAV, VIEW_TO_NAV, TILES, TITLES, TRIGGERS, type NavItem, type ViewKey, type TriggerRow } from './consoleData';
import { DashboardView, WorkflowsView, WorkflowDetailView, RunsView, RunDetailView, TriggersView, TriggerDetailView } from './ConsoleViewsObserve';
import { FunctionsView, FunctionDetailView, WorkersView, WorkerDetailView, StreamsView, KvView } from './ConsoleViewsInventory';
import { DlqView, DlqDetailView, LogsView, MetricsView, ConcurrencyView, AuditView, ConfigView } from './ConsoleViewsOperate';
import { ServerHealthView, ConsumersView, StreamDetailView, ServicesView, ServiceDetailView, ConnectionsView } from './ConsoleViewsSystem';
import { TracesView, TraceDetailView } from './ConsoleViewsTrace';
import { TRACES } from './consoleData';
const GROUPS = ['Inventory', 'Activity', 'System'];
export function ConsoleRedesign() {
  const [view, setView] = useState<ViewKey>('dashboard');
  const [readOnly, setReadOnly] = useState(false);
  const [allowDestructive, setAllowDestructive] = useState(false);
  const [copied, setCopied] = useState(false);
  const [drillWorkflow, setDrillWorkflow] = useState('image-pipeline');
  const [drillRun, setDrillRun] = useState('8aaf9b3a');
  const [drillDlq, setDrillDlq] = useState('dl-6689ab0b');
  const [drillWorker, setDrillWorker] = useState('wkr-3a1f');
  const [drillFunction, setDrillFunction] = useState('fetch-urls');
  const [drillTrigger, setDrillTrigger] = useState('trg-cron-nightly');
  const [drillTrace, setDrillTrace] = useState('a662ac7eddee');
  const [drillStream, setDrillStream] = useState('TASK_QUEUES');
  const [drillService, setDrillService] = useState('trigger-svc');
  // Trigger list lives at the shell so the list view, the detail view and the
  // add/edit modal all mutate the same source of truth (add/edit/delete/toggle).
  const [triggers, setTriggers] = useState<TriggerRow[]>(() => TRIGGERS.map(t => ({
    ...t
  })));
  function pick(item: NavItem) {
    setView(item.view);
  }
  const activeNav = VIEW_TO_NAV[view];
  const [title, sub] = TITLES[view];
  const tiles = TILES[view];
  return <div className="dn-shell">
      {/* ---------- side rail ---------- */}
      <aside className="dn-rail">
        <div className="dn-brand">
          <span className="dn-logo">◆</span> dagnats <small>v0.1.0</small>
        </div>
        <nav className="dn-nav">
          {(() => {
          const d = NAV.find(n => n.id === 'dashboard');
          return d ? <div key={d.id} className={`dn-navitem${activeNav === d.id ? ' dn-active' : ''}`} onClick={() => pick(d)} style={{
            marginBottom: 4
          }}>
              <span className="dn-i">{d.icon}</span> {d.label}
              {d.badge !== undefined && <span className="dn-badge">{d.badge}</span>}
            </div> : null;
        })()}
          {GROUPS.map(g => <div key={g}>
              <div className="dn-navgroup">{g}</div>
              {NAV.filter(n => n.group === g && n.id !== 'dashboard').map(n => <div key={n.id} className={`dn-navitem${activeNav === n.id ? ' dn-active' : ''}`} onClick={() => pick(n)}>
              
                  <span className="dn-i">{n.icon}</span> {n.label}
                  {n.badge !== undefined && <span className="dn-badge">{n.badge}</span>}
                </div>)}
            </div>)}
        </nav>
        <div className="dn-footer">
          <div className="dn-row">
            <span className="dn-onl">● ONLINE</span> <span>8/8 streams</span>
          </div>
          <div className="dn-row">
            <span>nats://127.0.0.1:4222</span>
            <span className="dn-copy" onClick={() => setCopied(true)}>
              {copied ? '✓ copied' : 'copy'}
            </span>
          </div>
          <div className="dn-row dn-dim">embedded · commit 5824fe3</div>
        </div>
      </aside>

      {/* ---------- main ---------- */}
      <div className="dn-main">
        <div className="dn-topbar">
          <div className="dn-titlerow">
            <h1>{title}</h1>
            <span className="dn-sub">{sub}</span>
            <span className="dn-spacer" />
            <span className="dn-toggle" onClick={() => setReadOnly(v => !v)}>
              Read-only: {readOnly ? 'on' : 'off'}
            </span>
            <span className="dn-toggle" onClick={() => setAllowDestructive(v => !v)}>
              Destructive: {allowDestructive ? 'on' : 'off'}
            </span>
            {(view === 'triggers' || view === 'triggersEmpty') && <span className="dn-toggle" onClick={() => setView(view === 'triggers' ? 'triggersEmpty' : 'triggers')}>
              
                {view === 'triggers' ? 'Show empty state' : 'Show populated'}
              </span>}
          </div>
          {tiles.length > 0 && <div className="dn-tiles">
              {tiles.map(([n, l, tone], i) => <div key={i} className={`dn-tile ${tone}`}>
                  <div className="dn-n">{n}</div>
                  <div className="dn-l">{l}</div>
                </div>)}
            </div>}
        </div>

        <div className="dn-content">
          {readOnly && <div className="dn-robanner">
              Read-only mode — mutations disabled. Inline actions are shown but inert.
            </div>}

          {allowDestructive && <div className="dn-dbanner">
              Destructive actions enabled — tier 2–3 unlocked (CONSOLE_ALLOW_DESTRUCTIVE).
            </div>}

          {view === 'dashboard' && <DashboardView />}

          {view === 'workflows' && <WorkflowsView onOpen={name => {
          setDrillWorkflow(name);
          setView('workflowDetail');
        }} />}
          {view === 'workflowDetail' && <WorkflowDetailView name={drillWorkflow} onBack={() => setView('workflows')} onOpenRun={id => {
          setDrillRun(id);
          setView('runDetail');
        }} />}

          {view === 'runs' && <RunsView onOpen={id => {
          setDrillRun(id);
          setView('runDetail');
        }} />}
          {view === 'runDetail' && <RunDetailView id={drillRun} onBack={() => setView('runs')} onOpenTrace={runId => {
          const t = TRACES.find(tr => tr.runId === runId) ?? TRACES[0];
          setDrillTrace(t.id);
          setView('traceDetail');
        }} />}

          {view === 'triggers' && <TriggersView triggers={triggers} setTriggers={setTriggers} readOnly={readOnly} onOpen={id => {
          setDrillTrigger(id);
          setView('triggerDetail');
        }} />}
          {view === 'triggerDetail' && <TriggerDetailView id={drillTrigger} triggers={triggers} setTriggers={setTriggers} readOnly={readOnly} onBack={() => setView('triggers')} />}
          {view === 'triggersEmpty' && <div className="dn-empty">
              <div className="dn-big">0 TRIGGERS</div>
              <p>
                Workflows fire when triggers match. Configure cron schedules, NATS subject
                subscriptions, webhooks, or synchronous HTTP endpoints.
              </p>
              <div className="dn-acts">
                <button className="dn-btn dn-primary">+ Add trigger</button>
                <button className="dn-btn">Docs: trigger types →</button>
              </div>
            </div>}

          {view === 'functions' && <FunctionsView readOnly={readOnly} onOpen={id => {
          setDrillFunction(id);
          setView('functionDetail');
        }} />}
          {view === 'functionDetail' && <FunctionDetailView id={drillFunction} readOnly={readOnly} onBack={() => setView('functions')} onOpenWorker={id => {
          setDrillWorker(id);
          setView('workerDetail');
        }} onNavigate={v => setView(v)} />}
          {view === 'workers' && <WorkersView readOnly={readOnly} onOpen={id => {
          setDrillWorker(id);
          setView('workerDetail');
        }} />}
          {view === 'workerDetail' && <WorkerDetailView id={drillWorker} readOnly={readOnly} allowDestructive={allowDestructive} onBack={() => setView('workers')} />}
          {view === 'health' && <ServerHealthView readOnly={readOnly} allowDestructive={allowDestructive} />}
          {view === 'services' && <ServicesView onOpen={n => {
          setDrillService(n);
          setView('serviceDetail');
        }} />}
          {view === 'serviceDetail' && <ServiceDetailView name={drillService} onBack={() => setView('services')} />}
          {view === 'connections' && <ConnectionsView readOnly={readOnly} allowDestructive={allowDestructive} />}
          {view === 'consumers' && <ConsumersView />}
          {view === 'streams' && <StreamsView onOpen={name => {
          setDrillStream(name);
          setView('streamDetail');
        }} />}
          {view === 'streamDetail' && <StreamDetailView name={drillStream} readOnly={readOnly} allowDestructive={allowDestructive} onBack={() => setView('streams')} />}
          {view === 'kv' && <KvView readOnly={readOnly} allowDestructive={allowDestructive} />}

          {view === 'dlq' && <DlqView onOpen={id => {
          setDrillDlq(id);
          setView('dlqDetail');
        }} />}
          {view === 'dlqDetail' && <DlqDetailView id={drillDlq} onBack={() => setView('dlq')} />}
          {view === 'logs' && <LogsView onOpenTrace={traceId => {
          const t = TRACES.find(tr => tr.id === traceId || tr.id.startsWith(traceId)) ?? TRACES[0];
          setDrillTrace(t.id);
          setView('traceDetail');
        }} />}
          {view === 'traces' && <TracesView onOpen={id => {
          setDrillTrace(id);
          setView('traceDetail');
        }} />}
          {view === 'traceDetail' && <TraceDetailView id={drillTrace} onBack={() => setView('traces')} />}
          {view === 'metrics' && <MetricsView onViewRuns={() => setView('runs')} />}
          {view === 'concurrency' && <ConcurrencyView />}
          {view === 'audit' && <AuditView onOpenTarget={(kind, id) => {
          if (kind === 'dlq') {
            setDrillDlq(id);
            setView('dlqDetail');
          } else if (kind === 'trigger') {
            setDrillTrigger(id);
            setView('triggerDetail');
          } else if (kind === 'workflow') {
            setDrillWorkflow(id);
            setView('workflowDetail');
          } else if (kind === 'run') {
            setDrillRun(id);
            setView('runDetail');
          }
        }} />}
          {view === 'config' && <ConfigView />}
        </div>
      </div>

      <div className="dn-note">
        redesign prototype · IoskeleyMono for data cells · click the rail to switch views
      </div>
    </div>;
}