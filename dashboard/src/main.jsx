import React, {useEffect, useMemo, useRef, useState} from 'react';
import {createRoot} from 'react-dom/client';
import {
  Activity, AlertTriangle, Ban, CheckCircle2, ChevronRight, CircleDashed,
  FlaskConical, GitBranch, History, Play, RotateCcw, ShieldCheck, Wrench,
  XCircle, Zap,
} from 'lucide-react';
import './style.css';

const STATUS_COLORS = {
  pending: '#94a3b8', running: '#d97706', succeeded: '#16a34a',
  failed: '#dc2626', uncertain: '#7c3aed', skipped: '#64748b',
};
const LAB_FAMILIES = [
  ['crash_after_append', 'Crash after a side effect'],
  ['crash_before_write', 'Crash before a side effect'],
  ['transient_storm', 'Transient failure storm'],
  ['uncertain_write', 'Uncertain write outcome'],
  ['timeout_pure_tool', 'Timeout on a pure tool'],
];

function depthMap(steps) {
  const byId = Object.fromEntries(steps.map(s => [s.id, s]));
  const depth = {};
  const visit = (id, seen) => {
    if (depth[id] !== undefined) return depth[id];
    if (seen.has(id)) return 0;
    seen.add(id);
    const deps = byId[id]?.depends_on || [];
    depth[id] = deps.length ? 1 + Math.max(...deps.map(d => visit(d, seen))) : 0;
    return depth[id];
  };
  steps.forEach(s => visit(s.id, new Set()));
  return depth;
}

function Graph({steps, states}) {
  const depth = useMemo(() => depthMap(steps), [steps]);
  const cols = useMemo(() => {
    const map = new Map();
    steps.forEach(s => {
      const d = depth[s.id] || 0;
      if (!map.has(d)) map.set(d, []);
      map.get(d).push(s);
    });
    return [...map.entries()].sort((a, b) => a[0] - b[0]);
  }, [steps, depth]);
  const pos = {};
  cols.forEach(([d, nodes]) => nodes.forEach((s, i) => {
    pos[s.id] = {x: 24 + d * 224, y: 24 + i * 84};
  }));
  const width = 48 + cols.length * 224;
  const height = 48 + Math.max(1, ...cols.map(([, n]) => n.length)) * 84;
  return (
    <svg className="graph" viewBox={`0 0 ${width} ${height}`} width="100%"
         style={{minWidth: Math.min(width, 640)}}>
      {steps.flatMap(s => (s.depends_on || []).map(dep => {
        const a = pos[dep], b = pos[s.id];
        if (!a || !b) return null;
        const x1 = a.x + 176, y1 = a.y + 27, x2 = b.x, y2 = b.y + 27;
        const mid = (x1 + x2) / 2;
        return <path key={dep + s.id}
          d={`M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`}
          className="edge" />;
      }))}
      {steps.map(s => {
        const p = pos[s.id];
        const st = states[s.id] || 'pending';
        return (
          <g key={s.id} transform={`translate(${p.x},${p.y})`}>
            <rect width="176" height="54" rx="10"
              className="nodebox" style={{stroke: STATUS_COLORS[st]}} />
            <circle cx="16" cy="27" r="5" fill={STATUS_COLORS[st]}
              className={st === 'running' ? 'pulse' : ''} />
            <text x="30" y="23" className="nodetitle">
              {s.title || s.id}
            </text>
            <text x="30" y="41" className="nodesub">
              {s.id} · {s.tool}
            </text>
            <text x="166" y="20" textAnchor="end" className="nodestate"
              fill={STATUS_COLORS[st]}>{st}</text>
          </g>
        );
      })}
    </svg>
  );
}

function App() {
  const [examples, setExamples] = useState([]);
  const [task, setTask] = useState(null);
  const [faultTool, setFaultTool] = useState('');
  const [faultKind, setFaultKind] = useState('none');
  const [faultCount, setFaultCount] = useState(1);
  const [runId, setRunId] = useState(null);
  const [states, setStates] = useState({});
  const [timeline, setTimeline] = useState([]);
  const [actions, setActions] = useState([]);
  const [report, setReport] = useState(null);
  const [benchmarks, setBenchmarks] = useState(null);
  const [lab, setLab] = useState(null);
  const [labBusy, setLabBusy] = useState('');
  const [pastRuns, setPastRuns] = useState([]);
  const [connected, setConnected] = useState(false);
  const labIndex = useRef(0);
  const runIdRef = useRef(null);
  runIdRef.current = runId;

  useEffect(() => {
    fetch('/api/examples').then(r => r.json()).then(list => {
      setExamples(list);
      if (list.length) selectTask(list[0]);
    });
    fetch('/api/benchmarks').then(r => r.json()).then(setBenchmarks);
    loadPastRuns();
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${proto}://${location.host}/ws`);
    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      const {kind, payload} = msg;
      if (payload.run_id && runIdRef.current &&
          payload.run_id !== runIdRef.current) return;
      const nodeKinds = {
        node_started: 'running', node_succeeded: 'succeeded',
        node_failed: 'failed', node_uncertain: 'uncertain',
        node_skipped: 'skipped',
      };
      if (nodeKinds[kind] && payload.node) {
        setStates(prev => ({...prev, [payload.node]: nodeKinds[kind]}));
      }
      if (kind !== 'hello') {
        setTimeline(prev => [{
          kind, node: payload.node || '', tool: payload.tool || '',
          at: new Date().toLocaleTimeString(),
        }, ...prev].slice(0, 40));
      }
      if (kind === 'report_ready' && payload.run_id) {
        loadPastRuns();
        fetch(`/api/runs/${payload.run_id}`).then(r => r.json()).then(data => {
          const st = {};
          Object.entries(data.states || {}).forEach(([k, v]) => {
            st[k] = v.status;
          });
          setStates(st);
          setActions(data.actions || []);
          setReport(data.report);
        });
      }
    };
    return () => ws.close();
  }, []);

  function loadPastRuns() {
    fetch('/api/runs').then(r => r.json()).then(setPastRuns).catch(() => {});
  }

  async function openPastRun(id) {
    const data = await (await fetch(`/api/runs/${id}`)).json();
    if (data.task) {
      setTask(data.task);
      setFaultTool(data.task.steps[0]?.tool || '');
    }
    setRunId(id);
    const st = {};
    Object.entries(data.states || {}).forEach(([k, v]) => { st[k] = v.status; });
    setStates(st);
    setActions(data.actions || []);
    setReport(data.report || null);
    setTimeline((data.events || []).slice().reverse().map(e => ({
      kind: e.kind, node: e.payload?.node || '',
      tool: e.payload?.tool || '', at: '',
    })));
  }

  function selectTask(t) {
    setTask(t);
    const tools = [...new Set(t.steps.map(s => s.tool))];
    setFaultTool(tools[0] || '');
    setStates({});
    setTimeline([]);
    setActions([]);
    setReport(null);
    setRunId(null);
  }

  function buildFaults() {
    if (faultKind === 'none' || !faultTool) return null;
    if (faultKind === 'transient') return {fail_times: {[faultTool]: faultCount}};
    if (faultKind === 'uncertain') return {uncertain_after: {[faultTool]: 1}};
    if (faultKind === 'delay') return {delay: {[faultTool]: faultCount}};
    return null;
  }

  async function startRun() {
    const resp = await fetch('/api/runs', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({task, faults: buildFaults()}),
    });
    const data = await resp.json();
    setRunId(data.run_id);
    setReport(null);
    setActions([]);
    setTimeline([]);
    const st = {};
    task.steps.forEach(s => { st[s.id] = 'pending'; });
    setStates(st);
  }

  async function resume() {
    await fetch(`/api/runs/${runId}/resume`, {method: 'POST'});
  }

  async function runLab(family) {
    setLabBusy(family);
    setLab(null);
    const idx = labIndex.current % 10;
    labIndex.current += 1;
    const resp = await fetch(`/api/lab/${family}?index=${idx}`,
      {method: 'POST'});
    setLab(await resp.json());
    setLabBusy('');
  }

  const bench = benchmarks?.['results'];
  const labBench = benchmarks?.['failure-lab-results'];
  const tools = task ? [...new Set(task.steps.map(s => s.tool))] : [];

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand"><span className="logo">◈</span> Argon
          <span className="beta">LAB</span></div>
        <p className="tagline">Multi agent orchestration with scheduling,
          permissions, failure recovery, and verification.</p>
        <div className={'conn' + (connected ? ' on' : '')}>
          <i />{connected ? 'Live event stream connected' : 'Connecting…'}
        </div>

        <div className="sidehead"><Play size={14} /> RUN A WORKFLOW</div>
        <div className="tasklist">
          {examples.map(t => (
            <button key={t.id}
              className={'task' + (task?.id === t.id ? ' active' : '')}
              onClick={() => selectTask(t)}>
              <b>{t.title}</b><small>{t.steps.length} steps · {
                t.steps.map(s => s.tool).filter((v, i, a) => a.indexOf(v) === i).join(', ')
              }</small>
            </button>
          ))}
        </div>

        <div className="sidehead"><Wrench size={14} /> INJECT A FAULT</div>
        <label className="field">Tool
          <select value={faultTool} onChange={e => setFaultTool(e.target.value)}>
            {tools.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </label>
        <label className="field">Fault
          <select value={faultKind} onChange={e => setFaultKind(e.target.value)}>
            <option value="none">None, clean run</option>
            <option value="transient">Transient failures, then recover</option>
            <option value="uncertain">Side effect lands, outcome unknown</option>
            <option value="delay">Slow call (seconds)</option>
          </select>
        </label>
        {faultKind !== 'none' && faultKind !== 'uncertain' && (
          <label className="field">Count / seconds
            <input type="number" min="1" max="5" value={faultCount}
              onChange={e => setFaultCount(Number(e.target.value))} />
          </label>
        )}
        <button className="primary" onClick={startRun} disabled={!task}>
          <Play size={15} /> Run workflow
        </button>
        {report && report.status !== 'succeeded' && (
          <button className="resume" onClick={resume}>
            <RotateCcw size={15} /> Resume from journal
          </button>
        )}

        <div className="sidehead"><FlaskConical size={14} /> FAILURE LAB</div>
        <p className="hint">Crash scenarios run in an isolated process,
          then resume from the journal alone.</p>
        <div className="lablist">
          {LAB_FAMILIES.map(([fam, label]) => (
            <button key={fam} onClick={() => runLab(fam)}
              disabled={!!labBusy}>
              {label}{labBusy === fam ? ' …' : ''}
            </button>
          ))}
        </div>
        {lab && (
          <div className={'labresult ' + (lab.passed ? 'pass' : 'fail')}>
            {lab.passed ? <CheckCircle2 size={16} /> : <XCircle size={16} />}
            <div><b>{lab.passed ? 'Recovered correctly' : 'Recovery failed'}</b>
              <small>{lab.detail}</small>
              <small>
                {lab.crashed_mid_run ? 'Crashed mid run · ' : ''}
                final: {lab.final_status} · {lab.duration_s}s
              </small>
            </div>
          </div>
        )}

        {bench && (
          <>
            <div className="sidehead"><Activity size={14} /> BENCHMARK</div>
            <div className="benchrow"><span>Argon</span>
              <div className="bar"><i style={{width: bench.argon.completed + '%'}} /></div>
              <b>{bench.argon.completed}/100</b></div>
            <div className="benchrow dim"><span>Single agent</span>
              <div className="bar"><i style={{width: bench.baseline.completed + '%'}} /></div>
              <b>{bench.baseline.completed}/100</b></div>
            <p className="hint">Same tasks, tools, and fault plans.
              Median task time: Argon {bench.argon.median_latency_s}s ·
              baseline {bench.baseline.median_latency_s}s (durability costs
              journal writes; it buys completion).</p>
            {labBench && <p className="hint">Failure lab: {
              labBench.passed}/{labBench.scenarios} scenarios recovered
              correctly.</p>}
          </>
        )}
      </aside>

      <main>
        <header>
          <div>Workspace <ChevronRight size={13} /> <b>{task?.title || '…'}</b>
          </div>
          {report && (
            <span className={'runstatus ' + report.status}>
              {report.status}
              {report.verified !== null && report.verified !== undefined &&
                ` · verifier ${report.verified ? 'passed' : 'failed'}`}
              {report.duration_s !== undefined &&
                ` · ${report.duration_s.toFixed(2)}s`}
            </span>
          )}
        </header>

        {pastRuns.length > 0 && (
          <div className="recent">
            <History size={13} /> Recent runs
            {pastRuns.slice(0, 8).map(r => (
              <button key={r.run_id} className="runchip"
                onClick={() => openPastRun(r.run_id)}>
                <i style={{background: STATUS_COLORS[r.status] || '#94a3b8'}} />
                {r.title?.slice(0, 34) || r.run_id} · {r.status}
              </button>
            ))}
          </div>
        )}

        <section className="panel">
          <div className="panelhead"><GitBranch size={15} /> DEPENDENCY GRAPH
            <span>planner output · live state</span></div>
          {task ? <Graph steps={task.steps} states={states} />
            : <p className="empty">Loading tasks…</p>}
        </section>

        <div className="twocol">
          <section className="panel">
            <div className="panelhead"><History size={15} /> TOOL ACTION JOURNAL
              <span>every call, recorded before it runs</span></div>
            {actions.length ? (
              <table className="journal">
                <thead><tr><th>Node</th><th>Tool</th><th>Status</th>
                  <th>Idempotency key</th></tr></thead>
                <tbody>{actions.map((a, i) => (
                  <tr key={i}>
                    <td>{a.node_id}</td><td>{a.tool}</td>
                    <td><span className={'chip ' + a.status}>{a.status}</span></td>
                    <td className="mono">{a.idempotency_key.slice(0, 18)}…</td>
                  </tr>
                ))}</tbody>
              </table>
            ) : <p className="empty">No actions yet. Run the workflow and
              watch the journal fill in.</p>}
          </section>

          <section className="panel">
            <div className="panelhead"><ShieldCheck size={15} /> VERIFIER EVIDENCE
              <span>checked by tools, not by trust</span></div>
            {report?.checks?.length ? report.checks.map((c, i) => (
              <div key={i} className="check">
                {c.ok ? <CheckCircle2 size={15} className="ok" />
                      : <XCircle size={15} className="bad" />}
                <div><b>{c.criterion.type}</b>
                  <small>{c.criterion.path || c.criterion.expression} · {
                    JSON.stringify(c.evidence).slice(0, 110)}</small>
                </div>
              </div>
            )) : <p className="empty">The verifier runs after execution and
              checks every acceptance criterion with its own read only
              tool calls.</p>}
          </section>
        </div>

        <section className="panel">
          <div className="panelhead"><Zap size={15} /> EVENT TIMELINE</div>
          {timeline.length ? timeline.map((e, i) => (
            <div key={i} className="event">
              <span className="etime">{e.at}</span>
              <span className={'ekind ' + e.kind}>{e.kind}</span>
              <span>{e.node}{e.tool ? ` · ${e.tool}` : ''}</span>
            </div>
          )) : <p className="empty">Events stream here as the scheduler
            works: node starts, action retries, uncertain outcomes,
            reconciliations.</p>}
        </section>

        <footer>
          <Ban size={13} /> Role scoped tools are enforced on every call:
          the planner cannot write, the verifier cannot execute.
          <AlertTriangle size={13} /> Uncertain actions are never retried
          blindly; they are reconciled against evidence first.
        </footer>
      </main>
    </div>
  );
}

createRoot(document.getElementById('root')).render(<App />);
