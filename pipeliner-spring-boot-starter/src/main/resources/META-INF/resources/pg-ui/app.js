/* Pipeliner 控制台：纯原生 JS + 内联 SVG，无外部依赖。 */
'use strict';

const API = '../pg';
const SVGNS = 'http://www.w3.org/2000/svg';
const $ = (id) => document.getElementById(id);

/* ------------------------------------------------------------------ helpers */
function el(tag, attrs, ...kids) {
  const e = document.createElement(tag);
  fill(e, attrs, kids);
  return e;
}
function sv(tag, attrs, ...kids) {
  const e = document.createElementNS(SVGNS, tag);
  fill(e, attrs, kids);
  return e;
}
function fill(e, attrs, kids) {
  if (attrs) {
    for (const k of Object.keys(attrs)) {
      const v = attrs[k];
      if (v === null || v === undefined || v === false) continue;
      if (k === 'text') e.textContent = v;
      else if (k === 'on') for (const ev of Object.keys(v)) e.addEventListener(ev, v[ev]);
      else e.setAttribute(k, v === true ? '' : String(v));
    }
  }
  for (const c of kids.flat()) {
    if (c === null || c === undefined || c === false) continue;
    e.appendChild(typeof c === 'object' ? c : document.createTextNode(String(c)));
  }
}
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const trunc = (s, n) => { s = s == null ? '' : String(s); return s.length > n ? s.slice(0, n - 1) + '…' : s; };
const fmtNum = (n) => (n == null ? '—' : Number(n).toLocaleString('en-US'));
function fmtDur(ms) {
  if (ms == null || isNaN(ms)) return '—';
  ms = Math.max(0, ms);
  const s = Math.floor(ms / 1000);
  if (s < 60) return s + 's';
  const m = Math.floor(s / 60);
  if (m < 60) return m + 'm ' + String(s % 60).padStart(2, '0') + 's';
  const h = Math.floor(m / 60);
  return h + 'h ' + String(m % 60).padStart(2, '0') + 'm';
}
function fmtTs(ms) {
  if (ms == null) return '—';
  const d = new Date(ms);
  const p = (n) => String(n).padStart(2, '0');
  const t = p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
  return new Date().toDateString() === d.toDateString() ? t : (d.getMonth() + 1) + '-' + p(d.getDate()) + ' ' + t;
}
const TERMINAL = { SUCCEEDED: 1, FAILED: 1, CANCELLED: 1 };
const GATE_ZH = { BARRIER: '屏障', CONDITION: '条件', EXTERNAL: '外部', MANUAL: '人工', TIMER: '定时' };
const GATE_GLYPH = { BARRIER: '‖', CONDITION: '?', EXTERNAL: '⇄', MANUAL: 'M', TIMER: 'T' };
const ST_ZH = { PENDING: '待运行', RUNNING: '运行中', SUCCEEDED: '成功', FAILED: '失败', CREATED: '已创建', PAUSED: '已暂停', CANCELLED: '已取消' };
const store = {
  get(k) { try { return window.sessionStorage.getItem(k); } catch (e) { return null; } },
  set(k, v) { try { window.sessionStorage.setItem(k, v); } catch (e) { /* ignore */ } },
  lget(k) { try { return window.localStorage.getItem(k); } catch (e) { return null; } },
  lset(k, v) { try { window.localStorage.setItem(k, v); } catch (e) { /* ignore */ } },
};

/* ------------------------------------------------------------------ state */
const state = {
  graphs: [], graph: null, plan: null,
  rounds: [], roundId: null, view: null, bp: null, workers: null,
  sel: null,                 // {kind: 'pipeline'|'run'|'gate', id}
  auto: true, busy: false, busyRound: null,
  seq: 0,                    // bumped on every selection change and refresh: responses of an older sequence are dropped
  panelHtml: '', summarySig: '',
  dead: {},                  // pipelineId -> {loading, items, err}
};
let L = null;                // layout + element refs
const vp = { x: 0, y: 0, k: 1 };

/* ------------------------------------------------------------------ api */
async function api(path, opts) {
  opts = opts || {};
  const headers = { Accept: 'application/json' };
  const t = $('tokenInp').value.trim();
  if (t) headers['X-PG-Token'] = t;   // always honoured by the server, whatever pipeliner.admin.security.header is
  const res = await fetch(API + path, { method: opts.method || 'GET', headers, cache: 'no-store' });
  const txt = await res.text();
  let body = null;
  try { body = txt ? JSON.parse(txt) : null; } catch (e) { body = txt; }
  if (!res.ok) {
    const msg = body && body.error ? body.error : (typeof body === 'string' && body ? body : res.statusText);
    const err = new Error(msg);
    err.status = res.status;
    err.method = opts.method || 'GET';
    throw err;
  }
  return body;
}
function describeError(e) {
  if (e.status === 401 && e.method !== 'GET') return '401 未授权：该操作需要 Admin token（缺失或无效）。请在右上角填写 token。' + (e.message ? '\n' + e.message : '');
  if (e.status === 401) return '401 未授权：服务器要求 token 才能读取（read-requires-token），或 token 无效。请在右上角填写 token。' + (e.message ? '\n' + e.message : '');
  if (e.status === 403) return '403 无权限：当前 token 是只读角色（VIEWER）或服务器未配置 Admin token。' + (e.message ? '\n' + e.message : '');
  return (e.status ? e.status + ' ' : '') + (e.message || String(e));
}
function showBanner(msg, warn) {
  const b = $('banner');
  if (!msg) { b.hidden = true; return; }
  b.hidden = false;
  b.className = 'banner' + (warn ? ' warn' : '');
  b.textContent = msg;
}
let toastTimer = null;
function toast(msg, isErr) {
  const t = $('toast');
  t.textContent = msg;
  t.className = isErr ? 'err' : '';
  t.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.hidden = true; }, isErr ? 8000 : 3000);
}

/* ------------------------------------------------------------------ loading */
async function loadGraphs() {
  state.graphs = await api('/graphs');
  const sel = $('graphSel');
  sel.textContent = '';
  state.graphs.forEach((g) => sel.appendChild(el('option', { value: g, text: g })));
  if (!state.graphs.length) return;
  if (!state.graph || state.graphs.indexOf(state.graph) < 0) state.graph = state.graphs[0];
  sel.value = state.graph;
}
async function loadPlan() {
  state.plan = await api('/graphs/' + encodeURIComponent(state.graph) + '/plan.json');
  buildTopology();
  fitView();
}
async function loadRounds() {
  if (!state.graph) return;
  try {
    state.rounds = await api('/rounds?graph=' + encodeURIComponent(state.graph) + '&limit=20');
    renderRoundSel();
  } catch (e) { showBanner(describeError(e)); }
}
function renderRoundSel() {
  const sel = $('roundSel');
  sel.textContent = '';
  sel.appendChild(el('option', { value: '', text: '仅拓扑（不选轮次）' }));
  let found = false;
  state.rounds.forEach((r) => {
    if (r.roundId === state.roundId) found = true;
    const t = r.createdAt ? fmtTs(r.createdAt) + '  ' : '';
    sel.appendChild(el('option', { value: r.roundId, text: t + (ST_ZH[r.status] || r.status) + '  ' + r.bizKey + '  [' + r.roundId + ']' }));
  });
  if (state.roundId && !found) sel.appendChild(el('option', { value: state.roundId, text: state.roundId }));
  sel.value = state.roundId || '';
}
async function refreshRound() {
  if (!state.roundId) return;
  const rid = state.roundId;
  if (state.busy && state.busyRound === rid) return;   // one in-flight refresh per selection; a new selection always loads
  const tok = ++state.seq;
  state.busy = true; state.busyRound = rid;
  try {
    const id = encodeURIComponent(rid);
    const [view, bp, workers] = await Promise.all([
      api('/rounds/' + id),
      api('/rounds/' + id + '/backpressure').catch(() => null),
      api('/cluster/workers').catch(() => null),
    ]);
    if (tok !== state.seq || rid !== state.roundId) return;   // selection changed while in flight: drop the stale response
    state.view = view; state.bp = bp; state.workers = workers;
    showBanner(null);
    $('updated').textContent = '更新于 ' + new Date().toLocaleTimeString('zh-CN', { hour12: false });
  } catch (e) {
    if (tok !== state.seq || rid !== state.roundId) return;
    showBanner(describeError(e));
    if (e.status === 404) { state.view = null; state.bp = null; }
  } finally {
    if (tok === state.seq) state.busy = false;
  }
  updateAll();
}
function clearRoundData() {
  state.view = null; state.bp = null; state.workers = null; state.dead = {};
  state.seq++; state.busy = false; state.busyRound = null;   // invalidates in-flight refreshes / loadDead of the previous selection
}
async function selectRound(id) {
  state.roundId = id || null;
  clearRoundData();
  syncUrl();
  renderRoundSel();
  updateAll();
  if (state.roundId) await refreshRound();
}
function syncUrl() {
  try {
    const q = new URLSearchParams(window.location.search);
    if (state.graph) q.set('graph', state.graph);
    if (state.roundId) q.set('round', state.roundId); else q.delete('round');
    window.history.replaceState(null, '', window.location.pathname + '?' + q.toString());
  } catch (e) { /* ignore */ }
}

/* ------------------------------------------------------------------ layout */
const NW = 200, NH = 124, HG = 34, VG = 16, RUN_PAD = 8, RUN_HDR = 34, COL_PAD = 8, RUN_GAP = 22, BAND_HDR = 38;
const MIN_GAP = 24, GATE_GAP = 124, GATE_SLOT = 100, GATE_HALF = 24, MARGIN = 14;

function computeLayout(plan) {
  const lay = { pipe: {}, run: {}, gate: {}, bands: [], width: 0, height: 0 };
  const maxStage = Math.max(plan.stageCount || 1, ...plan.runs.map((r) => r.stage), 1);
  const stageRuns = {};
  plan.runs.forEach((r) => { (stageRuns[r.stage] = stageRuns[r.stage] || []).push(r); });

  // inside each run: depth = longest path over stream edges, rows ordered by predecessor barycenter
  plan.runs.forEach((r) => {
    const ids = r.pipelineIds;
    const inRun = new Set(ids);
    const edges = plan.channels.filter((c) => inRun.has(c.from) && inRun.has(c.to));
    const depth = {};
    ids.forEach((i) => { depth[i] = 0; });
    for (let pass = 0; pass < ids.length; pass++) {
      let changed = false;
      edges.forEach((c) => { if (depth[c.to] < depth[c.from] + 1) { depth[c.to] = depth[c.from] + 1; changed = true; } });
      if (!changed) break;
    }
    const maxD = Math.max(0, ...ids.map((i) => depth[i]));
    const cols = [];
    for (let d = 0; d <= maxD; d++) cols.push([]);
    const rowOf = {};
    ids.forEach((i) => cols[depth[i]].push(i));
    cols.forEach((col, d) => {
      if (d > 0) {
        const bary = (id) => {
          const preds = edges.filter((c) => c.to === id).map((c) => rowOf[c.from]).filter((x) => x !== undefined);
          return preds.length ? preds.reduce((a, b) => a + b, 0) / preds.length : 1e9;
        };
        col.sort((a, b) => bary(a) - bary(b) || ids.indexOf(a) - ids.indexOf(b));
      }
      col.forEach((id, row) => { rowOf[id] = row; });
    });
    const maxRows = Math.max(1, ...cols.map((c) => c.length));
    const w = RUN_PAD * 2 + (maxD + 1) * NW + maxD * HG;
    const h = RUN_HDR + RUN_PAD * 2 + maxRows * NH + (maxRows - 1) * VG;
    const rel = {};
    cols.forEach((col, d) => {
      const off = ((maxRows - col.length) * (NH + VG)) / 2;
      col.forEach((id, row) => { rel[id] = { x: RUN_PAD + d * (NW + HG), y: RUN_HDR + RUN_PAD + off + row * (NH + VG) }; });
    });
    lay.run[r.runId] = { w, h, rel, x: 0, y: 0 };
  });

  // gate gap index: right after the latest "after" stage
  const stageOfRun = {};
  plan.runs.forEach((r) => { stageOfRun[r.runId] = r.stage; });
  const gapGates = {};
  plan.gates.forEach((g) => {
    const aft = (g.afterRuns || []).map((x) => stageOfRun[x]).filter((x) => x);
    const bef = (g.beforeRuns || []).map((x) => stageOfRun[x]).filter((x) => x);
    let gap = aft.length ? Math.max(...aft) : (bef.length ? Math.min(...bef) - 1 : 0);
    gap = clamp(gap, 0, maxStage - 1);
    (gapGates[gap] = gapGates[gap] || []).push(g);
    lay.gate[g.gateId] = { gap };
  });

  // columns
  const colW = {}, colH = {};
  for (let s = 1; s <= maxStage; s++) {
    const rs = stageRuns[s] || [];
    colW[s] = Math.max(NW + RUN_PAD * 2, ...rs.map((r) => lay.run[r.runId].w));
    colH[s] = BAND_HDR + COL_PAD * 2 + rs.reduce((a, r) => a + lay.run[r.runId].h, 0) + Math.max(0, rs.length - 1) * RUN_GAP;
  }
  const maxH = Math.max(...Object.values(colH));
  let x = MARGIN;
  const gapX = {};
  for (let g = 0; g <= maxStage; g++) {
    const hasG = (gapGates[g] || []).length > 0;
    const gw = g === maxStage ? (hasG ? GATE_GAP : 0) : (hasG ? GATE_GAP : (g === 0 ? 0 : MIN_GAP));
    gapX[g] = { x, w: gw };
    x += gw;
    if (g < maxStage) {
      const s = g + 1;
      const bw = colW[s] + COL_PAD * 2;
      lay.bands.push({ stage: s, x, w: bw, h: maxH });
      let y = BAND_HDR + COL_PAD + (maxH - colH[s]) / 2;
      (stageRuns[s] || []).forEach((r) => {
        const R = lay.run[r.runId];
        R.x = x + COL_PAD + (colW[s] - R.w) / 2;
        R.y = y;
        y += R.h + RUN_GAP;
        ids2abs(lay, r, R);
      });
      x += bw;
    }
  }
  lay.width = x + MARGIN;

  // gates: vertical position = mean of connected pipelines, de-overlapped per gap
  let maxY = maxH;
  Object.keys(gapGates).forEach((gk) => {
    const gs = gapGates[gk];
    const items = gs.map((g) => {
      const ps = (g.afterPipelines || []).concat(g.beforePipelines || []).map((p) => lay.pipe[p]).filter((p) => p);
      const ty = ps.length ? ps.reduce((a, p) => a + p.y + NH / 2, 0) / ps.length : maxH / 2;
      return { g, ty, y: ty };
    });
    items.sort((a, b) => a.ty - b.ty);
    let prev = BAND_HDR + 20;
    items.forEach((it) => { it.y = Math.max(it.ty, prev); prev = it.y + GATE_SLOT; });
    const disp = items.reduce((a, it) => a + (it.y - it.ty), 0) / items.length;
    const top = BAND_HDR + 20;
    const shift = Math.min(disp, items[0].y - top);
    items.forEach((it) => { it.y -= Math.max(0, shift); });
    const gx = gapX[gk].x + gapX[gk].w / 2;
    items.forEach((it) => { lay.gate[it.g.gateId].x = gx; lay.gate[it.g.gateId].y = it.y; maxY = Math.max(maxY, it.y + GATE_SLOT); });
  });
  lay.height = maxY + MARGIN;
  return lay;
}
function ids2abs(lay, r, R) {
  Object.keys(R.rel).forEach((id) => { lay.pipe[id] = { x: R.x + R.rel[id].x, y: R.y + R.rel[id].y }; });
}

/* ------------------------------------------------------------------ topology DOM */
function buildTopology() {
  const plan = state.plan;
  const svg = $('canvas');
  svg.textContent = '';
  L = null;
  if (!plan) return;
  const lay = computeLayout(plan);
  const refs = { pipe: {}, run: {}, gate: {}, edge: {}, band: {} };
  L = { lay, refs };

  const defs = sv('defs', null,
    sv('marker', { id: 'arr', viewBox: '0 0 10 10', refX: 9, refY: 5, markerWidth: 7, markerHeight: 7, orient: 'auto-start-reverse' },
      sv('path', { d: 'M0,0 L10,5 L0,10 z', fill: 'var(--edge)' })),
    sv('marker', { id: 'arrHot', viewBox: '0 0 10 10', refX: 9, refY: 5, markerWidth: 7, markerHeight: 7, orient: 'auto-start-reverse' },
      sv('path', { d: 'M0,0 L10,5 L0,10 z', fill: 'var(--fail)' })),
    sv('marker', { id: 'arrG', viewBox: '0 0 10 10', refX: 9, refY: 5, markerWidth: 6, markerHeight: 6, orient: 'auto-start-reverse' },
      sv('path', { d: 'M0,0 L10,5 L0,10 z', fill: 'var(--edge)' })));
  const root = sv('g', { id: 'vp' });
  const gBand = sv('g'), gRun = sv('g'), gEdge = sv('g'), gGEdge = sv('g'), gNode = sv('g'), gGate = sv('g'), gLbl = sv('g');
  root.append(gBand, gRun, gEdge, gGEdge, gNode, gGate, gLbl);
  svg.append(defs, root);
  L.root = root;

  lay.bands.forEach((b) => {
    const g = sv('g');
    const r = sv('rect', { class: 'band', x: b.x, y: 0, width: b.w, height: b.h, rx: 10 });
    const t = sv('text', { class: 'bandlbl', x: b.x + 14, y: 25, text: 'S' + b.stage + ' · 阶段 ' + b.stage });
    g.append(r, t);
    gBand.appendChild(g);
    refs.band[b.stage] = r;
  });

  plan.runs.forEach((r) => {
    const R = lay.run[r.runId];
    const g = sv('g', { class: 'runhit', on: { click: (e) => onSelect(e, 'run', r.runId) } });
    const box = sv('rect', { class: 'runbox', x: R.x, y: R.y, width: R.w, height: R.h, rx: 10 });
    const hdr = sv('text', { class: 'runhdr', x: R.x + 12, y: R.y + 22 });
    g.append(box, hdr);
    gRun.appendChild(g);
    refs.run[r.runId] = { box, hdr, mode: r.mode };
  });

  const pById = {};
  plan.pipelines.forEach((p) => { pById[p.id] = p; });

  plan.channels.forEach((c) => {
    const a = lay.pipe[c.from], b = lay.pipe[c.to];
    if (!a || !b) return;
    const x1 = a.x + NW, y1 = a.y + NH / 2, x2 = b.x, y2 = b.y + NH / 2;
    const path = sv('path', { class: 'edge', d: curve(x1, y1, x2, y2), 'marker-end': 'url(#arr)' });
    path.appendChild(sv('title', { text: c.edgeId + ': ' + c.from + (c.fromPort && c.fromPort !== 'out' ? '.' + c.fromPort : '') + ' → ' + c.to }));
    gEdge.appendChild(path);
    const lbl = sv('g', { class: 'elbl', transform: 'translate(' + ((x1 + x2) / 2) + ' ' + ((y1 + y2) / 2) + ')', visibility: 'hidden' });
    const rect = sv('rect', { x: -15, y: -9, width: 30, height: 18, rx: 9 });
    const txt = sv('text', { x: 0, y: 4.5, 'text-anchor': 'middle' });
    lbl.append(rect, txt);
    gLbl.appendChild(lbl);
    refs.edge[c.edgeId] = { path, lbl, rect, txt };
  });

  plan.pipelines.forEach((p) => {
    const pos = lay.pipe[p.id];
    if (!pos) return;
    const g = sv('g', { class: 'node NONE', transform: 'translate(' + pos.x + ' ' + pos.y + ')', on: { click: (e) => onSelect(e, 'pipeline', p.id) } });
    const rect = sv('rect', { class: 'nrect', width: NW, height: NH, rx: 8 });
    const dot = sv('circle', { class: 'dot', cx: 15, cy: 21, r: 5.5 });
    const name = sv('text', { class: 'nname', x: 28, y: 27, text: trunc(p.id, 14) });
    const par = sv('text', { class: 'nsmall', x: NW - 8, y: 26, 'text-anchor': 'end' });
    const srcTxt = p.source ? '源 ' + p.source.dataset : '流 ← ' + (p.inputEdgeIds || []).join(',');
    const line2 = sv('text', { class: 'nsmall detail', x: 10, y: 47, text: trunc(srcTxt, 21) });
    const barbg = sv('rect', { class: 'barbg', x: 10, y: 55, width: NW - 20, height: 7, rx: 3.5 });
    const barfill = sv('rect', { class: 'barfill', x: 10, y: 55, width: 0, height: 7, rx: 3.5 });
    const bardead = sv('rect', { class: 'barfill dead', x: 10, y: 55, width: 0, height: 7, rx: 3.5 });
    if (!p.source) { barbg.setAttribute('visibility', 'hidden'); }
    const prog = sv('text', { class: 'ntext', x: 10, y: 81 });
    const rows = sv('text', { class: 'nsmall detail', x: 10, y: 99 });
    const sinkTxt = sinkSummary(p);
    const pods = sv('text', { class: 'nsmall detail', x: 10, y: 116 });
    const dlq = sv('g', { class: 'dlqbadge', transform: 'translate(' + (NW - 68) + ' -9)', visibility: 'hidden' },
      sv('rect', { width: 62, height: 18, rx: 9 }), sv('text', { x: 31, y: 13, 'text-anchor': 'middle' }));
    const bot = sv('g', { class: 'botbadge', transform: 'translate(8 -9)', visibility: 'hidden' },
      sv('rect', { width: 48, height: 18, rx: 9 }), sv('text', { x: 24, y: 13, 'text-anchor': 'middle', text: '瓶颈' }));
    g.append(rect, dot, name, par, line2, barbg, barfill, bardead, prog, rows, pods, dlq, bot,
      sv('title', { text: p.id + (p.source ? '\n源: ' + p.source.type + ' / ' + p.source.dataset : '') + sinkTitle(p) }));
    gNode.appendChild(g);
    refs.pipe[p.id] = { g, par, barfill, bardead, prog, rows, pods, dlq, bot, sinkTxt, hasSource: !!p.source };
  });

  plan.gates.forEach((gt) => {
    const pos = lay.gate[gt.gateId];
    if (!pos || pos.x === undefined) return;
    const cx = pos.x, cy = pos.y;
    // dashed arrows: after pipelines -> diamond -> before pipelines
    (gt.afterPipelines || []).forEach((pid) => {
      const a = lay.pipe[pid];
      if (!a) return;
      const path = sv('path', { class: 'gedge', d: curve(a.x + NW, a.y + NH / 2, cx - GATE_HALF, cy), 'marker-end': 'url(#arrG)' });
      gGEdge.appendChild(path);
      (refs.gate[gt.gateId] = refs.gate[gt.gateId] || { edges: [] }).edges.push(path);
    });
    (gt.beforePipelines || []).forEach((pid) => {
      const b = lay.pipe[pid];
      if (!b) return;
      const path = sv('path', { class: 'gedge', d: curve(cx + GATE_HALF, cy, b.x, b.y + NH / 2), 'marker-end': 'url(#arrG)' });
      gGEdge.appendChild(path);
      (refs.gate[gt.gateId] = refs.gate[gt.gateId] || { edges: [] }).edges.push(path);
    });
    const g = sv('g', { class: 'gate', transform: 'translate(' + cx + ' ' + cy + ')', on: { click: (e) => onSelect(e, 'gate', gt.gateId) } });
    const shape = sv('path', { class: 'gshape', d: 'M0,-' + GATE_HALF + ' L' + GATE_HALF + ',0 L0,' + GATE_HALF + ' L-' + GATE_HALF + ',0 Z' });
    const glyph = sv('text', { class: 'gglyph', x: 0, y: 6, text: GATE_GLYPH[gt.type] || '?' });
    const l1 = sv('text', { class: 'glbl', x: 0, y: GATE_HALF + 17, text: trunc(gt.gateId, 21) });
    const l2 = sv('text', { class: 'gst', x: 0, y: GATE_HALF + 33 });
    g.append(shape, glyph, l1, l2, sv('title', { text: gt.gateId + ' (' + (GATE_ZH[gt.type] || gt.type) + (gt.auto ? '，自动 barrier' : '') + ')' }));
    gGate.appendChild(g);
    const ref = refs.gate[gt.gateId] = refs.gate[gt.gateId] || { edges: [] };
    ref.g = g; ref.l2 = l2;
  });

  applyView();
  updateAll();
}
function sinkSummary(p) {
  const out = [];
  Object.keys(p.sinks || {}).forEach((port) => (p.sinks[port] || []).forEach((s) => out.push(s.dataset || s.type)));
  return out;
}
function sinkTitle(p) {
  const out = [];
  Object.keys(p.sinks || {}).forEach((port) => (p.sinks[port] || []).forEach((s) => out.push(port + ' → ' + s.type + ' / ' + s.dataset)));
  return out.length ? '\n汇: ' + out.join('; ') : '';
}
function curve(x1, y1, x2, y2) {
  const dx = Math.max(24, (x2 - x1) / 2);
  return 'M' + x1 + ',' + y1 + ' C' + (x1 + dx) + ',' + y1 + ' ' + (x2 - dx) + ',' + y2 + ' ' + x2 + ',' + y2;
}

/* ------------------------------------------------------------------ dynamic update */
function viewMaps() {
  const v = state.view;
  const m = { run: {}, pipe: {}, gate: {} };
  if (v) {
    (v.runs || []).forEach((r) => { m.run[r.runId] = r; });
    (v.pipelines || []).forEach((p) => { m.pipe[p.pipelineId] = p; });
    (v.gates || []).forEach((g) => { m.gate[g.gateId] = g; });
  }
  return m;
}
function gateState(rec) {
  if (!rec) return null;
  if (rec.status === 'CLOSED' && rec.armedAt) return 'ARMED';
  return rec.status;
}
function podsFor(pid) {
  const out = [];
  if (!state.workers || !state.roundId) return out;
  state.workers.forEach((w) => (w.instances || []).forEach((i) => {
    if (i.round === state.roundId && i.pipeline === pid) out.push({ pod: w.podId, threads: i.threads, attempt: i.attempt });
  }));
  return out;
}
function chunkStats(pid) {
  const c = (state.view && state.view.chunkCounts && state.view.chunkCounts[pid]) || {};
  const n = (k) => Number(c[k] || 0);
  const total = n('PENDING') + n('DONE') + n('DEAD') + n('SKIPPED');
  return { total, pending: n('PENDING'), done: n('DONE'), dead: n('DEAD'), skipped: n('SKIPPED') };
}
/** Where the round is right now: running runs (+ their running pipelines), else the gates it waits on, else the final status. */
function locationInfo() {
  const v = state.view, plan = state.plan;
  if (!v || !plan) return null;
  const r = v.round, m = viewMaps();
  if (TERMINAL[r.status]) {
    return { text: (r.status === 'SUCCEEDED' ? '已完成（成功）' : (ST_ZH[r.status] || r.status)) + (r.status === 'FAILED' ? '' : ''), cls: r.status };
  }
  const runs = plan.runs.filter((x) => (m.run[x.runId] || {}).status === 'RUNNING');
  if (runs.length) {
    const parts = runs.map((x) => {
      const ps = x.pipelineIds.filter((p) => (m.pipe[p] || {}).status === 'RUNNING');
      return x.runId + (ps.length ? '（' + ps.join(', ') + '）' : '');
    });
    return { text: parts.join('；'), cls: 'RUNNING' };
  }
  if (r.status === 'PAUSED') return { text: '已暂停', cls: 'PAUSED' };
  const waits = [];
  plan.gates.forEach((g) => {
    const rec = m.gate[g.gateId];
    if (!rec || rec.status !== 'CLOSED' || !rec.armedAt) return;
    const before = rec.beforeRuns || g.beforeRuns || [];
    if (before.length && !before.every((x) => ((m.run[x] || {}).status || 'PENDING') === 'PENDING')) return;
    waits.push({ gate: g.gateId, type: rec.type || g.type, since: rec.armedAt });
  });
  if (waits.length) return { waits, cls: 'ARMED' };
  return { text: r.status === 'CREATED' ? '等待调度' : '调度中', cls: '' };
}
function renderLocation(box) {
  const loc = locationInfo();
  box.textContent = '';
  if (!loc) { box.textContent = '—'; return; }
  box.className = 'val loc ' + (loc.cls || '');
  if (!loc.waits) { box.textContent = loc.text; box.title = loc.text; return; }
  loc.waits.forEach((w, i) => {
    if (i) box.appendChild(document.createTextNode('；'));
    const zh = GATE_ZH[w.type] || w.type;
    const head = w.type === 'EXTERNAL' ? '等待外部回调 ' + w.gate : '等待闸门 ' + w.gate + '（' + zh + '）';
    box.appendChild(document.createTextNode(head + ' '));
    box.appendChild(el('span', { 'data-since': w.since, 'data-pre': '', text: fmtDur(Date.now() - w.since) }));
  });
}
function stageStates() {
  const plan = state.plan, m = viewMaps();
  const out = {};
  if (!plan) return out;
  const n = plan.stageCount;
  for (let s = 1; s <= n; s++) {
    const rs = plan.runs.filter((r) => r.stage === s).map((r) => (m.run[r.runId] || {}).status || 'PENDING');
    let st = 'pending';
    if (rs.length && rs.every((x) => x === 'SUCCEEDED')) st = 'ok';
    else if (rs.some((x) => x === 'FAILED')) st = 'fail';
    else if (rs.some((x) => x === 'RUNNING')) st = 'run';
    out[s] = st;
  }
  return out;
}
function currentStage() {
  const ss = stageStates();
  const keys = Object.keys(ss).map(Number).sort((a, b) => a - b);
  let c = keys.find((k) => ss[k] === 'run' || ss[k] === 'fail');
  if (c === undefined) c = keys.find((k) => ss[k] !== 'ok');
  return c === undefined ? null : c;
}

function updateAll() {
  if (!state.plan) {   // graphs/plan failed to load (e.g. 401 before the token was entered): nothing to draw yet
    const em = $('emptyMsg');
    em.hidden = false;
    em.textContent = state.graphs.length ? '拓扑尚未加载，请在右上角填写 token 后点击刷新' : '没有已注册的 PipelineGraph（或加载失败，请填写 token 后刷新）';
    return;
  }
  updateTopology();
  updateSummary();
  updatePanel();
  updateLegend();
  const empty = $('emptyMsg');
  if (!state.graphs.length) { empty.hidden = false; empty.textContent = '没有已注册的 PipelineGraph'; }
  else empty.hidden = true;
}

function updateTopology() {
  if (!L) return;
  const plan = state.plan, refs = L.refs, m = viewMaps(), v = state.view;
  const hw = state.bp ? Number(state.bp.highWatermark) : null;
  const lens = (state.bp && state.bp.channels) || {};
  const bots = new Set((state.bp && state.bp.bottlenecks) || []);
  const cur = currentStage();

  Object.keys(refs.band).forEach((s) => {
    refs.band[s].setAttribute('class', 'band' + (v && Number(s) === cur && !TERMINAL[v.round.status] ? ' active' : ''));
  });

  plan.runs.forEach((r) => {
    const ref = refs.run[r.runId];
    const rec = m.run[r.runId];
    const st = rec ? rec.status : '';
    ref.box.setAttribute('class', 'runbox ' + st + (isSel('run', r.runId) ? ' sel' : ''));
    ref.hdr.textContent = '';
    ref.hdr.appendChild(document.createTextNode(r.runId + ' · ' + r.mode));
    if (rec) {
      ref.hdr.appendChild(document.createTextNode(' · '));
      const t = sv('tspan', { class: 'st', text: ST_ZH[st] || st });
      t.setAttribute('style', 'fill:' + stColor(st));
      ref.hdr.appendChild(t);
      ref.hdr.appendChild(document.createTextNode(' · #' + rec.attempt));
    }
  });

  plan.pipelines.forEach((p) => {
    const ref = refs.pipe[p.id];
    if (!ref) return;
    const rec = m.pipe[p.id];
    const st = rec ? rec.status : 'NONE';
    ref.g.setAttribute('class', 'node ' + st + (isSel('pipeline', p.id) ? ' sel' : ''));
    const par = rec && rec.parallelism ? rec.parallelism : p.parallelism;
    ref.par.textContent = '×' + par;
    const cs = chunkStats(p.id);
    if (!v) {
      ref.prog.textContent = '';
      ref.rows.textContent = trunc(ref.sinkTxt.length ? '汇 ' + ref.sinkTxt.join(', ') : '', 22);
      ref.pods.textContent = '';
      ref.barfill.setAttribute('width', 0); ref.bardead.setAttribute('width', 0);
    } else {
      if (ref.hasSource) {
        const total = cs.total, done = cs.done + cs.skipped;
        ref.prog.textContent = total ? done + '/' + total + ' splits' + (cs.dead ? '  · 死信 ' + cs.dead : '') : (st === 'PENDING' ? '未开始' : '0/0 splits');
        const bw = NW - 20;
        const frac = st === 'SUCCEEDED' ? 1 : (total ? done / total : 0);
        ref.barfill.setAttribute('width', Math.max(0, bw * frac));
        const dfrac = total ? cs.dead / total : 0;
        ref.bardead.setAttribute('x', 10 + bw * frac);
        ref.bardead.setAttribute('width', Math.min(bw * dfrac, bw * (1 - frac)));
      } else {
        ref.prog.textContent = cs.done + ' chunks' + (cs.dead ? '  · 死信 ' + cs.dead : '');
      }
      const rin = v.rowsIn ? v.rowsIn[p.id] : null, rout = v.rowsOut ? v.rowsOut[p.id] : null;
      ref.rows.textContent = '行 ' + fmtNum(rin || 0) + ' → ' + fmtNum(rout || 0);
      const pods = podsFor(p.id);
      ref.pods.textContent = pods.length ? 'pods: ' + trunc(pods.map((x) => x.pod + '×' + x.threads).join(', '), 17) : (st === 'RUNNING' ? 'pods: —' : '');
    }
    const dl = Number(lens['dlq:' + p.id] || 0);
    if (v && dl > 0) { ref.dlq.setAttribute('visibility', 'visible'); ref.dlq.querySelector('text').textContent = 'DLQ ' + dl; }
    else ref.dlq.setAttribute('visibility', 'hidden');
    ref.bot.setAttribute('visibility', v && bots.has(p.id) ? 'visible' : 'hidden');
  });

  plan.channels.forEach((c) => {
    const ref = refs.edge[c.edgeId];
    if (!ref) return;
    const len = lens[c.edgeId];
    if (!v || len === undefined || len === null) {
      ref.lbl.setAttribute('visibility', 'hidden');
      ref.path.setAttribute('class', 'edge');
      ref.path.setAttribute('marker-end', 'url(#arr)');
      return;
    }
    const hot = hw != null && Number(len) >= hw;
    ref.path.setAttribute('class', 'edge' + (hot ? ' hot' : (Number(len) > 0 ? ' flow' : '')));
    ref.path.setAttribute('marker-end', hot ? 'url(#arrHot)' : 'url(#arr)');
    ref.txt.textContent = String(len);
    const w = Math.max(28, String(len).length * 8 + 14);
    ref.rect.setAttribute('x', -w / 2); ref.rect.setAttribute('width', w);
    ref.lbl.setAttribute('class', 'elbl' + (hot ? ' hot' : ''));
    ref.lbl.setAttribute('visibility', 'visible');
  });

  plan.gates.forEach((gt) => {
    const ref = refs.gate[gt.gateId];
    if (!ref || !ref.g) return;
    const rec = m.gate[gt.gateId];
    const gs = gateState(rec);
    ref.g.setAttribute('class', 'gate' + (gs ? ' ' + gs : '') + (isSel('gate', gt.gateId) ? ' sel' : ''));
    ref.edges.forEach((e) => e.setAttribute('class', 'gedge' + (gs ? ' ' + gs : '')));
    const l2 = ref.l2;
    l2.removeAttribute('data-since');
    l2.removeAttribute('data-pre');
    const zh = GATE_ZH[gt.type] || gt.type;
    if (!rec) l2.textContent = zh;
    else if (gs === 'ARMED') {
      l2.setAttribute('data-since', rec.armedAt);
      l2.setAttribute('data-pre', zh + ' · 等待 ');
      l2.textContent = zh + ' · 等待 ' + fmtDur(Date.now() - rec.armedAt);
    } else if (gs === 'CLOSED') l2.textContent = zh + ' · CLOSED';
    else l2.textContent = zh + ' · ' + gs;
  });
}
function stColor(st) {
  switch (st) {
    case 'RUNNING': return 'var(--running)';
    case 'SUCCEEDED': return 'var(--ok)';
    case 'FAILED': return 'var(--fail)';
    default: return 'var(--muted)';
  }
}
function isSel(kind, id) { return state.sel && state.sel.kind === kind && state.sel.id === id; }

/* ------------------------------------------------------------------ summary strip */
function updateSummary() {
  const box = $('summary');
  const v = state.view;
  if (!v) {
    const sig = 'none:' + (state.plan ? state.plan.graphName + state.plan.hash : '');
    if (state.summarySig === sig) return;
    state.summarySig = sig;
    box.textContent = '';
    if (state.plan) {
      box.append(item('图', state.plan.graphName), item('计划 hash', trunc(state.plan.hash, 10)),
        item('规模', state.plan.pipelines.length + ' pipelines · ' + state.plan.runs.length + ' runs · ' + state.plan.gates.length + ' gates · ' + state.plan.stageCount + ' 阶段'),
        el('span', { class: 'muted', text: '仅拓扑：未选择轮次。请在上方选择一个轮次以查看实时状态。' }));
    }
    return;
  }
  const r = v.round;
  const m = viewMaps();
  const running = state.plan.runs.filter((x) => (m.run[x.runId] || {}).status === 'RUNNING').map((x) => x.runId);
  const cur = currentStage();
  const ss = stageStates();
  const n = state.plan.stageCount;
  const locSig = JSON.stringify(locationInfo());
  const sig = [r.roundId, r.status, r.incomplete, r.compacted, cur, locSig, JSON.stringify(ss), r.finishedAt].join('|');
  if (state.summarySig === sig) return;
  state.summarySig = sig;
  box.textContent = '';
  const pv = r.params && (r.params.planVersion || r.params.version);
  const stageTxt = TERMINAL[r.status] && r.status === 'SUCCEEDED' ? '全部完成' : (cur ? 'S' + cur + '/S' + n : '—');
  const bar = el('div', { class: 'stagebar' });
  for (let s = 1; s <= n; s++) bar.appendChild(el('i', { class: ss[s] === 'pending' ? '' : ss[s], title: 'S' + s }));
  const elapsed = el('span', { class: 'val' });
  if (TERMINAL[r.status]) elapsed.textContent = fmtDur((r.finishedAt || Date.now()) - (r.startedAt || r.createdAt));
  else if (r.startedAt || r.createdAt) {
    elapsed.setAttribute('data-since', r.startedAt || r.createdAt);
    elapsed.setAttribute('data-pre', '');
    elapsed.textContent = fmtDur(Date.now() - (r.startedAt || r.createdAt));
  }
  const locBox = el('span', { class: 'val loc' });
  renderLocation(locBox);
  const canPause = r.status === 'RUNNING', canResume = r.status === 'PAUSED', canCancel = !TERMINAL[r.status];
  box.append(
    el('div', { class: 'item' }, el('span', { class: 'lbl', text: '轮次状态' }), el('span', { class: 'val' }, el('span', { class: 'badge ' + r.status, text: (ST_ZH[r.status] || r.status) + ' · ' + r.status }))),
    item('bizKey', r.bizKey),
    item('planVersion', pv || '—', 'hash ' + trunc(r.planHash, 8)),
    el('div', { class: 'item' }, el('span', { class: 'lbl', text: '已用时' }), elapsed),
    el('div', { class: 'item' }, el('span', { class: 'lbl', text: '当前阶段' }), el('span', { class: 'val', text: stageTxt })),
    el('div', { class: 'item locitem' }, el('span', { class: 'lbl', text: '当前位置' }), locBox),
    el('div', { class: 'item' }, bar, el('div', { class: 'stagelbl' }, el('span', { text: 'S1' }), el('span', { text: '阶段进度' }), el('span', { text: 'S' + n }))),
    el('div', { class: 'item' },
      r.incomplete ? el('span', { class: 'chip', text: 'incomplete（已跳过死信）', title: '部分死信被跳过，结果不完整' }) : null,
      r.compacted ? el('span', { class: 'chip', text: 'compacted（chunk 已压缩）', title: 'chunk 明细已被保留任务压缩' }) : null,
      r.error ? el('span', { class: 'chip red', text: trunc(r.error, 60), title: r.error }) : null),
    el('div', { class: 'actions' },
      el('button', { class: 'btn', text: '暂停', disabled: !canPause, on: { click: () => roundAction('pause') } }),
      el('button', { class: 'btn', text: '恢复', disabled: !canResume, on: { click: () => roundAction('resume') } }),
      el('button', { class: 'btn danger', text: '取消', disabled: !canCancel, on: { click: () => roundAction('cancel') } })));
}
function item(lbl, val, sub) {
  return el('div', { class: 'item' }, el('span', { class: 'lbl', text: lbl }), el('span', { class: 'val', text: val == null ? '—' : val }),
    sub ? el('span', { class: 'lbl', text: sub }) : null);
}
function roundAction(kind) {
  const id = state.roundId;
  const msgs = {
    pause: '暂停轮次后，master 停止启动新的 run，并让 worker 暂停取 chunk；已在处理的 chunk 会做完。可随时恢复。',
    resume: '恢复轮次，继续调度和处理。',
    cancel: '取消整个轮次：所有 run 停止，通道清理，轮次进入 CANCELLED 终态，不可恢复（需重新提交）。已写入目标表的数据不会回滚。',
  };
  confirmAction({ title: { pause: '暂停轮次', resume: '恢复轮次', cancel: '取消轮次' }[kind], msg: msgs[kind] + '\n\n轮次：' + id, danger: kind === 'cancel',
    run: () => api('/rounds/' + encodeURIComponent(id) + '/' + kind, { method: 'POST' }) });
}

/* ------------------------------------------------------------------ legend */
function updateLegend() {
  const lg = $('legend');
  if (lg.dataset.built) return;
  lg.dataset.built = '1';
  const sw = (c, t) => el('span', null, el('i', { class: 'sw', style: 'background:' + c }), t);
  lg.append(sw('var(--pending)', '待运行'), sw('var(--running)', '运行中'), sw('var(--ok)', '成功'), sw('var(--fail)', '失败 / 通道达高水位'),
    el('span', { text: '实线 = stream 通道（数字为队列长度）' }), el('span', { text: '虚线 + 菱形 = 闸门' }), el('span', { text: '滚轮缩放 · 拖动平移' }));
}

/* ------------------------------------------------------------------ selection + side panel */
function onSelect(e, kind, id) {
  if (dragMoved) return;
  e.stopPropagation();
  state.sel = { kind, id };
  state.panelHtml = '';
  updateTopology();
  updatePanel();
}
function clearSel() {
  if (!state.sel) return;
  state.sel = null;
  state.panelHtml = '';
  updateTopology();
  updatePanel();
}
function kv(rows) {
  const t = el('table');
  rows.forEach((r) => {
    if (!r) return;
    t.appendChild(el('tr', null, el('td', { text: r[0] }), el('td', null, r[1] instanceof Node ? r[1] : (r[1] == null || r[1] === '' ? '—' : String(r[1])))));
  });
  return t;
}
function links(kind, ids) {
  if (!ids || !ids.length) return '—';
  const s = el('span');
  ids.forEach((id, i) => {
    if (i) s.appendChild(document.createTextNode(', '));
    s.appendChild(el('a', { href: '#', text: id, on: { click: (e) => { e.preventDefault(); state.sel = { kind, id }; state.panelHtml = ''; updateTopology(); updatePanel(); } } }));
  });
  return s;
}
function badge(st) { return el('span', { class: 'badge ' + st, text: ST_ZH[st] || st }); }

function updatePanel() {
  const side = $('side');
  const wasShown = side.style.display !== 'none';
  if (!state.sel || !state.plan) {
    side.hidden = true; side.style.display = 'none';
    state.panelHtml = '';
    if (wasShown) refitIfPristine();
    return;
  }
  side.hidden = false; side.style.display = '';
  if (!wasShown) refitIfPristine();
  const box = el('div');
  const s = state.sel;
  if (s.kind === 'pipeline') buildPipelinePanel(box, s.id);
  else if (s.kind === 'run') buildRunPanel(box, s.id);
  else buildGatePanel(box, s.id);
  const html = box.innerHTML;
  if (html === state.panelHtml) return;
  state.panelHtml = html;
  side.textContent = '';
  side.appendChild(box);
  tickClocks();
}

function runActionButtons(runId) {
  const noRound = !state.roundId || !state.view;
  const mk = (label, kind, cls) => el('button', { class: 'btn ' + (cls || ''), text: label, disabled: noRound, on: { click: () => runAction(kind, runId) } });
  return el('div', { class: 'btnrow' }, mk('重试', 'retry'), mk('从此处重跑', 'rerun', 'danger'), mk('跳过死信', 'skip', 'danger'));
}
function runAction(kind, runId) {
  const rid = state.roundId;
  const defs = {
    retry: { title: '重试 run ' + runId, msg: '把该 run 的死信 chunk 重新入队，失败的 pipeline 与 run 回到 RUNNING，轮次回到 RUNNING。\n只重试失败的部分，已成功的 chunk 不受影响。' },
    rerun: { title: '从 ' + runId + ' 起重跑', msg: '重置该 run 以及所有下游 run：run/pipeline 回到 PENDING，attempt+1，chunk 记录与通道被清空，下游闸门（含 signal）复位为 CLOSED，轮次回到 RUNNING。\n已写入目标表的结果会被新一轮（更高写入版本）覆盖；这是重量级操作。' },
    skip: { title: '跳过 run ' + runId + ' 的死信', msg: '丢弃该 run 的死信队列，死信 chunk 标记为 SKIPPED，失败的 pipeline 回到 RUNNING 继续往下走，轮次被标记为 incomplete。\n这些 chunk 对应的数据将不会被处理，结果会缺少这部分数据。' },
  };
  const d = defs[kind];
  confirmAction({ title: d.title, msg: d.msg + '\n\n轮次：' + rid, danger: kind !== 'retry',
    run: () => api('/runs/' + encodeURIComponent(rid) + '/' + encodeURIComponent(runId) + '/' + kind, { method: 'POST' }) });
}

function buildRunPanel(box, runId) {
  const pr = state.plan.runs.find((r) => r.runId === runId);
  if (!pr) return;
  const rec = viewMaps().run[runId];
  box.append(el('h3', { text: 'Run ' + runId }), el('div', null, rec ? badge(rec.status) : el('span', { class: 'hint', text: '（未选择轮次）' })));
  box.appendChild(kv([
    ['阶段', 'S' + pr.stage], ['模式', pr.mode],
    rec ? ['attempt', rec.attempt] : null,
    rec ? ['assignee', rec.assignee] : null,
    rec ? ['开始', fmtTs(rec.startedAt)] : null,
    rec ? ['结束', fmtTs(rec.finishedAt)] : null,
    rec ? ['耗时', rec.startedAt ? fmtDur((rec.finishedAt || Date.now()) - rec.startedAt) : '—'] : null,
    ['pipelines', links('pipeline', pr.pipelineIds)],
    ['入口闸门', links('gate', pr.inGateIds)],
    ['上游 run', links('run', pr.upstreamRunIds)],
  ]));
  if (rec && rec.error) box.append(el('h4', { text: '错误' }), el('div', { class: 'err', text: rec.error }));
  box.append(el('h4', { text: '操作' }), runActionButtons(runId));
}

function buildPipelinePanel(box, pid) {
  const p = state.plan.pipelines.find((x) => x.id === pid);
  if (!p) return;
  const m = viewMaps(), v = state.view;
  const rec = m.pipe[pid];
  const lens = (state.bp && state.bp.channels) || {};
  box.append(el('h3', { text: pid }), el('div', null, rec ? badge(rec.status) : el('span', { class: 'hint', text: '（未选择轮次）' })));
  box.appendChild(kv([
    ['所属 run', links('run', [p.runId])],
    ['来源', p.source ? p.source.type + ' · ' + p.source.dataset : '上游 stream'],
    ['runner', p.runner.class ? p.runner.class + '（' + p.runner.kind + (p.runner.perThread ? '，perThread' : '') + '）' : '无'],
    ['ports', (p.ports || []).join(', ') || '—'],
    ['sinks', sinkLines(p)],
    ['chunkKey', (p.chunkKey || []).join(', ') || '—'],
    ['输入通道', (p.inputEdgeIds || []).join(', ') || '—'],
    ['端口→通道', Object.keys(p.portToEdge || {}).map((k) => k + '→' + p.portToEdge[k]).join(', ') || '—'],
    ['并行度', rec ? rec.parallelism + '（计划 ' + p.parallelism + '）' : p.parallelism],
  ]));
  if (v && rec) {
    const cs = chunkStats(pid);
    box.append(el('h4', { text: '进度' }), kv([
      ['开始 / 结束', fmtTs(rec.startedAt) + ' / ' + fmtTs(rec.finishedAt)],
      ['enumDone', rec.enumDone ? '是' : '否'],
      ['chunk', 'PENDING ' + cs.pending + ' · DONE ' + cs.done + ' · DEAD ' + cs.dead + ' · SKIPPED ' + cs.skipped + '（共 ' + cs.total + '）'],
      ['行 in → out', fmtNum((v.rowsIn || {})[pid] || 0) + ' → ' + fmtNum((v.rowsOut || {})[pid] || 0)],
    ]));
    const q = [];
    if (p.source && lens['split:' + pid] !== undefined) q.push(['split 队列', lens['split:' + pid]]);
    (p.inputEdgeIds || []).forEach((e) => { if (lens[e] !== undefined) q.push(['通道 ' + e, lens[e]]); });
    if (lens['dlq:' + pid] !== undefined) q.push(['DLQ', lens['dlq:' + pid]]);
    if (q.length) box.append(el('h4', { text: '队列长度' }), kv(q));
    const pods = podsFor(pid);
    box.append(el('h4', { text: 'pods（心跳上报）' }), pods.length
      ? kv(pods.map((x) => [x.pod, x.threads + ' 线程 · attempt ' + x.attempt])) : el('div', { class: 'hint', text: '当前没有 pod 持有该 pipeline 的实例' }));
    if (rec.error) box.append(el('h4', { text: '错误' }), el('div', { class: 'err', text: rec.error }));
    // failed chunks
    const d = state.dead[pid];
    box.append(el('h4', { text: '失败 chunk' }),
      el('div', { class: 'btnrow' }, el('button', { class: 'btn', text: '查看失败 chunk', on: { click: () => loadDead(pid) } })));
    if (d) {
      if (d.loading) box.append(el('div', { class: 'hint', text: '加载中…' }));
      else if (d.err) box.append(el('div', { class: 'err', text: d.err }));
      else if (!d.items.length) box.append(el('div', { class: 'hint', text: '没有 DEAD 状态的 chunk' }));
      else {
        box.append(el('div', { class: 'hint', text: '共 ' + d.items.length + ' 条（最多 50）' }));
        d.items.forEach((c) => box.append(el('div', { class: 'chunkrow' },
          el('div', { class: 'mono', text: c.chunkId }),
          el('div', { text: 'attempt ' + c.attempt + ' · owner ' + (c.owner || '—') + ' · 行 ' + fmtNum(c.rowsIn) }),
          el('div', { class: 'err', text: c.error || '' }))));
      }
    }
    box.append(el('h4', { text: '操作（作用于 ' + p.runId + '）' }), runActionButtons(p.runId));
  } else {
    box.append(el('div', { class: 'hint', style: 'margin-top:10px', text: '选择一个轮次后可查看进度、队列、pods 和失败 chunk。' }));
  }
}
function sinkLines(p) {
  const s = el('div');
  const ports = Object.keys(p.sinks || {});
  if (!ports.length) return '—';
  ports.forEach((port) => (p.sinks[port] || []).forEach((k) => s.appendChild(el('div', { text: port + ' → ' + k.type + ' · ' + (k.dataset || '—') }))));
  return s;
}
async function loadDead(pid) {
  const rid = state.roundId, seq = state.seq, dead = state.dead;
  dead[pid] = { loading: true };
  state.panelHtml = '';
  updatePanel();
  let res;
  try {
    const items = await api('/rounds/' + encodeURIComponent(rid) + '/chunks?pipeline=' + encodeURIComponent(pid) + '&status=DEAD&limit=50');
    res = { items };
  } catch (e) { res = { err: describeError(e) }; }
  if (state.roundId !== rid || state.seq !== seq || state.dead !== dead) return;   // another round was selected meanwhile: do not show its DEAD chunks here
  state.dead[pid] = res;
  state.panelHtml = '';
  updatePanel();
}

function buildGatePanel(box, gid) {
  const g = state.plan.gates.find((x) => x.gateId === gid);
  if (!g) return;
  const rec = viewMaps().gate[gid];
  const gs = gateState(rec);
  box.append(el('h3', { text: gid }), el('div', null, gs ? el('span', { class: 'badge ' + gs, text: gs === 'ARMED' ? '等待中（已武装）' : gs }) : el('span', { class: 'hint', text: '（未选择轮次）' })));
  const rows = [
    ['类型', (GATE_ZH[g.type] || g.type) + ' · ' + g.type + (g.auto ? '（自动 barrier）' : '')],
    g.dataset ? ['dataset', g.dataset] : null,
    ['after pipelines', links('pipeline', g.afterPipelines)],
    ['before pipelines', links('pipeline', g.beforePipelines)],
    ['after runs', links('run', g.afterRuns)],
    ['before runs', links('run', g.beforeRuns)],
    ['超时', g.timeoutMillis > 0 ? fmtDur(g.timeoutMillis) + ' → ' + g.onTimeout : '无'],
  ];
  if (rec) {
    if (rec.armedAt) {
      const sp = rec.openedAt ? el('span', { text: fmtDur(rec.openedAt - rec.armedAt) }) : el('span', { 'data-since': rec.armedAt, 'data-pre': '' });
      rows.push(['武装时间', fmtTs(rec.armedAt)], ['已等待', sp]);
    }
    if (rec.deadlineAt) rows.push(['截止', fmtTs(rec.deadlineAt)]);
    if (rec.openedAt) rows.push(['打开', fmtTs(rec.openedAt) + ' · ' + (rec.openedBy || '—')]);
  }
  box.appendChild(kv(rows));
  if (rec && rec.context && Object.keys(rec.context).length) {
    box.append(el('h4', { text: '上下文 / signals（' + Object.keys(rec.context).length + '）' }),
      kv(Object.keys(rec.context).map((k) => [k, trunc(rec.context[k], 120)])));
  }
  box.append(el('h4', { text: '操作' }), el('div', { class: 'btnrow' },
    el('button', { class: 'btn primary', text: '打开闸门', disabled: !(rec && rec.status === 'CLOSED'), on: { click: () => openGate(gid) } })),
    rec && rec.status !== 'CLOSED' ? el('div', { class: 'hint', text: '只有 CLOSED 状态的闸门可以打开。' }) : '');
}
function openGate(gid) {
  const g = state.plan.gates.find((x) => x.gateId === gid);
  confirmAction({ title: '打开闸门 ' + gid,
    msg: '强制打开该闸门：不再等待其条件，立即放行下游 run（' + ((g && g.beforeRuns) || []).join(', ') + '）。\n操作人取自 token 对应的名称，并写入审计日志。\n\n轮次：' + state.roundId,
    run: () => api('/gates/' + encodeURIComponent(state.roundId) + '/' + encodeURIComponent(gid) + '/open?operator=ui', { method: 'POST' }) });
}

/* ------------------------------------------------------------------ confirm dialog */
function confirmAction(opt) {
  const modal = $('modal');
  const errBox = el('div', { class: 'errbox', hidden: true });
  const ok = el('button', { class: 'btn ' + (opt.danger ? 'danger' : 'primary'), text: '确认执行' });
  const cancel = el('button', { class: 'btn', text: '取消' });
  const close = () => { modal.hidden = true; modal.textContent = ''; };
  cancel.addEventListener('click', close);
  ok.addEventListener('click', async () => {
    ok.disabled = true;
    errBox.hidden = true;
    try {
      await opt.run();
      close();
      toast('已执行：' + opt.title);
      state.dead = {};
      await refreshRound();
      loadRounds();
    } catch (e) {
      errBox.hidden = false;
      errBox.textContent = describeError(e);
      ok.disabled = false;
    }
  });
  modal.textContent = '';
  modal.appendChild(el('div', { class: 'box' }, el('h3', { text: opt.title }), el('div', { class: 'msg', text: opt.msg }), errBox,
    el('div', { class: 'btnrow' }, cancel, ok)));
  modal.hidden = false;
  modal.onclick = (e) => { if (e.target === modal) close(); };
}

/* ------------------------------------------------------------------ pan / zoom */
let dragMoved = false;
let userMoved = false;   // once the user pans/zooms we stop auto-fitting
function refitIfPristine() { if (!userMoved) requestAnimationFrame(fitView); }
function applyView() {
  if (!L) return;
  L.root.setAttribute('transform', 'translate(' + vp.x + ' ' + vp.y + ') scale(' + vp.k + ')');
  $('canvas').classList.toggle('lod', vp.k < 0.3);
}
function zoomAt(mx, my, f) {
  userMoved = true;
  const nk = clamp(vp.k * f, 0.06, 3);
  const r = nk / vp.k;
  vp.x = mx - (mx - vp.x) * r;
  vp.y = my - (my - vp.y) * r;
  vp.k = nk;
  applyView();
}
function fitView() {
  if (!L) return;
  const svg = $('canvas');
  const w = svg.clientWidth || 1200, h = svg.clientHeight || 700;
  const k = clamp(Math.min((w - 8) / L.lay.width, (h - 8) / L.lay.height), 0.06, 1.5);
  vp.k = k;
  vp.x = (w - L.lay.width * k) / 2;
  vp.y = Math.max(4, (h - L.lay.height * k) / 2);
  applyView();
}
function setupPanZoom() {
  const svg = $('canvas');
  svg.addEventListener('wheel', (e) => {
    e.preventDefault();
    const r = svg.getBoundingClientRect();
    zoomAt(e.clientX - r.left, e.clientY - r.top, Math.exp(-e.deltaY * (e.ctrlKey ? 0.01 : 0.0016)));
  }, { passive: false });
  svg.addEventListener('pointerdown', (e) => {
    if (e.button !== 0) return;
    const sx = e.clientX, sy = e.clientY, ox = vp.x, oy = vp.y;
    dragMoved = false;
    const mv = (ev) => {
      const dx = ev.clientX - sx, dy = ev.clientY - sy;
      if (!dragMoved && Math.abs(dx) + Math.abs(dy) < 4) return;
      if (!dragMoved) svg.classList.add('dragging');
      dragMoved = true; userMoved = true;
      vp.x = ox + dx; vp.y = oy + dy;
      applyView();
    };
    const up = () => {
      window.removeEventListener('pointermove', mv);
      window.removeEventListener('pointerup', up);
      svg.classList.remove('dragging');
      setTimeout(() => { dragMoved = false; }, 0);
    };
    window.addEventListener('pointermove', mv);
    window.addEventListener('pointerup', up);
  });
  svg.addEventListener('click', () => { if (!dragMoved) clearSel(); });
  const cz = () => { const r = svg.getBoundingClientRect(); return [r.width / 2, r.height / 2]; };
  $('zoomIn').addEventListener('click', () => { const c = cz(); zoomAt(c[0], c[1], 1.25); });
  $('zoomOut').addEventListener('click', () => { const c = cz(); zoomAt(c[0], c[1], 0.8); });
  $('zoomFit').addEventListener('click', () => { userMoved = false; fitView(); });
}

/* ------------------------------------------------------------------ theme */
function applyTheme(t) {
  if (t === 'dark' || t === 'light') document.documentElement.setAttribute('data-theme', t);
  else document.documentElement.removeAttribute('data-theme');
  $('themeBtn').textContent = '主题：' + ({ dark: '深色', light: '浅色' }[t] || '自动');
}

/* ------------------------------------------------------------------ init */
async function init() {
  const q = new URLSearchParams(window.location.search);
  applyTheme(q.get('theme') || store.lget('pg-theme') || 'auto');
  $('themeBtn').addEventListener('click', () => {
    const cur = document.documentElement.getAttribute('data-theme') || 'auto';
    const nxt = { auto: 'light', light: 'dark', dark: 'auto' }[cur];
    store.lset('pg-theme', nxt);
    applyTheme(nxt);
  });
  $('tokenInp').value = store.get('pg-token') || '';
  $('tokenInp').addEventListener('change', () => { store.set('pg-token', $('tokenInp').value.trim()); refreshAll(); });
  $('autoChk').addEventListener('change', (e) => { state.auto = e.target.checked; });
  $('refreshBtn').addEventListener('click', refreshAll);
  $('graphSel').addEventListener('change', async (e) => {
    state.graph = e.target.value; state.roundId = null; clearRoundData(); state.sel = null; state.summarySig = '';
    syncUrl();
    try { await loadPlan(); await loadRounds(); } catch (err) { showBanner(describeError(err)); }
  });
  $('roundSel').addEventListener('change', (e) => selectRound(e.target.value));
  setupPanZoom();
  window.addEventListener('resize', () => { /* keep the user's view */ });

  state.graph = q.get('graph');
  urlHadGraph = !!state.graph;
  state.roundId = q.get('round');
  try {
    await loadGraphs();
    if (state.roundId && !urlHadGraph) await resolveGraphFromRound();
    if (state.graph) {
      await loadPlan();
      await loadRounds();
      if (state.roundId) await refreshRound();
      syncUrl();
    }
    const sp = (q.get('sel') || '').split(':');   // deep link: sel=pipeline:core-grade | run:R2 | gate:approve
    if (sp.length === 2 && ['pipeline', 'run', 'gate'].indexOf(sp[0]) >= 0) state.sel = { kind: sp[0], id: sp[1] };
    updateAll();
  } catch (e) {
    showBanner(describeError(e));
  }

  let n = 0;
  setInterval(async () => {
    if (!state.auto) return;
    n++;
    if ((!state.graphs.length || !state.plan) && !missingBusy) {   // initial load failed: keep retrying (also recovers once a token is entered)
      missingBusy = true;
      try { await loadMissing(); updateAll(); } catch (e) { /* banner already shows the first failure */ } finally { missingBusy = false; }
    }
    // poll while a round is selected and its view is not loaded yet (failed first fetch) or not terminal
    const active = state.roundId && (!state.view || !TERMINAL[state.view.round.status]);
    if (active) await refreshRound();
    if (n % 3 === 0) await loadRounds();
  }, 2000);
  setInterval(tickClocks, 1000);
}
let urlHadGraph = false;
async function resolveGraphFromRound() {
  try {
    const v = await api('/rounds/' + encodeURIComponent(state.roundId));
    if (v && v.round && state.graphs.indexOf(v.round.graphName) >= 0) { state.graph = v.round.graphName; $('graphSel').value = state.graph; }
  } catch (e) { /* the plan load / refresh below reports it */ }
}
/** Reloads whatever is still missing (graph list, plan) after a failed first load, e.g. the 401 before a token was entered. */
async function loadMissing() {
  if (!state.graphs.length) await loadGraphs();
  if (!state.plan && state.roundId && !urlHadGraph) await resolveGraphFromRound();
  if (state.graph && !state.plan) { await loadPlan(); syncUrl(); }
  const sp = (new URLSearchParams(window.location.search).get('sel') || '').split(':');
  if (!state.sel && sp.length === 2 && ['pipeline', 'run', 'gate'].indexOf(sp[0]) >= 0) state.sel = { kind: sp[0], id: sp[1] };
}
let missingBusy = false;
async function refreshAll() {
  try {
    await loadMissing();
    await loadRounds();
    if (state.roundId) await refreshRound(); else updateAll();
  } catch (e) { showBanner(describeError(e)); updateAll(); }
}
function tickClocks() {
  document.querySelectorAll('[data-since]').forEach((n) => {
    const s = Number(n.getAttribute('data-since'));
    n.textContent = (n.getAttribute('data-pre') || '') + fmtDur(Date.now() - s);
  });
}
window.addEventListener('DOMContentLoaded', init);
