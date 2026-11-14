#!/usr/bin/env node
// Records the pg-ui admin console following one demo round through its graph.
// Zero deps: drives headless Chrome over CDP (node >= 22: global fetch + WebSocket).
// Needs a running demo pod (see run-pod.sh) and ffmpeg. Output goes to docs/assets/.
//   node pipeliner-example/scripts/capture-ui.mjs [--theme=light|dark] [--out=DIR] [--stills]
// Env: BASE (http://127.0.0.1:8081), PG_ADMIN_TOKEN, CHROME, FPS (12), WIDTH (1200), SPEED (1.2), COLORS (128), DITHER (none), KEEP
// Pods: run-pod.sh x3 with --demo.core-grade-delay-ms=2 (R2 streams for ~13 s)
import { spawn, execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const arg = (k, d) => (process.argv.find((a) => a.startsWith('--' + k + '=')) || '').split('=')[1] ?? d;
const THEME = arg('theme', 'light');
const STILLS = process.argv.includes('--stills');
const HERE = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(arg('out', join(HERE, '../../docs/assets')));
const BASE = process.env.BASE || 'http://127.0.0.1:8081';
const TOKEN = process.env.PG_ADMIN_TOKEN || 'dev-admin-token-change-me';
const CHROME = process.env.CHROME || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const FPS = +(process.env.FPS || 12), WIDTH = +(process.env.WIDTH || 1200), SPEED = +(process.env.SPEED || 1.2);
const W = 1440, H = 640, PORT = 9333 + Math.floor(Math.random() * 500);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const H_AUTH = { Authorization: 'Bearer ' + TOKEN };

const work = mkdtempSync(join(tmpdir(), 'pgui-'));
const chrome = spawn(CHROME, ['--headless=new', '--disable-gpu', '--hide-scrollbars', `--remote-debugging-port=${PORT}`,
  `--user-data-dir=${work}/profile`, `--window-size=${W},${H}`, 'about:blank'], { stdio: 'ignore' });
const cleanup = () => { try { chrome.kill(); } catch {} };
process.on('exit', cleanup);

async function connect() {
  for (let i = 0; i < 50; i++) {
    try {
      const pg = await (await fetch(`http://127.0.0.1:${PORT}/json/new?about:blank`, { method: 'PUT' })).json();
      if (pg.webSocketDebuggerUrl) return pg.webSocketDebuggerUrl;
    } catch {}
    await sleep(200);
  }
  throw new Error('chrome did not start');
}
const ws = new WebSocket(await connect());
await new Promise((r) => (ws.onopen = r));
let nid = 0; const pend = new Map();
ws.onmessage = (m) => { const d = JSON.parse(m.data); if (d.id && pend.has(d.id)) { pend.get(d.id)(d); pend.delete(d.id); } };
const cdp = (method, params = {}) => new Promise((res, rej) => {
  const id = ++nid; pend.set(id, (d) => (d.error ? rej(new Error(method + ': ' + d.error.message)) : res(d.result))); ws.send(JSON.stringify({ id, method, params }));
});
const ev = async (expr) => (await cdp('Runtime.evaluate', { expression: expr, awaitPromise: true, returnByValue: true })).result.value;

await cdp('Page.enable'); await cdp('Page.bringToFront');
await cdp('Emulation.setDeviceMetricsOverride', { width: W, height: H, deviceScaleFactor: 1, mobile: false });
await cdp('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-color-scheme', value: THEME }] });

// ---- round control (API; reads need no token)
const post = async (p) => (await fetch(BASE + p, { method: 'POST', headers: H_AUTH })).json();
const view = async (id) => (await (await fetch(`${BASE}/pg/rounds/${id}`)).json());
await post('/demo/seed?materials=600&seed=42');
const { roundId } = await post('/demo/rounds?planVersion=readme-' + Date.now());
console.log('round', roundId);

// ---- frames
mkdirSync(OUT, { recursive: true });
const frames = []; // {file, t}
let n = 0; let t0 = Date.now();
async function snap() {
  const { data } = await cdp('Page.captureScreenshot', { format: 'png' });
  const file = join(work, `f${String(n++).padStart(5, '0')}.png`);
  writeFileSync(file, Buffer.from(data, 'base64'));
  frames.push({ file, t: Date.now() - t0 });
}
async function still(name) {
  const { data } = await cdp('Page.captureScreenshot', { format: 'png', clip: { x: 0, y: 0, width: W, height: H, scale: 2 } });
  writeFileSync(join(OUT, name), Buffer.from(data, 'base64'));
}
async function until(pred, label, maxMs = 120000) {
  const s = Date.now();
  for (;;) {
    await snap();
    const v = await view(roundId);
    if (pred(v)) return v;
    if (Date.now() - s > maxMs) throw new Error('timeout waiting for ' + label);
  }
}
async function hold(ms) { const s = Date.now(); while (Date.now() - s < ms) await snap(); }
const runSt = (v, id) => v.runs.find((r) => r.runId === id)?.status;
const gateSt = (v, id) => v.gates.find((g) => g.gateId === id)?.status;

await cdp('Page.navigate', { url: `${BASE}/pg-ui/?round=${encodeURIComponent(roundId)}&theme=${THEME}` });
await sleep(1500);
await ev(`(async()=>{for(let i=0;i<50&&!document.querySelector('#canvas .pipe, #canvas g');i++)await new Promise(r=>setTimeout(r,100))})()`);
frames.length = 0; n = 0; t0 = Date.now(); // story starts on a drawn topology

await until((v) => runSt(v, 'R2') === 'RUNNING', 'R2 running');
const clickText = (sel, text) => ev(`(()=>{const e=[...document.querySelectorAll(${JSON.stringify(sel)})].find(x=>x.textContent.includes(${JSON.stringify(text)}));if(!e)return false;e.dispatchEvent(new MouseEvent('click',{bubbles:true}));return true})()`);
await hold(3000);
if (STILLS) await still('pg-ui-running.png');
console.log('R2 running shot at', Date.now() - t0);
await until((v) => gateSt(v, 'approve') === 'CLOSED' && v.gates.find((g) => g.gateId === 'approve').armedAt && runSt(v, 'R4') === 'SUCCEEDED' && runSt(v, 'R5') === 'SUCCEEDED', 'R4/R5 done');
console.log('approve armed at', Date.now() - t0, 'select approve gate', await clickText('#canvas g.gate', 'approve'));
await hold(1800);
if (STILLS) await still('pg-ui-gate.png');
await hold(300);

// operator action through the UI: token -> open button -> confirm
await ev(`(()=>{const i=document.getElementById('tokenInp');i.focus();i.value='';})()`);
await cdp('Input.insertText', { text: TOKEN });
for (const type of ['keyDown', 'keyUp']) await cdp('Input.dispatchKeyEvent', { type, key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 13, text: type === 'keyDown' ? '\r' : undefined });
await hold(700);
await ev(`(()=>{const b=[...document.querySelectorAll('#side button')].find(x=>x.textContent.includes('打开闸门'));b.click();})()`);
await hold(1200);
await ev(`(()=>{const b=[...document.querySelectorAll('#modal button')].find(x=>x.textContent.includes('确认执行'));b.click();})()`);
console.log('gate opened at', Date.now() - t0);
await until((v) => v.round.status === 'SUCCEEDED', 'round SUCCEEDED');
await hold(1500); // let the 2s poll repaint SUCCEEDED
await snap(); const tEnd = Date.now() - t0;
const errShown = await ev(`!document.getElementById('banner').hidden`);
console.log('banner shown at end:', errShown);
await hold(2000);
cleanup();

// ---- encode: concat with real per-frame durations -> palette GIF
const list = frames.map((f, i) => `file '${f.file}'\nduration ${(((frames[i + 1]?.t ?? f.t + 2000) - f.t) / 1000).toFixed(3)}`).join('\n') + `\nfile '${frames.at(-1).file}'\n`;
writeFileSync(join(work, 'list.txt'), list);
const name = THEME === 'dark' ? 'pg-ui-demo-dark.gif' : 'pg-ui-demo.gif';
const vf = `setpts=PTS/${SPEED},fps=${FPS},scale=${WIDTH}:-1:flags=lanczos`;
const pal = join(work, 'pal.png');
execFileSync('ffmpeg', ['-v', 'error', '-y', '-f', 'concat', '-safe', '0', '-i', join(work, 'list.txt'), '-vf', `${vf},palettegen=max_colors=${process.env.COLORS || 128}:stats_mode=diff`, pal]);
execFileSync('ffmpeg', ['-v', 'error', '-y', '-f', 'concat', '-safe', '0', '-i', join(work, 'list.txt'), '-i', pal, '-lavfi', `${vf}[x];[x][1:v]paletteuse=dither=${process.env.DITHER || 'none'}:diff_mode=rectangle`, '-loop', '0', join(OUT, name)]);
console.log('wrote', join(OUT, name), 'frames', frames.length, 'real seconds', ((frames.at(-1).t) / 1000).toFixed(1));
if (process.env.KEEP) console.log('frames kept in', work); else rmSync(work, { recursive: true, force: true });
process.exit(0);
