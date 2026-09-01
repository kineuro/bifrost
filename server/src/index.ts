// Bifrost: kineuro's bridge for exchanging data with partners. One HTTP server on the VM, behind the edge.
import { createServer } from 'node:http';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Hono } from 'hono';
import { getRequestListener } from '@hono/node-server';
import { serveStatic } from '@hono/node-server/serve-static';
import { admin } from './api/admin.js';
import { pub } from './api/public.js';
import { config } from './config.js';
import { audit, db, now, q } from './db.js';
import { dropUpload, HttpError, removeShareData, sweepTemp } from './files.js';
import { metrics, render } from './metrics.js';
import { notify } from './notify.js';
import { handleTus } from './tus.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const publicDir = fs.existsSync(path.join(here, '..', 'public')) ? path.join(here, '..', 'public') : path.join(here, '..', '..', 'public');
const binDir = process.env.BIN_DIR ?? path.join(here, '..', 'bin');

const app = new Hono();
app.use('*', async (c, next) => {
  metrics.requests.inc();
  await next();
  if (c.res.status >= 500) metrics.errors.inc();
  c.header('X-Content-Type-Options', 'nosniff');
  c.header('Referrer-Policy', 'strict-origin-when-cross-origin');
});
app.onError((err, c) => {
  if (err instanceof HttpError) return c.json({ error: err.message }, err.status as any);
  console.error(now(), 'error', c.req.method, c.req.path, err);
  metrics.errors.inc();
  return c.json({ error: 'internal error' }, 500);
});

app.get('/api/health', (c) => c.json({ ok: true, version: config.version, exchange: fs.existsSync(config.inRoot) }));
app.get('/metrics', (c) => c.text(render(), 200, { 'content-type': 'text/plain; version=0.0.4' }));
// CLI distribution: the binaries are built by CI into bin/, served here with an install script.
app.get('/api/cli', async (c) => {
  const files = (await fsp.readdir(binDir).catch(() => [] as string[])).filter((f) => f.startsWith('bifrost-'));
  const version = (await fsp.readFile(path.join(binDir, 'VERSION'), 'utf8').catch(() => 'dev')).trim();
  return c.json({ version, files, base: `${config.publicUrl}/dl/` });
});
// The download list: one small page in the site's style, built from what is in bin/.
app.get('/dl', (c) => c.redirect('/dl/'));
app.get('/dl/', async (c) => {
  const version = (await fsp.readFile(path.join(binDir, 'VERSION'), 'utf8').catch(() => 'dev')).trim();
  const sums = new Map((await fsp.readFile(path.join(binDir, 'SHA256SUMS'), 'utf8').catch(() => '')).split('\n').filter(Boolean).map((l) => { const [h, f] = l.trim().split(/\s+/); return [f, h] as [string, string]; }));
  const names: Record<string, string> = { 'linux-amd64': 'Linux, x86-64', 'linux-arm64': 'Linux, ARM64', 'darwin-arm64': 'macOS, Apple silicon', 'darwin-amd64': 'macOS, Intel', 'windows-amd64.exe': 'Windows, x86-64', 'windows-arm64.exe': 'Windows, ARM64' };
  const rows: string[] = [];
  for (const [key, label] of Object.entries(names)) {
    const f = `bifrost-${key}`;
    const st = await fsp.stat(path.join(binDir, f)).catch(() => null);
    if (!st) continue;
    rows.push(`<div class="row"><span class="name"><span>${label}</span></span><span class="size">${(st.size / 1e6).toFixed(1)} MB</span><span class="when"><a href="/dl/${f}">${f}</a></span><span class="sum">${sums.get(f) ?? ''}</span></div>`);
  }
  const esc = (t: string) => t.replace(/[&<>]/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[ch]!));
  const html = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex"><title>Downloads · Bifrost</title><link rel="icon" href="/favicon.svg" type="image/svg+xml"><link rel="stylesheet" href="/global.css"><style>.sum{grid-column:1/-1;font-family:var(--mono);font-size:11.5px;color:var(--muted);word-break:break-all}.row{grid-template-columns:1fr auto auto}@media (max-width:760px){.row{grid-template-columns:1fr auto}.row .when{grid-column:1/-1;white-space:normal;word-break:break-all}}</style></head><body>
<header class="top"><div class="wrap"><a class="brand" href="/"><img src="/kineuro-mark.svg" alt="" width="26" height="26"><span><span class="name">Bifrost</span><br><span class="sub">Experimental Neuroradiology Research at Karolinska Institutet · data bridge</span></span></a><nav class="top"><a href="/">Your bridge</a><a href="/docs/">How to use it</a><a href="/docs/cli/">Command line</a><a href="https://kineuro.se">kineuro.se</a></nav></div></header>
<main><section class="band last"><div class="wrap" style="display:flex;flex-direction:column;gap:20px"><p class="eyebrow">Downloads</p><h1>The <code>bifrost</code> command, version ${esc(version)}</h1>
<p class="lede">Pick the build for your system, or let the installer choose: <code>curl -fsSL https://bifrost.kineuro.se/get | sh</code> on Linux and macOS, <code>irm https://bifrost.kineuro.se/get.ps1 | iex</code> on Windows. Each file's SHA-256 is under it; the full list is <a href="/dl/SHA256SUMS">SHA256SUMS</a>. After a manual download: <code>chmod +x</code> on Linux and macOS (and <code>xattr -d com.apple.quarantine bifrost</code> on macOS), then put it on your PATH as <code>bifrost</code>.</p>
<div class="list">${rows.join('') || '<div class="row"><span class="muted">No builds published yet.</span></div>'}</div>
<p class="muted" style="font-size:13px">Later, <code>bifrost update</code> replaces the binary in place with whatever version this page shows. <a href="/docs/cli/">How to use the command line</a>.</p></div></section></main>
<footer><div class="wrap"><span>Bifrost is run by Experimental Neuroradiology Research at Karolinska Institutet.</span><span>Questions: <a href="mailto:admin@kineuro.se">admin@kineuro.se</a></span></div></footer></body></html>`;
  return c.html(html);
});
app.get('/dl/:file', async (c) => {
  const f = c.req.param('file');
  if (!/^(bifrost-[a-z0-9-]+(\.exe)?|SHA256SUMS|VERSION)$/.test(f)) return c.notFound();
  const p = path.join(binDir, f);
  if (!fs.existsSync(p)) return c.notFound();
  const st = await fsp.stat(p);
  c.header('Content-Length', String(st.size));
  c.header('Content-Type', 'application/octet-stream');
  c.header('Content-Disposition', `attachment; filename="${f}"`);
  return c.body(fs.createReadStream(p) as any);
});
app.route('/api', pub);
app.route('/admin', admin);

app.get('/get', async (c) => c.text(await fsp.readFile(path.join(publicDir, 'get.sh'), 'utf8'), 200, { 'content-type': 'text/x-shellscript' }));
app.get('/get.ps1', async (c) => c.text(await fsp.readFile(path.join(publicDir, 'get.ps1'), 'utf8'), 200, { 'content-type': 'text/plain' }));

// Static site (the collaborator page and the docs), built by web/ into public/.
app.use('/*', serveStatic({ root: path.relative(process.cwd(), publicDir) || '.', rewriteRequestPath: (p) => (p.endsWith('/') ? `${p}index.html` : p) }));
app.get('/*', async (c) => {
  const p = c.req.path.replace(/\/$/, '');
  for (const cand of [`${p}.html`, `${p}/index.html`]) {
    const f = path.join(publicDir, cand);
    if (f.startsWith(publicDir) && fs.existsSync(f)) return c.html(await fsp.readFile(f, 'utf8'));
  }
  return c.text('not found', 404);
});

// Housekeeping every hour: stale part uploads (7 days), expired bridges frozen, closed bridges emptied after the grace period.
async function housekeeping() {
  const stale = q.staleUploads.all(new Date(Date.now() - 7 * 86400_000).toISOString());
  for (const u of stale) await dropUpload(u.id);
  await sweepTemp(config.inRoot).catch(() => {});
  const t = now();
  for (const s of q.shares.all()) {
    if (s.status === 'open' && s.expires_at && s.expires_at < t) {
      db.prepare("UPDATE shares SET status = 'frozen', updated_at = ? WHERE id = ?").run(t, s.id);
      audit('housekeeping', 'share.expired', s.id, {});
      await notify('BifrostBridgeExpired', `Bifrost: bridge "${s.name}" expired`, `Bridge ${s.id} is frozen; its data stays for ${config.graceDays} days unless accepted or closed.`, { bridge: s.id }).catch(() => {});
    }
    if (s.status === 'closed' && s.updated_at < new Date(Date.now() - config.graceDays * 86400_000).toISOString()) {
      const dir = path.join(config.inRoot, s.id);
      if (fs.existsSync(dir) || fs.existsSync(path.join(config.outRoot, s.id))) await removeShareData(s, 'housekeeping');
    }
    if (s.status === 'open' && s.expires_at) {
      const days = (new Date(s.expires_at).getTime() - Date.now()) / 86400_000;
      const flag = `warned:${s.id}`;
      if (days > 0 && days < 3 && !warned.has(flag)) { warned.add(flag); await notify('BifrostBridgeExpiring', `Bifrost: bridge "${s.name}" expires in ${Math.ceil(days)} day(s)`, `Extend it in the portal if the partner still needs it.`, { bridge: s.id }).catch(() => {}); }
    }
  }
}
const warned = new Set<string>();
setInterval(() => housekeeping().catch((e) => console.error('housekeeping', e)), 3600_000);
housekeeping().catch(() => {});

process.on('uncaughtException', (e) => console.error(now(), 'uncaught', e));
process.on('unhandledRejection', (e) => console.error(now(), 'unhandled', e));

const hono = getRequestListener(app.fetch);
const server = createServer((req, res) => {
  if (req.url?.startsWith('/api/tus')) return void handleTus(req, res);
  return hono(req, res);
});
server.requestTimeout = 0; server.headersTimeout = 60_000; server.keepAliveTimeout = 75_000;
server.listen(config.port, '0.0.0.0', () => console.log(now(), `bifrost ${config.version} listening on ${config.port}, exchange ${config.exchange}, site ${publicDir}`));
