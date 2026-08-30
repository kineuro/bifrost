// Bifrost: the group's bridge for exchanging data with partners. One HTTP server on the VM, behind the edge.
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
import { dropUpload, HttpError, removeShareData } from './files.js';
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
app.route('/api', pub);
app.route('/admin', admin);

// CLI distribution: the binaries are built by CI into bin/, served here with an install script.
app.get('/api/cli', async (c) => {
  const files = (await fsp.readdir(binDir).catch(() => [] as string[])).filter((f) => f.startsWith('bifrost-'));
  const version = (await fsp.readFile(path.join(binDir, 'VERSION'), 'utf8').catch(() => 'dev')).trim();
  return c.json({ version, files, base: `${config.publicUrl}/dl/` });
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
