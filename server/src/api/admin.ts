// The admin API, used by the staff portal (server side, over the guest network, with ADMIN_KEY). Never exposed at the edge.
import { Hono } from 'hono';
import { timingSafeEqual } from 'node:crypto';
import { hashSecret, newToken } from '../auth.js';
import { config } from '../config.js';
import { audit, db, now, q, shareUsage, type Box, type Direction, type Share, type ShareStatus } from '../db.js';
import { ensureBoxes, HttpError, listDir, removeShareData, streams, walk, walkStream } from '../files.js';
import { randomBytes } from 'node:crypto';

export const admin = new Hono();

admin.use('/*', async (c, next) => {
  const k = c.req.header('x-bifrost-admin') ?? '';
  const a = Buffer.from(k), b = Buffer.from(config.adminKey);
  if (a.length !== b.length || !timingSafeEqual(a, b)) return c.json({ error: 'admin key required' }, 401);
  await next();
});
const actor = (c: any) => String(c.req.header('x-bifrost-actor') ?? 'portal');

const slugify = (s: string) => s.toLowerCase().normalize('NFKD').replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 48) || 'bridge';
const dirs: Direction[] = ['in', 'out', 'both'];

function full(share: Share) {
  const creds = q.credsOf.all(share.id).map((k) => ({ id: k.id, label: k.label, passcode: !!k.passcode_hash, created_at: k.created_at, revoked_at: k.revoked_at, last_used_at: k.last_used_at, last_ip: k.last_ip }));
  const uploads = q.uploadsOf.all(share.id).map((u) => ({ id: u.id, path: u.path, size: u.size, parts_done: u.parts_done ? u.parts_done.split(',').length : 0, parts_total: u.parts_total, updated_at: u.updated_at }));
  return { ...share, allowed_cidrs: JSON.parse(share.allowed_cidrs || '[]'), ...shareUsage(share.id), credentials: creds, uploads, expired: !!share.expires_at && new Date(share.expires_at).getTime() < Date.now() };
}

admin.get('/stats', (c) => {
  const shares = q.shares.all();
  const totals = shares.reduce((t, s) => { const u = shareUsage(s.id); t.bytes += u.used_bytes; t.files += u.files; t.down += u.downloaded_bytes; return t; }, { bytes: 0, files: 0, down: 0 });
  const recent = db.prepare("SELECT kind, SUM(bytes) AS bytes, SUM(files) AS files FROM transfers WHERE at > ? GROUP BY kind").all(new Date(Date.now() - 7 * 86400_000).toISOString());
  return c.json({ shares: shares.length, open: shares.filter((s) => s.status === 'open').length, ...totals, week: recent, streams: streams(), version: config.version });
});
admin.get('/shares', (c) => c.json({ shares: q.shares.all().map(full) }));
admin.get('/shares/:id', (c) => { const s = q.share.get(c.req.param('id')); if (!s) throw new HttpError(404, 'no such bridge'); return c.json({ share: full(s) }); });

admin.post('/shares', async (c) => {
  const b = await c.req.json();
  const name = String(b.name ?? '').trim();
  if (!name) throw new HttpError(400, 'name required');
  const direction = dirs.includes(b.direction) ? (b.direction as Direction) : 'in';
  let id = slugify(b.id || name);
  if (q.share.get(id)) id = `${id}-${randomBytes(2).toString('hex')}`;
  const t = now();
  db.prepare(`INSERT INTO shares (id, name, partner, direction, message, quota_bytes, max_files, max_download_bytes, expires_at, allowed_cidrs, status, created_by, created_at, updated_at, note, notify)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(
    id, name, String(b.partner ?? ''), direction, String(b.message ?? ''), Number(b.quota_bytes ?? 0), Number(b.max_files ?? 0), Number(b.max_download_bytes ?? 0),
    b.expires_at ? new Date(b.expires_at).toISOString() : null, JSON.stringify(Array.isArray(b.allowed_cidrs) ? b.allowed_cidrs : []), 'open', actor(c), t, t, String(b.note ?? ''), b.notify === false ? 0 : 1);
  const share = q.share.get(id)!;
  await ensureBoxes(share);
  audit(actor(c), 'share.created', id, { name, direction });
  return c.json({ share: full(share) }, 201);
});
admin.patch('/shares/:id', async (c) => {
  const s = q.share.get(c.req.param('id')); if (!s) throw new HttpError(404, 'no such bridge');
  const b = await c.req.json();
  const set: string[] = []; const vals: unknown[] = [];
  const f = (k: string, v: unknown) => { set.push(`${k} = ?`); vals.push(v); };
  if (b.name !== undefined) f('name', String(b.name));
  if (b.partner !== undefined) f('partner', String(b.partner));
  if (b.message !== undefined) f('message', String(b.message));
  if (b.note !== undefined) f('note', String(b.note));
  if (b.direction !== undefined && dirs.includes(b.direction)) f('direction', b.direction);
  if (b.quota_bytes !== undefined) f('quota_bytes', Number(b.quota_bytes));
  if (b.max_files !== undefined) f('max_files', Number(b.max_files));
  if (b.max_download_bytes !== undefined) f('max_download_bytes', Number(b.max_download_bytes));
  if (b.expires_at !== undefined) f('expires_at', b.expires_at ? new Date(b.expires_at).toISOString() : null);
  if (b.allowed_cidrs !== undefined) f('allowed_cidrs', JSON.stringify(Array.isArray(b.allowed_cidrs) ? b.allowed_cidrs : []));
  if (b.notify !== undefined) f('notify', b.notify ? 1 : 0);
  if (b.status !== undefined && ['open', 'frozen', 'accepted', 'closed'].includes(b.status)) f('status', b.status as ShareStatus);
  if (!set.length) throw new HttpError(400, 'nothing to change');
  f('updated_at', now()); vals.push(s.id);
  db.prepare(`UPDATE shares SET ${set.join(', ')} WHERE id = ?`).run(...vals);
  audit(actor(c), 'share.updated', s.id, b);
  return c.json({ share: full(q.share.get(s.id)!) });
});
admin.delete('/shares/:id', async (c) => {
  const s = q.share.get(c.req.param('id')); if (!s) throw new HttpError(404, 'no such bridge');
  if (c.req.query('data') === '1') await removeShareData(s, actor(c));
  db.prepare('DELETE FROM shares WHERE id = ?').run(s.id);
  audit(actor(c), 'share.deleted', s.id, { data: c.req.query('data') === '1' });
  return c.body(null, 204);
});

// Credentials: the token is returned once, here, and never again.
admin.post('/shares/:id/credentials', async (c) => {
  const s = q.share.get(c.req.param('id')); if (!s) throw new HttpError(404, 'no such bridge');
  const b = await c.req.json().catch(() => ({}));
  const t = newToken();
  let passcode_hash: string | null = null, passcode_salt: string | null = null;
  if (b.passcode) { passcode_salt = randomBytes(16).toString('hex'); passcode_hash = hashSecret(String(b.passcode), passcode_salt); }
  db.prepare('INSERT INTO credentials (id, share_id, label, hash, salt, passcode_hash, passcode_salt, created_at) VALUES (?,?,?,?,?,?,?,?)')
    .run(t.id, s.id, String(b.label ?? ''), t.hash, t.salt, passcode_hash, passcode_salt, now());
  audit(actor(c), 'credential.created', s.id, { id: t.id, label: b.label, passcode: !!b.passcode });
  return c.json({ id: t.id, token: t.token, link: `${config.publicUrl}/#${t.token}` }, 201);
});
admin.delete('/credentials/:id', (c) => {
  const k = q.cred.get(c.req.param('id')); if (!k) throw new HttpError(404, 'no such credential');
  db.prepare('UPDATE credentials SET revoked_at = ? WHERE id = ?').run(now(), k.id);
  audit(actor(c), 'credential.revoked', k.share_id, { id: k.id, label: k.label });
  return c.body(null, 204);
});

const NDJSON = { 'content-type': 'application/x-ndjson; charset=utf-8', 'cache-control': 'no-store' };
const MANIFEST_PAGE = 2000;
const LINES_PER_CHUNK = 500;

// Records as an ndjson body: one JSON object per line, encoded a chunk at a time and only as the reader asks for
// it. Memory holds a chunk, never the listing, and the source is closed if the reader gives up early.
function lines(src: AsyncGenerator<unknown>): ReadableStream<Uint8Array> {
  const enc = new TextEncoder();
  return new ReadableStream({
    async pull(ctrl) {
      const buf: string[] = [];
      while (buf.length < LINES_PER_CHUNK) {
        const { value, done } = await src.next();
        if (done) break;
        buf.push(JSON.stringify(value));
      }
      if (buf.length) ctrl.enqueue(enc.encode(buf.join('\n') + '\n'));
      else ctrl.close();
    },
    cancel: (r) => void src.return?.(r),
  });
}

admin.get('/shares/:id/files', async (c) => {
  const s = q.share.get(c.req.param('id')); if (!s) throw new HttpError(404, 'no such bridge');
  const box = (c.req.query('box') ?? 'in') as Box;
  const all = c.req.query('all') === '1';
  const rel = c.req.query('path') ?? '';
  // A whole tree as one JSON array only works while the tree is small. Asked for as ndjson it is streamed
  // instead, a file per line, so neither the server nor the caller ever holds the whole listing.
  if (all && c.req.query('format') === 'ndjson') return c.body(lines(walkStream(s, box, rel)), 200, NDJSON);
  return c.json({ box, entries: all ? await walk(s, box, rel) : await listDir(s, box, rel) });
});

// The manifest of a box: what the bridge recorded it accepted, one JSON object per line. It comes from the
// records, not from a walk of the tree, because the records already carry the size and the checksum of every
// file: a manifest of millions of files is then a handful of indexed page reads rather than millions of stats.
// Reading it in pages also keeps bytes moving, so a slow caller never leaves the socket idle long enough for
// `server.timeout` to drop it, and neither side has to hold the manifest whole.
admin.get('/shares/:id/manifest', (c) => {
  const s = q.share.get(c.req.param('id')); if (!s) throw new HttpError(404, 'no such bridge');
  const box = (c.req.query('box') ?? 'in') as Box;
  const id = s.id;
  async function* rows() {
    let after = '';
    for (;;) {
      const page = q.filesPage.all(id, box, after, MANIFEST_PAGE);
      if (!page.length) return;
      after = page[page.length - 1].path;
      for (const r of page) yield r;
    }
  }
  return c.body(lines(rows()), 200, { ...NDJSON, 'x-bifrost-files': String(q.usage.get(s.id, box)?.files ?? 0) });
});
admin.get('/shares/:id/activity', (c) => {
  const s = q.share.get(c.req.param('id')); if (!s) throw new HttpError(404, 'no such bridge');
  return c.json({ transfers: q.transfersOf.all(s.id, 200), audit: q.auditOf.all(s.id, 200) });
});
admin.get('/audit', (c) => c.json({ audit: q.auditAll.all(Number(c.req.query('n') ?? 300)) }));

// Accept: the inbox is complete; freeze it and mark it for the move into tank/source (done on Asgard by `bifrost-accept`).
admin.post('/shares/:id/accept', (c) => {
  const s = q.share.get(c.req.param('id')); if (!s) throw new HttpError(404, 'no such bridge');
  db.prepare("UPDATE shares SET status = 'accepted', updated_at = ? WHERE id = ?").run(now(), s.id);
  audit(actor(c), 'share.accepted', s.id, {});
  return c.json({ share: full(q.share.get(s.id)!) });
});
