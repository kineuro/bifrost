// The collaborator API: what the browser page and the `bifrost` CLI talk to.
import { Hono, type Context } from 'hono';
import { getCookie, setCookie, deleteCookie } from 'hono/cookie';
import { stream as honoStream } from 'hono/streaming';
import archiver from 'archiver';
import { createHash, randomBytes } from 'node:crypto';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { Readable, Transform, Writable, PassThrough } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { canDownload, canUpload, checkPasscode, credentialFromToken, issueSession, rateLimited, readSession, shareOpen, touch } from '../auth.js';
import { config } from '../config.js';
import { audit, now, q, shareUsage, type Box, type Credential, type Share } from '../db.js';
import { acquire, checkDownloadBudget, checkQuota, cleanPath, dropUpload, HttpError, listDir, receiveBatch, receiveFile, resolveIn, sendBatch, walk } from '../files.js';
import { notifyTransfer } from '../notify.js';
import { metrics } from '../metrics.js';

type Env = { Variables: { cred: Credential; share: Share; ip: string } };
export const pub = new Hono<Env>();

const clientIp = (c: Context) => (c.req.header('x-forwarded-for') ?? '').split(',')[0].trim() || (c.env as any)?.incoming?.socket?.remoteAddress || '0.0.0.0';

function shareView(share: Share) {
  const u = shareUsage(share.id);
  return {
    id: share.id, name: share.name, partner: share.partner, direction: share.direction, message: share.message,
    status: share.status, expires_at: share.expires_at,
    quota_bytes: share.quota_bytes, max_files: share.max_files, max_download_bytes: share.max_download_bytes,
    used_bytes: u.used_bytes, files: u.files, downloaded_bytes: u.downloaded_bytes,
    can_upload: canUpload(share), can_download: canDownload(share),
    limits: { part_size: config.partSize, large_file: config.largeFile, batch_bytes: config.batchBytes, batch_files: config.batchFiles, streams: config.maxStreamsPerClient },
  };
}

// --- authentication -------------------------------------------------------------------------------------------------
pub.post('/session', async (c) => {
  const ip = clientIp(c);
  if (rateLimited(ip)) return c.json({ error: 'too many attempts, try again in ten minutes' }, 429);
  const body = await c.req.json().catch(() => ({}));
  const cred = credentialFromToken(String(body.token ?? ''));
  if (!cred) { audit(ip, 'login.failed', null, {}); return c.json({ error: 'that token is not valid' }, 401); }
  if (!checkPasscode(cred, body.passcode)) return c.json({ error: cred.passcode_hash && !body.passcode ? 'passcode required' : 'wrong passcode', passcode: true }, 401);
  const share = q.share.get(cred.share_id)!;
  const denied = shareOpen(share, ip);
  if (denied) return c.json({ error: denied.error }, denied.status as any);
  setCookie(c, 'bfr', issueSession(cred.id), { httpOnly: true, secure: true, sameSite: 'Strict', path: '/', maxAge: config.sessionHours * 3600 });
  touch(cred, ip);
  audit(`${cred.label || cred.id}@${share.id}`, 'login', share.id, { ip, via: 'browser' });
  return c.json({ share: shareView(share), credential: { id: cred.id, label: cred.label } });
});
pub.post('/logout', (c) => { deleteCookie(c, 'bfr', { path: '/' }); return c.body(null, 204); });

// Everything below needs a credential: Bearer token (CLI) or session cookie (browser).
pub.use('/*', async (c, next) => {
  const ip = clientIp(c);
  let cred: Credential | null = null;
  const auth = c.req.header('authorization');
  if (auth?.startsWith('Bearer ')) {
    if (rateLimited(ip) && !credentialFromToken(auth.slice(7))) return c.json({ error: 'too many attempts' }, 429);
    cred = credentialFromToken(auth.slice(7));
    if (cred && !checkPasscode(cred, c.req.header('x-bifrost-passcode'))) return c.json({ error: 'passcode required', passcode: true }, 401);
  } else {
    const id = readSession(getCookie(c, 'bfr'));
    if (id) { cred = q.cred.get(id) ?? null; if (cred?.revoked_at) cred = null; }
  }
  if (!cred) return c.json({ error: 'not signed in' }, 401);
  const share = q.share.get(cred.share_id);
  if (!share) return c.json({ error: 'bridge missing' }, 404);
  const denied = shareOpen(share, ip);
  if (denied) return c.json({ error: denied.error }, denied.status as any);
  c.set('cred', cred); c.set('share', share); c.set('ip', ip);
  await next();
});

pub.get('/share', (c) => c.json({ share: shareView(c.get('share')), credential: { id: c.get('cred').id, label: c.get('cred').label } }));

const boxOf = (c: Context<Env>, wanted?: string): Box => {
  const share = c.get('share');
  const box = (wanted ?? c.req.query('box') ?? (canDownload(share) ? 'out' : 'in')) as Box;
  if (box !== 'in' && box !== 'out') throw new HttpError(400, 'box must be in or out');
  if (box === 'out' && !canDownload(share)) throw new HttpError(403, 'this bridge has nothing to download');
  if (box === 'in' && !(share.direction === 'in' || share.direction === 'both')) throw new HttpError(403, 'this bridge does not receive');
  return box;
};

pub.get('/ls', async (c) => {
  const box = boxOf(c);
  return c.json({ box, path: c.req.query('path') ?? '', entries: await listDir(c.get('share'), box, c.req.query('path') ?? '') });
});
pub.get('/manifest', async (c) => {
  const box = boxOf(c);
  return c.json({ box, files: await walk(c.get('share'), box, c.req.query('path') ?? '') });
});
pub.post('/mkdir', async (c) => {
  const share = c.get('share');
  if (!canUpload(share)) throw new HttpError(403, 'this bridge does not receive');
  const { path: p } = await c.req.json();
  await fsp.mkdir(resolveIn(share, 'in', p), { recursive: true });
  return c.json({ ok: true });
});

// --- push planning: which of these files are already here? --------------------------------------------------------
pub.post('/plan', async (c) => {
  const share = c.get('share');
  if (!canUpload(share)) throw new HttpError(403, 'this bridge does not receive');
  const { files } = (await c.req.json()) as { files: { path: string; size: number; sha256?: string }[] };
  if (!Array.isArray(files)) throw new HttpError(400, 'files[] required');
  const have: string[] = []; const missing: { path: string; size: number }[] = []; let missingBytes = 0;
  const resumable: { path: string; upload_id: string; parts_done: number[] }[] = [];
  for (const f of files) {
    const p = cleanPath(f.path);
    const row = q.file.get(share.id, 'in', p);
    if (row && row.size === f.size && (!f.sha256 || !row.sha256 || row.sha256 === f.sha256)) { have.push(p); continue; }
    if (f.sha256) {
      const u = q.uploadFor.get(share.id, p, f.size, f.sha256);
      if (u) resumable.push({ path: p, upload_id: u.id, parts_done: partsList(u.parts_done) });
    }
    missing.push({ path: p, size: f.size }); missingBytes += f.size;
  }
  checkQuota(share, missingBytes, missing.length);
  return c.json({ have, missing, resumable, limits: shareView(share).limits });
});

// --- large files: parts ---------------------------------------------------------------------------------------------
const partsList = (s: string) => (s ? s.split(',').map(Number) : []);
pub.post('/upload/init', async (c) => {
  const share = c.get('share'), cred = c.get('cred');
  if (!canUpload(share)) throw new HttpError(403, 'this bridge does not receive');
  const body = await c.req.json();
  const p = cleanPath(String(body.path)); const size = Number(body.size); const sha256 = String(body.sha256 ?? '');
  if (!Number.isFinite(size) || size < 0) throw new HttpError(400, 'size required');
  if (!/^[0-9a-f]{64}$/.test(sha256)) throw new HttpError(400, 'sha256 required');
  const existing = q.file.get(share.id, 'in', p);
  if (existing && existing.size === size && existing.sha256 === sha256) return c.json({ already: true });
  let u = q.uploadFor.get(share.id, p, size, sha256);
  if (!u) {
    checkQuota(share, size, 1);
    const id = randomBytes(12).toString('hex');
    const total = Math.max(1, Math.ceil(size / config.partSize));
    await fsp.mkdir(path.join(config.stateRoot, 'parts'), { recursive: true });
    const fh = await fsp.open(path.join(config.stateRoot, 'parts', `${id}.part`), 'w');
    await fh.truncate(size); await fh.close();
    q.insertUpload.run(id, share.id, cred.id, p, size, sha256, config.partSize, total, '', now(), now());
    u = q.upload.get(id)!;
  }
  return c.json({ upload_id: u.id, part_size: u.part_size, parts_total: u.parts_total, parts_done: partsList(u.parts_done) });
});
pub.put('/upload/:id/part/:n', async (c) => {
  const share = c.get('share'), cred = c.get('cred');
  const u = q.upload.get(c.req.param('id'));
  if (!u || u.share_id !== share.id) throw new HttpError(404, 'no such upload');
  const n = Number(c.req.param('n'));
  if (!Number.isInteger(n) || n < 0 || n >= u.parts_total) throw new HttpError(400, 'bad part number');
  const release = acquire(cred.id);
  if (!release) { c.header('Retry-After', '3'); return c.json({ error: 'busy', streams: config.maxStreamsPerClient }, 503); }
  try {
    const expect = c.req.header('x-part-sha256');
    const h = createHash('sha256');
    const offset = n * u.part_size;
    const expectedLen = Math.min(u.part_size, u.size - offset);
    const fh = await fsp.open(path.join(config.stateRoot, 'parts', `${u.id}.part`), 'r+');
    let pos = offset, len = 0;
    try {
      await pipeline(Readable.fromWeb(c.req.raw.body as any), new Writable({
        async write(chunk: Buffer, _e, cb) {
          try { h.update(chunk); await fh.write(chunk, 0, chunk.length, pos); pos += chunk.length; len += chunk.length; cb(); } catch (e) { cb(e as Error); }
        }, highWaterMark: 4 * 1024 * 1024,
      }));
    } finally { await fh.close(); }
    if (len !== expectedLen) throw new HttpError(400, `part ${n}: expected ${expectedLen} bytes, got ${len}`);
    const sum = h.digest('hex');
    if (expect && expect !== sum) throw new HttpError(409, `part ${n}: checksum mismatch`);
    const done = new Set(partsList(q.upload.get(u.id)!.parts_done)); done.add(n);
    q.setParts.run([...done].sort((a, b) => a - b).join(','), now(), u.id);
    metrics.bytesIn.inc(len);
    return c.json({ part: n, sha256: sum, parts_done: done.size, parts_total: u.parts_total });
  } finally { release(); }
});
pub.post('/upload/:id/complete', async (c) => {
  const share = c.get('share'), cred = c.get('cred');
  const u = q.upload.get(c.req.param('id'));
  if (!u || u.share_id !== share.id) throw new HttpError(404, 'no such upload');
  const done = partsList(u.parts_done);
  if (done.length !== u.parts_total) throw new HttpError(409, `missing parts: ${u.parts_total - done.length} of ${u.parts_total}`);
  const src = path.join(config.stateRoot, 'parts', `${u.id}.part`);
  const dst = resolveIn(share, 'in', u.path);
  // Final whole-file verification against the client's hash, then move into place (same dataset: a rename).
  const h = createHash('sha256');
  await pipeline(fs.createReadStream(src, { highWaterMark: 8 * 1024 * 1024 }), new Writable({ write(ch, _e, cb) { h.update(ch); cb(); } }));
  const sum = h.digest('hex');
  if (sum !== u.sha256) { await dropUpload(u.id); throw new HttpError(409, 'whole-file checksum mismatch; upload again'); }
  await fsp.mkdir(path.dirname(dst), { recursive: true });
  await fsp.rename(src, dst);
  q.upsertFile.run(share.id, 'in', u.path, u.size, sum, null, cred.id, now());
  q.deleteUpload.run(u.id);
  q.transfer.run(share.id, cred.id, 'upload', u.size, 1, now(), c.get('ip'), c.req.header('user-agent') ?? '');
  return c.json({ ok: true, path: u.path, size: u.size, sha256: sum });
});
pub.delete('/upload/:id', async (c) => {
  const u = q.upload.get(c.req.param('id'));
  if (u && u.share_id === c.get('share').id) await dropUpload(u.id);
  return c.body(null, 204);
});

// --- small files: tar batches ---------------------------------------------------------------------------------------
pub.post('/upload/batch', async (c) => {
  const share = c.get('share'), cred = c.get('cred');
  if (!canUpload(share)) throw new HttpError(403, 'this bridge does not receive');
  const declared = Number(c.req.header('x-batch-bytes') ?? 0), files = Number(c.req.header('x-batch-files') ?? 0);
  checkQuota(share, declared, files);
  const release = acquire(cred.id);
  if (!release) { c.header('Retry-After', '3'); return c.json({ error: 'busy' }, 503); }
  try {
    const zstd = (c.req.header('content-encoding') ?? '').includes('zstd') || c.req.query('zstd') === '1';
    const results = await receiveBatch(share, 'in', Readable.fromWeb(c.req.raw.body as any), zstd, cred.id);
    const bytes = results.reduce((n, r) => n + r.size, 0);
    metrics.bytesIn.inc(bytes);
    q.transfer.run(share.id, cred.id, 'upload', bytes, results.length, now(), c.get('ip'), c.req.header('user-agent') ?? '');
    return c.json({ files: results });
  } finally { release(); }
});
// One file straight in (browser fallback and tiny uploads from scripts).
pub.put('/upload/file', async (c) => {
  const share = c.get('share'), cred = c.get('cred');
  if (!canUpload(share)) throw new HttpError(403, 'this bridge does not receive');
  const p = cleanPath(c.req.query('path') ?? '');
  const declared = Number(c.req.header('content-length') ?? 0);
  checkQuota(share, declared, 1);
  const release = acquire(cred.id);
  if (!release) { c.header('Retry-After', '3'); return c.json({ error: 'busy' }, 503); }
  try {
    const r = await receiveFile(resolveIn(share, 'in', p), Readable.fromWeb(c.req.raw.body as any));
    q.upsertFile.run(share.id, 'in', p, r.size, r.sha256, null, cred.id, now());
    q.transfer.run(share.id, cred.id, 'upload', r.size, 1, now(), c.get('ip'), c.req.header('user-agent') ?? '');
    metrics.bytesIn.inc(r.size);
    return c.json({ path: p, ...r });
  } finally { release(); }
});
// The client says a push is finished: one notification, one audit line.
pub.post('/upload/done', async (c) => {
  const share = c.get('share'), cred = c.get('cred');
  const body = await c.req.json().catch(() => ({}));
  audit(`${cred.label || cred.id}@${share.id}`, 'push.done', share.id, { files: body.files, bytes: body.bytes, client: c.req.header('user-agent') });
  if (share.notify) notifyTransfer(share, cred, 'upload', Number(body.bytes ?? 0), Number(body.files ?? 0)).catch(() => {});
  return c.json({ ok: true });
});

// --- downloads --------------------------------------------------------------------------------------------------------
pub.get('/download', async (c) => {
  const share = c.get('share'), cred = c.get('cred');
  const box = boxOf(c, c.req.query('box'));
  if (box === 'in' && !c.req.query('mine')) throw new HttpError(403, 'uploads cannot be read back');
  const p = cleanPath(c.req.query('path') ?? '');
  const abs = resolveIn(share, box, p);
  const st = await fsp.stat(abs).catch(() => null);
  if (!st || !st.isFile()) throw new HttpError(404, 'no such file');
  let start = 0, end = st.size - 1;
  const range = c.req.header('range');
  const m = range && /^bytes=(\d*)-(\d*)$/.exec(range);
  if (m) {
    if (m[1] === '' && m[2] !== '') start = Math.max(0, st.size - Number(m[2]));
    else { start = Number(m[1] || 0); if (m[2] !== '') end = Math.min(Number(m[2]), st.size - 1); }
    if (start > end || start >= st.size) { c.header('Content-Range', `bytes */${st.size}`); return c.body(null, 416); }
  }
  const len = end - start + 1;
  checkDownloadBudget(share, len);
  const release = acquire(cred.id);
  if (!release) { c.header('Retry-After', '3'); return c.json({ error: 'busy' }, 503); }
  c.header('Accept-Ranges', 'bytes');
  c.header('Content-Length', String(len));
  c.header('Content-Type', 'application/octet-stream');
  c.header('X-Content-Type-Options', 'nosniff');
  c.header('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(path.basename(p))}`);
  if (m) { c.header('Content-Range', `bytes ${start}-${end}/${st.size}`); c.status(206); }
  const rs = fs.createReadStream(abs, { start, end, highWaterMark: 4 * 1024 * 1024 });
  rs.on('error', () => {});
  let sent = 0;
  rs.on('data', (ch) => { sent += ch.length; });
  const finish = () => { release(); metrics.bytesOut.inc(sent); if (sent > 0) q.transfer.run(share.id, cred.id, 'download', sent, 1, now(), c.get('ip'), c.req.header('user-agent') ?? ''); };
  rs.once('close', finish);
  return c.body(Readable.toWeb(rs) as any);
});
pub.post('/download/batch', async (c) => {
  const share = c.get('share'), cred = c.get('cred');
  const box = boxOf(c, c.req.query('box'));
  if (box === 'in') throw new HttpError(403, 'uploads cannot be read back');
  const { paths, zstd } = await c.req.json();
  if (!Array.isArray(paths) || !paths.length) throw new HttpError(400, 'paths[] required');
  const b = await sendBatch(share, box, paths.map(String), !!zstd);
  checkDownloadBudget(share, b.bytes);
  const release = acquire(cred.id);
  if (!release) { c.header('Retry-After', '3'); return c.json({ error: 'busy' }, 503); }
  c.header('Content-Type', zstd ? 'application/zstd' : 'application/x-tar');
  c.header('X-Batch-Bytes', String(b.bytes)); c.header('X-Batch-Files', String(b.files));
  b.stream.once('close', () => { release(); metrics.bytesOut.inc(b.bytes); q.transfer.run(share.id, cred.id, 'download', b.bytes, b.files, now(), c.get('ip'), c.req.header('user-agent') ?? ''); });
  return c.body(Readable.toWeb(b.stream) as any);
});
// A folder as a zip, for the browser (stored, not compressed: imaging data does not compress and this keeps it fast).
pub.get('/download/zip', async (c) => {
  const share = c.get('share'), cred = c.get('cred');
  const box = boxOf(c, c.req.query('box'));
  if (box === 'in') throw new HttpError(403, 'uploads cannot be read back');
  const p = c.req.query('path') ?? '';
  const files = await walk(share, box, p);
  const bytes = files.reduce((n, f) => n + f.size, 0);
  checkDownloadBudget(share, bytes);
  const release = acquire(cred.id);
  if (!release) { c.header('Retry-After', '3'); return c.json({ error: 'busy' }, 503); }
  const name = (p ? path.basename(cleanPath(p)) : share.id) + '.zip';
  c.header('Content-Type', 'application/zip');
  c.header('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(name)}`);
  const zip = archiver('zip', { store: true });
  zip.on('error', () => {});
  const out = new PassThrough();
  out.on('error', () => zip.abort());
  zip.pipe(out);
  (async () => {
    for (const f of files) zip.file(resolveIn(share, box, f.path), { name: f.path, date: new Date(f.mtime) });
    await zip.finalize();
  })().catch((e) => out.destroy(e));
  out.once('close', () => { release(); metrics.bytesOut.inc(bytes); q.transfer.run(share.id, cred.id, 'download', bytes, files.length, now(), c.get('ip'), c.req.header('user-agent') ?? ''); });
  return c.body(Readable.toWeb(out) as any);
});
pub.post('/download/done', async (c) => {
  const share = c.get('share'), cred = c.get('cred');
  const body = await c.req.json().catch(() => ({}));
  audit(`${cred.label || cred.id}@${share.id}`, 'pull.done', share.id, { files: body.files, bytes: body.bytes, client: c.req.header('user-agent') });
  if (share.notify) notifyTransfer(share, cred, 'download', Number(body.bytes ?? 0), Number(body.files ?? 0)).catch(() => {});
  return c.json({ ok: true });
});

// Files the collaborator already sent (names and sizes only, so they can see what arrived).
pub.get('/sent', async (c) => {
  const share = c.get('share');
  if (!(share.direction === 'in' || share.direction === 'both')) throw new HttpError(403, 'this bridge does not receive');
  return c.json({ entries: await listDir(share, 'in', c.req.query('path') ?? '') });
});
