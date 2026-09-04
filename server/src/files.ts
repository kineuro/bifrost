// File-system side: safe paths inside a share's box, listing, hashing, tar batches, streams and the concurrency budget.
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { Readable, Transform, Writable } from 'node:stream';
import zlib from 'node:zlib';
import tar from 'tar-stream';
import { config } from './config.js';
import { audit, now, q, shareUsage, type Box, type Share } from './db.js';

export class HttpError extends Error { constructor(public status: number, message: string) { super(message); } }

export function boxRoot(share: Share, box: Box) {
  return path.join(box === 'in' ? config.inRoot : config.outRoot, share.id);
}
export async function ensureBoxes(share: Share) {
  for (const b of ['in', 'out'] as Box[]) await fsp.mkdir(boxRoot(share, b), { recursive: true, mode: 0o750 });
}

// Normalise a client path: forward slashes, no empty, no '.', no '..', no absolute, no control characters, no hidden state dir.
export function cleanPath(p: string): string {
  const parts = p.replace(/\\/g, '/').split('/').filter((s) => s !== '' && s !== '.');
  if (parts.some((s) => s === '..' || /[\x00-\x1f]/.test(s) || s.length > 255)) throw new HttpError(400, `invalid path: ${p}`);
  if (parts.length > 64) throw new HttpError(400, 'path too deep');
  return parts.join('/');
}
export function resolveIn(share: Share, box: Box, rel: string): string {
  const root = boxRoot(share, box);
  const abs = path.resolve(root, cleanPath(rel));
  if (abs !== root && !abs.startsWith(root + path.sep)) throw new HttpError(400, 'invalid path');
  return abs;
}

export interface Entry { name: string; path: string; dir: boolean; size: number; mtime: string; sha256?: string | null }

// A stat is a libuv threadpool job, and the pool is shared with every read, write and hash the server has in
// flight. Awaited one after another, a listing is a queue of round trips through it: on a busy server that cost
// per file is thousands of times the cost of the stat itself. These two numbers fan the work out instead, wide
// enough to keep the pool fed and bounded so that one listing cannot take it over.
const STAT_FANOUT = 32;
const DIR_FANOUT = 16;

async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
  let next = 0;
  const worker = async () => { for (let i = next++; i < items.length; i = next++) out[i] = await fn(items[i]); };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return out;
}

export async function listDir(share: Share, box: Box, rel: string): Promise<Entry[]> {
  const abs = resolveIn(share, box, rel);
  let ents: fs.Dirent[];
  try { ents = await fsp.readdir(abs, { withFileTypes: true }); } catch (e: any) {
    if (e.code === 'ENOENT') return [];
    throw e;
  }
  const keep = ents.filter((e) => !e.name.startsWith('.bifrost') && !e.name.includes('.bifrost-tmp'));
  const stats = await mapLimit(keep, STAT_FANOUT, (e) => fsp.stat(path.join(abs, e.name)).catch(() => null));
  const base = rel ? cleanPath(rel) : '';
  const out: Entry[] = [];
  keep.forEach((e, i) => {
    const st = stats[i];
    if (!st) return;
    const p = base ? `${base}/${e.name}` : e.name;
    const row = st.isFile() ? q.file.get(share.id, box, p) : null;
    out.push({ name: e.name, path: p, dir: st.isDirectory(), size: st.isDirectory() ? 0 : st.size, mtime: st.mtime.toISOString(), sha256: row?.sha256 ?? null });
  });
  return out.sort((a, b) => (a.dir === b.dir ? a.name.localeCompare(b.name) : a.dir ? -1 : 1));
}

// Every file under a box, handed over as it is found. Nothing is kept but the directory being listed and the
// path back to the root, so the tree can be far larger than memory. The speed is in listDir, which stats a
// directory's entries together; racing whole directories against each other as well was not worth what it cost.
export async function* walkStream(share: Share, box: Box, rel = ''): AsyncGenerator<Entry> {
  for (const e of await listDir(share, box, rel)) {
    if (e.dir) yield* walkStream(share, box, e.path);
    else yield e;
  }
}

// The same walk collected, for the callers that need the whole list at once (a batch download, a small listing).
export async function walk(share: Share, box: Box, rel = ''): Promise<Entry[]> {
  const out: Entry[] = [];
  for await (const e of walkStream(share, box, rel)) out.push(e);
  return out;
}

export async function sha256File(abs: string): Promise<string> {
  const h = createHash('sha256');
  await pipeline(fs.createReadStream(abs, { highWaterMark: 4 * 1024 * 1024 }), new Writable({ write(c, _e, cb) { h.update(c); cb(); } }));
  return h.digest('hex');
}

// Quota checks for an inbox.
export function checkQuota(share: Share, addBytes: number, addFiles: number) {
  // A bridge with neither limit set has nothing to check, and asking anyway is not free: the usage query sums
  // `size` over every row the bridge owns, and `size` is not in the primary key, so SQLite visits the table row
  // by row. On the migration bridges that is six and a half million rows and four and a half seconds, run
  // synchronously, on the event loop, for every plan a push sends. It is why the server kept going quiet.
  if (share.quota_bytes <= 0 && share.max_files <= 0) return;
  const u = shareUsage(share.id);
  if (share.quota_bytes > 0 && u.used_bytes + addBytes > share.quota_bytes) throw new HttpError(507, `over quota: ${fmt(share.quota_bytes - u.used_bytes)} left of ${fmt(share.quota_bytes)}`);
  if (share.max_files > 0 && u.files + addFiles > share.max_files) throw new HttpError(507, `file limit reached (${share.max_files})`);
}
export function checkDownloadBudget(share: Share, addBytes: number) {
  if (share.max_download_bytes <= 0) return;
  const u = shareUsage(share.id);
  if (u.downloaded_bytes + addBytes > share.max_download_bytes) throw new HttpError(429, 'download allowance for this bridge is used up');
}
export const fmt = (n: number) => {
  const u = ['B', 'kB', 'MB', 'GB', 'TB', 'PB'];
  let i = 0; let v = n;
  while (v >= 1000 && i < u.length - 1) { v /= 1000; i++; }
  return `${i ? v.toFixed(1) : v} ${u[i]}`;
};

// Write a stream into a file at a temporary name, hashing as it goes; rename into place on success.
export async function receiveFile(abs: string, body: Readable, mtime?: Date): Promise<{ size: number; sha256: string }> {
  await fsp.mkdir(path.dirname(abs), { recursive: true });
  const tmp = `${abs}.bifrost-tmp-${process.pid}-${Math.random().toString(36).slice(2)}`;
  const h = createHash('sha256');
  let size = 0;
  try {
    const out = fs.createWriteStream(tmp, { highWaterMark: 4 * 1024 * 1024 });
    const hasher = new Transform({ transform(c, _e, cb) { h.update(c); size += c.length; cb(null, c); }, highWaterMark: 4 * 1024 * 1024 });
    await pipeline(body, hasher, out);
    if (mtime) await fsp.utimes(tmp, mtime, mtime).catch(() => {});
    await fsp.rename(tmp, abs);
  } catch (e) {
    await fsp.unlink(tmp).catch(() => {});
    throw e;
  }
  return { size, sha256: h.digest('hex') };
}

// Untar a (possibly zstd-compressed) batch into a box. Returns one record per file with the server-computed hash.
export async function receiveBatch(share: Share, box: Box, body: Readable, zstd: boolean, credId: string): Promise<{ path: string; size: number; sha256: string }[]> {
  const root = boxRoot(share, box);
  const extract = tar.extract();
  const results: { path: string; size: number; sha256: string }[] = [];
  extract.on('entry', (header, stream, next) => {
    if (header.type !== 'file') { stream.resume(); stream.on('end', () => next()); return; }
    let rel: string;
    try { rel = cleanPath(header.name); } catch (e) { stream.resume(); stream.on('end', () => next(e as Error)); return; }
    receiveFile(path.join(root, rel), stream as unknown as Readable, header.mtime)
      .then(({ size, sha256 }) => {
        q.upsertFile.run(share.id, box, rel, size, sha256, header.mtime ? header.mtime.toISOString() : null, credId, now());
        results.push({ path: rel, size, sha256 });
        next();
      })
      .catch((e) => next(e));
  });
  // The body goes through pipeline() with the decompressor, never body.pipe(): pipe() forwards data but not
  // errors, so a client that vanished mid-upload left the extractor waiting forever, the request never
  // returned, and the caller's stream slot stayed taken (2026-09-02: 17 such requests pinned the budget).
  if (zstd) await pipeline(body, zlib.createZstdDecompress(), extract);
  else await pipeline(body, extract);
  return results;
}

// Pack files of a box into a tar stream (optionally zstd). `paths` may include directories; they are expanded.
export async function sendBatch(share: Share, box: Box, paths: string[], zstd: boolean): Promise<{ stream: Readable; bytes: number; files: number }> {
  const files: Entry[] = [];
  for (const p of paths) {
    const abs = resolveIn(share, box, p);
    const st = await fsp.stat(abs).catch(() => null);
    if (!st) throw new HttpError(404, `not found: ${p}`);
    if (st.isDirectory()) files.push(...(await walk(share, box, p)));
    else files.push({ name: path.basename(p), path: cleanPath(p), dir: false, size: st.size, mtime: st.mtime.toISOString() });
  }
  const bytes = files.reduce((n, f) => n + f.size, 0);
  const pack = tar.pack();
  pack.on('error', () => {});
  (async () => {
    try {
      for (const f of files) {
        const abs = resolveIn(share, box, f.path);
        const entry = pack.entry({ name: f.path, size: f.size, mtime: new Date(f.mtime), mode: 0o644 });
        await pipeline(fs.createReadStream(abs, { highWaterMark: 4 * 1024 * 1024 }), entry);
      }
      pack.finalize();
    } catch (e) { pack.destroy(e as Error); }
  })();
  const stream = zstd ? pack.pipe(zlib.createZstdCompress({ params: { [zlib.constants.ZSTD_c_compressionLevel]: 3 } })) : pack;
  (stream as any).on('error', () => pack.destroy());
  return { stream: stream as unknown as Readable, bytes, files: files.length };
}

// Concurrency budget: a global cap and a per-credential cap on simultaneous transfer streams.
const active = new Map<string, number>();
let total = 0;
export function acquire(credId: string): (() => void) | null {
  const mine = active.get(credId) ?? 0;
  if (total >= config.maxStreams || mine >= config.maxStreamsPerClient) return null;
  total++; active.set(credId, mine + 1);
  let done = false;
  return () => { if (done) return; done = true; total--; active.set(credId, (active.get(credId) ?? 1) - 1); };
}
export const streams = () => ({ total, max: config.maxStreams, perClient: config.maxStreamsPerClient });

// Remove abandoned temporary files (a batch that died mid-way) older than a day. The candidates in a directory
// are checked together; the walk itself stays sequential so a sweep of a live migration inbox cannot run the
// server out of CPU while everyone is uploading.
export async function sweepTemp(root: string, cutoff = Date.now() - 86400_000) {
  const olds: string[] = [];
  const dirs: string[] = [];
  for (const e of await fsp.readdir(root, { withFileTypes: true }).catch(() => [] as fs.Dirent[])) {
    const p = path.join(root, e.name);
    if (e.isDirectory()) dirs.push(p);
    else if (e.name.includes('.bifrost-tmp')) olds.push(p);
  }
  await mapLimit(olds, STAT_FANOUT, async (p) => {
    const st = await fsp.stat(p).catch(() => null);
    if (st && st.mtimeMs < cutoff) await fsp.unlink(p).catch(() => {});
  });
  for (const d of dirs) await sweepTemp(d, cutoff);
}

// Delete an upload's partial file and record.
export async function dropUpload(id: string) {
  await fsp.unlink(path.join(config.stateRoot, 'parts', `${id}.part`)).catch(() => {});
  q.deleteUpload.run(id);
}

export async function removeShareData(share: Share, actor: string) {
  for (const b of ['in', 'out'] as Box[]) await fsp.rm(boxRoot(share, b), { recursive: true, force: true });
  for (const u of q.uploadsOf.all(share.id)) await dropUpload(u.id);
  audit(actor, 'data.removed', share.id, {});
}
