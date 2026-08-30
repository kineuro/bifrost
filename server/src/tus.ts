// Browser uploads use tus (resumable), backed by a file store on the exchange dataset.
// When an upload finishes the file is moved into the share's inbox, hashed and recorded like any other.
import { Server, type Upload } from '@tus/server';
import { FileStore } from '@tus/file-store';
import type { IncomingMessage, ServerResponse } from 'node:http';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { canUpload, readSession, shareOpen } from './auth.js';
import { config } from './config.js';
import { audit, now, q } from './db.js';
import { checkQuota, cleanPath, resolveIn, sha256File } from './files.js';
import { metrics } from './metrics.js';

const dir = path.join(config.stateRoot, 'tus');
fs.mkdirSync(dir, { recursive: true });

function credOf(req: IncomingMessage) {
  const cookie = (req.headers.cookie ?? '').split(';').map((s) => s.trim()).find((s) => s.startsWith('bfr='));
  const id = readSession(cookie?.slice(4));
  const cred = id ? q.cred.get(id) : null;
  if (!cred || cred.revoked_at) return null;
  const share = q.share.get(cred.share_id);
  if (!share) return null;
  return { cred, share };
}

export const tus = new Server({
  path: '/api/tus',
  datastore: new FileStore({ directory: dir }),
  respectForwardedHeaders: true,
  maxSize: 16 * 1024 ** 4,
  async onIncomingRequest(req) {
    if (req.method === 'OPTIONS') return;
    const who = credOf(req as IncomingMessage);
    if (!who) throw { status_code: 401, body: 'not signed in' };
    const ip = String(req.headers['x-forwarded-for'] ?? '').split(',')[0].trim();
    const denied = shareOpen(who.share, ip);
    if (denied) throw { status_code: denied.status, body: denied.error };
    if (!canUpload(who.share)) throw { status_code: 403, body: 'this bridge does not receive' };
  },
  async onUploadCreate(req, res, upload) {
    const who = credOf(req as IncomingMessage)!;
    const rel = cleanPath(String(upload.metadata?.relativePath && upload.metadata.relativePath !== 'null' ? upload.metadata.relativePath : upload.metadata?.filename ?? 'file'));
    try { checkQuota(who.share, upload.size ?? 0, 1); } catch (e: any) { throw { status_code: e.status ?? 507, body: e.message }; }
    return { res, metadata: { ...upload.metadata, bifrostShare: who.share.id, bifrostCred: who.cred.id, bifrostPath: rel } };
  },
  async onUploadFinish(_req, res, upload: Upload) {
    const shareId = String(upload.metadata?.bifrostShare), credId = String(upload.metadata?.bifrostCred), rel = String(upload.metadata?.bifrostPath);
    const share = q.share.get(shareId);
    if (!share) return { res };
    const src = path.join(dir, upload.id);
    const dst = resolveIn(share, 'in', rel);
    await fsp.mkdir(path.dirname(dst), { recursive: true });
    await fsp.rename(src, dst);
    await fsp.unlink(`${src}.json`).catch(() => {});
    const sha = await sha256File(dst);
    const size = upload.size ?? (await fsp.stat(dst)).size;
    q.upsertFile.run(share.id, 'in', rel, size, sha, null, credId, now());
    q.transfer.run(share.id, credId, 'upload', size, 1, now(), null, 'browser');
    metrics.bytesIn.inc(size);
    res.setHeader('X-Bifrost-Sha256', sha);
    return { res };
  },
});

export function handleTus(req: IncomingMessage, res: ServerResponse) {
  return tus.handle(req, res);
}
export { audit };
