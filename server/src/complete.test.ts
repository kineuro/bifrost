// Completing a large file reads all of it once more, and for hundreds of GB that outlasts the client's wait for an
// answer, so the client asks again while the first check runs (2026-11-02, two 362 GB files: four extra full reads
// each, the extra asks failing on the rename with ENOENT, and the CLI reporting as failed files that had landed).
// These pin that asks for one upload share one check and one answer, before and after the file is in place.
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, mock, test } from 'node:test';
import { Hono } from 'hono';

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bifrost-complete-'));
process.env.EXCHANGE_ROOT = root;
process.env.SECRET = 'test-secret';
process.env.ADMIN_KEY = 'test-admin-key';

let db: typeof import('./db.js');
let files: typeof import('./files.js');
let app: Hono;
let token = '', other = '';
before(async () => {
  db = await import('./db.js'); files = await import('./files.js');
  const { pub } = await import('./api/public.js');
  const { newToken } = await import('./auth.js');
  app = new Hono();
  app.onError((err, c) => err instanceof files.HttpError ? c.json({ error: err.message }, err.status as any) : c.json({ error: String(err) }, 500));
  app.route('/api', pub);
  for (const id of ['b1', 'b2']) {
    db.db.prepare("INSERT INTO shares (id, name, partner, direction, created_at, updated_at) VALUES (?,?,'','in',?,?)").run(id, id, t(), t());
    const k = newToken();
    db.db.prepare('INSERT INTO credentials (id, share_id, label, hash, salt, created_at) VALUES (?,?,?,?,?,?)').run(k.id, id, '', k.hash, k.salt, t());
    if (id === 'b1') token = k.token; else other = k.token;
  }
});
after(() => { mock.restoreAll(); fs.rmSync(root, { recursive: true, force: true }); });

const t = () => new Date().toISOString();

// An upload whose one part has arrived, as the CLI leaves it before it asks to complete.
function staged(id: string, rel: string, bytes: Buffer) {
  const sha = createHash('sha256').update(bytes).digest('hex');
  const part = files.partPath({ id, share_id: 'b1' });
  fs.mkdirSync(path.dirname(part), { recursive: true });
  fs.writeFileSync(part, bytes);
  const cred = db.q.credsOf.all('b1')[0];
  db.q.insertUpload.run(id, 'b1', cred.id, rel, bytes.length, sha, bytes.length, 1, '0', t(), t());
  return sha;
}
const complete = (id: string, tok = token) =>
  app.request(`/api/upload/${id}/complete`, { method: 'POST', headers: { authorization: `Bearer ${tok}` } });

test('asks that come while the check runs join it, and one after it gets the same answer', async () => {
  const bytes = randomBytes(16 * 1024 * 1024);
  const sha = staged('u1', 'big/scan.npy', bytes);
  const reads = mock.method(fs, 'createReadStream');
  const asks = await Promise.all([complete('u1'), complete('u1'), complete('u1'), complete('u1'), complete('u1')]);
  const late = await complete('u1');
  reads.mock.restore();
  for (const r of [...asks, late]) {
    assert.equal(r.status, 200);
    assert.deepEqual(await r.json(), { ok: true, path: 'big/scan.npy', size: bytes.length, sha256: sha });
  }
  assert.equal(reads.mock.callCount(), 1, 'the file is read once, however often the client asks');
  assert.deepEqual(fs.readFileSync(path.join(root, 'in', 'b1', 'big', 'scan.npy')), bytes);
  assert.equal(db.q.upload.get('u1'), undefined);
  assert.equal(db.db.prepare("SELECT COUNT(*) AS n FROM transfers WHERE share_id = 'b1' AND kind = 'upload'").get().n, 1);
});

test('another bridge cannot read the answer for an upload that is not its own', async () => {
  staged('u2', 'other.npy', randomBytes(1024));
  assert.equal((await complete('u2', other)).status, 404);
  assert.equal((await complete('u2')).status, 200);
  assert.equal((await complete('u2', other)).status, 404);
});

test('a failed check is not kept: the next ask is answered from the database', async () => {
  const bytes = randomBytes(1024);
  staged('u3', 'bad.npy', bytes);
  db.db.prepare("UPDATE uploads SET sha256 = ? WHERE id = 'u3'").run('0'.repeat(64));
  const r = await complete('u3');
  assert.equal(r.status, 409);
  assert.equal((await complete('u3')).status, 404);
});
