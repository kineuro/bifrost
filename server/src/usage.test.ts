// The counters in `usage` are the answer to "what does this bridge hold", and nothing recomputes them: if a
// trigger ever missed a write, a quota would be enforced against a number that had quietly drifted. These
// exercise every path that changes the files table and compare the counters with a count taken from scratch.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, test } from 'node:test';

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bifrost-usage-'));
process.env.EXCHANGE_ROOT = root;
process.env.SECRET = 'test-secret';
process.env.ADMIN_KEY = 'test-admin-key';

let db: typeof import('./db.js');
before(async () => { db = await import('./db.js'); });
after(() => fs.rmSync(root, { recursive: true, force: true }));

const t = () => new Date().toISOString();
const mkShare = (id: string) => db.db.prepare(
  "INSERT INTO shares (id, name, partner, direction, created_at, updated_at) VALUES (?,?,'','in',?,?)").run(id, id, t(), t());
const put = (share: string, box: string, p: string, size: number) => db.q.upsertFile.run(share, box, p, size, null, null, null, t());
const counted = (share: string, box: string) =>
  db.db.prepare('SELECT COALESCE(SUM(size),0) AS bytes, COUNT(*) AS files FROM files WHERE share_id = ? AND box = ?').get(share, box) as { bytes: number; files: number };
const kept = (share: string, box: string) => db.q.usage.get(share, box) ?? { bytes: 0, files: 0 };
const agree = (share: string, box: string, why: string) => assert.deepEqual(kept(share, box), counted(share, box), why);

test('a new file adds to the counters', () => {
  mkShare('a');
  put('a', 'in', 'one.dcm', 100);
  put('a', 'in', 'two.dcm', 250);
  assert.deepEqual(kept('a', 'in'), { bytes: 350, files: 2 });
  agree('a', 'in', 'after two inserts');
});

test('sending a file again replaces its size rather than adding to it', () => {
  put('a', 'in', 'one.dcm', 400); // the same path, a different size: the upsert takes the conflict branch
  assert.deepEqual(kept('a', 'in'), { bytes: 650, files: 2 });
  agree('a', 'in', 'after an upsert over an existing path');
});

test('a deleted file comes back out of the counters', () => {
  db.q.deleteFile.run('a', 'in', 'two.dcm');
  assert.deepEqual(kept('a', 'in'), { bytes: 400, files: 1 });
  agree('a', 'in', 'after a delete');
});

test('the two boxes are counted apart', () => {
  put('a', 'out', 'sent.dcm', 7);
  assert.deepEqual(kept('a', 'out'), { bytes: 7, files: 1 });
  assert.deepEqual(kept('a', 'in'), { bytes: 400, files: 1 });
});

test('bridges do not count each other', () => {
  mkShare('b');
  put('b', 'in', 'one.dcm', 999);
  assert.deepEqual(kept('b', 'in'), { bytes: 999, files: 1 });
  agree('a', 'in', 'the first bridge is untouched');
});

test('dropping a bridge takes its counters with it', () => {
  db.db.prepare('DELETE FROM shares WHERE id = ?').run('b');
  assert.equal((db.db.prepare("SELECT COUNT(*) AS c FROM usage WHERE share_id = 'b'").get() as { c: number }).c, 0);
  assert.equal((db.db.prepare("SELECT COUNT(*) AS c FROM files WHERE share_id = 'b'").get() as { c: number }).c, 0);
});

test('shareUsage reports what the counters hold', () => {
  const u = db.shareUsage('a');
  assert.equal(u.used_bytes, 400);
  assert.equal(u.files, 1);
});

test('a bridge that has received nothing reads as empty, not as an error', () => {
  mkShare('empty');
  assert.deepEqual(db.shareUsage('empty'), { used_bytes: 0, files: 0, downloaded_bytes: 0 });
});

test('a thousand writes in one transaction stay in step', () => {
  mkShare('bulk');
  let want = 0;
  db.db.transaction(() => {
    for (let i = 0; i < 1000; i++) { put('bulk', 'in', `f/${i}.dcm`, i); want += i; }
    for (let i = 0; i < 100; i++) { db.q.deleteFile.run('bulk', 'in', `f/${i}.dcm`); want -= i; }
    for (let i = 500; i < 600; i++) { put('bulk', 'in', `f/${i}.dcm`, 1000 + i); want += 1000; }
  })();
  assert.deepEqual(kept('bulk', 'in'), { bytes: want, files: 900 });
  agree('bulk', 'in', 'after a mixed batch');
});

test('a recount from the files table agrees with what the triggers kept', () => {
  const before = { in: kept('bulk', 'in'), out: kept('a', 'out') };
  db.db.prepare('UPDATE usage SET bytes = 0, files = 0').run(); // as if they had drifted
  db.recountUsage();
  assert.deepEqual(kept('bulk', 'in'), before.in);
  assert.deepEqual(kept('a', 'out'), before.out);
  for (const s of ['a', 'bulk'] as const) for (const b of ['in', 'out'] as const) agree(s, b, `${s}/${b} after a recount`);
});
