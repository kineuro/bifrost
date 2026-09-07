// A tar batch is unpacked several files at a time now, from memory, with the rows recorded together at the end.
// These check that what lands is exactly what was sent (content, size, hash, mtime, directories), that the
// counters and the returned records agree, that an entry too big to hold still arrives, and that a bad batch
// leaves nothing behind but the files it had already finished.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { PassThrough, Readable } from 'node:stream';
import { after, before, test } from 'node:test';
import zlib from 'node:zlib';
import tar from 'tar-stream';

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bifrost-batch-'));
process.env.EXCHANGE_ROOT = root;
process.env.SECRET = 'test-secret';
process.env.ADMIN_KEY = 'test-admin-key';
process.env.BATCH_PARALLEL = '3';
process.env.BATCH_BUFFER = String(256 * 1024); // so that a 100 kB entry is "too big to hold" and streams

let db: typeof import('./db.js');
let files: typeof import('./files.js');
before(async () => { db = await import('./db.js'); files = await import('./files.js'); });
after(() => fs.rmSync(root, { recursive: true, force: true }));

const t = () => new Date().toISOString();
const mkShare = (id: string) => {
  db.db.prepare("INSERT INTO shares (id, name, partner, direction, created_at, updated_at) VALUES (?,?,'','in',?,?)").run(id, id, t(), t());
  return db.q.share.get(id)!;
};
const sha = (b: Buffer) => createHash('sha256').update(b).digest('hex');
const body = (n: number) => Buffer.from(Array.from({ length: n }, (_, i) => (i * 7 + n) & 0xff));

type Entry = { name: string; data: Buffer; mtime?: Date };
function pack(entries: Entry[], zstd = false): Readable {
  const p = tar.pack();
  for (const e of entries) p.entry({ name: e.name, size: e.data.length, mtime: e.mtime ?? new Date(1700000000000), mode: 0o644 }, e.data);
  p.finalize();
  if (!zstd) return p as unknown as Readable;
  const out = new PassThrough();
  p.pipe(zlib.createZstdCompress()).pipe(out);
  return out;
}
const leftovers = (dir: string): string[] => {
  const out: string[] = [];
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) out.push(...leftovers(p));
    else if (e.name.includes('.bifrost-tmp')) out.push(p);
  }
  return out;
};

test('a batch lands whole: content, size, hash, mtime, directories, rows and counters', async () => {
  const share = mkShare('a');
  await files.ensureBoxes(share);
  const when = new Date(1600000000000);
  const entries: Entry[] = [];
  for (let i = 0; i < 40; i++) entries.push({ name: `sub-${i % 4}/ses-1/file-${i}.dcm`, data: body(1000 + i * 37), mtime: when });
  entries.push({ name: 'empty.txt', data: Buffer.alloc(0), mtime: when });
  entries.push({ name: 'big/held-by-stream.bin', data: body(100 * 1024), mtime: when }); // above buffer / 4
  const res = await files.receiveBatch(share, 'in', pack(entries), false, 'cred-1');
  assert.equal(res.length, entries.length);
  const byPath = new Map(res.map((r) => [r.path, r]));
  for (const e of entries) {
    const abs = path.join(root, 'in', 'a', e.name);
    const got = fs.readFileSync(abs);
    assert.ok(got.equals(e.data), `${e.name} content`);
    assert.equal(Math.floor(fs.statSync(abs).mtimeMs / 1000), Math.floor(when.getTime() / 1000), `${e.name} mtime`);
    const r = byPath.get(e.name)!;
    assert.equal(r.size, e.data.length, `${e.name} size`);
    assert.equal(r.sha256, sha(e.data), `${e.name} hash`);
    const row = db.q.file.get('a', 'in', e.name)!;
    assert.equal(row.sha256, r.sha256, `${e.name} row`);
    assert.equal(row.credential_id, 'cred-1');
  }
  assert.deepEqual(db.q.usage.get('a', 'in'), { bytes: entries.reduce((n, e) => n + e.data.length, 0), files: entries.length });
  assert.deepEqual(leftovers(path.join(root, 'in', 'a')), []);
});

test('the same batch compressed with zstd', async () => {
  const share = mkShare('z');
  await files.ensureBoxes(share);
  const entries: Entry[] = Array.from({ length: 25 }, (_, i) => ({ name: `d/${i}.bin`, data: body(500 + i * 11) }));
  const res = await files.receiveBatch(share, 'in', pack(entries, true), true, 'cred-1');
  assert.equal(res.length, 25);
  for (const e of entries) assert.ok(fs.readFileSync(path.join(root, 'in', 'z', e.name)).equals(e.data));
});

test('a second send of a file replaces it and keeps one row', async () => {
  const share = mkShare('r');
  await files.ensureBoxes(share);
  await files.receiveBatch(share, 'in', pack([{ name: 'x/one.dcm', data: body(300) }]), false, 'cred-1');
  await files.receiveBatch(share, 'in', pack([{ name: 'x/one.dcm', data: body(900) }]), false, 'cred-2');
  assert.ok(fs.readFileSync(path.join(root, 'in', 'r', 'x/one.dcm')).equals(body(900)));
  assert.deepEqual(db.db.prepare("SELECT COUNT(*) AS n FROM files WHERE share_id = 'r'").get(), { n: 1 });
  assert.deepEqual(db.q.usage.get('r', 'in'), { bytes: 900, files: 1 });
  assert.equal(db.q.file.get('r', 'in', 'x/one.dcm')!.credential_id, 'cred-2');
});

test('a bad path fails the batch, leaves no temporary files, and records nothing from it', async () => {
  const share = mkShare('b');
  await files.ensureBoxes(share);
  const entries: Entry[] = [
    { name: 'ok/1.bin', data: body(400) },
    { name: 'ok/2.bin', data: body(400) },
    { name: '../escape.bin', data: body(10) },
    { name: 'ok/3.bin', data: body(400) },
  ];
  await assert.rejects(files.receiveBatch(share, 'in', pack(entries), false, 'cred-1'), /invalid path/);
  assert.deepEqual(leftovers(path.join(root, 'in', 'b')), []);
  assert.ok(!fs.existsSync(path.join(root, 'escape.bin')));
  assert.equal(db.q.usage.get('b', 'in'), undefined, 'no rows from a failed batch');
});

test('a body that stops mid-way fails the batch and leaves no temporary files', async () => {
  const share = mkShare('c');
  await files.ensureBoxes(share);
  const entries: Entry[] = Array.from({ length: 10 }, (_, i) => ({ name: `m/${i}.bin`, data: body(20000) }));
  const whole: Buffer[] = [];
  for await (const c of pack(entries)) whole.push(c as Buffer);
  const cut = Buffer.concat(whole).subarray(0, 45000);
  await assert.rejects(files.receiveBatch(share, 'in', Readable.from([cut]), false, 'cred-1'));
  assert.deepEqual(leftovers(path.join(root, 'in', 'c')), []);
});
