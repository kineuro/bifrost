// A bridge's inbox can be a ZFS dataset of its own (bifrost-inbox-dataset on Asgard), so that bifrost-accept can rename
// a finished inbox into the archive instead of copying it. That only holds while nothing the server does assumes the
// inbox shares a file system with .bifrost/: the parts of a large file are staged inside the inbox, and a finished
// upload that has to cross file systems is copied into place under a temporary name. These pin both, and the rule
// that a client can never name a .bifrost path.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { after, before, mock, test } from 'node:test';

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bifrost-staging-'));
process.env.EXCHANGE_ROOT = root;
process.env.SECRET = 'test-secret';
process.env.ADMIN_KEY = 'test-admin-key';

let db: typeof import('./db.js');
let files: typeof import('./files.js');
before(async () => { db = await import('./db.js'); files = await import('./files.js'); });
after(() => { mock.restoreAll(); fs.rmSync(root, { recursive: true, force: true }); });

const t = () => new Date().toISOString();

test('parts are staged inside the bridge inbox, and listings do not show them', async () => {
  db.db.prepare("INSERT INTO shares (id, name, partner, direction, created_at, updated_at) VALUES ('b1','b1','','in',?,?)").run(t(), t());
  const part = files.partPath({ id: 'abc', share_id: 'b1' });
  assert.equal(part, path.join(root, 'in', 'b1', '.bifrost-parts', 'abc.part'));
  fs.mkdirSync(path.dirname(part), { recursive: true });
  fs.writeFileSync(part, 'partial');
  fs.writeFileSync(path.join(root, 'in', 'b1', 'scan.dcm'), 'done');
  const names = (await files.listDir(db.q.share.get('b1')!, 'in', '')).map((e) => e.name);
  assert.deepEqual(names, ['scan.dcm']);
});

test('an upload begun before the change still finishes from .bifrost/parts', async () => {
  const legacy = path.join(root, '.bifrost', 'parts', 'old.part');
  fs.mkdirSync(path.dirname(legacy), { recursive: true });
  fs.writeFileSync(legacy, 'old');
  assert.equal(await files.partFile({ id: 'old', share_id: 'b1' }), legacy);
  assert.equal(await files.partFile({ id: 'new', share_id: 'b1' }), files.partPath({ id: 'new', share_id: 'b1' }));
});

test('a client path naming .bifrost is refused', () => {
  assert.throws(() => files.cleanPath('.bifrost-parts/abc.part'), /invalid path/);
  assert.throws(() => files.cleanPath('sub/.bifrost-tmp-1-x'), /invalid path/);
  assert.equal(files.cleanPath('sub/.hidden/scan.dcm'), 'sub/.hidden/scan.dcm');
});

test('moving on one file system is a rename', async () => {
  const src = path.join(root, 'a.bin'), dst = path.join(root, 'in', 'b1', 'a.bin');
  fs.writeFileSync(src, 'same fs');
  await files.moveInto(src, dst);
  assert.equal(fs.readFileSync(dst, 'utf8'), 'same fs');
  assert.equal(fs.existsSync(src), false);
});

test('moving across file systems copies under a temporary name, then renames, then removes the source', async () => {
  const src = path.join(root, 'b.bin'), dst = path.join(root, 'in', 'b1', 'b.bin');
  fs.writeFileSync(src, 'across datasets');
  const real = fsp.rename;
  let refused = 0;
  const m = mock.method(fsp, 'rename', async (from: fs.PathLike, to: fs.PathLike) => {
    if (from === src) { refused++; throw Object.assign(new Error('cross-device link'), { code: 'EXDEV' }); }
    return real(from, to);
  });
  try {
    await files.moveInto(src, dst);
  } finally { m.mock.restore(); }
  assert.equal(refused, 1);
  assert.equal(fs.readFileSync(dst, 'utf8'), 'across datasets');
  assert.equal(fs.existsSync(src), false);
  assert.deepEqual(fs.readdirSync(path.dirname(dst)).filter((n) => n.includes('.bifrost-tmp')), []);
});
