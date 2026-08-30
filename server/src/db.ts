// SQLite state: shares, credentials, files, uploads in flight, transfers and the audit log.
import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import { config } from './config.js';

export type Direction = 'in' | 'out' | 'both';
export type Box = 'in' | 'out';
export type ShareStatus = 'open' | 'frozen' | 'accepted' | 'closed';

export interface Share {
  id: string; name: string; partner: string; direction: Direction; message: string;
  quota_bytes: number; max_files: number; max_download_bytes: number;
  expires_at: string | null; allowed_cidrs: string; status: ShareStatus;
  created_by: string; created_at: string; updated_at: string; note: string;
  notify: number;
}
export interface Credential {
  id: string; share_id: string; label: string; hash: string; salt: string;
  passcode_hash: string | null; passcode_salt: string | null;
  created_at: string; revoked_at: string | null; last_used_at: string | null; last_ip: string | null;
}
export interface FileRow {
  share_id: string; box: Box; path: string; size: number; sha256: string | null;
  mtime: string | null; credential_id: string | null; completed_at: string;
}
export interface Upload {
  id: string; share_id: string; credential_id: string; path: string; size: number;
  sha256: string; part_size: number; parts_total: number; parts_done: string;
  created_at: string; updated_at: string;
}

fs.mkdirSync(path.dirname(config.dbPath), { recursive: true });
export const db = new Database(config.dbPath);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('foreign_keys = ON');

db.exec(`
CREATE TABLE IF NOT EXISTS shares (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, partner TEXT NOT NULL DEFAULT '', direction TEXT NOT NULL,
  message TEXT NOT NULL DEFAULT '', quota_bytes INTEGER NOT NULL DEFAULT 0, max_files INTEGER NOT NULL DEFAULT 0,
  max_download_bytes INTEGER NOT NULL DEFAULT 0, expires_at TEXT, allowed_cidrs TEXT NOT NULL DEFAULT '[]',
  status TEXT NOT NULL DEFAULT 'open', created_by TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL, note TEXT NOT NULL DEFAULT '', notify INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS credentials (
  id TEXT PRIMARY KEY, share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
  label TEXT NOT NULL DEFAULT '', hash TEXT NOT NULL, salt TEXT NOT NULL,
  passcode_hash TEXT, passcode_salt TEXT, created_at TEXT NOT NULL, revoked_at TEXT,
  last_used_at TEXT, last_ip TEXT
);
CREATE TABLE IF NOT EXISTS files (
  share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE, box TEXT NOT NULL, path TEXT NOT NULL,
  size INTEGER NOT NULL, sha256 TEXT, mtime TEXT, credential_id TEXT, completed_at TEXT NOT NULL,
  PRIMARY KEY (share_id, box, path)
);
CREATE TABLE IF NOT EXISTS uploads (
  id TEXT PRIMARY KEY, share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
  credential_id TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL, sha256 TEXT NOT NULL,
  part_size INTEGER NOT NULL, parts_total INTEGER NOT NULL, parts_done TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS transfers (
  id INTEGER PRIMARY KEY AUTOINCREMENT, share_id TEXT NOT NULL, credential_id TEXT, kind TEXT NOT NULL,
  bytes INTEGER NOT NULL DEFAULT 0, files INTEGER NOT NULL DEFAULT 0, at TEXT NOT NULL, ip TEXT, client TEXT
);
CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
  share_id TEXT, detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS transfers_share ON transfers(share_id, at);
CREATE INDEX IF NOT EXISTS audit_share ON audit(share_id, at);
`);

export const now = () => new Date().toISOString();

export function audit(actor: string, action: string, share_id: string | null, detail: unknown = '') {
  db.prepare('INSERT INTO audit (at, actor, action, share_id, detail) VALUES (?,?,?,?,?)')
    .run(now(), actor, action, share_id, typeof detail === 'string' ? detail : JSON.stringify(detail));
}

export const q = {
  share: db.prepare<[string], Share>('SELECT * FROM shares WHERE id = ?'),
  shares: db.prepare<[], Share>('SELECT * FROM shares ORDER BY created_at DESC'),
  cred: db.prepare<[string], Credential>('SELECT * FROM credentials WHERE id = ?'),
  credsOf: db.prepare<[string], Credential>('SELECT * FROM credentials WHERE share_id = ? ORDER BY created_at'),
  filesOf: db.prepare<[string, string], FileRow>('SELECT * FROM files WHERE share_id = ? AND box = ? ORDER BY path'),
  file: db.prepare<[string, string, string], FileRow>('SELECT * FROM files WHERE share_id = ? AND box = ? AND path = ?'),
  upsertFile: db.prepare(`INSERT INTO files (share_id, box, path, size, sha256, mtime, credential_id, completed_at) VALUES (?,?,?,?,?,?,?,?)
    ON CONFLICT(share_id, box, path) DO UPDATE SET size=excluded.size, sha256=excluded.sha256, mtime=excluded.mtime, credential_id=excluded.credential_id, completed_at=excluded.completed_at`),
  deleteFile: db.prepare('DELETE FROM files WHERE share_id = ? AND box = ? AND path = ?'),
  usedBytes: db.prepare<[string], { n: number; c: number }>("SELECT COALESCE(SUM(size),0) AS n, COUNT(*) AS c FROM files WHERE share_id = ? AND box = 'in'"),
  downloaded: db.prepare<[string], { n: number }>("SELECT COALESCE(SUM(bytes),0) AS n FROM transfers WHERE share_id = ? AND kind = 'download'"),
  upload: db.prepare<[string], Upload>('SELECT * FROM uploads WHERE id = ?'),
  uploadFor: db.prepare<[string, string, number, string], Upload>('SELECT * FROM uploads WHERE share_id = ? AND path = ? AND size = ? AND sha256 = ?'),
  uploadsOf: db.prepare<[string], Upload>('SELECT * FROM uploads WHERE share_id = ? ORDER BY created_at'),
  insertUpload: db.prepare('INSERT INTO uploads (id, share_id, credential_id, path, size, sha256, part_size, parts_total, parts_done, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)'),
  setParts: db.prepare('UPDATE uploads SET parts_done = ?, updated_at = ? WHERE id = ?'),
  deleteUpload: db.prepare('DELETE FROM uploads WHERE id = ?'),
  staleUploads: db.prepare<[string], Upload>('SELECT * FROM uploads WHERE updated_at < ?'),
  transfer: db.prepare('INSERT INTO transfers (share_id, credential_id, kind, bytes, files, at, ip, client) VALUES (?,?,?,?,?,?,?,?)'),
  transfersOf: db.prepare<[string, number], { id: number; credential_id: string; kind: string; bytes: number; files: number; at: string; ip: string; client: string }>('SELECT * FROM transfers WHERE share_id = ? ORDER BY at DESC LIMIT ?'),
  auditOf: db.prepare<[string, number], { id: number; at: string; actor: string; action: string; detail: string }>('SELECT * FROM audit WHERE share_id = ? ORDER BY at DESC LIMIT ?'),
  auditAll: db.prepare<[number], { id: number; at: string; actor: string; action: string; share_id: string; detail: string }>('SELECT * FROM audit ORDER BY at DESC LIMIT ?'),
  touchCred: db.prepare('UPDATE credentials SET last_used_at = ?, last_ip = ? WHERE id = ?'),
};

export function shareUsage(id: string) {
  const u = q.usedBytes.get(id)!;
  const d = q.downloaded.get(id)!;
  return { used_bytes: u.n, files: u.c, downloaded_bytes: d.n };
}
