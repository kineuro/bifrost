// Bifrost server configuration. Everything comes from the environment; compose.yaml sets the defaults for the VM.
import path from 'node:path';

const env = (k: string, d?: string): string => {
  const v = process.env[k];
  if (v === undefined || v === '') {
    if (d === undefined) throw new Error(`missing environment variable ${k}`);
    return d;
  }
  return v;
};
const num = (k: string, d: number) => Number(env(k, String(d)));

export const config = {
  port: num('PORT', 8080),
  publicUrl: env('PUBLIC_URL', 'https://bifrost.kineuro.se'),
  // The exchange dataset from Midgard. in/<share> receives, out/<share> serves, .bifrost/ is our own state.
  exchange: env('EXCHANGE_ROOT', '/exchange'),
  get inRoot() { return path.join(this.exchange, 'in'); },
  get outRoot() { return path.join(this.exchange, 'out'); },
  get stateRoot() { return path.join(this.exchange, '.bifrost'); },
  get dbPath() { return env('DB_PATH', path.join(this.stateRoot, 'bifrost.db')); },
  // Secrets: session cookies are signed with SECRET; the staff portal talks to /admin with ADMIN_KEY.
  secret: env('SECRET'),
  adminKey: env('ADMIN_KEY'),
  // Transfer tuning.
  partSize: num('PART_SIZE', 32 * 1024 * 1024),        // large-file parts
  largeFile: num('LARGE_FILE', 64 * 1024 * 1024),      // files at or above this go as parts, below as batches
  batchBytes: num('BATCH_BYTES', 256 * 1024 * 1024),   // advisory batch size for clients
  batchFiles: num('BATCH_FILES', 5000),
  maxStreams: num('MAX_STREAMS', 48),                  // global concurrent transfer streams
  maxStreamsPerClient: num('MAX_STREAMS_PER_CLIENT', 8),
  sessionHours: num('SESSION_HOURS', 12),
  // Where the Alertmanager lives, for Teams notifications (empty = off).
  alertmanager: env('ALERTMANAGER_URL', ''),
  // Grace period after expiry before an inbox is deleted (days).
  graceDays: num('GRACE_DAYS', 30),
  version: env('BIFROST_VERSION', 'dev'),
};
