// Credentials: a token is `bfr_` + 8 hex (credential id) + 40 hex (secret). Only a scrypt hash of the secret is stored.
// Browser sessions are an HMAC-signed cookie carrying the credential id; the CLI sends the token as a Bearer header.
import { createHmac, randomBytes, scryptSync, timingSafeEqual } from 'node:crypto';
import { isIP } from 'node:net';
import { config } from './config.js';
import { db, now, q, type Credential, type Share } from './db.js';

export function newToken(): { id: string; token: string; hash: string; salt: string } {
  const id = randomBytes(4).toString('hex');
  const secret = randomBytes(20).toString('hex');
  const salt = randomBytes(16).toString('hex');
  return { id, token: `bfr_${id}${secret}`, hash: hashSecret(secret, salt), salt };
}
export function hashSecret(secret: string, salt: string): string {
  return scryptSync(secret, salt, 32, { N: 16384, r: 8, p: 1 }).toString('hex');
}
function eq(a: string, b: string) {
  const x = Buffer.from(a), y = Buffer.from(b);
  return x.length === y.length && timingSafeEqual(x, y);
}

// Verified tokens are remembered for ten minutes so scrypt runs once per session, not per part.
const verified = new Map<string, number>();
export function credentialFromToken(token: string): Credential | null {
  const m = /^bfr_([0-9a-f]{8})([0-9a-f]{40})$/.exec(token.trim());
  if (!m) return null;
  const cred = q.cred.get(m[1]);
  if (!cred || cred.revoked_at) return null;
  const key = `${cred.id}:${m[2]}`;
  const t = verified.get(key);
  if (t && t > Date.now()) return cred;
  if (!eq(hashSecret(m[2], cred.salt), cred.hash)) return null;
  verified.set(key, Date.now() + 10 * 60_000);
  return cred;
}
export function checkPasscode(cred: Credential, passcode: string | undefined): boolean {
  if (!cred.passcode_hash) return true;
  if (!passcode) return false;
  return eq(hashSecret(passcode, cred.passcode_salt!), cred.passcode_hash);
}

// Session cookie: base64(json).signature
export function issueSession(credId: string): string {
  const body = Buffer.from(JSON.stringify({ c: credId, e: Date.now() + config.sessionHours * 3600_000 })).toString('base64url');
  return `${body}.${sign(body)}`;
}
export function readSession(cookie: string | undefined): string | null {
  if (!cookie) return null;
  const [body, sig] = cookie.split('.');
  if (!body || !sig || !eq(sign(body), sig)) return null;
  try {
    const { c, e } = JSON.parse(Buffer.from(body, 'base64url').toString());
    return typeof c === 'string' && e > Date.now() ? c : null;
  } catch { return null; }
}
const sign = (s: string) => createHmac('sha256', config.secret).update(s).digest('base64url');

// Share state checks shared by every authenticated request.
export type Denied = { status: number; error: string };
export function shareOpen(share: Share, ip: string): Denied | null {
  if (share.status === 'closed') return { status: 403, error: 'This bridge is closed.' };
  if (share.status === 'accepted') return { status: 403, error: 'This bridge has been closed: its data was accepted.' };
  if (share.expires_at && new Date(share.expires_at).getTime() < Date.now()) return { status: 403, error: `This bridge expired on ${share.expires_at.slice(0, 10)}.` };
  const cidrs: string[] = JSON.parse(share.allowed_cidrs || '[]');
  if (cidrs.length && !cidrs.some((c) => ipInCidr(ip, c))) return { status: 403, error: 'This bridge is not open from your network.' };
  return null;
}
export function canUpload(share: Share) { return share.status === 'open' && (share.direction === 'in' || share.direction === 'both'); }
export function canDownload(share: Share) { return share.direction === 'out' || share.direction === 'both'; }

export function ipInCidr(ip: string, cidr: string): boolean {
  const [net, bitsStr] = cidr.split('/');
  const bits = bitsStr === undefined ? (isIP(net) === 6 ? 128 : 32) : Number(bitsStr);
  const a = ipToBytes(ip), b = ipToBytes(net);
  if (!a || !b || a.length !== b.length) return false;
  for (let i = 0; i < bits; i++) {
    const byte = i >> 3, bit = 7 - (i & 7);
    if (((a[byte] >> bit) & 1) !== ((b[byte] >> bit) & 1)) return false;
  }
  return true;
}
function ipToBytes(ip: string): number[] | null {
  if (ip.startsWith('::ffff:')) ip = ip.slice(7);
  if (isIP(ip) === 4) return ip.split('.').map(Number);
  if (isIP(ip) === 6) {
    const [head, tail = ''] = ip.split('::');
    const h = head ? head.split(':') : [], t = tail ? tail.split(':') : [];
    const groups = [...h, ...Array(8 - h.length - t.length).fill('0'), ...t].map((g) => parseInt(g || '0', 16));
    return groups.flatMap((g) => [g >> 8, g & 255]);
  }
  return null;
}

// Fixed-window limit on FAILED token attempts: after 20 failures in 10 minutes an address must wait. Successes never count.
const failures = new Map<string, { n: number; t: number }>();
export function rateLimited(ip: string): boolean {
  const a = failures.get(ip);
  return !!a && a.t > Date.now() && a.n >= 20;
}
export function recordFailure(ip: string) {
  const a = failures.get(ip);
  const t = Date.now();
  if (!a || a.t < t) failures.set(ip, { n: 1, t: t + 600_000 }); else a.n++;
}

export function touch(cred: Credential, ip: string) {
  q.touchCred.run(now(), ip, cred.id);
}
export { db };
