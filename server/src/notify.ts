// Notifications go through the platform's Alertmanager (which posts to the Teams chat), never straight to Teams.
import { config } from './config.js';
import { fmt } from './files.js';
import type { Credential, Share } from './db.js';

export async function notify(alertname: string, summary: string, description: string, labels: Record<string, string> = {}) {
  if (!config.alertmanager) return;
  const startsAt = new Date().toISOString();
  const endsAt = new Date(Date.now() + 5 * 60_000).toISOString();
  await fetch(`${config.alertmanager}/api/v2/alerts`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify([{ labels: { alertname, severity: 'info', service: 'bifrost', ...labels }, annotations: { summary, description }, startsAt, endsAt, generatorURL: config.publicUrl }]),
    signal: AbortSignal.timeout(5000),
  });
}

export function notifyTransfer(share: Share, cred: Credential, kind: 'upload' | 'download', bytes: number, files: number) {
  const who = cred.label || cred.id;
  const what = kind === 'upload' ? `sent ${files} files (${fmt(bytes)}) into` : `downloaded ${files} files (${fmt(bytes)}) from`;
  return notify('BifrostTransfer', `Bifrost: ${who} ${what} "${share.name}"`, `Bridge ${share.id}${share.partner ? `, partner ${share.partner}` : ''}. Details in the portal: ${config.publicUrl.replace('bifrost.', '')}/portal/bifrost/${share.id}/`, { bridge: share.id, kind });
}
