// Prometheus text exposition, hand-rolled: a few counters and gauges are all we need.
import { q, shareUsage } from './db.js';
import { streams } from './files.js';

class Counter { v = 0; inc(n = 1) { this.v += n; } }
export const metrics = { bytesIn: new Counter(), bytesOut: new Counter(), requests: new Counter(), errors: new Counter(), busy: new Counter(), aborted: new Counter() };

// What a bridge holds is a sum of `size` over every row it owns, and better-sqlite3 is synchronous, so asking
// costs the event loop the whole query: on the migration bridges that was around seven seconds. Prometheus
// scrapes every thirty seconds, so the monitoring was freezing the server it was watching and then reporting it
// as unreachable. The numbers are refreshed on a slow timer instead, off the request path, and a scrape reads
// whatever the last refresh left behind.
const usedBytesCache = new Map<string, number>();
export function refreshUsage() {
  for (const sh of q.shares.all()) usedBytesCache.set(sh.id, shareUsage(sh.id).used_bytes);
}

export function render(): string {
  const lines: string[] = [];
  const g = (name: string, help: string, type: string, rows: [Record<string, string>, number][]) => {
    lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`);
    for (const [labels, v] of rows) {
      const l = Object.entries(labels).map(([k, x]) => `${k}="${x.replace(/"/g, '\\"')}"`).join(',');
      lines.push(`${name}${l ? `{${l}}` : ''} ${v}`);
    }
  };
  g('bifrost_bytes_received_total', 'Bytes received from partners', 'counter', [[{}, metrics.bytesIn.v]]);
  g('bifrost_bytes_sent_total', 'Bytes sent to partners', 'counter', [[{}, metrics.bytesOut.v]]);
  g('bifrost_requests_total', 'API requests', 'counter', [[{}, metrics.requests.v]]);
  g('bifrost_errors_total', 'API errors (5xx other than busy)', 'counter', [[{}, metrics.errors.v]]);
  g('bifrost_busy_total', 'Transfers refused with 503 because the stream budget was full', 'counter', [[{}, metrics.busy.v]]);
  g('bifrost_aborted_total', 'Uploads cut off by the client before they were complete', 'counter', [[{}, metrics.aborted.v]]);
  const s = streams();
  g('bifrost_streams_active', 'Transfer streams in flight', 'gauge', [[{}, s.total]]);
  g('bifrost_streams_max', 'Transfer stream budget', 'gauge', [[{}, s.max]]);
  const shares = q.shares.all();
  g('bifrost_bridges', 'Bridges by status', 'gauge', ['open', 'frozen', 'accepted', 'closed'].map((st) => [{ status: st }, shares.filter((x) => x.status === st).length]));
  const rows: [Record<string, string>, number][] = [], quota: [Record<string, string>, number][] = [], exp: [Record<string, string>, number][] = [];
  for (const sh of shares) {
    const used = usedBytesCache.get(sh.id);
    if (used !== undefined) rows.push([{ bridge: sh.id }, used]);
    if (sh.quota_bytes) quota.push([{ bridge: sh.id }, sh.quota_bytes]);
    if (sh.expires_at) exp.push([{ bridge: sh.id }, Math.floor(new Date(sh.expires_at).getTime() / 1000)]);
  }
  g('bifrost_bridge_used_bytes', 'Bytes in a bridge inbox', 'gauge', rows);
  g('bifrost_bridge_quota_bytes', 'Inbox quota', 'gauge', quota);
  g('bifrost_bridge_expires_seconds', 'Expiry as unix time', 'gauge', exp);
  return lines.join('\n') + '\n';
}
