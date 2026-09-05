# Changelog

All notable changes to Bifrost are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow [Semantic Versioning](https://semver.org/): the major number changes when the partner API or the CLI's behaviour changes incompatibly, the minor when something is added, the patch for fixes.

The server, the web page and the CLI are released together under one version. `bifrost version` prints it; `/api/health` returns it; the GitHub release carries the CLI binaries and their checksums.

## [Unreleased]

## [1.1.0] - 2026-09-05

### Fixed
- A push no longer holds the whole study in memory. It indexed the entire tree first, keeping each file's path twice over (once relative, once absolute) plus a map across every one of them, then planned all of it, then sent it: about 1.4 kB a file, so a few million files cost gigabytes. On 5 September the Synology's kernel killed a three day multiple_ms push at 6.9 GB of 7.9, on its third of four attempts, and the study came within one attempt of being abandoned. The tree is now counted once for the totals the progress line needs, then walked, planned and sent one chunk of 200,000 files at a time, with the walk running a single chunk ahead of the transfer. Only the relative path is kept and the absolute one is rebuilt when a file is opened; the map from remote path back to file is built per plan request, which is the only place an answer is ever read, instead of across the study. Measured on a tree of the same shape: 900,000 files went from 1,226 MB resident to 288 MB, and 300,000 from 455 MB to 158 MB. What a push holds no longer follows how large the study is, and a soft memory limit of 1 GiB keeps the garbage collector from drifting above it (`GOMEMLIMIT` still wins if it is set).
- The walk spawned a goroutine for every directory it discovered and let them queue for one of sixteen turns, so a wide tree carried tens of thousands of stacks it was not using. A fixed pool of sixteen takes directories off a queue instead.

### Changed
- `bifrost push --json` writes one `plan` event per chunk rather than one for the whole tree, and `total_files` and `total_bytes` in the progress events start at the size of the tree and come down as each plan reports what the bridge already holds. A first push therefore has its totals from the outset; a resume settles onto the smaller figure as it plans. The human-readable summary of what was already there and what was left to send now prints when the transfer ends rather than before it starts, because that is when a streaming push knows it.

## [1.0.5] - 2026-09-04

### Fixed
- What a bridge holds is now kept as it changes instead of counted when asked, which was the last and largest of the stalls. Summing `size` over a bridge means visiting every row it owns, because `size` is not in the primary key: across the eleven bridges of the migration that is 14.4 million rows and around 25 seconds, and better-sqlite3 is synchronous, so the whole server waits for it. `/admin/stats` took 26 s and `/admin/shares` 19 s, so opening the bridge list in the portal asked for 45 seconds of frozen server and timed out at the portal's own 15 second limit; the ten minute refresher 1.0.4 introduced for `/metrics` did the same thing on a clock, which the health probe had been recording as a 9.5 second outage every ten minutes since. A `usage` table, filled once at start-up and maintained by three triggers on `files`, answers the same question with one row read: the eleven bridges together now cost under a millisecond. `recountUsage()` rebuilds it from the files table if it is ever doubted, and the counters carry their own tests.
- `POST /api/plan` was the one call a push makes that had no retry around it, so a single 502 from a restarting or stalled server ended the whole run rather than resuming: it is what ended the multiple_ms recovery push on 1 September. `GET /api/share` and `GET /api/manifest` had the same gap. All four now retry.
- A server that is not there is no longer treated as a server that refused. A connection that will not open, and the 502 and 504 a gateway answers with while the container behind it is being replaced, now wait on their own five minute budget without spending an attempt, and hold every worker off while they wait. The old budget was five attempts of exponential backoff, about fifteen seconds, which is shorter than a deploy takes: that is why restarting the server during a migration cost 19,748 uploads. A restart now passes underneath a running transfer, which is what a resumable push was always meant to do.
- Every upload plan asked the database what the bridge had used so far, even when the bridge had no quota and no file limit to check it against, which cost the same walk of the table. A bridge with no limits no longer asks, and the question is a row read now in any case.
- The manifest header counted the rows of a box with `COUNT(*)` (9 s on the largest bridge) to report a number the counters already hold.
- 1.0.4 asked for a libuv threadpool of 32 on a four core VM whose exchange is NFS. That many metadata operations at once put every new one behind a deep queue, `/api/health` answered it with a blocking `existsSync` on the event loop's own thread, and the bridge's availability fell from 100% to about 75% with the door alarm going off. The pool is eight, health answers from a value refreshed in the background rather than touching the mount on the request path, and the sweep for day-old scraps runs once a day instead of hourly and at every start.
- The concurrent directory walk added in 1.0.4 raced whole directories against each other with `Promise.race` over a set of pending promises, which attaches a fresh pair of handlers to every one of them on every pass: on a sweep of a live migration inbox that is hundreds of thousands of passes, and the server sat at 100% of a core, climbing through gigabytes, answering nothing. The walk and the sweep recurse one directory at a time again. The speed was never in that part: it is in listing a directory's entries together and in giving the threadpool room, both of which stay.
- A deploy whose container build ran past five minutes was reported as failed while the VM went on to finish it and swap the container, which is how 1.0.4 shipped under a red workflow run. The gateway drops a session that carries nothing for five minutes and the build was piped through `tail`, so it said nothing at all until it was done. The build now runs from `deploy/on-vm.sh`, which keeps the session talking while it works, keeps the build log, and prints it on failure.

## [1.0.4] - 2026-09-04

### Fixed
- Accepting a very large inbox failed. `bifrost-accept` asks the server for a manifest of everything the bridge received; the server answered by walking the tree, stat by stat, holding every entry in memory and returning them as one JSON array. At migration scale (4.2 million files) the walk ran for hours while the socket carried nothing, so the twenty minute idle timeout added in 1.0.3 dropped it, and the array would in any case have exceeded the largest string the runtime can make. The manifest now comes from the bridge's own records, read in pages off the primary key and streamed as ndjson: a bridge of millions of files is answered in seconds, and neither side holds more than a page.

### Changed
- Directory listings stat their entries several at a time instead of one after another, and a walk lists several directories at once and hands entries over as it finds them. A stat is a threadpool job, so a serial walk on a busy server spent nearly all its time queueing: listing part of a large inbox went from 36 ms a file to well under one. `GET /admin/shares/:id/files` takes `format=ndjson` to stream a tree rather than build it in memory.
- The container asks for a libuv threadpool of 32 rather than the default of 4. Every read, write and hash the server does passes through it, so on a four thread pool dozens of transfer streams and any listing all queue behind one another.
- The hourly sweep for abandoned upload temporaries walked every inbox one directory after another. During a migration that is millions of directories, and it took long enough that the timer started the next sweep on top of the one still running: the pile of them was one of the things holding the threadpool down. The sweep now walks several directories at once, and housekeeping refuses to start while a run is still going.
- Deploys cache their dependencies (two `npm ci` runs against the registry were nine of the thirteen minutes a deploy took) and run the two installs at once; the workflow actions move to the current majors. The runner now pins the gateway's host key and refuses a certificate that does not verify, rather than trusting whatever answered on the first connection.

## [1.0.3] - 2026-09-02

### Fixed
- An upload cut off by the client (a killed `bifrost push`, a dropped connection) left its request hanging on the server and its transfer stream taken for ever; enough of them pinned the budget, every client then got `503 busy`, and transfers on all bridges stalled. The batch body now goes through a proper pipeline, an abort ends the request and frees the stream at once, and a socket silent for twenty minutes is dropped.
- `bifrost push` could hold far more streams than `--workers`: each worker that met a large file opened `workers/2` part streams of its own. The parts now share the worker budget, so `--workers` is the number of streams a push holds, and two pushes sized within the bridge's budget no longer trip it.

### Changed
- Metrics: `bifrost_errors_total` no longer counts `503 busy` (that is the budget at work, not a fault) nor counts an internal error twice; new `bifrost_busy_total` and `bifrost_aborted_total`. Aborted uploads are logged with the bridge id.

## [1.0.2] - 2026-09-01

### Added
- Dark theme (follows the system, with a light/dark toggle in the header) on the partner page and the docs.

### Fixed
- `bifrost push` crashed on files whose names contain a backslash (or a `.`/empty path segment): the server normalises such paths, so its planning response no longer matched the client's records. The CLI now applies the same normalisation — a file named `DICOM\I0` arrives as `DICOM/I0` — and an unrecognised path in the response is reported instead of crashing.

## [1.0.1] - 2026-08-30

### Changed
- Installers and `bifrost update` fetch the CLI from the latest GitHub release, with the bridge's `/dl/` as fallback; the deploy takes the release binaries instead of building its own.
- `bifrost update` no longer replaces a release build with whatever the bridge serves; a `+N.sha` server version counts as current.

## [1.0.0] - 2026-08-30

First release, in production at bifrost.kineuro.se.

### Added
- Bridges with a direction (send, receive or both), space and file limits, download allowance, closing date, allowed networks, a message to the partner and Teams notifications.
- Tokens stored as scrypt hashes, optional passcode, one-click revocation; browser sessions as signed cookies, CLI as bearer tokens; failed attempts rate-limited per address.
- Partner page: drag-and-drop uploads of files and folders with tus (resumable), per-file verification, browsing and downloading with zip on the fly.
- `bifrost` CLI for Linux, macOS and Windows (amd64 and arm64): `login`, `push`, `pull`, `ls`, `status`, `verify`, `update`; server-side plan so a rerun is a resume; large files as parallel 32 MB parts, small files as zstd-compressed tar batches; bandwidth cap, excludes, dry run, JSON events; exit codes 0, 2, 130.
- Admin API for a staff portal: bridges, tokens, files, activity and audit log, accept and close.
- Server: SQLite state on the exchange dataset, global and per-client stream budget with `503 Retry-After`, hourly housekeeping (expiry, grace period, abandoned parts and temp files), Prometheus metrics, notifications through Alertmanager, CLI distribution with installers at `/get` and `/get.ps1` and a download page at `/dl/`.
- Documentation for partners (browser and command line) served by the service itself.

### Notes for operators
- The reverse proxy in front must not impose a request read timeout (Traefik v3 defaults to 60 s); transfers run for hours.
- `/admin` must never be routed publicly.

[Unreleased]: https://github.com/kineuro/bifrost/compare/v1.0.2...HEAD
[1.0.2]: https://github.com/kineuro/bifrost/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/kineuro/bifrost/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/kineuro/bifrost/releases/tag/v1.0.0
