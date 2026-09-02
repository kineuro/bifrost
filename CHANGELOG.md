# Changelog

All notable changes to Bifrost are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow [Semantic Versioning](https://semver.org/): the major number changes when the partner API or the CLI's behaviour changes incompatibly, the minor when something is added, the patch for fixes.

The server, the web page and the CLI are released together under one version. `bifrost version` prints it; `/api/health` returns it; the GitHub release carries the CLI binaries and their checksums.

## [Unreleased]

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
