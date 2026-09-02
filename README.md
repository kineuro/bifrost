<p align="center"><img src="web/public/favicon.svg" width="72" alt="Bifrost"></p>

# Bifrost

A data bridge between a research group and its partners: one link per collaboration, with a direction (send, receive or both), a space limit, a closing date and checksums on every file. Partners use it in the browser (resumable drag-and-drop uploads with tus) or with a small command line tool that moves terabytes and hundreds of thousands of files with parallel streams and resumes after any interruption. Administrators run it from their own portal through a small admin API. No accounts for partners, no SFTP, nothing to install for the browser path.

Built and run by [Experimental Neuroradiology Research at Karolinska Institutet](https://kineuro.se) (kineuro), where it carries imaging studies between hospital sites and the research platform. Live documentation, as partners see it: [how to use it](https://bifrost.kineuro.se/docs/) and [the command line](https://bifrost.kineuro.se/docs/cli/).

## What is in the repository

| Directory | What | Stack |
|---|---|---|
| `server/` | The service: partner API (`/api`), tus endpoint for the browser, admin API (`/admin`), Prometheus metrics, CLI distribution (`/get`, `/dl`) | Node 22, Hono, SQLite (better-sqlite3), @tus/server, tar-stream, zstd from node:zlib |
| `web/` | The partner page and the documentation, built as static files into `server/public` | Astro |
| `cli/` | `bifrost`: `login`, `push`, `pull`, `ls`, `status`, `verify`, `update` | Go, no dependencies beyond klauspost/compress |
| `compose.yaml`, `Dockerfile` | One container; data on a bind-mounted exchange directory | Docker |
| `.github/workflows/deploy.yml` | Build site, server and CLI binaries for six platforms, sync to the host, restart | GitHub Actions |

## How a transfer works

- **Plan first.** The client indexes the folder and asks the server which files it already has (by path and size, or hash with `--checksum`). Only the rest is sent, so a rerun is a resume.
- **Two lanes.** Files above 64 MB go as 32 MB parts, several in flight, each part hashed and checked on arrival, then the whole file is hashed again before it is placed. Smaller files are packed into tar batches of about 256 MB or 5,000 files, zstd-compressed on the fly, and unpacked and hashed on the server, so a DICOM study is a few hundred requests instead of a few hundred thousand.
- **Budget.** The server caps concurrent streams globally and per client and answers `503 Retry-After` when busy; the CLI waits and continues. A push never holds more streams than its `--workers`, parts of large files included; a stream whose client vanishes is released at once, and a socket silent for twenty minutes is dropped.
- **State on the server.** Received files and in-progress parts are recorded in SQLite, so a resume needs no local state and works from another machine.
- **Downloads** mirror it: HTTP Range for large files, tar batches for small ones, zip on the fly for the browser.

## Running it yourself

You need a host with Docker, a directory (ideally its own dataset with snapshots) for the exchange, and a reverse proxy that terminates TLS for one name and forwards to port 8080. Traefik users: set no request read/write timeout on that entry point; transfers run for hours.

```
cp .env.example .env         # SECRET, ADMIN_KEY (random strings), optional ALERTMANAGER_URL
docker compose up -d --build
curl http://127.0.0.1:8080/api/health
```

Create a bridge and its first token through the admin API (`X-Bifrost-Admin: <ADMIN_KEY>`); the reference deployment does this from a staff portal, and `server/src/api/admin.ts` documents every call. Keep `/admin` off the public proxy. Point `ALERTMANAGER_URL` at an Alertmanager to get a notice per completed transfer.

The CLI binaries are built by the release workflow and attached to each GitHub release; the installers (`/get`, `/get.ps1`) and `bifrost update` fetch from the latest release, with the server's `/dl/` mirror as fallback. The deploy takes the same release binaries into `bin/`. To build locally: `cd cli && go build -o bifrost .`

## Versions and releases

One version for server, page and CLI, as a git tag `vX.Y.Z` (semantic versioning). Each tag builds a GitHub release with the CLI binaries, their checksums and the release notes taken from [CHANGELOG.md](CHANGELOG.md); the deployed service reports the same version at `/api/health` and `bifrost version`, with `+N.sha` appended when the host runs commits past the last tag.

## Security

See [SECURITY.md](SECURITY.md) for the threat model and how to report a vulnerability.

## Name and mark

Bifrost is the bridge between worlds in Norse mythology; our machines are Asgard (compute) and Midgard (storage). The mark is a plum square with the bridge drawn in one line.

## License

MIT. Contributions are welcome; open an issue first for anything larger than a fix.
