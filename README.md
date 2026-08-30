# Bifrost

The data bridge of Experimental Neuroradiology Research at Karolinska Institutet: a secure way to receive data from partners and to give data to them, over one HTTPS name, with tokens that carry direction, quota, expiry and limits.

- `server/`: Node 22 + Hono + SQLite. Collaborator API (`/api`), admin API for the staff portal (`/admin`), tus endpoint for browser uploads, Prometheus metrics, CLI distribution (`/get`, `/dl`).
- `web/`: the collaborator page and the documentation (Astro, static, built into `server/public`).
- `cli/`: the `bifrost` command (Go, static binaries for Linux, macOS, Windows).
- Runs as one container on the `bifrost` VM on Asgard; data lives on Midgard's `tank/exchange`, mounted at `/exchange` (`in/<bridge>`, `out/<bridge>`, `.bifrost/` state).

Deploy: push to `main`. The workflow builds the site, the server and the CLI for every platform, then syncs to the VM through the gateway and restarts the container. Administration happens in the staff portal at `kineuro.se/portal/bifrost/`; procedures are in the administrator SOP.
