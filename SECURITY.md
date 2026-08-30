# Security

Bifrost moves research data between institutions, so we take reports seriously.

**Report a vulnerability** to admin@kineuro.se. Please do not open a public issue for it. We answer within a week, fix confirmed issues as fast as we can, and credit you in the release notes if you wish.

**What Bifrost protects, and how**

- A *bridge* is opened by an administrator; a partner gets a token (`bfr_` + 8 hex id + 40 hex secret from `crypto.randomBytes`). Only a salted scrypt hash of the secret is stored. Tokens can carry a passcode and an allowed-network list, and are revoked in one click.
- Sessions are HMAC-signed, `HttpOnly`, `Secure`, `SameSite=Strict` cookies; the CLI sends the token as a bearer header. Failed attempts are rate-limited per address.
- Every path from a client is normalised and confined to the bridge's own inbox or outbox; the state directory is never reachable.
- Every file is hashed (SHA-256) on arrival; large files are verified part by part and as a whole before they are placed.
- The admin API (`/admin`) is never routed by the edge; it is meant for a trusted network and a shared key.
- The reference deployment runs the server as an unprivileged user in a container on a dedicated VM that mounts one exchange dataset and nothing else; data enters an archive only after a human accepts it.

**Out of scope**: the security of the operating system, reverse proxy, storage and identity services you run Bifrost behind; denial of service through legitimate large transfers (use the stream budget and quotas).
