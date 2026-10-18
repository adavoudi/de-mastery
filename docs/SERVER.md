# Progress server

A small hub that stores each learner's progress and shows a dashboard. It never runs anything on a
learner's machine: the `de` CLI connects out, signs in with a device code, and uploads results.

```
learner machine                          server
 de test  -> progress.json (local)
 de login ----- POST /api/device/code ->  code ABCD-EFGH
 browser  ----- GitHub login, approve ->  code bound to your account
 de sync  ----- POST /api/sync (token) ->  merge, store, return merged state
```

What is uploaded: exercise ids, status, attempts, minutes, timestamps, quiz best scores.
Never uploaded: your code, file paths, hints or solutions viewed.

## Learner usage

```bash
export DE_SERVER_URL=https://de.example.com   # once, or pass --server
uv run de login      # prints a code and a URL; approve with GitHub
uv run de sync       # push + pull; safe to run any time, from any machine
uv run de whoami / uv run de logout
```

The merge never loses progress: `passed` wins, counters only grow, the earliest timestamps win, the
best quiz score wins. Revoke a machine from the dashboard under "Connected CLIs".

## Hosting

1. **GitHub OAuth App** (Settings → Developer settings → OAuth Apps → New): homepage
   `https://de.example.com`, callback `https://de.example.com/auth/github/callback`.
2. **Install** (the `server` extra is only needed on the server):
   ```bash
   git clone <course repo> /opt/de-mastery && cd /opt/de-mastery
   uv sync --extra server --no-dev
   ```
3. **Environment** (`/etc/de-server.env`, mode 600):
   ```
   DE_BASE_URL=https://de.example.com
   DE_SESSION_SECRET=<python -c "import secrets; print(secrets.token_urlsafe(48))">
   GITHUB_CLIENT_ID=...
   GITHUB_CLIENT_SECRET=...
   DE_DB_PATH=/var/lib/de-server/de.sqlite3
   DE_REPO_ROOT=/opt/de-mastery        # optional: enables per-track totals on the dashboard
   ```
4. **systemd** (`/etc/systemd/system/de-server.service`):
   ```ini
   [Service]
   User=de-server
   WorkingDirectory=/opt/de-mastery
   EnvironmentFile=/etc/de-server.env
   ExecStart=/opt/de-mastery/.venv/bin/python -m de_server
   StateDirectory=de-server
   Restart=on-failure
   NoNewPrivileges=true
   ProtectSystem=strict
   ProtectHome=true
   PrivateTmp=true
   [Install]
   WantedBy=multi-user.target
   ```
5. **nginx** (TLS via certbot; the app listens on 127.0.0.1:8100 only):
   ```nginx
   server {
     server_name de.example.com;
     listen 443 ssl;   # certbot fills in the certificate lines
     client_max_body_size 2m;
     location / {
       proxy_pass http://127.0.0.1:8100;
       proxy_set_header Host $host;
       proxy_set_header X-Forwarded-For $remote_addr;
       proxy_set_header X-Forwarded-Proto $scheme;
     }
   }
   ```
6. Back up `DE_DB_PATH` (one SQLite file).

Resource use is small (about 100 MB RAM), so it fits on a small VPS next to other services. Run a
single worker: rate limits are in-memory.

## Security notes

- GitHub scope is `read:user` only (public profile). No email, no repo access.
- Device codes expire after 10 minutes, are single-use, and wrong-code guesses are rate-limited.
- Access tokens are random, stored only as SHA-256 hashes, revocable, and the CLI stores its token
  in a mode-600 file. The CLI refuses plain `http://` except to localhost.
- Cookies are `HttpOnly`, `SameSite=Lax`, `Secure` over HTTPS; state-changing forms carry a CSRF token.
- Strict CSP (no inline scripts or styles), payloads are validated and size-capped.
- A compromised server can only read or alter progress data; it has no channel to run code on learners.
