# Course website

`de web` serves the whole course as a website on your machine: the curriculum, every track
syllabus, lessons, exercise statements with starter code and hints, quizzes, interview drills,
build-your-own stages, and the guides. It is read-only. It has no accounts and no database, and it
only reads the files in this repo, so `git pull` and a refresh is all it takes to see new lessons.

It is for reading. Solving and grading exercises happens in your terminal with the `de` CLI, and
your progress stays in the local `progress.json` (see [TUTORIAL.md](TUTORIAL.md)).

Solutions, mutants and tests are never served.

## Run it

```bash
uv sync --extra web      # once: installs the few packages the site needs
uv run de web            # http://127.0.0.1:8000
uv run de web --port 9000
```

Every exercise page ends with a "Run it locally" box with the exact `de start` / `de test`
commands. Quizzes on the site are practice-only (instant feedback, nothing recorded); the scored run
is `uv run de quiz <MODULE>`.

## Host it for others

The site is a small Python app (about 100 MB of RAM). Serve it with uvicorn behind a reverse proxy
that terminates TLS.

```bash
git clone https://github.com/adavoudi/de-mastery && cd de-mastery
uv sync --extra web --no-dev
uv run python -m de_web --host 127.0.0.1 --port 8100
```

systemd unit (`/etc/systemd/system/de-web.service`):

```ini
[Service]
User=de-web
WorkingDirectory=/opt/de-mastery
ExecStart=/opt/de-mastery/.venv/bin/python -m de_web --port 8100
Restart=on-failure
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
[Install]
WantedBy=multi-user.target
```

nginx (certbot adds the certificate lines):

```nginx
server {
  server_name de.example.com;
  listen 443 ssl;
  location / {
    proxy_pass http://127.0.0.1:8100;
    proxy_set_header Host $host;
  }
}
```

To publish new content: `git pull && systemctl restart de-web`.

## Notes

- Fonts (Newsreader, Inter, JetBrains Mono, all SIL OFL) are bundled in `web/de_web/static/fonts`.
  The site loads nothing from third parties.
- Responses carry a strict Content-Security-Policy (no inline scripts or styles).
- Lessons can contain raw HTML, so only serve a checkout whose content you trust.
