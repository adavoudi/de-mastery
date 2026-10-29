"""Static export of the course site, for hosting on GitHub Pages (or any static file host).

    python -m de_web.export --out dist --base /de-mastery --site-url https://adavoudi.info

Every page is fetched from the app and written as ``<path>/index.html``. Root-relative links are
prefixed with ``--base`` so the site works under a sub-path such as ``https://example.com/de-mastery``.
Static hosts serve ``<path>/index.html`` at ``<path>/`` and redirect ``<path>`` there, so page links
get a trailing slash. With ``--site-url``, each page also gets a canonical URL and share-card tags,
and the export writes a ``sitemap.xml``.
"""

from __future__ import annotations

import argparse
import re
import shutil
from datetime import date
from pathlib import Path
from xml.sax.saxutils import escape

from fastapi.testclient import TestClient

from de_web.app import HERE, create_app, default_repo_root
from de_web.library import Library

_ATTR = re.compile(r'(\b(?:href|src|action)=")/(?!/)')
_CSS_URL = re.compile(r'url\((["\']?)/(?!/)')
# a root-relative link whose last segment has no dot (a page, not a file) and no trailing slash
_PAGE_HREF = re.compile(r'(\bhref=")(/(?:[^"#?]*/)?[^"#?./]+)(?=[#?"])')


def page_paths(lib: Library) -> list[str]:
    paths = ["/", "/roadmap"] + [f"/track/{t}" for t in lib.tracks]
    for m in lib.modules.values():
        paths.append(f"/m/{m.id}")
        if m.exercises:
            paths.append(f"/m/{m.id}/exercises")
            paths += [f"/m/{m.id}/x/{e.slug}" for e in m.exercises]
        if m.has("quiz.yaml"):
            paths.append(f"/m/{m.id}/quiz")
        paths += [f"/m/{m.id}/{p}" for p in ("drills", "explain") if m.has(f"{p}.md")]
    for c in lib.challenges.values():
        paths.append(f"/byo/{c.id}")
        paths += [f"/byo/{c.id}/{s.number:02d}" for s in c.stages]
    paths += [f"/docs/{d}" for d in lib.docs]
    return paths


def prefix_html(html: str, base: str) -> str:
    html = _PAGE_HREF.sub(lambda m: f"{m.group(1)}{m.group(2)}/", html)
    return _ATTR.sub(lambda m: f"{m.group(1)}{base}/", html) if base else html


def page_url(site_url: str, base: str, path: str) -> str:
    return f"{site_url}{base}{path.rstrip('/')}/"


def add_share_tags(html: str, url: str, image: str) -> str:
    tags = (f'<link rel="canonical" href="{url}">\n<meta property="og:url" content="{url}">\n'
            f'<meta property="og:image" content="{image}">\n')
    return html.replace("</head>", tags + "</head>", 1)


def sitemap(urls: list[str]) -> str:
    today = date.today().isoformat()
    items = "".join(f"<url><loc>{escape(u)}</loc><lastmod>{today}</lastmod></url>" for u in urls)
    return ('<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{items}</urlset>\n')


def export(out: Path, base: str = "", repo_root: Path | None = None, site_url: str = "") -> int:
    base = base.rstrip("/")
    site_url = site_url.rstrip("/")
    image = f"{site_url}{base}/static/brand/og.png"
    root = repo_root or default_repo_root()
    app = create_app(root)
    lib = Library(root)
    client = TestClient(app)

    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    shutil.copytree(HERE / "static", out / "static")
    shutil.copy(HERE / "static" / "brand" / "favicon.ico", out / "favicon.ico")
    (out / ".nojekyll").write_text("")

    if base:
        for f in list((out / "static").rglob("*.css")) + [out / "static" / "brand" / "site.webmanifest"]:
            text = f.read_text()
            text = _CSS_URL.sub(lambda m: f"url({m.group(1)}{base}/", text)
            text = re.sub(r'("(?:src|start_url)": ")/', lambda m: f"{m.group(1)}{base}/", text)
            f.write_text(text)

    urls = []
    for path in page_paths(lib):
        r = client.get(path)
        if r.status_code != 200:
            raise SystemExit(f"{path}: HTTP {r.status_code}")
        html = prefix_html(r.text, base)
        if site_url:
            urls.append(page_url(site_url, base, path))
            html = add_share_tags(html, urls[-1], image)
        dest = out / path.strip("/") / "index.html" if path != "/" else out / "index.html"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(html)
    (out / "404.html").write_text(prefix_html(client.get("/m/no-such-page").text, base))
    if site_url:
        (out / "sitemap.xml").write_text(sitemap(urls))
    return len(page_paths(lib))


def main() -> None:
    ap = argparse.ArgumentParser(description="Export the course site as static files.")
    ap.add_argument("--out", type=Path, default=Path("dist"))
    ap.add_argument("--base", default="", help="URL sub-path the site is served under, e.g. /de-mastery")
    ap.add_argument("--site-url", default="", help="scheme and host, e.g. https://adavoudi.info; "
                    "adds canonical URLs, share-card tags and sitemap.xml")
    a = ap.parse_args()
    print(f"wrote {export(a.out, a.base, site_url=a.site_url)} pages to {a.out}")


if __name__ == "__main__":
    main()
