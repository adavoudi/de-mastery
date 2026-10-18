"""Course pages: every indexed page renders, links resolve, and spoilers never leak."""

from pathlib import Path

import pytest

from de_web.library import Library

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def lib():
    return Library(ROOT)


def all_paths(lib):
    paths = ["/"]
    paths += [f"/track/{t}" for t in lib.tracks]
    for m in lib.modules.values():
        paths += [f"/m/{m.id}", f"/m/{m.id}/exercises"]
        paths += [f"/m/{m.id}/{p}" for p in ("drills", "explain", "quiz") if m.has(f"{p}.md") or m.has("quiz.yaml")]
        paths += [f"/m/{m.id}/x/{e.slug}" for e in m.exercises]
    for c in lib.challenges.values():
        paths.append(f"/byo/{c.id}")
        paths += [f"/byo/{c.id}/{s.number:02d}" for s in c.stages]
    paths += [f"/docs/{d}" for d in lib.docs] + [f"/capstone/{c}" for c in lib.capstones]
    return paths


def test_every_page_renders(client, lib):
    bad = []
    for p in all_paths(lib):
        r = client.get(p)
        if r.status_code != 200 or "<h1" not in r.text:
            bad.append((p, r.status_code))
    assert not bad, bad[:5]


def test_library_is_not_empty(lib):
    assert len(lib.modules) > 50 and len(lib.exercises) > 80 and lib.challenges


def test_solution_code_never_appears_in_exercise_pages(client, lib):
    """For every exercise, a distinctive line of the reference solution must not be served."""
    leaks = []
    for ex in lib.exercises.values():
        sol = ex.dir / "solution"
        if not sol.is_dir():
            continue
        visible = (ex.readme.read_text() if ex.readme.exists() else "") + \
            "".join(t for _, t in ex.starter_files())
        for f in sol.rglob("*.py"):
            lines = [l.strip() for l in f.read_text().splitlines()
                     if len(l.strip()) > 45 and not l.strip().startswith(("#", '"""'))]
            unique = [l for l in lines if l not in visible]
            if not unique:
                continue
            html = client.get(f"/m/{ex.module_id}/x/{ex.slug}").text
            import html as html_mod
            page = html_mod.unescape(__import__("re").sub(r"<[^>]+>", "", html))
            if any(l in page for l in unique[:15]):
                leaks.append(ex.id)
            break
    assert not leaks, leaks[:5]


def test_unknown_ids_are_404_and_paths_are_not_files(client):
    for p in ("/m/NOPE-99", "/track/ZZZ", "/account", "/auth/login", "/api/sync", "/m/SPK-07/x/..%2f..%2fsolution", "/m/SPK-07/secret",
              "/byo/NOPE", "/byo/KFK-BYO/99", "/docs/..%2fpyproject", "/capstone/x"):
        assert client.get(p).status_code in (404, 405, 422), p


def test_static_fonts_and_assets_served(client):
    for p in ("/static/style.css", "/static/app.js", "/static/fonts/newsreader-normal.woff2"):
        assert client.get(p).status_code == 200, p


def test_links_resolve_to_site_routes_or_github(lib):
    r = lib.make_resolver("tracks/SPK-spark.md")
    assert r("../certs/README.md#x")[0] == "/docs/certs#x"
    href, ext = lib.make_resolver("docs/TUTORIAL.md")("../labs/README.md")
    assert ext and href.endswith("/blob/main/labs/README.md")
    assert lib.make_resolver("a.md")("https://example.com") == ("https://example.com", True)
    assert lib.make_resolver("a.md")("#frag") == ("#frag", False)


def test_quiz_page_has_questions_and_answers(client, lib):
    html = client.get("/m/SPK-07/quiz").text
    assert 'class="q"' in html and "data-answer=" in html and "/static/quiz.js?v=" in html
    assert "<script>" not in html and "onclick" not in html      # CSP: no inline script


def test_no_inline_styles_or_scripts_anywhere(client, lib):
    for p in all_paths(lib)[:200:7]:
        html = client.get(p).text
        assert ' style="' not in html and "<script>" not in html, p


def test_security_headers(client):
    r = client.get("/")
    csp = r.headers["content-security-policy"]
    assert "default-src 'none'" in csp and "script-src 'self'" in csp and "unsafe-inline" not in csp
    assert r.headers["x-content-type-options"] == "nosniff"


def test_brand_assets_served(client):
    for p, kind in (("/favicon.ico", "image/x-icon"), ("/static/brand/favicon.svg", "image/svg+xml"),
                    ("/static/brand/logo.svg", "image/svg+xml"), ("/static/brand/apple-touch-icon.png", "image/png"),
                    ("/static/brand/site.webmanifest", None)):
        r = client.get(p)
        assert r.status_code == 200, p
        if kind:
            assert r.headers["content-type"].startswith(kind), p
    html = client.get("/").text
    for needle in ('rel="icon"', 'rel="apple-touch-icon"', 'rel="manifest"', "brand/logo-mark.svg"):
        assert needle in html, needle


def test_healthz(client):
    assert client.get("/healthz").json() == {"ok": True}


def test_no_account_or_sync_features_remain(client):
    html = client.get("/").text.lower()
    assert "sign in" not in html and "oauth" not in html
