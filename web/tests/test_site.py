"""Course pages: every indexed page renders, links resolve, and spoilers never leak."""

from pathlib import Path

import pytest

from de_web.library import Library

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def lib():
    return Library(ROOT)


def all_paths(lib):
    paths = ["/", "/roadmap"]
    paths += [f"/track/{t}" for t in lib.tracks]
    for m in lib.modules.values():
        paths += [f"/m/{m.id}", f"/m/{m.id}/exercises"]
        paths += [f"/m/{m.id}/{p}" for p in ("drills", "explain", "quiz") if m.has(f"{p}.md") or m.has("quiz.yaml")]
        paths += [f"/m/{m.id}/x/{e.slug}" for e in m.exercises]
    for c in lib.challenges.values():
        paths.append(f"/byo/{c.id}")
        paths += [f"/byo/{c.id}/{s.number:02d}" for s in c.stages]
    paths += [f"/docs/{d}" for d in lib.docs]
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
              "/byo/NOPE", "/byo/KFK-BYO/99", "/docs/..%2fpyproject", "/capstone/x", "/docs/certs", "/track/CDW"):
        assert client.get(p).status_code in (404, 405, 422), p


def test_static_fonts_and_assets_served(client):
    for p in ("/static/style.css", "/static/app.js", "/static/fonts/newsreader-normal.woff2"):
        assert client.get(p).status_code == 200, p


def test_links_resolve_to_site_routes_or_github(lib):
    r = lib.make_resolver("tracks/SPK-spark.md")
    assert r("../ROADMAP.md#x")[0] == "/roadmap#x"
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


def test_roadmap_lists_every_module_and_syllabus_only_tracks_are_gone(client, lib):
    html = client.get("/roadmap").text
    for m in lib.modules.values():
        assert f'href="/m/{m.id}"' in html, m.id
    assert "CDW" not in lib.tracks and "ARC" not in lib.tracks
    assert "Certifications" not in client.get("/").text


def test_diagrams_are_inlined_safely(lib, client):
    """Every ![..](diagrams/x.svg) in a lesson exists, is valid SVG, and is served inline and CSP-safe."""
    import re
    import xml.etree.ElementTree as ET
    seen = 0
    for m in lib.modules.values():
        lesson = m.dir / "lesson.md"
        refs = re.findall(r"!\[[^\]]*\]\((diagrams/[\w-]+\.svg)\)", lesson.read_text()) if lesson.exists() else []
        for ref in refs:
            svg = m.dir / ref
            assert svg.is_file(), f"{m.id}: missing {ref}"
            root = ET.parse(svg).getroot()
            assert root.tag.endswith("svg") and root.get("viewBox"), f"{ref}: needs a viewBox"
            text = svg.read_text()
            assert "<script" not in text and "onclick" not in text and "foreignObject" not in text, ref
            html = client.get(f"/m/{m.id}").text
            assert 'class="diagram"' in html and f"{ref}" not in html.split("<main")[1], f"{m.id}: {ref} not inlined"
            assert "<style" not in html.split("<main")[1], f"{m.id}: inline <style> would break the CSP"
            seen += 1
    assert seen >= 0


def test_diagram_renderer(tmp_path):
    from de_web import render as r
    (tmp_path / "diagrams").mkdir()
    (tmp_path / "diagrams" / "a.svg").write_text(
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10"><style>x{}</style>'
        '<script>1</script><rect style="fill:red" onclick="x()"/><animate/></svg>')
    html = r.render("# T\n\n![Cap & co](diagrams/a.svg)\n\n![x](diagrams/../../etc/passwd)\n", base_dir=tmp_path).html
    assert '<figure class="diagram" data-anim><svg' in html and "Cap &amp; co" in html
    assert "<script" not in html and "<style" not in html and "onclick" not in html and "style=" not in html
    assert "<img" in html  # path traversal falls back to a plain image, never a file read
