"""Course pages: curriculum map, tracks, modules (lesson, exercises, quiz, drills, explain), BYO."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse

from de_web import library as lib_mod
from de_web import render as render_mod
from de_web.library import Library

MODULE_PARTS = {"drills": "Interview drills", "explain": "Explain it back"}


def register(app: FastAPI, lib: Library, templates) -> None:
    def page(request: Request, name: str, status: int = 200, **ctx):
        ctx.update(lib=lib, path=request.url.path)
        return templates.TemplateResponse(request, name, ctx, status_code=status)

    def not_found(request: Request):
        return page(request, "notfound.html", status=404)

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request):
        return page(request, "home.html")

    @app.get("/track/{tid}", response_class=HTMLResponse)
    def track(tid: str, request: Request):
        t = lib.tracks.get(tid)
        if t is None:
            return not_found(request)
        syllabus = lib.render_file(t.syllabus) if t.syllabus else None
        return page(request, "track.html", track=t, syllabus=syllabus)

    def module_page(request: Request, m, part: str, **ctx):
        prev, nxt = lib.neighbours(m.id)
        return page(request, "module.html", module=m, part=part, prev_module=prev,
                    next_module=nxt, parts=MODULE_PARTS, **ctx)

    @app.get("/m/{mid}", response_class=HTMLResponse)
    def lesson(mid: str, request: Request):
        m = lib.modules.get(mid)
        if m is None:
            return not_found(request)
        doc = lib.render_file(m.dir / "lesson.md") if m.has("lesson.md") else None
        return module_page(request, m, "lesson", doc=doc)

    @app.get("/m/{mid}/{part}", response_class=HTMLResponse)
    def part_page(mid: str, part: str, request: Request):
        m = lib.modules.get(mid)
        if m is None:
            return not_found(request)
        if part in MODULE_PARTS and m.has(f"{part}.md"):
            return module_page(request, m, part, doc=lib.render_file(m.dir / f"{part}.md"))
        if part == "exercises":
            return module_page(request, m, "exercises", doc=None)
        if part == "quiz":
            qs = []
            for q in lib_mod.load_quiz(m):
                raw = q.get("answer")
                answers = [str(a) for a in (raw if isinstance(raw, list) else [raw])]
                qs.append({
                    "id": q.get("id", ""), "multi": q.get("type") == "multi",
                    "prompt": render_mod.render_inline(str(q.get("prompt", ""))),
                    "options": [(k, render_mod.render_inline(str(v)))
                                for k, v in (q.get("options") or {}).items()],
                    "answers": answers,
                    "explanation": render_mod.render_inline(str(q.get("explanation", "")).strip()),
                    "source": q.get("source", ""),
                })
            return module_page(request, m, "quiz", doc=None, questions=qs)
        return not_found(request)

    @app.get("/m/{mid}/x/{slug}", response_class=HTMLResponse)
    def exercise(mid: str, slug: str, request: Request):
        m = lib.modules.get(mid)
        ex = next((e for e in m.exercises if e.slug == slug), None) if m else None
        if ex is None:
            return not_found(request)
        files = [(name, render_mod.highlight_code(text, name.rsplit(".", 1)[-1] if "." in name else ""))
                 for name, text in ex.starter_files()]
        return module_page(request, m, "exercises", doc=lib.render_file(ex.readme),
                           exercise=ex, starter=files)

    @app.get("/byo/{cid}", response_class=HTMLResponse)
    def byo(cid: str, request: Request):
        c = lib.challenges.get(cid)
        return page(request, "byo.html", challenge=c) if c else not_found(request)

    @app.get("/byo/{cid}/{num}", response_class=HTMLResponse)
    def byo_stage(cid: str, num: int, request: Request):
        c = lib.challenges.get(cid)
        st = next((s for s in c.stages if s.number == num), None) if c else None
        if st is None:
            return not_found(request)
        readme = st.dir / "README.md"
        return page(request, "byo_stage.html", challenge=c, stage=st,
                    doc=lib.render_file(readme) if readme.is_file() else None)

    @app.get("/docs/{slug}", response_class=HTMLResponse)
    def doc(slug: str, request: Request):
        d = lib.docs.get(slug)
        return page(request, "doc.html", doc_meta=d, doc=lib.render_file(d.path)) if d else not_found(request)

    @app.get("/capstone/{cid}", response_class=HTMLResponse)
    def capstone(cid: str, request: Request):
        d = lib.capstones.get(cid)
        return page(request, "doc.html", doc_meta=d, doc=lib.render_file(d.path)) if d else not_found(request)
