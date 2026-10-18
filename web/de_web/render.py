"""Markdown -> HTML for course content (lessons, drills, READMEs).

Content comes from the course repo, which maintainers review, so raw HTML in it is allowed;
the page CSP (no inline script, no external hosts) is the second line of defence.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable

from markdown_it import MarkdownIt
from markdown_it.token import Token
from mdit_py_plugins.anchors import anchors_plugin
from mdit_py_plugins.footnote import footnote_plugin
from mdit_py_plugins.tasklists import tasklists_plugin
from pygments import highlight as pyg_highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import TextLexer, get_lexer_by_name
from pygments.util import ClassNotFound

_FORMATTER = HtmlFormatter(nowrap=True)
# Fence info strings that pygments knows under a different name.
_LANG_ALIASES = {"yml": "yaml", "sh": "bash", "shell": "bash", "kt": "kotlin", "dockerfile": "docker"}


def slugify(text: str) -> str:
    s = re.sub(r"[^\w\s-]", "", text.lower(), flags=re.UNICODE)
    return re.sub(r"[\s_]+", "-", s).strip("-") or "section"


def highlight_code(code: str, lang: str = "") -> str:
    """Return ``<pre class="hl">`` markup; the language label is shown by CSS."""
    name = _LANG_ALIASES.get(lang.lower(), lang.lower())
    try:
        lexer = get_lexer_by_name(name) if name else TextLexer()
    except ClassNotFound:
        lexer = TextLexer()
    body = pyg_highlight(code.rstrip("\n"), lexer, _FORMATTER)
    label = f' data-lang="{name}"' if name and name != "text" else ""
    return f'<pre class="hl"{label}><code>{body}</code></pre>'


def _highlight_fence(code: str, lang: str, attrs: str) -> str:
    return highlight_code(code, lang)


def _rewrite_links(state) -> None:
    resolve: Callable[[str], tuple[str, bool]] | None = state.env.get("resolve")
    if resolve is None:
        return
    for tok in state.tokens:
        for child in tok.children or []:
            if child.type != "link_open":
                continue
            href = child.attrGet("href") or ""
            new, external = resolve(href)
            child.attrSet("href", new)
            if external:
                child.attrSet("rel", "noopener noreferrer")
                child.attrSet("class", "ext")


_SVG_STRIP = [re.compile(p, re.S | re.I) for p in (
    r"<\?xml[^>]*\?>", r"<!DOCTYPE[^>]*>", r"<script\b.*?</script>", r"<style\b.*?</style>",
    r"<foreignObject\b.*?</foreignObject>", r'\s(?:style|on\w+)\s*=\s*"[^"]*"')]


def load_diagram(base_dir: Path, src: str) -> str | None:
    """Return the sanitised markup of ``diagrams/<name>.svg`` next to a markdown file, else None.

    Only files directly inside ``<base_dir>/diagrams`` are served. ``<style>`` blocks are dropped
    because the page CSP forbids them; the site styles diagrams through the ``dg-*`` classes in
    style.css, while the ``<style>`` block keeps the file readable when opened on its own.
    """
    m = re.fullmatch(r"diagrams/([\w-]+)\.svg", src)
    if not m:
        return None
    path = base_dir / "diagrams" / f"{m.group(1)}.svg"
    try:
        svg = path.read_text()
    except OSError:
        return None
    for rx in _SVG_STRIP:
        svg = rx.sub("", svg)
    return svg.strip() if svg.lstrip().startswith("<svg") else None


def _inline_diagrams(state) -> None:
    """Turn a paragraph that is just ``![caption](diagrams/x.svg)`` into an inline <figure>."""
    base_dir: Path | None = state.env.get("base_dir")
    if base_dir is None:
        return
    out, toks, i = [], state.tokens, 0
    while i < len(toks):
        t = toks[i]
        if (t.type == "paragraph_open" and i + 2 < len(toks) and toks[i + 2].type == "paragraph_close"
                and len(toks[i + 1].children or []) == 1 and toks[i + 1].children[0].type == "image"):
            img = toks[i + 1].children[0]
            svg = load_diagram(base_dir, img.attrGet("src") or "")
            if svg:
                caption = _inline_text(img) or ""
                animated = "<animate" in svg
                tok = Token("html_block", "", 0)
                tok.content = (f'<figure class="diagram"{" data-anim" if animated else ""}>{svg}'
                               f'<figcaption>{_esc(caption)}</figcaption></figure>\n')
                out.append(tok)
                i += 3
                continue
        out.append(t)
        i += 1
    state.tokens = out


def _esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def make_parser() -> MarkdownIt:
    md = (MarkdownIt("commonmark", {"html": True, "typographer": False, "highlight": _highlight_fence})
          .enable("table").enable("strikethrough")
          .use(anchors_plugin, min_level=2, max_level=3, slug_func=slugify, permalink=False)
          .use(tasklists_plugin)
          .use(footnote_plugin))
    md.core.ruler.push("de_rewrite_links", _rewrite_links)
    md.core.ruler.push("de_inline_diagrams", _inline_diagrams)
    return md


_MD = make_parser()


@dataclass
class Rendered:
    html: str
    title: str | None              # text of the leading H1, which pages show in their own header
    toc: list[tuple[int, str, str]]  # (level, id, text)


def _inline_text(tok: Token) -> str:
    return "".join(c.content for c in (tok.children or []) if c.type in ("text", "code_inline"))


def render(text: str, resolve: Callable[[str], tuple[str, bool]] | None = None,
           base_dir: Path | None = None) -> Rendered:
    env: dict = {"resolve": resolve} if resolve else {}
    if base_dir is not None:
        env["base_dir"] = base_dir
    tokens = _MD.parse(text, env)
    title = None
    if len(tokens) >= 3 and tokens[0].type == "heading_open" and tokens[0].tag == "h1":
        title = _inline_text(tokens[1])
        tokens = tokens[3:]
    toc = []
    for i, t in enumerate(tokens):
        if t.type == "heading_open" and t.tag in ("h2", "h3"):
            toc.append((int(t.tag[1]), t.attrGet("id") or "", _inline_text(tokens[i + 1])))
    return Rendered(_MD.renderer.render(tokens, _MD.options, env), title, toc)


def render_inline(text: str) -> str:
    return _MD.renderInline(text)


def pygments_css_note() -> str:  # pragma: no cover - documentation helper
    return "Token colours live in static/style.css, driven by CSS variables (light/dark)."


def normalize_href(base: PurePosixPath, href: str) -> tuple[str, str]:
    """Split ``href`` into (repo-relative posix path, fragment) relative to the file at ``base``."""
    path, _, frag = href.partition("#")
    if not path:
        return str(base), frag
    joined = PurePosixPath(base.parent, path)
    parts: list[str] = []
    for p in joined.parts:
        if p == "..":
            if parts:
                parts.pop()
        elif p != ".":
            parts.append(p)
    return "/".join(parts), frag
