# Writing a lesson diagram

A diagram is a hand-written SVG in `content/<TRACK>/<MODULE>/diagrams/<name>.svg`, shown in a
lesson with one line on its own, directly after the paragraph or heading it explains:

```markdown
![Short caption that says what to notice](diagrams/leader-failover.svg)
```

The site inlines the SVG (so it follows light/dark mode). GitHub shows it as an image. Names use
lowercase letters, digits and `-`. Start from [diagrams/_template.svg](diagrams/_template.svg).

## Rules

- **One idea per diagram.** If the caption needs "and", split it. Fewer than ~12 shapes is typical.
- **Teach, don't decorate.** Show the mechanism the lesson explains, with the lesson's own names
  and numbers. Never contradict the lesson text.
- **`viewBox` required**, width 640–760, height at most 420. No fixed `width`/`height` on `<svg>`.
  Text is 12 px or larger and never overlaps shapes or other text.
- **Style only with the `dg-*` classes** from the template (`dg-box`, `dg-accent`, `dg-ok`, `dg-bad`,
  `dg-warn`, `dg-text`, `dg-muted`, `dg-mono`, `dg-b`, `dg-line`, `dg-dash`, `dg-head`, `dg-dot`,
  `dg-dot-ok`, `dg-dot-bad`). No hex colours or `style=` on elements, so dark mode works.
  Copy the template's `<style>` block unchanged: it is only the fallback for opening the file
  alone, and the site removes it.
- **No scripts, no `foreignObject`, no external URLs or fonts.**
- **Unique ids**: prefix every `id` with the file name (`leader-failover-arrow`), because several
  diagrams can share one page. Add `<title id=…>` and `aria-labelledby`.
- **Animation is SMIL only** (`<animate>`, `<animateTransform>`, `<set>`), looping with
  `repeatCount="indefinite"`, 6–12 s per loop. The site adds Pause and Replay buttons.
  The frame at t=0 is what readers see when paused or when they prefer reduced motion, so it must
  already show every label and the whole layout. Motion adds the sequence; it never carries a
  label that is missing from that first frame. Use motion only where order or time is the idea.
- **Check it**: `uv run pytest web -q`, then look at it:
  `google-chrome --headless --no-sandbox --disable-gpu --screenshot=OUT.png --window-size=780,460 file://$PWD/<path>.svg`
  and open `OUT.png`. Fix clipped or overlapping text before you finish.
