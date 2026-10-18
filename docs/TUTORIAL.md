# How to study with this course

Read this once (5 minutes). After that, the loop in section 2 is all you need.

## 1. Setup (once)

```bash
uv sync                                # install the tooling
cp learner.example.yaml learner.yaml   # your pace and dates (optional)
uv run de doctor                       # every row should say OK; follow any fix it prints
```

Later tracks need Kafka, Flink, Paimon or Fluss. Fetch them only when you reach that track
(scripts are in [labs/README.md](../labs/README.md)). `uv run de web` serves the course in your
browser, but you run and grade exercises in the terminal.

## 2. The loop

Every module works the same way:

| Step | What you do | Command |
|---|---|---|
| 1 | Find what is next | `de next` |
| 2 | Read the lesson | `content/<TRACK>/<MODULE>/lesson.md` |
| 3 | Start an exercise | `de start <EXERCISE_ID>` |
| 4 | Edit your code in `workspace/` and grade it. Repeat until it says PASSED | `de test` |
| 5 | Take the quiz | `de quiz <MODULE_ID>` |
| 6 | Explain the module in your own words (`explain.md`), then rehearse `drills.md` | – |

Do not skip step 6. It is what turns "the tests pass" into an answer you can give in an interview.

## 3. What is in a module

```
content/FND/FND-01/
├── lesson.md        read first: why it exists → how it works → from your world → when NOT to use it
├── exercises/       graded coding work; do them in number order
├── quiz.yaml        take it with `de quiz FND-01` (answers are inside, so don't open it)
├── explain.md       write ~200 words, then tick the self-check rubric (aim for 5 of 6)
└── drills.md        interview questions: cover the answer, say yours out loud, compare
```

## 4. Doing an exercise

```bash
de start FND-01/01-scale-up-vs-out    # copies the starter to workspace/FND-01/01-scale-up-vs-out
de test                               # grades the exercise you started last
```

- The exercise `README.md` is the spec. Read *What the tests check* closely.
- Edit only files in `workspace/`. All tests fail on a fresh starter; that is expected.
- Stuck? `de hint` gives a nudge. `--level 2` and `--level 3` get closer to the answer.
- `de solution` unlocks after a pass or 3 failed attempts. Read it after you pass too, and
  compare it with your code.
- To start over, delete the workspace folder and run `de start` again.

**Don't open these folders.** They contain answers: `solution/`, `alt_solutions/`, `mutants/`.

## 5. Build-your-own challenges

Each core track ends with one challenge. You write a single codebase that grows over several
stages, and each stage has its own tests.

| After | Challenge | You build |
|---|---|---|
| SPK | `SPK-BYO` | A columnar query engine |
| LKH | `LKH-BYO` | An Iceberg-like table format |
| KFK | `KFK-BYO` | A Kafka-protocol broker |
| FLK | `FLK-BYO` | A stream processor with watermarks and checkpoints |

```bash
de start SPK-BYO       # stage 1
de test                # this stage plus all earlier ones
de start SPK-BYO/02    # next stage, once stage 1 passes
```

`de next` does not suggest BYO stages, so start them yourself at the end of each track.
`de hint` and `de solution` don't work for BYO stages yet. Hints are in each stage's
`stage.yaml`, and the reference is in its `solution/` folder.

## 6. Order

Follow the [roadmap](../ROADMAP.md): **FND → SPK → LKH → JVM → KFK → FLK → STH**. `de next` respects
prerequisites for you. If you already know a module, take its quiz first, and if you pass, add
it to `tracks.skip_modules` in `learner.yaml`.

## 7. A weekly rhythm (6 h/week)

| Session | Time | Do |
|---|---|---|
| Wed | 1.5 h | Read the next lesson; start the first exercise |
| Sat | 2.5 h | Finish the exercises; read the solution; take the quiz |
| Sun | 2 h | `explain.md` and `drills.md`; redo last week's misses |

Stuck for more than 30 minutes? Take the next hint. Run `de plan` after changing `pace` or
`target_date` in `learner.yaml` to see your projected finish date.

## 8. Where your progress lives

`progress.json` (grades), `workspace/` (your code) and `learner.yaml` (your profile) are
gitignored, so `git pull` never conflicts with your work and nothing is uploaded. To keep them,
commit them in a fork with `git add -f`. To move machines, copy those paths.

The tick marks on the website's roadmap are separate: they live in your browser only.

## 9. Commands

| Command | Use |
|---|---|
| `de status` | Passed/total per track, and the current exercise |
| `de next` | Next exercise with its prerequisites met |
| `de start <ID>` | Begin an exercise or BYO stage |
| `de test [ID]` | Grade the current (or given) exercise |
| `de hint [--level N]` | Hints 1–3 |
| `de solution [ID]` | Reference solution (after a pass or 3 failures) |
| `de quiz <MODULE>` | Interactive quiz; best score saved |
| `de plan` | Projected finish date from `learner.yaml` |
| `de doctor` | Environment check |
