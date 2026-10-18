# How to use this repo (start here)

This guide shows you how to study with the repo: what to read, in what order, and how the
`de` CLI grades your work. It takes about 15 minutes to read. After that, you only need the
loop in section 1.

## 1. The short version

Each module follows the same loop:

```bash
uv run de next                      # 1. which exercise is next?
#    read content/<TRACK>/<MODULE>/lesson.md   # 2. read the lesson first
uv run de start <EXERCISE_ID>       # 3. copy the starter into workspace/
#    read the exercise README.md, edit files in workspace/...  # 4. do the work
uv run de test                      # 5. grade it (repeat 4-5 until PASSED)
uv run de quiz <MODULE_ID>          # 6. exam-style quiz for the module
#    do explain.md, then drills.md  # 7. explain it, then rehearse for interviews
```

Repeat until `de next` reports nothing pending.

## 2. One-time setup

```bash
uv sync                  # install the Python tooling into .venv
cp learner.example.yaml learner.yaml   # your pace and dates (gitignored)
uv run de doctor         # every row should say OK; follow any "fix" it prints
```

`de doctor` also checks the native runtimes that later tracks need: JDK, Spark jars, Kafka,
Flink, Paimon and Fluss. You can fetch them only when you reach the track that uses them. The
scripts are listed in [labs/README.md](../labs/README.md).

To avoid typing `uv run` before every command, activate the virtualenv once per shell:

```bash
source .venv/bin/activate
de status
```

The examples below leave out `uv run`.

## 3. What to read and what to skip

| Path | What it is | As a learner |
|---|---|---|
| `README.md`, `ROADMAP.md` | Overview, track order, the "from your world" mapping | Read once. Come back to ROADMAP at each track boundary |
| `tracks/*.md` | Syllabus for each track | Skim when you **start** a track to see where it leads |
| `content/<TRACK>/<MODULE>/` | **The actual course** | This is where you spend your time |
| `byo/<challenge>/` | "Build your own X" challenges | One per core track, done at the end of that track |
| `capstones/` | Large portfolio projects | After SPK+LKH (C1), KFK (C2), FLK+STH (C3) |
| `certs/README.md` | Exam objectives mapped to modules | Read when you plan to book an exam |
| `learner.example.yaml` | Template for your pace, dates and skips. Copy to `learner.yaml` (gitignored) | Edit whenever you like |
| `docs/PLATFORM.md` | How the course format and CLI are designed | Optional |
| `docs/TESTING.md` | How exercise tests are proven valid | **Skip** (for contributors) |

### Spoiler folders: don't open them

Every exercise and BYO stage contains its answers right next to the starter. To learn anything,
open only `README.md`, `starter/` and `tests/`, plus the copy of the starter in `workspace/`.
Avoid these:

- `solution/`: the reference answer. Use `de solution` instead (section 5).
- `alt_solutions/`: other correct answers.
- `mutants/`: deliberately broken answers used to prove the tests work.
- `quiz.yaml`: the answers are inside. Take the quiz with `de quiz`.

## 4. Anatomy of one module, using FND-01 as the example

```
content/FND/FND-01/
├── module.yaml        # hours, prerequisites, "from your world" one-liner
├── lesson.md          # read first
├── exercises/01-.../  # graded coding work
├── quiz.yaml          # taken via `de quiz FND-01`
├── explain.md         # Feynman write-up + self-check rubric
└── drills.md          # interview questions with model answers
```

Work through it in this order:

1. **`module.yaml`.** Ten seconds. Note `hours` (your budget) and `from_your_world` (the hook
   to what you already know).
2. **`lesson.md`.** Every lesson has the same four parts: *Why it exists → How it works → From
   your world → When NOT to use it*. Read the whole lesson before coding. Take notes on anything
   that surprises you, because those notes are what you will need in an interview.
3. **Exercises.** See section 5. A module can have more than one exercise. Do them in number
   order.
4. **Quiz.** `de quiz FND-01` asks each question interactively. Type one letter, or
   comma-separated letters such as `A,C` for multi-answer questions. It then shows the correct
   answers, explanations and a source link. Your best score is saved. If you score below ~80%,
   reread the matching section of the lesson.
5. **`explain.md`.** Write your answer in a scratch file or out loud. Keep it short, roughly
   200 words or 5 minutes of talking. Then tick the rubric. Aim for 5 of 6 boxes. This step
   shows whether you understood the module or only got the tests to pass.
6. **`drills.md`.** Cover the model answer, answer out loud, then compare. Mark the ones you
   fumbled and revisit them next week. (`de review` will automate this spaced repetition in
   Phase 6.)

## 5. Doing an exercise

```bash
de start FND-01/01-scale-up-vs-out
# Started FND-01/01-scale-up-vs-out. Edit files in:
#   workspace/FND-01/01-scale-up-vs-out
```

- **Spec:** `content/FND/FND-01/exercises/01-scale-up-vs-out/README.md`. It lists the functions
  you must implement and a section called *What the tests check*. Read it closely.
- **Your code:** `workspace/FND-01/01-scale-up-vs-out/`. Edit only here. The starter has
  function stubs, and sometimes an `explain.txt` that the tests also check.
- **The tests:** `content/.../tests/`. You may read them. They are the precise spec. Many
  compare your output against an independent DuckDB "oracle".

Then iterate:

```bash
de test                 # runs pytest against your workspace copy; prints PASSED ✓ / FAILED ✗
de hint                 # hint 1 (a nudge)
de hint --level 2       # hints 1-2
de hint --level 3       # hints 1-3 (close to the answer)
de solution             # unlocks after a pass OR 3 failed `de test` attempts
```

Tips:

- On a fresh starter every test fails. That is expected. Turn the failures green one at a time,
  reading pytest's assertion message each time.
- Read the reference solution with `de solution` **after you pass** as well. Comparing it with
  your own code is one of the best ways to learn here.
- `de test` with no argument always grades the *current* exercise, which is the last one you
  started. To grade a different one: `de test <EXERCISE_ID>`.
- **Starting over:** `de start` never overwrites existing work. To reset, delete the workspace
  folder (for example `rm -r workspace/FND-01/01-scale-up-vs-out`) and run `de start` again.
- **Prerequisites:** `de start` warns, but does not block, when a required module isn't passed
  yet. `de next` only suggests exercises whose prerequisites are met.
- Exercises need no Docker unless their `exercise.yaml` says otherwise. Only `KFK-07` uses
  `lab: streaming`.

## 6. Build-your-own (BYO) challenges

There are four, one per core track. Do each one after you finish that track's modules:

| After | Challenge | You build |
|---|---|---|
| SPK | `SPK-BYO` (`byo/mini-query-engine`) | A columnar query engine: scan, pushdown, join, shuffle, spill |
| LKH | `LKH-BYO` (`byo/mini-table-format`) | An Iceberg-like table format: snapshots, atomic commits, MoR deletes |
| KFK | `KFK-BYO` (`byo/mini-kafka`) | A Kafka-protocol broker: log segments, fetch, consumer groups, replication |
| FLK | `FLK-BYO` (`byo/mini-stream-processor`) | A stream processor: watermarks, windows, keyed state, checkpoint barriers |

```bash
de start SPK-BYO            # stage 1; the codebase lives in workspace/byo/SPK-BYO/
de test                     # runs this stage's tests + all earlier stages (regressions)
de start SPK-BYO/02         # once stage 1 passes, move on; same codebase, it grows
```

- You write **one codebase** across all stages. The spec for each stage is in
  `byo/<challenge>/stage-NN-<slug>/README.md`.
- `de next` suggests BYO stages only once all regular exercises are done, so start them
  yourself at the end of each track.
- **Known gap:** `de hint` and `de solution` do not work for BYO stages yet. They print "No
  exercise selected". The hints are in the `hints:` list of each stage's `stage.yaml`. Read one
  at a time. The reference is in that stage's `solution/` folder. Only open it after you pass,
  or when you are truly stuck.

## 7. What order to do things in

Follow `de next`. It walks the tracks in the order set in `learner.yaml`
(`FND → SPK → LKH → CDW → JVM → KFK → FLK → STH → ARC`) and respects module prerequisites.
On top of that:

- **Track boundaries.** When you finish a track, do its BYO challenge, then check
  [ROADMAP.md → ARC checkpoints](../ROADMAP.md#arc-checkpoints) and the capstone table.
- **CDW and ARC** have syllabi in `tracks/` but no graded content yet, so `de next` skips them.
  Study them from the syllabus at the checkpoints in ROADMAP. Graded CDW content comes in
  Phase 7.
- **Capstones.** Only C3 is scaffolded (`capstones/C3/`, with its own README). C1 and C2 are
  specified in `capstones/README.md`. Build them in your own repo as portfolio pieces.
- **Certs.** The suggested exam points are in ROADMAP → *Cert checkpoints*. Change each cert's
  `status` in `learner.yaml` as you go.
- **Already know something?** Add the module to `tracks.skip_modules` in `learner.yaml`. For
  example, LKH-01 may be review for you after years of Iceberg. Take its quiz first to make sure.

## 8. A sustainable weekly rhythm (6 h/week)

`de plan` currently projects about 455 h, which is roughly 76 weeks at 6 h/week. One way to fill
your three study days:

| Session | ~Time | Do |
|---|---|---|
| Wed | 1.5 h | Read the next lesson; start its first exercise |
| Sat | 2.5 h | Finish the exercises; read `de solution`; take the quiz |
| Sun | 2 h | `explain.md` + `drills.md`; re-drill last week's misses; start the next lesson |

Habits that make this work:

- **Finish the module before moving on.** The quiz, explain-it and drills take maybe 30 minutes,
  and they are what turns passing tests into interview answers.
- **Check `de status`** at the start of a session to see where you are.
- **Rerun `de plan`** after you edit `pace` or `target_date` in `learner.yaml`. If you set a
  target date, it tells you whether that date is feasible.
- **Stuck for more than 30 minutes?** Use the next hint level. The point is to learn, not to
  suffer.

## 9. Where your progress lives

`de test` and `de quiz` write results to `progress.json` at the repo root. Your code lives in
`workspace/`, and your profile is `learner.yaml` (copy it from `learner.example.yaml`). All three
are gitignored: the course repo contains content only, so `git pull` never conflicts with your
work, and nothing is uploaded anywhere.

To keep a history of your own solutions, fork the repo and commit `workspace/` and `progress.json`
there (use `git add -f`). To move to another machine, copy those two paths across.

Prefer reading in a browser? `uv run de web` serves every lesson, exercise statement, quiz and
syllabus at http://127.0.0.1:8000 (see [WEB.md](WEB.md)). Running and grading exercises still happens in
the terminal.

## 10. Command cheat sheet

| Command | Use |
|---|---|
| `de status` | Passed/total per track and BYO challenge, plus the current exercise |
| `de next` | Next exercise with its prerequisites met |
| `de start <ID>` | Begin an exercise (`FND-01/01-scale-up-vs-out`) or BYO stage (`SPK-BYO/03`) |
| `de test [ID]` | Grade the current (or given) exercise/stage |
| `de hint [--level N]` | Progressive hints 1-3 (regular exercises only) |
| `de solution [ID]` | Reference solution, after a pass or 3 failed attempts (regular exercises only) |
| `de quiz <MODULE>` | Interactive module quiz; best score saved |
| `de plan` | Projected finish date / required hours per week from `learner.yaml` |
| `de doctor` | Environment check with fixes |
| `de lab up/down/status <profile>` | Optional Docker labs (`core`, `streaming`, `flink`, `streamhouse`) |

`de mock`, `de review`, `de cost` and `de dispute` are placeholders for later phases.
