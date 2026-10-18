"""Interactive quiz runner for ``quiz.yaml`` (see docs/PLATFORM.md)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class Question:
    id: str
    type: str  # single | multi
    prompt: str
    options: dict
    answer: list
    explanation: str = ""
    source: str = ""
    objectives: list | None = None


def load_quiz(quiz_yaml: Path) -> list[Question]:
    raw = yaml.safe_load(quiz_yaml.read_text()) or []
    out: list[Question] = []
    for q in raw:
        out.append(Question(
            id=q["id"], type=q.get("type", "single"), prompt=q["prompt"],
            options=q.get("options", {}), answer=list(q.get("answer", [])),
            explanation=q.get("explanation", ""), source=q.get("source", ""),
            objectives=q.get("objectives", []),
        ))
    return out


def grade(questions: list[Question], responses: dict[str, list]) -> float:
    """Return fraction correct. A multi question is correct only if the selected set
    exactly matches the answer set."""
    if not questions:
        return 0.0
    correct = 0
    for q in questions:
        given = set(responses.get(q.id, []))
        if given == set(q.answer):
            correct += 1
    return correct / len(questions)


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]
