from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class DidaTask:
    id: str
    project_id: str
    title: str
    description: str
    status: Any = None
    tags: list[str] = field(default_factory=list)
    kind: str = "TEXT"


@dataclass(slots=True)
class TaskInput:
    task: DidaTask
    cwd: Path
    prompt_description: str


@dataclass(slots=True)
class TaskBinding:
    task_id: str
    cwd: str
    session_id: str | None
    comment_cursor: str


@dataclass(slots=True)
class PermissionRequest:
    id: str
    task_id: str
    title: str
    options: list[Any]
