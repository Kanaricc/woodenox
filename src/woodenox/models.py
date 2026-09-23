from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any


class TaskState(StrEnum):
    DISCOVERED = "DISCOVERED"
    RUNNING = "RUNNING"
    REQUIRES_ACTION = "REQUIRES_ACTION"
    INTERRUPTING = "INTERRUPTING"
    FINALIZING = "FINALIZING"
    COMPLETED = "COMPLETED"
    ABANDONED = "ABANDONED"
    INVALID = "INVALID"


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
    project_id: str
    agent: str
    cwd: str
    session_id: str | None
    state: TaskState
    prompt: str
    final_response: str


@dataclass(slots=True)
class PermissionRequest:
    id: str
    task_id: str
    title: str
    options: list[Any]
