from __future__ import annotations

import sqlite3
from pathlib import Path

from .models import TaskBinding, TaskState


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS task_bindings (
                task_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                agent TEXT NOT NULL,
                cwd TEXT NOT NULL,
                session_id TEXT,
                state TEXT NOT NULL,
                prompt TEXT NOT NULL,
                final_response TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_comments (
                comment_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL
            )
            """
        )
        self._db.commit()

    def get(self, task_id: str) -> TaskBinding | None:
        row = self._db.execute(
            "SELECT * FROM task_bindings WHERE task_id = ?", (task_id,)
        ).fetchone()
        return self._binding(row) if row else None

    def list_completed(self, project_id: str) -> list[TaskBinding]:
        rows = self._db.execute(
            "SELECT * FROM task_bindings WHERE project_id = ? AND state = ?",
            (project_id, TaskState.COMPLETED),
        ).fetchall()
        return [self._binding(row) for row in rows]

    def save(self, binding: TaskBinding) -> None:
        self._db.execute(
            """
            INSERT INTO task_bindings (
                task_id, project_id, agent, cwd, session_id, state, prompt, final_response
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                project_id = excluded.project_id,
                agent = excluded.agent,
                cwd = excluded.cwd,
                session_id = excluded.session_id,
                state = excluded.state,
                prompt = excluded.prompt,
                final_response = excluded.final_response,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                binding.task_id,
                binding.project_id,
                binding.agent,
                binding.cwd,
                binding.session_id,
                binding.state,
                binding.prompt,
                binding.final_response,
            ),
        )
        self._db.commit()

    def comment_processed(self, comment_id: str) -> bool:
        row = self._db.execute(
            "SELECT 1 FROM processed_comments WHERE comment_id = ?", (comment_id,)
        ).fetchone()
        return row is not None

    def mark_comment_processed(self, task_id: str, comment_id: str) -> None:
        self._db.execute(
            "INSERT OR IGNORE INTO processed_comments (comment_id, task_id) VALUES (?, ?)",
            (comment_id, task_id),
        )
        self._db.commit()

    @staticmethod
    def _binding(row: sqlite3.Row) -> TaskBinding:
        return TaskBinding(
            task_id=row["task_id"],
            project_id=row["project_id"],
            agent=row["agent"],
            cwd=row["cwd"],
            session_id=row["session_id"],
            state=TaskState(row["state"]),
            prompt=row["prompt"],
            final_response=row["final_response"],
        )
