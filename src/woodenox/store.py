from __future__ import annotations

import sqlite3
from pathlib import Path

from .models import TaskBinding


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS task_sessions (
                task_id TEXT PRIMARY KEY,
                cwd TEXT NOT NULL,
                session_id TEXT,
                comment_cursor TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._db.commit()

    def get(self, task_id: str) -> TaskBinding | None:
        row = self._db.execute(
            "SELECT * FROM task_sessions WHERE task_id = ?", (task_id,)
        ).fetchone()
        return self._binding(row) if row else None

    def save(self, binding: TaskBinding) -> None:
        self._db.execute(
            """
            INSERT INTO task_sessions (task_id, cwd, session_id, comment_cursor)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                cwd = excluded.cwd,
                session_id = excluded.session_id,
                comment_cursor = excluded.comment_cursor,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                binding.task_id,
                binding.cwd,
                binding.session_id,
                binding.comment_cursor,
            ),
        )
        self._db.commit()

    def delete(self, task_id: str) -> None:
        self._db.execute("DELETE FROM task_sessions WHERE task_id = ?", (task_id,))
        self._db.commit()

    @staticmethod
    def _binding(row: sqlite3.Row) -> TaskBinding:
        return TaskBinding(
            task_id=row["task_id"],
            cwd=row["cwd"],
            session_id=row["session_id"],
            comment_cursor=row["comment_cursor"],
        )
