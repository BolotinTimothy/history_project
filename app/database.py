from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def init_schema(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        schema = """
        CREATE TABLE IF NOT EXISTS stories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            short_description TEXT NOT NULL,
            intro_text TEXT NOT NULL,
            outro_text TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS story_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            story_id INTEGER NOT NULL,
            step_index INTEGER NOT NULL,
            narrative_text TEXT NOT NULL,
            question TEXT NOT NULL,
            explanation TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (story_id) REFERENCES stories(id) ON DELETE CASCADE,
            UNIQUE (story_id, step_index)
        );

        CREATE TABLE IF NOT EXISTS step_options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            step_id INTEGER NOT NULL,
            option_index INTEGER NOT NULL,
            text TEXT NOT NULL,
            outcome_text TEXT NOT NULL,
            is_correct INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (step_id) REFERENCES story_steps(id) ON DELETE CASCADE,
            UNIQUE (step_id, option_index)
        );

        CREATE TABLE IF NOT EXISTS chat_sessions (
            chat_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            username TEXT,
            current_story_id INTEGER,
            current_step_index INTEGER,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            FOREIGN KEY (current_story_id) REFERENCES stories(id)
        );

        CREATE TABLE IF NOT EXISTS user_answers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            story_id INTEGER NOT NULL,
            step_id INTEGER NOT NULL,
            selected_option_id INTEGER NOT NULL,
            correct_option_id INTEGER NOT NULL,
            is_correct INTEGER NOT NULL,
            answered_at TEXT NOT NULL,
            FOREIGN KEY (story_id) REFERENCES stories(id),
            FOREIGN KEY (step_id) REFERENCES story_steps(id),
            FOREIGN KEY (selected_option_id) REFERENCES step_options(id),
            FOREIGN KEY (correct_option_id) REFERENCES step_options(id)
        );
        """

        with self.connect() as connection:
            connection.executescript(schema)
            self._migrate_schema(connection)

    def _migrate_schema(self, connection: sqlite3.Connection) -> None:
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(step_options)").fetchall()}
        if "outcome_text" not in columns:
            connection.execute("ALTER TABLE step_options ADD COLUMN outcome_text TEXT NOT NULL DEFAULT ''")

    def seed_stories(self, stories_dir: Path) -> int:
        stories_dir.mkdir(parents=True, exist_ok=True)
        story_files = sorted(stories_dir.glob("*.json"))

        with self.connect() as connection:
            for story_file in story_files:
                payload = json.loads(story_file.read_text(encoding="utf-8"))
                self._upsert_story(connection, payload)

        return len(story_files)

    def _upsert_story(self, connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
        self._validate_story_payload(payload)
        timestamp = utc_now()

        existing_story = connection.execute(
            "SELECT id FROM stories WHERE slug = ?",
            (payload["slug"],),
        ).fetchone()

        if existing_story:
            story_id = existing_story["id"]
            connection.execute(
                """
                UPDATE stories
                SET title = ?, short_description = ?, intro_text = ?, outro_text = ?, is_active = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    payload["title"],
                    payload["short_description"],
                    payload["intro_text"],
                    payload["outro_text"],
                    int(payload.get("is_active", True)),
                    timestamp,
                    story_id,
                ),
            )
            connection.execute("DELETE FROM story_steps WHERE story_id = ?", (story_id,))
        else:
            cursor = connection.execute(
                """
                INSERT INTO stories (slug, title, short_description, intro_text, outro_text, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["slug"],
                    payload["title"],
                    payload["short_description"],
                    payload["intro_text"],
                    payload["outro_text"],
                    int(payload.get("is_active", True)),
                    timestamp,
                    timestamp,
                ),
            )
            story_id = cursor.lastrowid

        for step_index, step in enumerate(payload["steps"], start=1):
            step_cursor = connection.execute(
                """
                INSERT INTO story_steps (story_id, step_index, narrative_text, question, explanation, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    story_id,
                    step_index,
                    step["narrative_text"],
                    step["question"],
                    step["explanation"],
                    timestamp,
                    timestamp,
                ),
            )
            step_id = step_cursor.lastrowid

            for option_index, option in enumerate(step["options"], start=1):
                connection.execute(
                    """
                    INSERT INTO step_options (step_id, option_index, text, outcome_text, is_correct, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        step_id,
                        option_index,
                        option["text"],
                        option["outcome_text"],
                        int(option["is_correct"]),
                        timestamp,
                        timestamp,
                    ),
                )

    def _validate_story_payload(self, payload: dict[str, Any]) -> None:
        required_story_fields = ["slug", "title", "short_description", "intro_text", "outro_text", "steps"]
        missing_fields = [field for field in required_story_fields if field not in payload]
        if missing_fields:
            raise ValueError(f"Story is missing required fields: {', '.join(missing_fields)}")

        if not payload["steps"]:
            raise ValueError(f"Story '{payload['slug']}' must contain at least one step")

        for step_index, step in enumerate(payload["steps"], start=1):
            required_step_fields = ["narrative_text", "question", "explanation", "options"]
            missing_step_fields = [field for field in required_step_fields if field not in step]
            if missing_step_fields:
                raise ValueError(
                    f"Story '{payload['slug']}', step {step_index} is missing: {', '.join(missing_step_fields)}"
                )

            if len(step["options"]) < 2:
                raise ValueError(f"Story '{payload['slug']}', step {step_index} must contain at least two options")

            correct_options = sum(1 for option in step["options"] if option.get("is_correct"))
            if correct_options != 1:
                raise ValueError(
                    f"Story '{payload['slug']}', step {step_index} must contain exactly one correct option"
                )

            for option_index, option in enumerate(step["options"], start=1):
                if "text" not in option or "is_correct" not in option or "outcome_text" not in option:
                    raise ValueError(
                        f"Story '{payload['slug']}', step {step_index}, option {option_index} must contain "
                        "'text', 'is_correct' and 'outcome_text'"
                    )

    def get_active_stories(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, slug, title, short_description
                FROM stories
                WHERE is_active = 1
                ORDER BY title
                """
            ).fetchall()

    def get_story(self, story_id: int) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, slug, title, short_description, intro_text, outro_text
                FROM stories
                WHERE id = ? AND is_active = 1
                """,
                (story_id,),
            ).fetchone()

    def get_random_story(self) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, slug, title, short_description, intro_text, outro_text
                FROM stories
                WHERE is_active = 1
                ORDER BY RANDOM()
                LIMIT 1
                """
            ).fetchone()

    def get_story_step(self, story_id: int, step_index: int) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT
                    ss.id,
                    ss.story_id,
                    ss.step_index,
                    ss.narrative_text,
                    ss.question,
                    ss.explanation,
                    s.title AS story_title,
                    (
                        SELECT COUNT(*)
                        FROM story_steps
                        WHERE story_id = ss.story_id
                    ) AS total_steps
                FROM story_steps ss
                JOIN stories s ON s.id = ss.story_id
                WHERE ss.story_id = ? AND ss.step_index = ?
                """,
                (story_id, step_index),
            ).fetchone()

    def get_step_options(self, step_id: int) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, step_id, option_index, text, outcome_text, is_correct
                FROM step_options
                WHERE step_id = ?
                ORDER BY option_index
                """,
                (step_id,),
            ).fetchall()

    def start_story_for_chat(self, chat_id: int, user_id: int | None, username: str | None, story_id: int) -> None:
        timestamp = utc_now()

        with self.connect() as connection:
            connection.execute("DELETE FROM user_answers WHERE chat_id = ? AND story_id = ?", (chat_id, story_id))
            connection.execute(
                """
                INSERT INTO chat_sessions (
                    chat_id, user_id, username, current_story_id, current_step_index, status, started_at, updated_at, completed_at
                )
                VALUES (?, ?, ?, ?, 1, 'active', ?, ?, NULL)
                ON CONFLICT(chat_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    username = excluded.username,
                    current_story_id = excluded.current_story_id,
                    current_step_index = excluded.current_step_index,
                    status = excluded.status,
                    started_at = excluded.started_at,
                    updated_at = excluded.updated_at,
                    completed_at = excluded.completed_at
                """,
                (chat_id, user_id, username, story_id, timestamp, timestamp),
            )

    def get_active_session(self, chat_id: int) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT
                    cs.chat_id,
                    cs.user_id,
                    cs.username,
                    cs.current_story_id,
                    cs.current_step_index,
                    cs.status,
                    s.title AS story_title,
                    s.outro_text
                FROM chat_sessions cs
                JOIN stories s ON s.id = cs.current_story_id
                WHERE cs.chat_id = ? AND cs.status = 'active'
                """,
                (chat_id,),
            ).fetchone()

    def submit_answer(self, chat_id: int, option_id: int) -> dict[str, Any] | None:
        timestamp = utc_now()

        with self.connect() as connection:
            session = connection.execute(
                """
                SELECT chat_id, current_story_id, current_step_index, status
                FROM chat_sessions
                WHERE chat_id = ? AND status = 'active'
                """,
                (chat_id,),
            ).fetchone()
            if not session:
                return None

            step = connection.execute(
                """
                SELECT id, story_id, step_index, narrative_text, question, explanation
                FROM story_steps
                WHERE story_id = ? AND step_index = ?
                """,
                (session["current_story_id"], session["current_step_index"]),
            ).fetchone()
            if not step:
                return None

            selected_option = connection.execute(
                """
                SELECT id, step_id, option_index, text, outcome_text, is_correct
                FROM step_options
                WHERE id = ? AND step_id = ?
                """,
                (option_id, step["id"]),
            ).fetchone()
            if not selected_option:
                return {"status": "stale"}

            correct_option = connection.execute(
                """
                SELECT id, text
                FROM step_options
                WHERE step_id = ? AND is_correct = 1
                """,
                (step["id"],),
            ).fetchone()

            connection.execute(
                """
                INSERT INTO user_answers (
                    chat_id, story_id, step_id, selected_option_id, correct_option_id, is_correct, answered_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    session["current_story_id"],
                    step["id"],
                    selected_option["id"],
                    correct_option["id"],
                    int(selected_option["is_correct"]),
                    timestamp,
                ),
            )

            next_step = connection.execute(
                """
                SELECT id, step_index
                FROM story_steps
                WHERE story_id = ? AND step_index = ?
                """,
                (session["current_story_id"], session["current_step_index"] + 1),
            ).fetchone()

            if next_step:
                connection.execute(
                    """
                    UPDATE chat_sessions
                    SET current_step_index = ?, updated_at = ?
                    WHERE chat_id = ?
                    """,
                    (next_step["step_index"], timestamp, chat_id),
                )
                session_status = "active"
            else:
                connection.execute(
                    """
                    UPDATE chat_sessions
                    SET status = 'completed', updated_at = ?, completed_at = ?
                    WHERE chat_id = ?
                    """,
                    (timestamp, timestamp, chat_id),
                )
                session_status = "completed"

            story = connection.execute(
                "SELECT title, outro_text FROM stories WHERE id = ?",
                (session["current_story_id"],),
            ).fetchone()

            return {
                "status": session_status,
                "story_id": session["current_story_id"],
                "story_title": story["title"],
                "outro_text": story["outro_text"],
                "step_index": step["step_index"],
                "explanation": step["explanation"],
                "selected_text": selected_option["text"],
                "selected_outcome_text": selected_option["outcome_text"],
                "correct_text": correct_option["text"],
                "is_correct": bool(selected_option["is_correct"]),
                "next_step_index": next_step["step_index"] if next_step else None,
            }
