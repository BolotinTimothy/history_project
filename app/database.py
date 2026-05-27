from __future__ import annotations

import json
import sqlite3
from collections import Counter
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
            tags TEXT NOT NULL DEFAULT '[]',
            intro_text TEXT NOT NULL,
            outro_text TEXT NOT NULL,
            editorial_sources TEXT NOT NULL DEFAULT '[]',
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

        CREATE TABLE IF NOT EXISTS user_achievements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            achievement_key TEXT NOT NULL,
            unlocked_at TEXT NOT NULL,
            UNIQUE (chat_id, achievement_key)
        );
        """

        with self.connect() as connection:
            connection.executescript(schema)
            self._migrate_schema(connection)

    def _migrate_schema(self, connection: sqlite3.Connection) -> None:
        option_columns = {row["name"] for row in connection.execute("PRAGMA table_info(step_options)").fetchall()}
        if "outcome_text" not in option_columns:
            connection.execute("ALTER TABLE step_options ADD COLUMN outcome_text TEXT NOT NULL DEFAULT ''")

        story_columns = {row["name"] for row in connection.execute("PRAGMA table_info(stories)").fetchall()}
        if "tags" not in story_columns:
            connection.execute("ALTER TABLE stories ADD COLUMN tags TEXT NOT NULL DEFAULT '[]'")
        if "editorial_sources" not in story_columns:
            connection.execute("ALTER TABLE stories ADD COLUMN editorial_sources TEXT NOT NULL DEFAULT '[]'")

    def seed_stories(self, stories_dir: Path) -> int:
        stories_dir.mkdir(parents=True, exist_ok=True)
        story_files = sorted(stories_dir.glob("*.json"))

        with self.connect() as connection:
            active_slugs = []
            for story_file in story_files:
                payload = json.loads(story_file.read_text(encoding="utf-8"))
                self._upsert_story(connection, payload)
                active_slugs.append(payload["slug"])

            if active_slugs:
                placeholders = ", ".join("?" for _ in active_slugs)
                connection.execute(
                    f"UPDATE stories SET is_active = 0, updated_at = ? WHERE slug NOT IN ({placeholders})",
                    (utc_now(), *active_slugs),
                )
            else:
                connection.execute("UPDATE stories SET is_active = 0, updated_at = ?", (utc_now(),))

        return len(story_files)

    def _upsert_story(self, connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
        self._validate_story_payload(payload)
        tags = self._serialize_tags(payload)
        editorial_sources = self._serialize_editorial_sources(payload)
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
                SET title = ?,
                    short_description = ?,
                    tags = ?,
                    intro_text = ?,
                    outro_text = ?,
                    editorial_sources = ?,
                    is_active = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    payload["title"],
                    payload["short_description"],
                    tags,
                    payload["intro_text"],
                    payload["outro_text"],
                    editorial_sources,
                    int(payload.get("is_active", True)),
                    timestamp,
                    story_id,
                ),
            )
        else:
            cursor = connection.execute(
                """
                INSERT INTO stories (
                    slug, title, short_description, tags, intro_text, outro_text, editorial_sources, is_active, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["slug"],
                    payload["title"],
                    payload["short_description"],
                    tags,
                    payload["intro_text"],
                    payload["outro_text"],
                    editorial_sources,
                    int(payload.get("is_active", True)),
                    timestamp,
                    timestamp,
                ),
            )
            story_id = cursor.lastrowid

        active_step_indexes = []
        for step_index, step in enumerate(payload["steps"], start=1):
            active_step_indexes.append(step_index)
            existing_step = connection.execute(
                "SELECT id FROM story_steps WHERE story_id = ? AND step_index = ?",
                (story_id, step_index),
            ).fetchone()

            if existing_step:
                step_id = existing_step["id"]
                connection.execute(
                    """
                    UPDATE story_steps
                    SET narrative_text = ?,
                        question = ?,
                        explanation = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        step["narrative_text"],
                        step["question"],
                        step["explanation"],
                        timestamp,
                        step_id,
                    ),
                )
            else:
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

            active_option_indexes = []
            for option_index, option in enumerate(step["options"], start=1):
                active_option_indexes.append(option_index)
                existing_option = connection.execute(
                    "SELECT id FROM step_options WHERE step_id = ? AND option_index = ?",
                    (step_id, option_index),
                ).fetchone()

                if existing_option:
                    connection.execute(
                        """
                        UPDATE step_options
                        SET text = ?,
                            outcome_text = ?,
                            is_correct = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            option["text"],
                            option["outcome_text"],
                            int(option["is_correct"]),
                            timestamp,
                            existing_option["id"],
                        ),
                    )
                else:
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

            option_placeholders = ", ".join("?" for _ in active_option_indexes)
            stale_options_query = f"""
                SELECT id
                FROM step_options
                WHERE step_id = ? AND option_index NOT IN ({option_placeholders})
            """
            stale_option_params = (step_id, *active_option_indexes)
            connection.execute(
                f"""
                DELETE FROM user_answers
                WHERE step_id = ?
                  AND (
                    selected_option_id IN ({stale_options_query})
                    OR correct_option_id IN ({stale_options_query})
                  )
                """,
                (step_id, *stale_option_params, *stale_option_params),
            )
            connection.execute(
                f"""
                DELETE FROM step_options
                WHERE step_id = ? AND option_index NOT IN ({option_placeholders})
                """,
                stale_option_params,
            )

        step_placeholders = ", ".join("?" for _ in active_step_indexes)
        stale_steps_query = f"""
            SELECT id
            FROM story_steps
            WHERE story_id = ? AND step_index NOT IN ({step_placeholders})
        """
        stale_step_params = (story_id, *active_step_indexes)
        connection.execute(
            f"DELETE FROM user_answers WHERE step_id IN ({stale_steps_query})",
            stale_step_params,
        )
        connection.execute(
            f"DELETE FROM story_steps WHERE story_id = ? AND step_index NOT IN ({step_placeholders})",
            stale_step_params,
        )

    def _validate_story_payload(self, payload: dict[str, Any]) -> None:
        required_story_fields = ["slug", "title", "short_description", "intro_text", "outro_text", "steps"]
        missing_fields = [field for field in required_story_fields if field not in payload]
        if missing_fields:
            raise ValueError(f"Story is missing required fields: {', '.join(missing_fields)}")

        tags = payload.get("tags", [])
        if tags is not None and not isinstance(tags, list):
            raise ValueError(f"Story '{payload['slug']}' field 'tags' must be a list")

        self._validate_editorial_sources(payload)

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

    def _serialize_tags(self, payload: dict[str, Any]) -> str:
        tags = []
        for tag in payload.get("tags") or []:
            value = str(tag).strip()
            if value and value not in tags:
                tags.append(value)
        return json.dumps(tags, ensure_ascii=False)

    def _validate_editorial_sources(self, payload: dict[str, Any]) -> None:
        sources = payload.get("editorial_sources", [])
        if sources is None:
            return
        if not isinstance(sources, list):
            raise ValueError(f"Story '{payload['slug']}' field 'editorial_sources' must be a list")

        for source_index, source in enumerate(sources, start=1):
            if not isinstance(source, dict):
                raise ValueError(f"Story '{payload['slug']}', source {source_index} must be an object")

            title = str(source.get("title", "")).strip()
            url = str(source.get("url", "")).strip()
            if not title or not url:
                raise ValueError(f"Story '{payload['slug']}', source {source_index} must contain 'title' and 'url'")
            if not url.startswith(("http://", "https://")):
                raise ValueError(f"Story '{payload['slug']}', source {source_index} has invalid url: {url}")

    def _serialize_editorial_sources(self, payload: dict[str, Any]) -> str:
        sources = []
        for source in payload.get("editorial_sources") or []:
            sources.append(
                {
                    "type": str(source.get("type", "")).strip(),
                    "title": str(source["title"]).strip(),
                    "url": str(source["url"]).strip(),
                }
            )
        return json.dumps(sources, ensure_ascii=False)

    def get_active_stories(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, slug, title, short_description, tags
                FROM stories
                WHERE is_active = 1
                ORDER BY title
                """
            ).fetchall()

    def get_completed_story_ids(self, chat_id: int) -> set[int]:
        with self.connect() as connection:
            return self._get_completed_story_ids(connection, chat_id)

    def get_story(self, story_id: int) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, slug, title, short_description, tags, intro_text, outro_text, editorial_sources
                FROM stories
                WHERE id = ? AND is_active = 1
                """,
                (story_id,),
            ).fetchone()

    def get_random_story(self) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, slug, title, short_description, tags, intro_text, outro_text, editorial_sources
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

    def get_user_profile(self, chat_id: int) -> dict[str, Any]:
        with self.connect() as connection:
            total_stories = int(
                connection.execute("SELECT COUNT(*) FROM stories WHERE is_active = 1").fetchone()[0]
            )
            answer_totals = connection.execute(
                """
                SELECT COUNT(*) AS total_answers, COALESCE(SUM(is_correct), 0) AS correct_answers
                FROM user_answers
                WHERE chat_id = ?
                """,
                (chat_id,),
            ).fetchone()
            total_answers = int(answer_totals["total_answers"])
            correct_answers = int(answer_totals["correct_answers"])

            completed_rows = connection.execute(
                """
                WITH answered AS (
                    SELECT
                        story_id,
                        COUNT(DISTINCT step_id) AS answered_steps,
                        MAX(answered_at) AS last_answered_at
                    FROM user_answers
                    WHERE chat_id = ?
                    GROUP BY story_id
                ),
                step_counts AS (
                    SELECT story_id, COUNT(*) AS total_steps
                    FROM story_steps
                    GROUP BY story_id
                )
                SELECT
                    s.id,
                    s.title,
                    s.short_description,
                    s.tags,
                    a.answered_steps,
                    sc.total_steps,
                    a.last_answered_at
                FROM answered a
                JOIN step_counts sc ON sc.story_id = a.story_id
                JOIN stories s ON s.id = a.story_id
                WHERE s.is_active = 1 AND a.answered_steps >= sc.total_steps
                ORDER BY a.last_answered_at DESC
                """,
                (chat_id,),
            ).fetchall()

            interacted_rows = connection.execute(
                """
                WITH answered AS (
                    SELECT
                        story_id,
                        COUNT(DISTINCT step_id) AS answered_steps,
                        MAX(answered_at) AS last_answered_at
                    FROM user_answers
                    WHERE chat_id = ?
                    GROUP BY story_id
                ),
                step_counts AS (
                    SELECT story_id, COUNT(*) AS total_steps
                    FROM story_steps
                    GROUP BY story_id
                )
                SELECT
                    s.id,
                    s.title,
                    s.short_description,
                    s.tags,
                    a.answered_steps,
                    sc.total_steps,
                    a.last_answered_at
                FROM answered a
                JOIN step_counts sc ON sc.story_id = a.story_id
                JOIN stories s ON s.id = a.story_id
                WHERE s.is_active = 1
                ORDER BY a.last_answered_at DESC
                """,
                (chat_id,),
            ).fetchall()

            continue_row = connection.execute(
                """
                SELECT
                    cs.current_story_id AS id,
                    cs.current_step_index,
                    cs.updated_at,
                    s.title,
                    s.short_description,
                    s.tags,
                    (
                        SELECT COUNT(*)
                        FROM story_steps
                        WHERE story_id = s.id
                    ) AS total_steps
                FROM chat_sessions cs
                JOIN stories s ON s.id = cs.current_story_id
                WHERE cs.chat_id = ? AND cs.status = 'active' AND s.is_active = 1
                """,
                (chat_id,),
            ).fetchone()
            achievements = self._get_achievement_payloads(connection, chat_id)

        favorite_source_rows = completed_rows or interacted_rows
        topic_counts: Counter[str] = Counter()
        for row in favorite_source_rows:
            for tag in self._deserialize_tags(row["tags"]):
                topic_counts[tag] += 1

        if not topic_counts and continue_row:
            for tag in self._deserialize_tags(continue_row["tags"]):
                topic_counts[tag] += 1

        last_answer_row = interacted_rows[0] if interacted_rows else None
        last_story = self._profile_story_from_answer(last_answer_row) if last_answer_row else None
        if continue_row and (not last_answer_row or str(continue_row["updated_at"]) >= str(last_answer_row["last_answered_at"])):
            last_story = self._profile_story_from_session(continue_row)

        return {
            "completed_stories": len(completed_rows),
            "total_stories": total_stories,
            "correct_answers": correct_answers,
            "total_answers": total_answers,
            "correct_percent": round(correct_answers / total_answers * 100) if total_answers else 0,
            "favorite_topics": [
                {"name": name, "count": count}
                for name, count in topic_counts.most_common(3)
            ],
            "last_story": last_story,
            "continue_story": self._profile_story_from_session(continue_row) if continue_row else None,
            "achievements": achievements,
            "achievements_summary": {
                "unlocked": sum(1 for achievement in achievements if achievement["unlocked"]),
                "total": len(achievements),
            },
        }

    def _get_achievement_payloads(self, connection: sqlite3.Connection, chat_id: int) -> list[dict[str, Any]]:
        stats = self._collect_achievement_stats(connection, chat_id)
        definitions = self._build_achievement_definitions(stats)
        self._unlock_reached_achievements(connection, chat_id, definitions)
        unlocked_at_by_key = self._get_unlocked_achievement_map(connection, chat_id)
        return self._build_achievement_payloads(definitions, unlocked_at_by_key)

    def _unlock_new_achievements(self, connection: sqlite3.Connection, chat_id: int) -> list[dict[str, Any]]:
        stats = self._collect_achievement_stats(connection, chat_id)
        definitions = self._build_achievement_definitions(stats)
        new_keys = self._unlock_reached_achievements(connection, chat_id, definitions)
        if not new_keys:
            return []

        unlocked_at_by_key = self._get_unlocked_achievement_map(connection, chat_id)
        return [
            achievement
            for achievement in self._build_achievement_payloads(definitions, unlocked_at_by_key)
            if achievement["key"] in new_keys
        ]

    def _collect_achievement_stats(self, connection: sqlite3.Connection, chat_id: int) -> dict[str, Any]:
        active_stories = connection.execute(
            """
            SELECT id, title, short_description, tags
            FROM stories
            WHERE is_active = 1
            """
        ).fetchall()
        completed_story_ids = self._get_completed_story_ids(connection, chat_id)
        leningrad_story_ids = {
            int(story["id"])
            for story in active_stories
            if self._is_leningrad_story(story)
        }

        return {
            "completed_stories": len(completed_story_ids),
            "total_stories": len(active_stories),
            "current_correct_streak": self._get_current_correct_streak(connection, chat_id),
            "perfect_completed_stories": len(self._get_perfect_completed_story_ids(connection, chat_id)),
            "leningrad_completed_stories": len(completed_story_ids & leningrad_story_ids),
            "leningrad_total_stories": len(leningrad_story_ids),
        }

    def _get_completed_story_ids(self, connection: sqlite3.Connection, chat_id: int) -> set[int]:
        rows = connection.execute(
            """
            WITH answered AS (
                SELECT story_id, COUNT(DISTINCT step_id) AS answered_steps
                FROM user_answers
                WHERE chat_id = ?
                GROUP BY story_id
            ),
            step_counts AS (
                SELECT story_id, COUNT(*) AS total_steps
                FROM story_steps
                GROUP BY story_id
            )
            SELECT a.story_id
            FROM answered a
            JOIN step_counts sc ON sc.story_id = a.story_id
            JOIN stories s ON s.id = a.story_id
            WHERE s.is_active = 1 AND a.answered_steps >= sc.total_steps
            """,
            (chat_id,),
        ).fetchall()
        return {int(row["story_id"]) for row in rows}

    def _get_perfect_completed_story_ids(self, connection: sqlite3.Connection, chat_id: int) -> set[int]:
        rows = connection.execute(
            """
            WITH answered AS (
                SELECT
                    story_id,
                    COUNT(DISTINCT step_id) AS answered_steps,
                    SUM(CASE WHEN is_correct = 0 THEN 1 ELSE 0 END) AS wrong_answers
                FROM user_answers
                WHERE chat_id = ?
                GROUP BY story_id
            ),
            step_counts AS (
                SELECT story_id, COUNT(*) AS total_steps
                FROM story_steps
                GROUP BY story_id
            )
            SELECT a.story_id
            FROM answered a
            JOIN step_counts sc ON sc.story_id = a.story_id
            JOIN stories s ON s.id = a.story_id
            WHERE s.is_active = 1
              AND a.answered_steps >= sc.total_steps
              AND a.wrong_answers = 0
            """,
            (chat_id,),
        ).fetchall()
        return {int(row["story_id"]) for row in rows}

    def _get_current_correct_streak(self, connection: sqlite3.Connection, chat_id: int) -> int:
        rows = connection.execute(
            """
            SELECT is_correct
            FROM user_answers
            WHERE chat_id = ?
            ORDER BY id DESC
            """,
            (chat_id,),
        ).fetchall()

        streak = 0
        for row in rows:
            if not int(row["is_correct"]):
                break
            streak += 1
        return streak

    def _is_leningrad_story(self, story: sqlite3.Row) -> bool:
        text = " ".join(
            [
                str(story["title"] or ""),
                str(story["short_description"] or ""),
                " ".join(self._deserialize_tags(story["tags"])),
            ]
        ).casefold()
        return "ленинград" in text or "блокад" in text

    def _build_achievement_definitions(self, stats: dict[str, Any]) -> list[dict[str, Any]]:
        completed_stories = int(stats["completed_stories"])
        total_stories = int(stats["total_stories"])
        leningrad_completed = int(stats["leningrad_completed_stories"])
        leningrad_total = int(stats["leningrad_total_stories"])

        return [
            self._achievement_definition(
                "first_story",
                "Первый сюжет",
                "Завершите первую историю.",
                completed_stories,
                1,
            ),
            self._achievement_definition(
                "five_stories",
                "Пять сюжетов",
                "Пройдите 5 историй.",
                completed_stories,
                5,
            ),
            self._achievement_definition(
                "ten_stories",
                "Десять сюжетов",
                "Пройдите 10 историй.",
                completed_stories,
                10,
            ),
            self._achievement_definition(
                "all_stories",
                "Вся библиотека",
                "Завершите все активные истории.",
                completed_stories,
                total_stories,
            ),
            self._achievement_definition(
                "leningrad_chronicle",
                "Ленинградская хроника",
                "Пройдите все сюжеты о Ленинграде и блокаде.",
                leningrad_completed,
                leningrad_total,
            ),
            self._achievement_definition(
                "perfect_story",
                "Безошибочный сюжет",
                "Завершите историю без неверных решений.",
                int(stats["perfect_completed_stories"]),
                1,
            ),
            self._achievement_definition(
                "ten_correct_streak",
                "Точная серия",
                "Дайте 10 верных решений подряд.",
                int(stats["current_correct_streak"]),
                10,
            ),
        ]

    def _achievement_definition(
        self,
        key: str,
        title: str,
        description: str,
        progress: int,
        target: int,
    ) -> dict[str, Any]:
        capped_progress = max(0, min(progress, target)) if target > 0 else 0
        return {
            "key": key,
            "title": title,
            "description": description,
            "progress": capped_progress,
            "target": target,
            "is_reached": target > 0 and progress >= target,
        }

    def _unlock_reached_achievements(
        self,
        connection: sqlite3.Connection,
        chat_id: int,
        definitions: list[dict[str, Any]],
    ) -> set[str]:
        unlocked_at_by_key = self._get_unlocked_achievement_map(connection, chat_id)
        timestamp = utc_now()
        new_keys: set[str] = set()

        for achievement in definitions:
            key = achievement["key"]
            if not achievement["is_reached"] or key in unlocked_at_by_key:
                continue

            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO user_achievements (chat_id, achievement_key, unlocked_at)
                VALUES (?, ?, ?)
                """,
                (chat_id, key, timestamp),
            )
            if cursor.rowcount:
                new_keys.add(key)

        return new_keys

    def _get_unlocked_achievement_map(self, connection: sqlite3.Connection, chat_id: int) -> dict[str, str]:
        rows = connection.execute(
            """
            SELECT achievement_key, unlocked_at
            FROM user_achievements
            WHERE chat_id = ?
            """,
            (chat_id,),
        ).fetchall()
        return {str(row["achievement_key"]): str(row["unlocked_at"]) for row in rows}

    def _build_achievement_payloads(
        self,
        definitions: list[dict[str, Any]],
        unlocked_at_by_key: dict[str, str],
    ) -> list[dict[str, Any]]:
        payloads = []
        for achievement in definitions:
            target = int(achievement["target"])
            unlocked_at = unlocked_at_by_key.get(achievement["key"])
            progress = target if unlocked_at and target > 0 else int(achievement["progress"])
            payloads.append(
                {
                    "key": achievement["key"],
                    "title": achievement["title"],
                    "description": achievement["description"],
                    "progress": progress,
                    "target": target,
                    "progress_percent": round(progress / target * 100) if target else 0,
                    "unlocked": unlocked_at is not None,
                    "unlocked_at": unlocked_at,
                }
            )
        return payloads

    def _deserialize_tags(self, raw_tags: str | None) -> list[str]:
        if not raw_tags:
            return []
        try:
            tags = json.loads(raw_tags)
        except json.JSONDecodeError:
            return []
        if not isinstance(tags, list):
            return []
        return [str(tag).strip() for tag in tags if str(tag).strip()]

    def _profile_story_from_answer(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if not row:
            return None
        total_steps = int(row["total_steps"])
        answered_steps = int(row["answered_steps"])
        return {
            "id": row["id"],
            "title": row["title"],
            "short_description": row["short_description"],
            "status": "completed" if answered_steps >= total_steps else "started",
            "current_step": min(answered_steps + 1, total_steps),
            "total_steps": total_steps,
            "progress_percent": round(answered_steps / total_steps * 100) if total_steps else 0,
            "last_activity_at": row["last_answered_at"],
        }

    def _profile_story_from_session(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if not row:
            return None
        total_steps = int(row["total_steps"])
        current_step = int(row["current_step_index"])
        return {
            "id": row["id"],
            "title": row["title"],
            "short_description": row["short_description"],
            "status": "active",
            "current_step": current_step,
            "total_steps": total_steps,
            "progress_percent": round(current_step / total_steps * 100) if total_steps else 0,
            "last_activity_at": row["updated_at"],
        }

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
                    s.outro_text,
                    s.editorial_sources
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
                "SELECT title, outro_text, editorial_sources FROM stories WHERE id = ?",
                (session["current_story_id"],),
            ).fetchone()
            story_score = self._get_story_score(connection, chat_id, int(session["current_story_id"]))
            new_achievements = self._unlock_new_achievements(connection, chat_id)

            return {
                "status": session_status,
                "story_id": session["current_story_id"],
                "story_title": story["title"],
                "outro_text": story["outro_text"],
                "editorial_sources": story["editorial_sources"],
                "score": story_score,
                "step_index": step["step_index"],
                "explanation": step["explanation"],
                "selected_text": selected_option["text"],
                "selected_outcome_text": selected_option["outcome_text"],
                "correct_text": correct_option["text"],
                "is_correct": bool(selected_option["is_correct"]),
                "next_step_index": next_step["step_index"] if next_step else None,
                "new_achievements": new_achievements,
            }

    def _get_story_score(self, connection: sqlite3.Connection, chat_id: int, story_id: int) -> dict[str, int]:
        answer_totals = connection.execute(
            """
            SELECT COUNT(*) AS total_answers, COALESCE(SUM(is_correct), 0) AS correct_answers
            FROM user_answers
            WHERE chat_id = ? AND story_id = ?
            """,
            (chat_id, story_id),
        ).fetchone()
        total_steps = int(
            connection.execute(
                "SELECT COUNT(*) FROM story_steps WHERE story_id = ?",
                (story_id,),
            ).fetchone()[0]
        )

        return {
            "correct_answers": int(answer_totals["correct_answers"]),
            "total_answers": int(answer_totals["total_answers"]),
            "total_steps": total_steps,
        }
