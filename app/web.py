from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import Settings, load_settings
from app.database import Database
from app.telegram_auth import MiniAppUser, development_user, parse_and_validate_init_data


STATIC_DIR = Path(__file__).resolve().parent / "static"
INDEX_FILE = STATIC_DIR / "index.html"


def create_web_app(settings: Settings | None = None, database: Database | None = None) -> FastAPI:
    settings = settings or load_settings()
    database = database or Database(settings.database_path)
    database.init_schema()
    database.seed_stories(settings.stories_dir)

    web_app = FastAPI(title="History Mini App")
    web_app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    def get_current_user(
        x_telegram_init_data: str | None = Header(default=None, alias="X-Telegram-Init-Data"),
    ) -> MiniAppUser:
        user = parse_and_validate_init_data(
            x_telegram_init_data or "",
            settings.telegram_bot_token,
        )
        if user:
            return user

        if settings.telegram_auth_required:
            raise HTTPException(status_code=401, detail="Telegram initData is missing or invalid")

        return development_user()

    @web_app.get("/")
    async def index() -> FileResponse:
        return FileResponse(INDEX_FILE)

    @web_app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @web_app.get("/api/state")
    async def state(user: MiniAppUser = Depends(get_current_user)) -> dict[str, Any]:
        return {
            "user": _serialize_user(user),
            "stories": _serialize_stories(database.get_active_stories()),
            "session": _build_session_payload(database, user.session_id),
            "profile": database.get_user_profile(user.session_id),
        }

    @web_app.post("/api/stories/random/start")
    async def start_random_story(user: MiniAppUser = Depends(get_current_user)) -> dict[str, Any]:
        story = database.get_random_story()
        if not story:
            raise HTTPException(status_code=404, detail="Нет активных историй")
        return _start_story(database, user, int(story["id"]))

    @web_app.post("/api/stories/{story_id}/start")
    async def start_story(story_id: int, user: MiniAppUser = Depends(get_current_user)) -> dict[str, Any]:
        story = database.get_story(story_id)
        if not story:
            raise HTTPException(status_code=404, detail="История не найдена")
        return _start_story(database, user, story_id)

    @web_app.post("/api/answers/{option_id}")
    async def submit_answer(option_id: int, user: MiniAppUser = Depends(get_current_user)) -> dict[str, Any]:
        result = database.submit_answer(user.session_id, option_id)
        if not result:
            raise HTTPException(status_code=404, detail="Активная история не найдена")
        if result["status"] == "stale":
            raise HTTPException(status_code=409, detail="Этот вариант больше не относится к текущему шагу")

        payload: dict[str, Any] = {
            "feedback": {
                "is_correct": result["is_correct"],
                "verdict": "Верный выбор." if result["is_correct"] else "Не совсем так.",
                "selected_text": result["selected_text"],
                "selected_outcome_text": result["selected_outcome_text"],
                "correct_text": result["correct_text"],
                "explanation": result["explanation"],
            },
            "status": result["status"],
        }

        if result["status"] == "completed":
            payload["completion"] = {
                "story_id": result["story_id"],
                "story_title": result["story_title"],
                "outro_text": result["outro_text"],
                "editorial_sources": _parse_editorial_sources(result["editorial_sources"]),
            }
            payload["session"] = None
        else:
            payload["session"] = _build_session_payload(database, user.session_id)

        payload["profile"] = database.get_user_profile(user.session_id)
        return payload

    return web_app


def _start_story(database: Database, user: MiniAppUser, story_id: int) -> dict[str, Any]:
    database.start_story_for_chat(
        chat_id=user.session_id,
        user_id=user.user_id,
        username=user.username,
        story_id=story_id,
    )
    return {
        "session": _build_session_payload(database, user.session_id),
        "profile": database.get_user_profile(user.session_id),
    }


def _build_session_payload(database: Database, session_id: int) -> dict[str, Any] | None:
    session = database.get_active_session(session_id)
    if not session:
        return None

    story = database.get_story(int(session["current_story_id"]))
    if not story:
        return None

    step = database.get_story_step(int(session["current_story_id"]), int(session["current_step_index"]))
    if not step:
        return None

    options = database.get_step_options(int(step["id"]))
    total_steps = int(step["total_steps"])
    current_step = int(step["step_index"])

    return {
        "story": {
            "id": story["id"],
            "slug": story["slug"],
            "title": story["title"],
            "short_description": story["short_description"],
            "intro_text": story["intro_text"],
        },
        "step": {
            "id": step["id"],
            "index": current_step,
            "total": total_steps,
            "narrative_text": step["narrative_text"],
            "question": step["question"],
            "options": [
                {
                    "id": option["id"],
                    "text": option["text"],
                }
                for option in options
            ],
        },
        "progress": {
            "current": current_step,
            "total": total_steps,
            "percent": round(current_step / total_steps * 100) if total_steps else 0,
        },
    }


def _serialize_stories(stories: list[Any]) -> list[dict[str, Any]]:
    return [
        {
            "id": story["id"],
            "slug": story["slug"],
            "title": story["title"],
            "short_description": story["short_description"],
            "tags": _parse_tags(story["tags"]),
        }
        for story in stories
    ]


def _serialize_user(user: MiniAppUser) -> dict[str, Any]:
    return {
        "id": user.user_id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "is_development": user.is_development,
    }


def _parse_editorial_sources(raw_sources: str | None) -> list[dict[str, str]]:
    if not raw_sources:
        return []
    try:
        sources = json.loads(raw_sources)
    except json.JSONDecodeError:
        return []
    if not isinstance(sources, list):
        return []

    parsed_sources = []
    for source in sources:
        if not isinstance(source, dict):
            continue
        title = str(source.get("title", "")).strip()
        url = str(source.get("url", "")).strip()
        source_type = str(source.get("type", "")).strip()
        if title and url:
            parsed_sources.append({"type": source_type, "title": title, "url": url})
    return parsed_sources


def _parse_tags(raw_tags: str | None) -> list[str]:
    if not raw_tags:
        return []
    try:
        tags = json.loads(raw_tags)
    except json.JSONDecodeError:
        return []
    if not isinstance(tags, list):
        return []
    return [str(tag).strip() for tag in tags if str(tag).strip()]
