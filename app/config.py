from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = BASE_DIR / "history_bot.db"
DEFAULT_STORIES_DIR = BASE_DIR / "stories"


@dataclass(slots=True)
class Settings:
    telegram_bot_token: str
    telegram_webapp_url: str
    telegram_auth_required: bool
    database_path: Path
    stories_dir: Path
    webapp_host: str
    webapp_port: int


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_settings() -> Settings:
    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip().strip("'\"")
    webapp_url = os.getenv("TELEGRAM_WEBAPP_URL", "").strip().strip("'\"")
    telegram_auth_required = _parse_bool(os.getenv("TELEGRAM_AUTH_REQUIRED"), default=False)
    database_path = Path(os.getenv("DATABASE_PATH", str(DEFAULT_DB_PATH))).expanduser().resolve()
    webapp_host = os.getenv("WEBAPP_HOST", "127.0.0.1").strip() or "127.0.0.1"
    webapp_port = int(os.getenv("WEBAPP_PORT", "8000"))

    return Settings(
        telegram_bot_token=token,
        telegram_webapp_url=webapp_url,
        telegram_auth_required=telegram_auth_required,
        database_path=database_path,
        stories_dir=DEFAULT_STORIES_DIR,
        webapp_host=webapp_host,
        webapp_port=webapp_port,
    )
