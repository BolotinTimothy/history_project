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
    database_path: Path
    stories_dir: Path


def load_settings() -> Settings:
    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    database_path = Path(os.getenv("DATABASE_PATH", str(DEFAULT_DB_PATH))).expanduser().resolve()

    return Settings(
        telegram_bot_token=token,
        database_path=database_path,
        stories_dir=DEFAULT_STORIES_DIR,
    )
