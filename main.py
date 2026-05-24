from __future__ import annotations

import argparse
import logging
import sys

from app.config import load_settings
from app.database import Database


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interactive WWII history Telegram bot")
    parser.add_argument(
        "--mode",
        choices=("web", "bot"),
        default="web",
        help="Run Telegram Mini App web server or the launcher bot",
    )
    parser.add_argument(
        "--seed-only",
        action="store_true",
        help="Create tables and load story JSON files into the database without starting the bot",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    args = parse_args()
    settings = load_settings()
    database = Database(settings.database_path)
    database.init_schema()
    loaded_stories = database.seed_stories(settings.stories_dir)

    logging.info("Loaded %s story file(s) into %s", loaded_stories, settings.database_path)

    if args.seed_only:
        return 0

    if args.mode == "web":
        import uvicorn

        from app.web import create_web_app

        web_app = create_web_app(settings, database)
        logging.info("Mini App web server is running on http://%s:%s", settings.webapp_host, settings.webapp_port)
        uvicorn.run(web_app, host=settings.webapp_host, port=settings.webapp_port)
        return 0

    if not settings.telegram_bot_token:
        logging.error("Environment variable TELEGRAM_BOT_TOKEN is empty")
        return 1

    from telegram.error import InvalidToken

    from app.bot import HistoryBot

    application = HistoryBot(database, webapp_url=settings.telegram_webapp_url).build_application(
        settings.telegram_bot_token
    )
    logging.info("Bot is running")
    try:
        application.run_polling(allowed_updates=["message", "callback_query"])
    except InvalidToken:
        logging.error(
            "Telegram rejected TELEGRAM_BOT_TOKEN. Create a new token via @BotFather and update PyCharm/.env."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
