from __future__ import annotations

import html
import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from app.database import Database


logger = logging.getLogger(__name__)

MENU_CALLBACK = "menu"
RANDOM_STORY_CALLBACK = "story:random"
STORY_CALLBACK_PREFIX = "story:"
ANSWER_CALLBACK_PREFIX = "answer:"


class HistoryBot:
    def __init__(self, database: Database) -> None:
        self.database = database

    def build_application(self, token: str) -> Application:
        application = Application.builder().token(token).build()
        application.add_handler(CommandHandler("start", self.start_command))
        application.add_handler(CommandHandler("stories", self.start_command))
        application.add_handler(CallbackQueryHandler(self.handle_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text_message))
        return application

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat:
            return
        await self.send_story_menu(
            chat_id=update.effective_chat.id,
            context=context,
            text=(
                "Выберите историю о событиях Великой Отечественной войны.\n\n"
                "Можно начать конкретный сюжет или попросить бота выбрать случайную историю."
            ),
        )

    async def handle_text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat:
            return
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Для начала используйте /start и выбирайте варианты кнопками под сообщениями.",
        )

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query or not update.effective_chat:
            return

        data = query.data or ""
        await query.answer()
        await self._clear_old_keyboard(query)

        if data == MENU_CALLBACK:
            await self.send_story_menu(
                chat_id=update.effective_chat.id,
                context=context,
                text="Выберите новую историю или возьмите случайную.",
            )
            return

        if data == RANDOM_STORY_CALLBACK:
            story = self.database.get_random_story()
            if not story:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="Пока нет активных историй. Добавьте JSON-файл в папку stories и перезапустите бота.",
                )
                return

            await self.start_story(update, context, story["id"])
            return

        if data.startswith(STORY_CALLBACK_PREFIX):
            story_id = int(data.split(":")[-1])
            await self.start_story(update, context, story_id)
            return

        if data.startswith(ANSWER_CALLBACK_PREFIX):
            option_id = int(data.split(":")[-1])
            await self.process_answer(update, context, option_id)
            return

    async def start_story(self, update: Update, context: ContextTypes.DEFAULT_TYPE, story_id: int) -> None:
        if not update.effective_chat:
            return

        story = self.database.get_story(story_id)
        if not story:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Не удалось найти эту историю. Попробуйте выбрать другую.",
            )
            return

        user = update.effective_user
        self.database.start_story_for_chat(
            chat_id=update.effective_chat.id,
            user_id=user.id if user else None,
            username=user.username if user else None,
            story_id=story_id,
        )

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                f"<b>{html.escape(story['title'])}</b>\n\n"
                f"{html.escape(story['short_description'])}\n\n"
                f"{html.escape(story['intro_text'])}"
            ),
            parse_mode=ParseMode.HTML,
        )
        await self.send_current_step(update.effective_chat.id, context)

    async def send_story_menu(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
        stories = self.database.get_active_stories()
        keyboard = [
            [InlineKeyboardButton(story["title"], callback_data=f"{STORY_CALLBACK_PREFIX}{story['id']}")]
            for story in stories
        ]
        keyboard.append([InlineKeyboardButton("Случайная история", callback_data=RANDOM_STORY_CALLBACK)])

        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    async def send_current_step(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
        session = self.database.get_active_session(chat_id)
        if not session:
            await self.send_story_menu(
                chat_id=chat_id,
                context=context,
                text="Активная история не найдена. Выберите сюжет заново.",
            )
            return

        step = self.database.get_story_step(session["current_story_id"], session["current_step_index"])
        if not step:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Не удалось загрузить следующий этап истории.",
            )
            return

        options = self.database.get_step_options(step["id"])
        keyboard = [
            [InlineKeyboardButton(option["text"], callback_data=f"{ANSWER_CALLBACK_PREFIX}{option['id']}")]
            for option in options
        ]
        keyboard.append([InlineKeyboardButton("К списку историй", callback_data=MENU_CALLBACK)])

        message_text = (
            f"<b>{html.escape(step['story_title'])}</b>\n"
            f"Этап {step['step_index']} из {step['total_steps']}\n\n"
            f"{html.escape(step['narrative_text'])}\n\n"
            f"<b>Что вы сделаете?</b>\n"
            f"{html.escape(step['question'])}"
        )

        await context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    async def process_answer(self, update: Update, context: ContextTypes.DEFAULT_TYPE, option_id: int) -> None:
        if not update.effective_chat:
            return

        result = self.database.submit_answer(update.effective_chat.id, option_id)
        if not result:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Сессия не найдена. Начните заново через /start.",
            )
            return

        if result["status"] == "stale":
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Этот этап уже завершён или кнопка устарела. Продолжите текущую историю ниже.",
            )
            return

        verdict = "Верный выбор." if result["is_correct"] else "Не совсем так."
        feedback_text = "\n\n".join(
            [
                f"<b>{verdict}</b>",
                f"Ваш вариант: {html.escape(result['selected_text'])}",
                f"Что произошло после такого выбора: {html.escape(result['selected_outcome_text'])}",
                f"Исторически верное решение: {html.escape(result['correct_text'])}",
                html.escape(result["explanation"]),
            ]
        )

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=feedback_text,
            parse_mode=ParseMode.HTML,
        )

        if result["status"] == "completed":
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"<b>История завершена: {html.escape(result['story_title'])}</b>\n\n"
                    f"{html.escape(result['outro_text'])}"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Выбрать новую историю", callback_data=MENU_CALLBACK)]]
                ),
            )
            return

        await self.send_current_step(update.effective_chat.id, context)

    async def _clear_old_keyboard(self, query) -> None:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            logger.debug("Could not clear keyboard for message %s", query.message)
