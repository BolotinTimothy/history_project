# Telegram Mini App с интерактивными историческими сюжетами

Проект теперь работает как Telegram Mini App: Python-сервер отдаёт веб-интерфейс и JSON API, а Telegram-бот используется как точка входа с кнопкой открытия приложения. Данные историй по-прежнему лежат в `stories/*.json` и сидируются в SQLite.

## Что умеет приложение

- показывает список доступных историй в Mini App;
- открывает случайную историю;
- ведёт пользователя по этапам сюжета;
- сохраняет текущий прогресс пользователя в SQLite;
- проверяет выбор, показывает последствие решения и исторически верный вариант;
- показывает источники после завершения истории;
- валидирует Telegram `initData` в production-режиме.

## Структура

```text
app/
  bot.py                 # Telegram-бот с кнопкой запуска Mini App и старым fallback-меню
  config.py              # переменные окружения и настройки запуска
  database.py            # схема БД, сидирование, сессии и ответы
  telegram_auth.py       # проверка Telegram WebApp initData
  web.py                 # FastAPI-приложение и JSON API
  static/
    index.html           # оболочка Mini App
    styles.css           # интерфейс
    app.js               # клиентская логика
stories/
  *.json                 # истории для загрузки в БД
main.py                  # точка входа
```

## Установка

```bash
pip install -r requirements.txt
```

## Локальный запуск Mini App

```bash
python main.py
```

По умолчанию сервер стартует на `http://127.0.0.1:8000`. В обычном браузере приложение работает в preview-режиме без Telegram-подписи.

## Запуск бота-кнопки

Для Telegram Mini App нужен публичный HTTPS URL, например адрес после деплоя или туннеля.

```bash
$env:TELEGRAM_BOT_TOKEN="123456:bot-token"
$env:TELEGRAM_WEBAPP_URL="https://example.com"
python main.py --mode bot
```

После этого `/start` отправит кнопку `Открыть Mini App`. Команда `/stories` оставлена для старого inline-сценария.

## Production-переменные

```text
TELEGRAM_BOT_TOKEN=123456:bot-token
TELEGRAM_WEBAPP_URL=https://example.com
TELEGRAM_AUTH_REQUIRED=1
DATABASE_PATH=./history_bot.db
WEBAPP_HOST=0.0.0.0
WEBAPP_PORT=8000
```

`TELEGRAM_AUTH_REQUIRED=1` запрещает неподписанные запросы к API. Для локального preview можно не задавать эту переменную.

## Сидирование без запуска сервера

```bash
python main.py --seed-only
```
