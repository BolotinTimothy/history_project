from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from urllib.parse import parse_qsl


@dataclass(slots=True)
class MiniAppUser:
    session_id: int
    user_id: int | None
    username: str | None
    first_name: str | None
    last_name: str | None
    is_development: bool = False


def parse_and_validate_init_data(
    init_data: str,
    bot_token: str,
    *,
    max_age_seconds: int = 86400,
) -> MiniAppUser | None:
    if not init_data or not bot_token:
        return None

    values = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=False))
    received_hash = values.pop("hash", "")
    if not received_hash:
        return None

    data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(values.items()))
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    calculated_hash = hmac.new(
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(calculated_hash, received_hash):
        return None

    auth_date = _parse_int(values.get("auth_date"))
    if auth_date is None or time.time() - auth_date > max_age_seconds:
        return None

    user_payload = _parse_json_object(values.get("user"))
    chat_payload = _parse_json_object(values.get("chat"))

    user_id = _parse_int(user_payload.get("id"))
    chat_id = _parse_int(chat_payload.get("id"))
    session_id = chat_id or user_id
    if session_id is None:
        return None

    username = _string_or_none(user_payload.get("username"))
    first_name = _string_or_none(user_payload.get("first_name"))
    last_name = _string_or_none(user_payload.get("last_name"))

    return MiniAppUser(
        session_id=session_id,
        user_id=user_id,
        username=username,
        first_name=first_name,
        last_name=last_name,
    )


def development_user() -> MiniAppUser:
    return MiniAppUser(
        session_id=0,
        user_id=0,
        username="local-dev",
        first_name="Local",
        last_name="Preview",
        is_development=True,
    )


def _parse_json_object(raw_value: str | None) -> dict[str, object]:
    if not raw_value:
        return {}
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _parse_int(raw_value: object) -> int | None:
    try:
        return int(raw_value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _string_or_none(raw_value: object) -> str | None:
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    return value or None
