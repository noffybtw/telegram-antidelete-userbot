"""Первый запуск: логин, выбор лог-чата, проверка связи.

Запускать вручную и интерактивно — Telegram спросит код из приложения,
и пароль 2FA, если он стоит. После этого рядом появится antidelete.session,
дальше main.py логинится сам.
"""

import asyncio
import re
from pathlib import Path

from telethon import TelegramClient, utils
from telethon.tl.functions.channels import CreateChannelRequest

import config

ENV_PATH = Path(__file__).resolve().parent / ".env"


def write_env(key: str, value) -> None:
    text = ENV_PATH.read_text()
    line = f"{key}={value}"
    if re.search(rf"^{key}=.*$", text, re.M):
        text = re.sub(rf"^{key}=.*$", line, text, count=1, flags=re.M)
    else:
        text = text.rstrip("\n") + f"\n{line}\n"
    ENV_PATH.write_text(text)
    ENV_PATH.chmod(0o600)
    print(f"  → {key} записан в .env")


async def choose_log_chat(client, me) -> int:
    print("\nКуда слать алерты?")
    print("  1) создать новый приватный канал «Anti-Delete Log» (рекомендую)")
    print("  2) Saved Messages (Избранное) — ничего создавать не надо, но смешается с заметками")
    print("  3) ввести ID уже существующего чата вручную")
    choice = input("Выбор [1]: ").strip() or "1"

    if choice == "1":
        result = await client(CreateChannelRequest(
            title="Anti-Delete Log",
            about="Удалённые и отредактированные сообщения. Пишет antidelete-юзербот.",
            megagroup=False,
        ))
        channel = result.chats[0]
        chat_id = utils.get_peer_id(channel)
        print(f"  Канал создан: «{channel.title}», id {chat_id}")
        return chat_id

    if choice == "2":
        print(f"  Saved Messages, id {me.id}")
        return me.id

    while True:
        raw = input("  ID чата (например -1001234567890): ").strip()
        try:
            return int(raw)
        except ValueError:
            print("  Нужно именно число. У канала id начинается с -100, у личного\n"
                  "  чата — просто цифры. Не знаешь id — перешли любое сообщение из\n"
                  "  нужного чата боту @userinfobot, он его покажет.")


async def main() -> None:
    config.require_credentials()

    client = TelegramClient(config.SESSION_PATH, config.API_ID, config.API_HASH)
    # Telethon не принимает phone=None: нужен либо строка, либо callable.
    phone = config.PHONE or (
        lambda: input("Номер телефона в международном формате (+420123456789): ").strip()
    )
    await client.start(phone=phone)

    me = await client.get_me()
    name = " ".join(p for p in (me.first_name, me.last_name) if p)
    print(f"\n✅ Залогинен как {name} (@{me.username}, id {me.id})")
    print(f"   Файл сессии: {config.SESSION_PATH}.session")
    print("   ⚠️  Этот файл = полный доступ к аккаунту без пароля и 2FA. Никому и никуда.")

    if not config.PHONE:
        write_env("PHONE", me.phone and f"+{me.phone}" or "")

    chat_id = config.LOG_CHAT_ID
    if chat_id:
        print(f"\nLOG_CHAT_ID уже прописан: {chat_id}")
        if (input("Оставить его? [Y/n]: ").strip().lower() or "y") != "y":
            chat_id = await choose_log_chat(client, me)
            write_env("LOG_CHAT_ID", chat_id)
    else:
        chat_id = await choose_log_chat(client, me)
        write_env("LOG_CHAT_ID", chat_id)

    await client.send_message(
        chat_id,
        "✅ <b>Связь есть</b>\nСюда будут падать удалённые и отредактированные сообщения "
        "из личных чатов.",
        parse_mode="html",
    )
    print(f"\n✅ Тестовое сообщение ушло в чат {chat_id} — проверь, что дошло.")
    print("\nДальше: python3 main.py")
    await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (EOFError, KeyboardInterrupt):
        print("\n\nВход прерван. Ничего страшного — запусти скрипт заново.")
        raise SystemExit(1)
