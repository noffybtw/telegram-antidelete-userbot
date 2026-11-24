"""Одноразовый прогон по истории личных чатов: заливает текст в кэш.

Зачем: бот видит только то, что пришло при работающем скрипте. Всё, что было
до первого запуска, для него не существует — при удалении старого сообщения
придёт алерт без содержимого. Этот скрипт закрывает прошлое.

Только текст и метаданные. Историческое медиа не качается: годы фоток и видео
через VPS не протащить, да и смысла нет — файл помечается как неархивированный.

Прогон возобновляемый: прервал на середине — запустил снова, продолжит с того же
места. Прогресс хранится в самой базе.

    systemctl stop telegram-antidelete     # сессия занята сервисом
    ./venv/bin/python3 backfill.py --scan  # сначала посмотреть объём
    ./venv/bin/python3 backfill.py
    systemctl start telegram-antidelete

ВАЖНО: пока идёт прогон, сервис остановлен и удаления не отслеживаются.
"""

import argparse
import asyncio
import logging
import sys
import time

from telethon import TelegramClient
from telethon.errors import FloodWaitError

import config
from cache import Cache
from util import classify_chat, display_name, media_kind

logging.basicConfig(format="%(asctime)s %(levelname)-7s %(message)s",
                    level=logging.INFO, datefmt="%H:%M:%S")
logging.getLogger("telethon").setLevel(logging.WARNING)
log = logging.getLogger("backfill")

SAVE_EVERY = 200        # как часто фиксировать прогресс
SLEEP_BETWEEN_CHATS = 0.5


async def collect_dialogs(client, db: Cache, me_id: int):
    """Личные чаты и небольшие группы. Каналы, боты и большие чаты — мимо.

    Решение по каждому чату сразу пишется в базу: живой сервис потом берёт
    готовый ответ, а не переспрашивает Telegram по первому сообщению.
    """
    kept, skipped = [], []
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        chat_id = dialog.id
        if chat_id == config.LOG_CHAT_ID:
            continue
        kind, members, monitored, title = await classify_chat(client, entity, me_id)
        db.set_chat(chat_id, kind, title, members, monitored)
        (kept if monitored else skipped).append((chat_id, entity, kind, members, title))
        if kind == "supergroup":
            await asyncio.sleep(0.3)   # тут был отдельный запрос за числом участников
    return kept, skipped


async def chat_total(client, entity) -> int:
    try:
        return (await client.get_messages(entity, limit=0)).total
    except Exception:
        return 0


async def backfill_chat(client, db: Cache, entity, me, args) -> tuple:
    """Возвращает (сколько добавлено, сколько просмотрено, дошли ли до конца)."""
    from telethon import utils as tl_utils
    chat_id = tl_utils.get_peer_id(entity)
    is_group = chat_id < 0
    senders = {}   # id -> (имя, username), чтобы не дёргать Telegram на каждое сообщение
    done_key, offset_key = f"backfill:done:{chat_id}", f"backfill:offset:{chat_id}"

    if db.get_meta(done_key) == "1" and not args.force:
        return 0, 0, True

    offset = int(db.get_meta(offset_key, "0"))
    chat_title = display_name(entity)
    peer_username = getattr(entity, "username", None)
    added = seen = 0
    finished = False
    cutoff = int(time.time()) - args.days * 86400 if args.days else None

    while not finished:
        try:
            async for msg in client.iter_messages(entity, offset_id=offset):
                seen += 1
                offset = msg.id

                if msg.action is not None:          # служебные события чата
                    continue
                sent_ts = int(msg.date.timestamp())
                if cutoff and sent_ts < cutoff:
                    finished = True
                    break

                outgoing = bool(msg.out)
                has_media = msg.media is not None

                if outgoing:
                    s_id, s_name, s_user = me.id, display_name(me), me.username
                elif not is_group:
                    s_id, s_name, s_user = chat_id, chat_title, peer_username
                else:
                    s_id = msg.sender_id
                    if s_id not in senders:
                        sender = msg.sender
                        senders[s_id] = (display_name(sender),
                                         getattr(sender, "username", None))
                    s_name, s_user = senders[s_id]

                added += db.add_history_message(
                    chat_id=chat_id,
                    msg_id=msg.id,
                    sender_id=s_id,
                    sender_name=s_name,
                    sender_username=s_user,
                    chat_title=chat_title,
                    text=msg.text or None,
                    media_type=media_kind(msg) if has_media else None,
                    media_note="из истории, файл не архивировался" if has_media else None,
                    is_outgoing=int(outgoing),
                    sent_ts=sent_ts,
                )

                if seen % SAVE_EVERY == 0:
                    db.set_meta(offset_key, offset)
                    print(f"    {chat_title}: {seen} просмотрено, {added} добавлено",
                          end="\r", flush=True)
                if args.limit_per_chat and seen >= args.limit_per_chat:
                    finished = True
                    break
            else:
                finished = True   # генератор исчерпан — чат прочитан до конца
        except FloodWaitError as e:
            db.set_meta(offset_key, offset)
            log.warning("FloodWait %s сек — жду и продолжаю с того же места", e.seconds)
            await asyncio.sleep(e.seconds + 2)
        except Exception as e:
            db.set_meta(offset_key, offset)
            log.warning("%s: прервалось (%s), продолжу при следующем запуске", chat_title, e)
            return added, seen, False

    db.set_meta(offset_key, offset)
    if not args.limit_per_chat and not args.days:
        db.set_meta(done_key, "1")
    return added, seen, True


async def main() -> None:
    ap = argparse.ArgumentParser(description="Залить историю личных чатов в кэш")
    ap.add_argument("--scan", action="store_true", help="только посчитать объём, ничего не писать")
    ap.add_argument("--days", type=int, default=0, help="взять только последние N дней (0 = всю историю)")
    ap.add_argument("--limit-per-chat", type=int, default=0, help="не больше N сообщений на чат")
    ap.add_argument("--force", action="store_true", help="перечитать даже уже пройденные чаты")
    args = ap.parse_args()

    config.require_credentials()
    db = Cache()
    client = TelegramClient(config.SESSION_PATH, config.API_ID, config.API_HASH,
                            flood_sleep_threshold=300)
    await client.connect()
    if not await client.is_user_authorized():
        raise SystemExit("Сессия не авторизована. Сначала: python3 setup.py")

    me = await client.get_me()
    log.info("Аккаунт: %s (id %s)", display_name(me), me.id)

    log.info("Разбираю диалоги (личные чаты и группы до %s участников)…",
             config.GROUP_MAX_MEMBERS)
    kept, skipped = await collect_dialogs(client, db, me.id)
    log.info("Отслеживаем: %s чатов | пропускаем: %s", len(kept), len(skipped))

    by_kind = {}
    for _, _, kind, _, _ in kept:
        by_kind[kind] = by_kind.get(kind, 0) + 1
    log.info("Из них: %s", ", ".join(f"{k}: {v}" for k, v in sorted(by_kind.items())))

    if args.scan:
        total = 0
        for i, (chat_id, entity, kind, members, title) in enumerate(kept, 1):
            n = await chat_total(client, entity)
            total += n
            tag = f"[{kind}{'' if members is None else f', {members}'}]"
            print(f"  {i:3}. {title[:34]:<34} {tag:<20} {n:>7} сообщ.")
            await asyncio.sleep(0.2)
        print(f"\nВсего: {total} сообщений в {len(kept)} чатах")
        print(f"Оценка объёма в SQLite: ~{total * 250 / 1024 / 1024:.0f} МБ")
        await client.disconnect()
        return

    started = time.time()
    total_added = total_seen = 0
    for i, (chat_id, entity, kind, members, name) in enumerate(kept, 1):
        log.info("[%s/%s] %s [%s%s]", i, len(kept), name, kind,
                 "" if members is None else f", {members} участн.")
        added, seen, ok = await backfill_chat(client, db, entity, me, args)
        total_added += added
        total_seen += seen
        if seen:
            log.info("    добавлено %s из %s просмотренных%s",
                     added, seen, "" if ok else " (не до конца, продолжится при перезапуске)")
        await asyncio.sleep(SLEEP_BETWEEN_CHATS)

    mins = (time.time() - started) / 60
    log.info("Готово за %.1f мин: добавлено %s сообщений (просмотрено %s)",
             mins, total_added, total_seen)
    log.info("Теперь: systemctl start telegram-antidelete")
    await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано. Прогресс сохранён — просто запусти снова.")
        sys.exit(130)
