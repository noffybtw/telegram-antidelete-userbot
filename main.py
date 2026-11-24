"""Telegram Anti-Delete — юзербот, ловящий "Delete for everyone".

Следит за личными чатами и небольшими группами (порог в .env).
Каналы, боты и большие чаты игнорируются.

Логика:
  входящее сообщение -> строка в SQLite -> файл на диск
  удаление           -> достаём из кэша -> алерт в лог-чат -> локальный файл стираем
Медиа дальше живёт в облаке Telegram, на диске остаётся только хвост
неудалённых сообщений (скользящее окно из .env).
"""

import asyncio
import html
import logging
import os
import random
import re
import shutil
import signal
import sys
import time
from collections import defaultdict

from telethon import TelegramClient, events, types
from telethon.errors import (
    FileReferenceExpiredError,
    FilerefUpgradeNeededError,
    FloodWaitError,
)
from telethon.tl.functions.bots import SetBotCommandsRequest
from telethon.tl.functions.stories import (
    GetAllStoriesRequest,
    GetPeerStoriesRequest,
    GetPinnedStoriesRequest,
)
from telethon.tl.functions.users import GetFullUserRequest

import alerts
import config
import downloader
import export
import transcribe
from cache import Cache, chat_stats_ro
from util import classify_chat, display_name, is_voice_kind, media_kind

# Всё, что создаёт процесс — база, медиа, сессия — только для владельца.
# Не полагаемся на umask окружения: под другим деплоем он может быть 022.
os.umask(0o077)

logging.basicConfig(
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("telethon").setLevel(logging.WARNING)
log = logging.getLogger("antidelete")

config.require_credentials()
config.require_log_chat()

db = Cache()
client = TelegramClient(
    config.SESSION_PATH, config.API_ID, config.API_HASH, flood_sleep_threshold=120  # gitleaks:allow (keyword argument, not a secret)
)

UNKNOWN = "unknown"
_buffers = {}   # ключ чата -> накопитель удалений
_me_id = None
_send_lock = asyncio.Lock()   # сериализует отправку: см. notify / notify_group
_rand = random.SystemRandom()  # разброс интервалов, чтобы не палить машинную ровность
_monitor_cache = {}           # chat_id -> (следим ли, когда решили)
_monitor_inflight = {}        # chat_id -> Future: чат уже выясняют, не дублируем
bot = None                    # клиент-бот: только доставка алертов и команды
_bot_chat = None              # куда бот шлёт алерты
MAX_FLOOD_WAIT = 900          # дольше 15 мин ждать скачивание не будем
_bg_tasks = set()             # держим ссылки на фоновые задачи от сборки GC


def _task_done(t) -> None:
    """Снять задачу с учёта и НЕ проглотить её падение.

    Эти задачи никто не await'ит, поэтому без явного разбора исключение всплыло
    бы «Task exception was never retrieved» когда-нибудь при сборке мусора —
    без привязки к событию, из-за которого упало.
    """
    _bg_tasks.discard(t)
    if t.cancelled():
        return
    exc = t.exception()
    if exc is not None:
        log.error("Фоновая задача упала: %s", t.get_name(), exc_info=exc)


def spawn(coro):
    """create_task с удержанием ссылки — иначе задачу может собрать GC на середине."""
    t = asyncio.create_task(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_task_done)
    return t


def paused() -> bool:
    return db.get_meta("paused", "0") == "1"


# Переключаемые из меню бота настройки: ключ -> (метка, дефолт из .env).
# Значение в meta ("1"/"0") перекрывает дефолт. Так работает и /pause.
TOGGLES = {
    "track_edits":     ("Правки сообщений",        lambda: config.TRACK_EDITS),
    "own_deletes":     ("Удаление твоих сообщений", lambda: config.ALERT_OWN_DELETES),
    "view_once":       ("Сохранять «огонёк»",       lambda: config.CAPTURE_VIEW_ONCE),
    "unknown_deletes": ("Неизвестные удаления",      lambda: config.ALERT_UNKNOWN_DELETES),
    "block_check":     ("Детект блокировки",         lambda: config.BLOCK_CHECK_ENABLED),
    "transcribe":      ("Расшифровка голосовых",     lambda: config.TRANSCRIBE_ENABLED),
    "downloader":      ("Скачивание по ссылке",      lambda: config.DOWNLOADER_ENABLED),
    "ask_quality":     ("Спрашивать качество",       lambda: config.ASK_QUALITY),
    "profile_watch":   ("Слежка за профилем",        lambda: config.PROFILE_WATCH_ENABLED),
    "online_watch":    ("Онлайн-трекинг",            lambda: config.ONLINE_WATCH_ENABLED),
}


def setting(key: str) -> bool:
    """Эффективное значение переключателя: meta если задано, иначе дефолт .env."""
    v = db.get_meta("set:" + key)
    if v is not None:
        return v == "1"
    label_default = TOGGLES.get(key)
    return bool(label_default[1]()) if label_default else False


def set_toggle(key: str, value: bool) -> None:
    db.set_meta("set:" + key, "1" if value else "0")


# ---------------------------------------------------------------- подписки на людей

# Функции, которые можно навесить на конкретного человека. Ключ идёт и в базу,
# и в callback-данные кнопок — менять его после релиза нельзя.
SUB_FEATURES = {
    "voice":   ("🎙", "Расшифровывать его ГС"),
    "profile": ("👤", "Следить за профилем"),
    "online":  ("🟢", "Сообщать о заходах"),
}

# Подписки держим в памяти: sub_has дёргается на каждое событие статуса, а их
# от активных контактов прилетают сотни в час — ходить за каждым в базу незачем.
_subs = {}


def reload_subs() -> None:
    global _subs
    _subs = db.sub_all()


def sub_has(feature: str, peer_id) -> bool:
    return peer_id is not None and peer_id in _subs.get(feature, ())


def sub_set(feature: str, peer_id: int, on: bool, title: str = None) -> None:
    if on:
        db.sub_add(feature, peer_id, title)
    else:
        db.sub_del(feature, peer_id)
    reload_subs()
    if on and feature == "profile":
        # Точку отсчёта снимаем сразу, а не на ближайшем плановом опросе:
        # иначе изменение, случившееся до первого опроса, войдёт в сам снимок
        # и никогда не будет замечено.
        spawn(snapshot_profile(peer_id))


# ---------------------------------------------------------------- кэширование


async def should_monitor(event) -> bool:
    """Следим ли за этим чатом. Решение кэшируется, перепроверяется раз в сутки."""
    chat_id = event.chat_id
    if chat_id is None or chat_id == config.LOG_CHAT_ID:
        return False

    ttl = config.CHAT_RECHECK_HOURS * 3600
    now = time.time()

    cached = _monitor_cache.get(chat_id)
    if cached and now - cached[1] < ttl:
        return cached[0]

    row = db.get_chat(chat_id)
    if row is not None and row["override"] is not None:
        val = bool(row["override"])          # ручное решение всегда сильнее авто
        _monitor_cache[chat_id] = (val, now)
        return val
    if row and row["checked_ts"] and now - row["checked_ts"] < ttl:
        _monitor_cache[chat_id] = (bool(row["monitored"]), now)
        return bool(row["monitored"])

    # Telethon диспатчит апдейты параллельными задачами, поэтому на пачке
    # сообщений из НОВОГО чата сюда одновременно заходит десяток обработчиков.
    # Без этой отсечки все они пошли бы выяснять одно и то же по сети — десяток
    # одинаковых запросов на ровном месте и столько же строк в логе.
    inflight = _monitor_inflight.get(chat_id)
    if inflight is not None:
        return await inflight

    fut = asyncio.get_running_loop().create_future()
    _monitor_inflight[chat_id] = fut
    fallback = bool(row and row["monitored"])
    try:
        try:
            entity = await event.get_chat()
        except Exception as e:
            log.warning("Не смог опознать чат %s: %s", chat_id, e)
            fut.set_result(fallback)
            return fallback

        kind, members, monitored, title = await classify_chat(client, entity, _me_id)
        db.set_chat(chat_id, kind, title, members, monitored)
        _monitor_cache[chat_id] = (monitored, now)
        if row is None or bool(row["monitored"]) != monitored:
            log.info("Чат «%s» [%s, участников: %s] -> %s",
                     title, kind, members, "следим" if monitored else "игнорируем")
        fut.set_result(monitored)
        return monitored
    except BaseException:
        # Ждущим отдаём запасной ответ, а не исключение: иначе оно всплыло бы
        # у каждого из них по отдельности
        if not fut.done():
            fut.set_result(fallback)
        raise
    finally:
        _monitor_inflight.pop(chat_id, None)


@client.on(events.NewMessage(incoming=True, outgoing=True))
async def on_new_message(event):
    """Пишем в кэш каждое сообщение отслеживаемого чата.

    Своё — только текстом: свои файлы у тебя и так есть, качать их незачем.
    """
    if event.chat_id == config.LOG_CHAT_ID:
        return

    if not await should_monitor(event):
        # Личка или старая группа, за которой не следим (бот, разросшаяся группа).
        # Запоминаем ID: её удаления приходят без указания чата, и без этой
        # отметки каждое из них выглядело бы как «неизвестное удаление».
        if event.chat_id is not None and event.chat_id > config.SUPERGROUP_ID_THRESHOLD:
            db.note_ignored(event.message.id, event.chat_id)
        return

    msg = event.message
    # Не узнали, кто и где, — всё равно сохраняем. Раньше тут стоял return, и
    # сообщение вместе с фото/голосовым терялось целиком из-за одного сбоя
    # сети на get_chat/get_sender. Имя дотянется позже (resolve_person), а
    # файл можно скачать только сейчас.
    peer = sender = None
    try:
        peer = await event.get_chat()
    except Exception as e:
        log.warning("Не смог получить чат для %s: %s — сохраняю без названия", event.id, e)
    try:
        sender = await event.get_sender()
    except Exception as e:
        log.warning("Не смог получить автора для %s: %s — сохраняю без имени", event.id, e)

    has_media = msg.media is not None and not isinstance(msg.media, types.MessageMediaWebPage)

    db.add_message(
        chat_id=event.chat_id,
        msg_id=msg.id,
        sender_id=getattr(sender, "id", None) or getattr(msg, "sender_id", None),
        sender_name=display_name(sender) if sender is not None else None,
        sender_username=getattr(sender, "username", None),
        chat_title=display_name(peer) if peer is not None else None,
        text=msg.text or None,
        media_type=media_kind(msg) if has_media else None,
        is_outgoing=int(bool(event.out)),
        sent_ts=int(msg.date.timestamp()),
    )

    if has_media and not event.out:
        ttl = getattr(msg.media, "ttl_seconds", None)
        if ttl and setting("view_once"):
            await capture_view_once(event.chat_id, msg, peer, sender, ttl)
        else:
            await download_media(event.chat_id, msg)
            await maybe_transcribe_live(event.chat_id, msg, sender)


# ---------------------------------------------------------------- расшифровка


def _voice_target(chat_id, sender_id) -> bool:
    """Подписаны ли мы на расшифровку — по чату или по автору.

    Два ключа сразу: в личке они совпадают, а в группе подписка на человека
    должна работать и там, где чат — это group_id, а не он сам.
    """
    return sub_has("voice", chat_id) or sub_has("voice", sender_id)


async def maybe_transcribe_live(chat_id, msg, sender) -> None:
    """Голосовое от того, на кого подписан, расшифровываем сразу — не дожидаясь
    удаления. Именно это и просили: «всегда, независимо от того, удаляет он или нет»."""
    if not setting("transcribe"):
        return
    kind = media_kind(msg)
    if not is_voice_kind(kind):
        return
    if not _voice_target(chat_id, getattr(sender, "id", None)):
        return
    row = db.get(msg.id, chat_id)
    if row is None or not row["media_path"]:
        return
    spawn(run_transcript(row, kind))


def stash_voice(row):
    """Отложить копию голосового перед отправкой алерта.

    Нужно потому, что после успешной доставки файл с диска стирается, а
    расшифровка идёт фоном и к тому моменту не успевает. Голосовые весят
    килобайты — копия дешёвая.
    """
    if not (setting("transcribe") and config.TRANSCRIBE_DELETED):
        return None
    if not is_voice_kind(row["media_type"]) or not row["media_path"]:
        return None
    if row["transcript"]:
        return None      # уже расшифровано вживую, второй раз незачем
    try:
        dst = config.MEDIA_DIR / "tr" / f"{row['chat_id']}_{row['msg_id']}"
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(row["media_path"], dst)
        return str(dst)
    except OSError as e:
        log.debug("Не смог отложить копию голосового: %s", e)
        return None


# Какие сообщения расшифровываются прямо сейчас. Без этого живая расшифровка и
# расшифровка того же сообщения после удаления идут парой и дают два одинаковых
# алерта: обе стартовали, когда transcript в базе был ещё пуст.
_transcribing = set()


async def run_transcript(row, kind: str, path: str = None, temp: bool = False) -> None:
    """Расшифровать и прислать текст отдельным сообщением. Молчит при неудаче:
    это фоновая доработка к уже отправленному алерту, шуметь ошибкой незачем."""
    key = (row["chat_id"], row["msg_id"])
    src = path or row["media_path"]
    if key in _transcribing:
        log.debug("Расшифровка %s уже идёт — второй раз не запускаю", key)
        return
    _transcribing.add(key)
    cancelled = False
    try:
        # Перечитываем базу, а не полагаемся на снимок row: пока копия ждала
        # очереди, живая расшифровка могла всё уже сделать.
        if db.get_transcript(*key):
            return
        text, err = await transcribe.transcribe(src)
        if not text:
            log.info("Расшифровка %s/%s не вышла: %s",
                     row["chat_id"], row["msg_id"], err)
            return
        if db.get_transcript(*key):
            return   # успели вперёд нас, пока считали
        db.set_transcript(key[0], key[1], text)
        await notify(alerts.build_transcript_alert(row, text, kind))
        log.info("Расшифровано %s/%s (%s симв.)", row["chat_id"], row["msg_id"], len(text))
    except asyncio.CancelledError:
        cancelled = True
        raise
    except Exception:
        # Иначе исключение утонет до сборки мусора: задачу никто не await'ит
        log.exception("Расшифровка %s упала", key)
    finally:
        _transcribing.discard(key)
        # При отмене копию НЕ трогаем: оригинал к этому моменту уже уехал и
        # стёрт, и это единственный оставшийся файл. Подметёт _sweep_transcribe_tmp.
        if temp and src and not cancelled:
            try:
                os.remove(src)
            except OSError:
                pass


async def capture_view_once(chat_id, msg, peer, sender, ttl) -> None:
    """Сохранить «огонёк» сразу при получении, до сгорания.

    Не вызываем ReadMessageContents — отправитель видит его неоткрытым, таймер
    сгорания не стартует. Файл отдаём как обычный (без ttl=), иначе копия сама
    станет одноразовой. Помечаем ttl_captured, чтобы последующее самосгорание
    не улетело вторым алертом «удалено».
    """
    # После сгорания поля photo/document уже нет — download_media вернёт None.
    has_file = getattr(msg.media, "photo", None) is not None or \
        getattr(msg.media, "document", None) is not None
    if not has_file:
        log.info("Огонёк %s/%s уже недоступен", chat_id, msg.id)
        return

    await download_media(chat_id, msg)
    row = db.get(msg.id, chat_id)
    if row is None:
        return
    kind = media_kind(msg)
    once = ttl >= 0x7FFFFFFF

    # Пометку и удаление файла ставим ТОЛЬКО после успешной отправки. Иначе при
    # сбое огонёк «пропадал» совсем: флаг стоял, файл лежал, а алерта не было —
    # и последующее самосгорание тоже молчало из-за ttl_captured.
    # Голосовой «огонёк» тоже расшифровываем — копию снимаем до отправки,
    # потому что после неё файл с диска уйдёт.
    voice_copy = stash_voice(row)
    sent = await notify(alerts.build_viewonce_alert(row, kind, once),
                        file=row["media_path"], sensitive=True)
    # Расшифровку запускаем независимо от того, доехал ли файл: если не доехал,
    # текст ценнее вдвойне. Заодно копия гарантированно будет убрана за собой.
    if voice_copy:
        spawn(run_transcript(row, kind, path=voice_copy, temp=True))
    if sent:
        db.mark_ttl_captured(chat_id, msg.id)
        if row["media_path"]:
            db.drop_media_file(chat_id, msg.id)
        log.info("Огонёк сохранён: %s в %s", kind, display_name(peer))
    else:
        log.warning("Огонёк %s/%s не доставлен — не помечаю, файл оставляю", chat_id, msg.id)


DISK_FREE_FLOOR_MB = 512   # ниже этого не качаем ничего: место нужно самой базе


def _media_budget(size: int):
    """Влезет ли ещё один файл. Возвращает причину отказа или None.

    Раньше суммарный лимит проверялся только чисткой раз в шесть часов, и в
    промежутке любой собеседник мог залить сколько угодно: сотня файлов по
    190 МБ — это десятки гигабайт за час и мёртвый сервер. Считаем перед
    каждым скачиванием.
    """
    limit = config.MEDIA_MAX_TOTAL_MB * 1024 * 1024
    if limit:
        used = db.media_total_bytes(max_age_sec=60)
        if used + size > limit:
            db.cleanup()          # сначала пробуем освободить по сроку хранения
            used = db.media_total_bytes()
            if used + size > limit:
                return (f"не качали: занято {used // 1024 // 1024} МБ "
                        f"при лимите {config.MEDIA_MAX_TOTAL_MB} МБ")
    try:
        free = shutil.disk_usage(config.MEDIA_DIR).free
    except OSError:
        return None
    if free - size < DISK_FREE_FLOOR_MB * 1024 * 1024:
        return f"не качали: на диске осталось {free // 1024 // 1024} МБ"
    return None


MEDIA_TRIES = 3                 # попыток скачать файл, прежде чем сдаться
MEDIA_RETRY_SLEEP = (2, 6, 15)  # пауза перед 2-й, 3-й… попыткой, сек
MEDIA_WAIT_ON_DELETE = 120      # сколько удаление ждёт файл, который ещё качается
_downloads = {}                 # (chat_id, msg_id) -> Future: файл качается прямо сейчас


def _has_file(msg) -> bool:
    """Есть ли что скачивать: фото или документ (голосовое, кружок, видео…).
    Геолокация, контакт и опрос файла не несут — их повторами не мучаем."""
    m = getattr(msg, "media", None)
    if isinstance(m, types.MessageMediaPhoto):
        return isinstance(getattr(m, "photo", None), types.Photo)
    if isinstance(m, types.MessageMediaDocument):
        return isinstance(getattr(m, "document", None), types.Document)
    return False


async def _refetch(chat_id: int, msg_id: int):
    """Перечитать сообщение: у свежей копии свежий file reference. None — если
    его уже нет (удалили) или в нём больше нет файла."""
    try:
        fresh = await client.get_messages(chat_id, ids=msg_id)
    except Exception as e:
        log.info("Не смог перечитать %s/%s: %s", chat_id, msg_id, e)
        return None
    if fresh is None or not _has_file(fresh):
        return None
    return fresh


async def download_media(chat_id: int, msg) -> bool:
    """Качаем сразу: после удаления file reference протухает и файл уже не достать.

    Пока качается, сообщение отмечено в _downloads — удаление, прилетевшее в
    эти секунды, дождётся файла, а не уйдёт алертом без него.
    """
    key = (chat_id, msg.id)
    fut = asyncio.get_running_loop().create_future()
    _downloads[key] = fut
    ok = False
    try:
        ok = await _download_media(chat_id, msg)
        return ok
    finally:
        if not fut.done():
            fut.set_result(ok)
        if _downloads.get(key) is fut:
            _downloads.pop(key, None)


async def _download_media(chat_id: int, msg) -> bool:
    size = getattr(msg.file, "size", None) or 0
    limit = config.MEDIA_MAX_FILE_MB * 1024 * 1024
    if size > limit:
        db.set_media_path(chat_id, msg.id, None,
                          f"не качали, {size // 1024 // 1024} МБ > лимита {config.MEDIA_MAX_FILE_MB} МБ")
        return False

    deny = _media_budget(size)
    if deny:
        log.warning("Медиа %s/%s пропущено: %s", chat_id, msg.id, deny)
        db.set_media_path(chat_id, msg.id, None, deny)
        return False

    target = config.MEDIA_DIR / str(chat_id)
    target.mkdir(parents=True, exist_ok=True)
    base = str(target / str(msg.id))   # Telethon допишет расширение; путь один и тот же
    file_kind = _has_file(msg)
    tries = MEDIA_TRIES if file_kind else 1
    src, reason, attempt = msg, None, 0

    while attempt < tries:
        attempt += 1
        path = None
        try:
            path = await src.download_media(file=base)
        except FloodWaitError as e:
            if e.seconds > MAX_FLOOD_WAIT:
                reason = f"FloodWait {e.seconds} сек — столько не ждём"
                log.warning("FloodWait %s сек — слишком долго, файл пропускаем", e.seconds)
                break
            log.warning("FloodWait %s сек на скачивании — жду", e.seconds)
            reason = f"FloodWait {e.seconds} сек"
            await asyncio.sleep(e.seconds + 2)
            continue              # ссылка тут ни при чём — перечитывать незачем
        except (FileReferenceExpiredError, FilerefUpgradeNeededError):
            # Telethon сам обновляет ссылку только у документов, у фото — нет
            reason = "ссылка на файл протухла"
        except asyncio.CancelledError:
            raise
        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
        else:
            if path and os.path.isfile(path) and os.path.getsize(path) > 0:
                db.set_media_path(chat_id, msg.id, path, None)
                if attempt > 1:
                    log.info("Медиа %s/%s скачано с %s-й попытки", chat_id, msg.id, attempt)
                return True
            if not file_kind:
                return False      # геолокация и прочее без файла — это не сбой
            # Telethon открывает файл до первого байта: сбой посередине давал
            # 0 байт на диске, и раньше это считалось успехом
            reason = "Telegram отдал пустой файл (0 байт)" if path else "Telegram не отдал файл"

        log.warning("Медиа %s/%s: попытка %s/%s не удалась — %s",
                    chat_id, msg.id, attempt, tries, reason)
        if attempt >= tries:
            break
        await asyncio.sleep(MEDIA_RETRY_SLEEP[min(attempt - 1, len(MEDIA_RETRY_SLEEP) - 1)])
        fresh = await _refetch(chat_id, msg.id)
        if fresh is None:
            reason += "; сообщение уже удалено или без файла — достать нечем"
            break
        src = fresh

    if not file_kind:
        if reason:
            db.set_media_path(chat_id, msg.id, None, f"не скачалось: {reason}")
        return False
    tries_txt = "с 1 попытки" if attempt == 1 else f"после {attempt} попыток"
    note = alerts.media_fail_note(tries_txt, reason or "неизвестно")
    db.set_media_path(chat_id, msg.id, None, note)
    log.warning("Медиа %s/%s НЕ сохранено: %s", chat_id, msg.id, note)
    return False


async def wait_downloads(keys, timeout: float = None) -> None:
    """Дождаться файлов, которые ещё качаются, — но не дольше timeout."""
    futs = [_downloads[k] for k in keys if k in _downloads]
    if not futs:
        return
    log.info("Удаление ждёт %s файл(ов), которые ещё качаются", len(futs))
    await asyncio.wait(futs, timeout=MEDIA_WAIT_ON_DELETE if timeout is None else timeout)


# ---------------------------------------------------------------- удаления


@client.on(events.MessageDeleted)
async def on_deleted(event):
    """Telegram присылает только список ID. Чат восстанавливаем из кэша сами."""
    now = int(time.time())
    grouped = defaultdict(list)

    # В буфер кладём только ID. Саму строку сохранять смысла нет: _emit всё
    # равно перечитывает её заново — к моменту отправки могло дозаписаться
    # media_path (файл качается десятки секунд), и снимок был бы устаревшим.
    if event.chat_id is not None:
        # Супергруппа: чат известен, ID нумеруются внутри неё — ищем точно.
        if not db.is_monitored(event.chat_id):
            return
        grouped[event.chat_id].extend(event.deleted_ids)
    else:
        # Личка или старая группа: чат Telegram не сообщает, поднимаем по ID.
        for msg_id in event.deleted_ids:
            row = db.get(msg_id)
            if row is not None:
                if not db.is_monitored(row["chat_id"]):
                    continue
                grouped[row["chat_id"]].append(msg_id)
            elif not db.is_ignored(msg_id):
                grouped[UNKNOWN].append(msg_id)

    for key, items in grouped.items():
        buf = _buffers.get(key)
        if buf is None:
            buf = _buffers[key] = {"items": [], "first": now, "last": now, "task": None}
            buf["task"] = asyncio.create_task(_flush_later(key))
        buf["items"].extend(items)
        buf["last"] = now


async def _flush_later(key) -> None:
    """Ждём, пока пачка удалений договорит: клиент шлёт их не одним апдейтом."""
    while True:
        await asyncio.sleep(config.DEBOUNCE_SEC)
        buf = _buffers.get(key)
        if buf is None:
            return
        quiet = time.time() - buf["last"] >= config.DEBOUNCE_SEC
        expired = time.time() - buf["first"] >= config.PURGE_WINDOW_SEC
        if quiet or expired:
            break

    buf = _buffers.pop(key, None)
    if buf:
        try:
            await _emit(key, buf)
        except Exception:
            log.exception("Не смог отправить алерт об удалении")


async def _emit(key, buf, wait_files: float = None) -> None:
    deleted_ts = buf["last"]
    window = max(1, buf["last"] - buf["first"])

    # Файл ещё качается (крупное видео, повтор после сбоя) — подождём его,
    # иначе алерт ушёл бы без вложения, хотя файл вот-вот будет на диске.
    if key != UNKNOWN:
        await wait_downloads([(key, mid) for mid in buf["items"]], wait_files)

    # Строки читаем ЗАНОВО: media_path мог дозаписаться уже после события
    # (файл качается десятки секунд), в снимке из буфера его ещё нет.
    chat_for_lookup = None if key == UNKNOWN else key
    fresh = [(mid, db.get(mid, chat_for_lookup)) for mid in buf["items"]]
    rows = [r for _, r in fresh if r is not None]
    unknown_ids = [mid for mid, r in fresh if r is None]

    if paused():
        log.info("Пауза: пропустил %s удалений", len(buf["items"]))
        return

    # Чат не опознан — сводка про чистку тут бессмысленна, шлём как есть
    if key == UNKNOWN:
        if setting("unknown_deletes"):
            await notify(alerts.build_unknown_alert(unknown_ids, deleted_ts))
        return

    # Пачкой — одной сводкой, чтобы не заваливать лог-чат
    if len(buf["items"]) >= config.PURGE_MIN_COUNT:
        # Огоньки уже сохранены при получении — из сводки о чистке их исключаем
        rows = [r for r in rows if not r["ttl_captured"]]
        if not rows and not unknown_ids:
            return
        # Голосовые из пачки тоже расшифровываем. Копии снимаем ДО отправки —
        # ниже вложения уезжают и файлы стираются. Потолок тот же, что на
        # вложения: чистка на 500 сообщений иначе заняла бы ядра на час.
        voice_stashes = []
        for r in rows:
            if len(voice_stashes) >= config.PURGE_MAX_MEDIA:
                break
            if is_voice_kind(r["media_type"]) and r["media_path"]:
                p = stash_voice(r)
                if p:
                    voice_stashes.append((r, p))
        db.incr("stat:purges")
        db.incr("hb:purges")
        title = db.peer_name(key)
        try:
            peer, people = await _purge_names(key, rows)
        except Exception:
            log.exception("Не смог дотянуть имена для сводки о чистке")
            peer, people = None, None
        await deliver("purge", key,
                      alerts.build_purge_alert(title, rows, unknown_ids, deleted_ts, window,
                                               peer=peer, people=people, chat_id=key))
        # Вложения шлём только вне режима дайджеста — иначе они прилетят
        # раньше самой сводки, которая ждёт в очереди.
        if not _digest_active("purge"):
            attach = [r for r in rows if r["media_path"]][: config.PURGE_MAX_MEDIA]
            # Одним блоком: иначе между сводкой и её вложениями может вклиниться
            # чужой алерт, и станет непонятно, к чему эти файлы относятся.
            results = await notify_group([
                (f"↑ вложение к сообщению от {alerts.fmt_time(r['sent_ts'])}",
                 r["media_path"]) for r in attach])
            for r, ok in zip(attach, results):
                if ok:
                    db.drop_media_file(r["chat_id"], r["msg_id"])
        for r, p in voice_stashes:
            spawn(run_transcript(r, r["media_type"], path=p, temp=True))
        log.info("Чистка в %s: %s сообщений", title, len(buf["items"]))
        return

    for row in rows:
        if row["ttl_captured"]:
            continue   # это был огонёк, уже сохранён при получении
        # Своё сообщение может снести и собеседник — в личке это разрешено, — но
        # отличить это от «удалил сам» нечем: сигнала об авторе удаления нет.
        if row["is_outgoing"] and not setting("own_deletes"):
            continue
        db.incr("stat:deletes")
        db.incr("hb:deletes")
        # Копию голосового снимаем ДО отправки: успешная доставка стирает файл,
        # а расшифровка идёт фоном и к тому моменту его уже не застанет.
        voice_copy = stash_voice(row)
        peer = None
        if row["is_outgoing"] and row["chat_id"] > 0:
            # Твоё сообщение в личке: назвать собеседника, который мог его снести
            try:
                peer = await resolve_person(row["chat_id"], row["chat_id"],
                                            row["chat_title"])
            except Exception:
                log.debug("Не дотянул имя собеседника", exc_info=True)
        sent = await deliver("delete", row["chat_id"],
                             alerts.build_delete_alert(row, deleted_ts, peer=peer),
                             file=row["media_path"])
        if sent and row["media_path"]:
            db.drop_media_file(row["chat_id"], row["msg_id"])
        if voice_copy:
            spawn(run_transcript(row, row["media_type"], path=voice_copy, temp=True))
        log.info("Удаление в %s (msg %s)", row["chat_title"], row["msg_id"])

    if unknown_ids and setting("unknown_deletes"):
        # Здесь чат ИЗВЕСТЕН (супергруппа) — не выдаём «чат неизвестен»
        await deliver("delete", key,
                      alerts.build_unknown_alert(unknown_ids, deleted_ts,
                                                 chat_title=db.peer_name(key)))


# Кого уже спрашивали у Telegram: id -> (сведения, когда). Один запрос на
# человека в час, а не на каждую сводку: при сносе переписки сводок бывает
# несколько подряд.
_people = {}
PEOPLE_TTL = 3600


def purge_people_reset() -> None:
    _people.clear()


async def _lookup_person(user_id: int) -> dict:
    """Имя и @username из Telegram (сущность из кэша сессии или запрос)."""
    hit = _people.get(user_id)
    if hit and time.time() - hit[1] < PEOPLE_TTL:
        return hit[0]
    info = {}
    try:
        ent = await asyncio.wait_for(client.get_entity(user_id), timeout=10)
        name = display_name(ent)
        info = {"name": None if name in ("без имени", "неизвестно") else name,
                "username": getattr(ent, "username", None)}
    except Exception as e:
        log.debug("Не узнал, кто такой %s: %s", user_id, e)
    _people[user_id] = (info, time.time())
    return info


async def resolve_person(chat_id, user_id, name=None, username=None) -> dict:
    """Дотянуть имя и @username: сперва из своей базы, потом у Telegram.

    Зачем: в алерте о чистке человек должен быть назван всегда. Чаще всего
    имени нет там, где собеседник снёс переписку целиком — от него не осталось
    ни одного входящего, только твои сообщения.
    """
    info = {"id": user_id, "name": name, "username": username}
    if not user_id or (name and username):
        return info
    try:
        hint = db.person_hint(chat_id, user_id)
    except Exception:
        hint = None
    if hint is not None:
        info["name"] = info["name"] or hint["sender_name"]
        info["username"] = info["username"] or hint["sender_username"]
    if not info["username"]:
        got = await _lookup_person(user_id)
        info["name"] = got.get("name") or info["name"]
        info["username"] = got.get("username")
    return info


async def _purge_names(key, rows) -> tuple:
    """(peer, people) для build_purge_alert."""
    if key is None or key == UNKNOWN:
        return None, None
    if key > 0:
        inc = next((r for r in rows if not r["is_outgoing"]), None)
        name = inc["sender_name"] if inc else next(
            (r["chat_title"] for r in rows if r["chat_title"]), None)
        peer = await resolve_person(key, key, name, inc["sender_username"] if inc else None)
        return peer, None
    people = {}
    for r in rows:
        sid = r["sender_id"]
        if r["is_outgoing"] or sid is None or sid in people:
            continue
        people[sid] = await resolve_person(key, sid, r["sender_name"], r["sender_username"])
    return None, people


def _digest_active(kind: str) -> bool:
    if config.DIGEST_MODE == "all":
        return True
    if config.DIGEST_MODE == "purge":
        return kind == "purge"
    return False


async def notify(text: str, file: str = None, sensitive: bool = False):
    """Отправить алерт, не давая другим вклиниться в середину."""
    async with _send_lock:
        return await _send_one(text, file, sensitive)


async def notify_group(items):
    """Несколько отправок подряд под одним замком: между ними ничего не влезет.
    Нужно для «сводка + её вложения» — иначе вложения могут разъехаться с текстом,
    к которому относятся. Замок не реентерабельный, поэтому внутри строго
    _send_one, а не notify."""
    out = []
    async with _send_lock:
        for text, file in items:
            out.append(await _send_one(text, file))
    return out


async def _send_one(text: str, file: str = None, sensitive: bool = False):
    """Одна отправка БЕЗ захвата замка. Звать только из notify/notify_group.

    Через бота, если он настроен: сообщение от бота приходит как обычное
    входящее — непрочитанным и с уведомлением. Юзербот же пишет от твоего
    имени, а о собственных сообщениях Telegram не уведомляет.

    sensitive=True (перезалив чужого «огонька»): при сбое бота НЕ откатываемся
    на юзербота молча — перезалив чужого самоуничтожающегося контента от твоего
    аккаунта самый весомый вектор жалобы. Вместо файла шлём текстовое
    предупреждение (тоже через бота), чтобы ты знал, что огонёк был.
    """
    if bot is not None:
        sent = await alerts.send(bot, text, file=file, chat=_bot_chat)
        if sent:
            return sent
        if sensitive and file:
            log.warning("Бот не доставил огонёк — через аккаунт не гоню")
            await alerts.send(bot, text + "\n<i>⚠️ файл не доставлен ботом; "
                              "через аккаунт не перезаливаю ради безопасности</i>",
                              chat=_bot_chat)
            return None
        log.warning("Бот не доставил — шлю через юзербота")
    if sensitive and file and bot is None:
        # Бота нет вовсе — единственный канал юзербот. По умолчанию так и есть,
        # риск описан в .env. Но предупредим отдельной строкой.
        log.info("Огонёк уходит через аккаунт (бот не настроен)")
    return await alerts.send(client, text, file=file)


async def deliver(kind: str, chat_id, payload: str, file: str = None):
    """Мгновенно или в дайджест.

    Алерты с файлом всегда идут сразу: медиа ценно и его нельзя мариновать на
    диске сутками. В дайджест уходит только текстовый шум (правки, чистки).
    """
    if file or not _digest_active(kind):
        return await notify(payload, file=file)
    db.enqueue_digest(kind, chat_id, payload)
    return True   # «доставлено» в смысле «принято в очередь»


# ---------------------------------------------------------------- правки


@client.on(events.MessageEdited(incoming=True))
async def on_edited(event):
    if not setting("track_edits"):
        return
    if not db.is_monitored(event.chat_id):
        return

    row = db.get(event.message.id, event.chat_id)
    new_text = event.message.text or ""
    if row is None:
        return
    old_text = row["text"] or ""
    if old_text == new_text:
        return  # подтянулось превью ссылки или что-то ещё не текстовое

    db.add_edit(event.chat_id, event.message.id, old_text, new_text)
    db.update_text(event.chat_id, event.message.id, new_text)
    if not paused():
        db.incr("stat:edits")
        db.incr("hb:edits")
        await deliver("edit", event.chat_id,
                      alerts.build_edit_alert(row, old_text, new_text, int(time.time())))
        log.info("Правка в %s (msg %s)", row["chat_title"], event.message.id)


# ---------------------------------------------------------------- онлайн-трекинг


@client.on(events.UserUpdate)
async def on_user_update(event):
    """Заходы в сеть у тех, на кого подписан.

    Полностью пассивно: Telegram сам присылает updateUserStatus по контактам,
    ни одного исходящего запроса мы не делаем. Работает, только пока человек не
    скрыл «был в сети» — скрытый статус нам просто не приходит.
    """
    if not setting("online_watch") or paused():
        return
    uid = event.user_id
    if uid is None or uid == _me_id or not sub_has("online", uid):
        return
    # У события два разных смысла: смена статуса и «печатает». Второе не про нас,
    # и .online тогда возвращает None.
    try:
        online = event.online
    except Exception:
        return
    if online is None:
        return

    now = int(time.time())
    prev = db.get_online_state(uid)
    was = bool(prev["online"]) if prev else False
    if was == online:
        return   # статус не менялся, это повтор

    name = db.peer_name(uid)
    if online:
        # Клиент мигает статусом при каждом открытии приложения — не частим.
        # Равенство since_ts и notified_ts дальше служит признаком «о заходе
        # сообщали»: только на такой заход имеет смысл сообщать и о выходе.
        last = (prev["notified_ts"] if prev else 0) or 0
        if now - last < config.ONLINE_MIN_GAP_SEC:
            # Заход подавлен антиспамом. since_ts=0 — это пометка «о заходе не
            # объявляли»; по ней ниже решаем, объявлять ли о выходе.
            db.set_online_state(uid, True, 0, last)
            return
        db.set_online_state(uid, True, now, now)
        await notify(alerts.build_online_alert(name, True))
    else:
        since = (prev["since_ts"] if prev else 0) or 0
        notified = (prev["notified_ts"] if prev else 0) or 0
        db.set_online_state(uid, False, since, notified)
        # О заходе промолчали — молчим и о выходе, иначе на каждое мигание
        # статуса приходило бы «вышел» без парного «зашёл».
        if since:
            await notify(alerts.build_online_alert(name, False, now - since))


# ---------------------------------------------------------------- смена профиля


async def profile_watcher() -> None:
    """Периодически сверяет профили подписанных людей со снимком в базе."""
    await asyncio.sleep(90)
    while True:
        if setting("profile_watch") and _subs.get("profile"):
            try:
                await _check_profiles()
            except Exception:
                log.exception("Проверка профилей упала")
        base = max(1, config.PROFILE_CHECK_INTERVAL_MIN) * 60
        await asyncio.sleep(base + _rand.randint(0, max(1, base // 5)))


async def _check_profiles() -> None:
    # На паузе не опрашиваем вообще. Иначе снимок профиля обновился бы молча,
    # и изменение, случившееся во время паузы, потерялось бы навсегда: сравнивать
    # после снятия паузы было бы уже не с чем.
    if paused():
        return
    peers = sorted(p for p in _subs.get("profile", ()) if p > 0)
    # Потолок за прогон с прокруткой: 100 подписок иначе давали бы 100 запросов
    # к Telegram каждый час. Кого не успели — возьмём в следующий раз.
    cap = max(1, config.PROFILE_CHECK_MAX_PEERS)
    if len(peers) > cap:
        start = db.get_int("profile_rotate", 0) % len(peers)
        peers = (peers + peers)[start:start + cap]
        db.set_meta("profile_rotate", start + cap)

    for user_id in peers:
        await check_one_profile(user_id)
        await asyncio.sleep(1.2 + _rand.random())


def _profile_fields(ent):
    """Что именно сравниваем: @username, имя, id фото, признак удаления."""
    return (getattr(ent, "username", None),
            display_name(ent),
            getattr(getattr(ent, "photo", None), "photo_id", None),
            bool(getattr(ent, "deleted", False)))


_profile_busy = set()   # кого проверяем прямо сейчас — не дублируем


async def snapshot_profile(user_id: int) -> bool:
    """Запомнить профиль как точку отсчёта.

    Зовётся сразу при подписке. Без этого снимок снимался только на ближайшем
    плановом опросе — и если человек успевал сменить аву раньше, изменение уже
    входило в первый снимок, и сравнивать было не с чем: выглядело как «не работает».
    """
    try:
        ent = await client.get_entity(user_id)
    except Exception as e:
        log.info("Не смог снять снимок профиля %s: %s", user_id, e)
        return False
    db.set_profile_state(user_id, *_profile_fields(ent))
    log.info("Снимок профиля %s снят — с ним и будем сравнивать", user_id)
    return True


async def check_one_profile(user_id: int) -> None:
    """Сверить профиль одного человека со снимком."""
    if user_id <= 0 or user_id in _profile_busy:
        return   # у групп профиля в этом смысле нет; и не дублируем проверку
    _profile_busy.add(user_id)
    try:
        if paused():
            return
        try:
            ent = await client.get_entity(user_id)
        except FloodWaitError as e:
            log.warning("FloodWait %s сек в проверке профилей", e.seconds)
            await asyncio.sleep(e.seconds + 2)
            return
        except Exception as e:
            log.debug("Профиль %s не получен: %s", user_id, e)
            return

        username, name, photo_id, gone = _profile_fields(ent)
        prev = db.get_profile_state(user_id)
        if prev is None:
            db.set_profile_state(user_id, username, name, photo_id, gone)
            return   # первый раз — только запоминаем, сравнивать не с чем

        if gone and not prev["is_deleted"]:
            db.set_profile_state(user_id, username, name, photo_id, True)
            await notify(alerts.build_deleted_account_alert(prev["name"] or name, user_id))
            return

        changes = []
        if (prev["username"] or None) != (username or None):
            changes.append(("@username", _at(prev["username"]), _at(username)))
        if (prev["name"] or None) != (name or None):
            changes.append(("Имя", prev["name"], name))
        if prev["photo_id"] != photo_id:
            changes.append(("Фото профиля",
                            "было" if prev["photo_id"] else None,
                            "новое" if photo_id else None))

        if changes:
            db.set_profile_state(user_id, username, name, photo_id, gone)
            await notify(alerts.build_profile_alert(name, user_id, changes))
            log.info("Профиль %s изменился: %s", user_id,
                     ", ".join(c[0] for c in changes))
    finally:
        _profile_busy.discard(user_id)


@client.on(events.Raw(types=[types.UpdateUser, types.UpdateUserName]))
async def on_profile_hint(event):
    """Мгновенная реакция на смену профиля.

    Telegram сам присылает «этот человек изменился» по тем, с кем есть диалог:
    UpdateUserName — про имя и @username, UpdateUser — общий сигнал «перечитай»
    (отдельного updateUserPhoto в текущем слое API уже нет). Плановый опрос
    остаётся страховкой на случай, если апдейт не придёт.
    """
    uid = getattr(event, "user_id", None)
    if uid is None or not sub_has("profile", uid):
        return
    if not setting("profile_watch") or paused():
        return
    spawn(check_one_profile(uid))


def _at(username):
    return f"@{username}" if username else None


# ---------------------------------------------------------------- команды


# ---------------------------------------------------------------- команды


async def reply_long(event, text: str) -> None:
    """Длинный ответ — туда же, откуда пришла команда, с разбивкой по лимиту."""
    for chunk in alerts.split_text(text):
        await event.respond(chunk, parse_mode="html", link_preview=False)


async def resolve_chat(arg: str):
    """Опознать чат: числовой id, @username или подстрока названия.

    @username резолвится через Telegram (get_entity) — так можно смотреть истории
    даже у тех, с кем ты не переписывался и кого нет в кэше.
    """
    arg = arg.strip()
    if not arg:
        return None, "укажи id, @username или часть названия чата"
    try:
        cid = int(arg)
        row = db.get_chat(cid)
        return (cid, row["title"] if row else str(cid)), None
    except ValueError:
        pass
    # Сначала локальный кэш — чтобы «/read иван» не слал впустую ResolveUsername
    # (пачка неудачных резолвов = спам-сигнал для Telegram).
    hits = db.find_chats(arg, limit=10)
    if len(hits) == 1:
        return (hits[0]["chat_id"], hits[0]["title"]), None
    if len(hits) > 1:
        lst = "\n".join(f"  • {alerts.esc(h['title'])} (id {h['chat_id']})" for h in hits[:8])
        return None, f"под «{alerts.esc(arg)}» подходит несколько, уточни:\n{lst}"
    # Не нашли локально: резолвим как username ТОЛЬКО при явной @
    if arg.startswith("@"):
        try:
            ent = await client.get_entity(arg)
            from telethon import utils as _tlutils
            return (_tlutils.get_peer_id(ent), display_name(ent)), None
        except Exception as e:
            return None, f"не нашёл {alerts.esc(arg)}: {alerts.esc(e)}"
    return None, f"по «{alerts.esc(arg)}» ничего не нашёл (дай @username, если это не из переписки)"


@client.on(events.NewMessage())
async def on_command(event):
    # Лог-чат — основной канал управления, Избранное — запасной: если с каналом
    # что-то не так, остаётся способ дотянуться до бота.
    if event.chat_id not in (config.LOG_CHAT_ID, _me_id):
        return
    # Команды даёт только ВЛАДЕЛЕЦ. Если лог-чат окажется не одиночным каналом,
    # а группой с другими участниками — иначе любой её член мог бы вытащить
    # архив через /find, /read, /story. Свои сообщения: sender_id == me, либо
    # пост от имени канала (out=True, sender_id может быть None).
    if not (event.sender_id == _me_id or getattr(event, "out", False)):
        return
    # Пересланное сообщение может случайно начинаться с «/» — командой не считаем.
    if event.forward is not None:
        return
    text = (event.raw_text or "").strip()
    # Кнопки нижней клавиатуры присылают свой текст обычным сообщением —
    # переводим его в соответствующую команду.
    mapped = KEYBOARD_ACTIONS.get(text)
    if mapped:
        text = mapped
    if not text.startswith("/"):
        return
    parts = text.split(maxsplit=1)
    cmd = parts[0].lower().lstrip("/")
    arg = parts[1] if len(parts) > 1 else ""

    if cmd == "stats":
        if arg:
            # «/stats иван» — сводка по одному чату, а не по всему архиву
            target, err = await resolve_chat(arg)
            if err:
                await event.respond(err, parse_mode="html")
                return
            # В потоке: на крупном чате агрегаты считаются около секунды,
            # а секунда в event loop — это секунда без обработки удалений
            st = await asyncio.to_thread(chat_stats_ro, target[0])
            await reply_long(event, alerts.build_chat_stats(target[1], st))
            return
        st = db.stats()
        await event.respond(
            "📊 <b>Статистика</b>\n"
            f"В кэше: {st['messages']} сообщ. из {st['chats']} чатов\n"
            f"Отслеживаем чатов: {db.count_monitored()}\n"
            f"За сутки: {st['messages_24h']} сообщ., правок {st['edits_24h']}\n"
            f"Поймано всего: удалений {db.get_int('stat:deletes')}, "
            f"правок {db.get_int('stat:edits')}, чисток {db.get_int('stat:purges')}\n"
            f"Файлов на диске: {st['files']} ({st['media_bytes'] / 1024 / 1024:.1f} МБ "
            f"из {config.MEDIA_MAX_TOTAL_MB} МБ)\n"
            f"В дайджесте ждёт: {db.digest_count()}\n"
            f"Состояние: {'⏸ пауза' if paused() else '▶️ работает'}",
            parse_mode="html",
        )

    elif cmd == "find":
        if not arg:
            st = db.stats()
            await event.respond(
                f"🔎 <b>Поиск по архиву</b> — {st['messages']} сообщений, "
                "включая удалённые.\n"
                "Напиши слово следом: <code>/find пароль</code>\n"
                "<i>Ищет и по началу слова: «сейф» найдёт «сейфа», «сейфов».</i>",
                parse_mode="html")
            return
        rows = db.search(arg, config.SEARCH_LIMIT)
        if not rows:
            await event.respond(f"По «{alerts.esc(arg)}» ничего.", parse_mode="html")
            return
        out = [f"🔎 <b>Найдено ({len(rows)}):</b>"]
        for r in rows:
            who = alerts.esc(r["sender_name"] or "?")
            where = alerts.esc(r["chat_title"] or r["chat_id"])
            snip = alerts.esc(r["snip"] or "").replace("\x01", "<b>").replace("\x02", "</b>")
            out.append(f"\n<b>{alerts.fmt_time(r['sent_ts'])}</b> · {who} в «{where}»\n{snip}")
        await reply_long(event, "\n".join(out))

    elif cmd == "chats":
        rows = db.find_chats(arg) if arg else db.monitored_chats()
        if not rows:
            await event.respond("Ничего не нашёл.")
            return
        lines = [f"👁 <b>Отслеживаемые чаты ({len(rows)}):</b>" if not arg
                 else f"🔎 <b>Чаты по «{alerts.esc(arg)}»:</b>"]
        for r in rows[:40]:
            mark = "✅" if (r["override"] if r["override"] is not None else r["monitored"]) else "🚫"
            pin = " 📌" if r["override"] is not None else ""
            m = f", {r['members']}" if r["members"] else ""
            lines.append(f"{mark} {alerts.esc(r['title'])} <i>[{r['kind']}{m}]</i>{pin} "
                         f"<code>{r['chat_id']}</code>")
        await reply_long(event, "\n".join(lines))

    elif cmd in ("watch", "unwatch", "auto"):
        target, err = await resolve_chat(arg)
        if err:
            await event.respond(err, parse_mode="html")
            return
        cid, title = target
        value = {"watch": 1, "unwatch": 0, "auto": None}[cmd]
        db.set_override(cid, value)
        _monitor_cache.pop(cid, None)   # чтобы решение вступило сразу
        word = {"watch": "следим принудительно", "unwatch": "игнорируем принудительно",
                "auto": "снова по автоправилу"}[cmd]
        await event.respond(f"✅ «{alerts.esc(title)}» — {word}.", parse_mode="html")

    elif cmd == "archive":
        if not config.STORAGE_CHAT_ID:
            await event.respond("Не задан STORAGE_CHAT_ID в .env — некуда складывать.")
            return
        target, err = await resolve_chat(arg)
        if err:
            await event.respond(err, parse_mode="html")
            return
        spawn(archive_media(target[0], target[1], event))
        await event.respond(
            f"📦 Начал архивировать медиа из «{alerts.esc(target[1])}». "
            f"Пауза {config.ARCHIVE_DELAY_SEC}с между файлами, максимум "
            f"{config.ARCHIVE_MAX_PER_RUN} за раз — чтобы не словить флуд.",
            parse_mode="html",
        )

    elif cmd == "read":
        if not arg:
            await show_people_picker(
                event, "read",
                "📖 <b>Чей чат прочитать?</b> <i>(скрыто, без «прочитано»)</i>\n"
                "Или руками: <code>/read имя 30</code>")
            return
        bits = arg.split()
        n = config.READ_DEFAULT
        if len(bits) > 1 and bits[-1].isdigit():
            n = min(int(bits[-1]), config.READ_MAX)
            bits = bits[:-1]
        # Название чата может быть из нескольких слов: раньше бралось только
        # первое, и «/read вася петров» искал чат «вася».
        target, err = await resolve_chat(" ".join(bits))
        if err:
            await event.respond(err, parse_mode="html")
            return
        spawn(read_history(target[0], target[1], n, event))

    elif cmd in ("voice", "listen"):
        if not arg:
            await show_people_picker(
                event, "voice",
                "🎧 <b>Чьё голосовое прослушать скрытно?</b>\n"
                "Пришлю сюда файл и расшифровку — он не увидит, что ты слушал.\n"
                "Или руками: <code>/voice иван 3</code>")
            return
        bits = arg.split()
        n = 1
        if len(bits) > 1 and bits[-1].isdigit():
            n = min(int(bits[-1]), 5)
            bits = bits[:-1]
        target, err = await resolve_chat(" ".join(bits))
        if err:
            await event.respond(err, parse_mode="html")
            return
        spawn(listen_secretly(target[0], target[1], event, n))

    elif cmd in ("story", "stories"):
        if not arg:
            await stories_picker(event)   # список людей с активными историями
            return
        target, err = await resolve_chat(arg)
        if err:
            await event.respond(err, parse_mode="html")
            return
        spawn(view_stories(target[0], target[1], event))

    elif cmd in ("menu", "settings"):
        # Кнопки умеет только бот. Из лог-чата (юзербот) их не отрисовать —
        # тогда даём текстовый список с /toggle, а не молча ломаемся.
        if not await _respond_buttons(event, "⚙️ <b>Настройки</b>\nЧто присылать:",
                                      settings_keyboard()):
            await event.respond(_settings_text(), parse_mode="html")

    elif cmd == "toggle":
        key = arg.strip().lower()
        if key == "pause":
            db.set_meta("paused", "0" if paused() else "1")
        elif key in TOGGLES:
            set_toggle(key, not setting(key))
            _monitor_cache.clear()
        else:
            await event.respond("Что переключить: " + ", ".join(TOGGLES) + ", pause")
            return
        await event.respond(_settings_text(), parse_mode="html")

    elif cmd in ("keyboard", "kb"):
        if not await _respond_buttons(event, "Кнопки внизу включены.", main_keyboard()):
            await event.respond("Нижняя клавиатура работает только в чате с ботом.")

    elif cmd == "digest":
        n = db.digest_count()
        if n == 0:
            await event.respond("Дайджест пуст.")
        else:
            await flush_digest()
            await event.respond(f"📨 Досыл дайджеста: {n} шт.")

    elif cmd == "stop":
        n = 0
        for t in list(_story_tasks):
            t.cancel()
            n += 1
        await event.respond(f"⛔️ Прервал выгрузок: {n}." if n else "Сейчас нечего останавливать.")

    elif cmd in ("pause", "resume"):
        db.set_meta("paused", "1" if cmd == "pause" else "0")
        await event.respond("⏸ Пауза. Кэш пишется, алерты молчат."
                            if cmd == "pause" else "▶️ Снова слежу.")

    elif cmd == "cleanup":
        st = db.cleanup()
        await event.respond(
            f"🧹 Вычищено: по сроку {st['files_by_age']} файлов, "
            f"по месту {st['files_by_size']}, строк {st['rows']}, пометок {st['ignored']}."
        )

    elif cmd == "sub":
        # «/sub иван» — карточка человека с кнопками.
        # «/sub voice иван» — переключить одну функцию текстом (когда кнопок нет).
        bits = arg.split(maxsplit=1)
        if len(bits) == 2 and bits[0].lower() in SUB_FEATURES:
            feature, rest = bits[0].lower(), bits[1]
            target, err = await resolve_chat(rest)
            if err:
                await event.respond(err, parse_mode="html")
                return
            cid, title = target
            now_on = not sub_has(feature, cid)
            sub_set(feature, cid, now_on, title)
            await event.respond(
                f"{'✅ Включил' if now_on else '❌ Выключил'} "
                f"«{SUB_FEATURES[feature][1]}» для {alerts.esc(title)}.",
                parse_mode="html")
            return
        if not arg:
            await show_all_subs(event)
            return
        target, err = await resolve_chat(arg)
        if err:
            await event.respond(err, parse_mode="html")
            return
        await show_person_subs(event, target[0], target[1])

    elif cmd == "subs":
        await show_all_subs(event)

    elif cmd in ("dl", "download"):
        if not setting("downloader"):
            await event.respond("Загрузчик выключен — включи в /menu.")
            return
        url = downloader.find_url(arg)
        if not url:
            await event.respond(
                "Пришли ссылку: <code>/dl ссылка</code>\n"
                "Или просто кинь ссылку боту без команды.\n"
                "Только звук: <code>/dl ссылка mp3</code>", parse_mode="html")
            return
        audio = any(w in arg.lower() for w in ("mp3", "звук", "audio", "аудио"))
        spawn(start_download(url, event, audio_only=audio))

    elif cmd in ("proxy", "proxies"):
        st = downloader.proxy_status()
        if arg.strip().lower() in ("reload", "перечитать"):
            n = downloader.reload_proxies()
            downloader.clear_proxy_block()
            await event.respond(f"🔀 Перечитал список: адресов {n}.")
            return
        if not st["count"]:
            await event.respond(
                "🔀 <b>Прокси не настроены</b> — и чаще всего не нужны.\n"
                "Они нужны только <b>YouTube</b>, и то как запасной путь: с "
                "присланным <code>cookies.txt</code> он качается напрямую. "
                "TikTok, Instagram, Pinterest и прямые ссылки идут мимо прокси "
                "всегда.\n\n"
                "Если YouTube всё же не даётся — пришли файл со списком (по "
                "одному в строке, <code>адрес:порт:логин:пароль</code>), "
                "подхвачу сразу.",
                parse_mode="html")
            return
        if st["blocked_sec"]:
            await event.respond(
                f"🔀 Адресов в списке: {st['count']}, но они <b>не пускают</b> — "
                f"скорее всего кончился трафик.\n"
                f"Повторю попытку через {st['blocked_sec'] // 60} мин, либо "
                f"пришли новый файл со списком прямо сейчас.", parse_mode="html")
        else:
            used = db.get_int("proxy_bytes", 0)
            path = ("напрямую" if downloader.direct_first()
                    else "сначала с куками, потом через прокси")
            await event.respond(
                f"🔀 Прокси в работе: <b>{st['count']}</b>.\n"
                f"Через них ушло: <b>{_human(used)}</b>\n"
                f"Сейчас YouTube берётся <b>{path}</b> — начинаю с того, что "
                "сработало в прошлый раз, чтобы зря не жечь платный трафик.\n"
                "—\n"
                "<i>Прокси нужны только YouTube, да и тем запасным путём: с "
                "куками он идёт напрямую. TikTok, Instagram и прямые ссылки "
                "идут мимо прокси всегда.</i>\n"
                "Обновить: пришли новый файл. Перечитать: <code>/proxy reload</code>",
                parse_mode="html")

    elif cmd == "cookies":
        # Считаем ВСЕ файлы, а не только общий: после разделения по
        # площадкам куки Instagram лежат в своём файле, и старая проверка
        # отвечала «куков нет», хотя они только что приняты.
        jars = downloader.all_jars()
        have = jars[0][1] if jars else None
        if arg.strip().lower() in ("off", "del", "удалить", "выкл"):
            # Чистим и общий файл, и файлы площадок: иначе «убрать всё»
            # оставляло бы куки Instagram лежать и работать дальше.
            try:
                downloader.COOKIES_PATH.unlink(missing_ok=True)
                for f in downloader.COOKIES_PATH.parent.glob("cookies.*.txt"):
                    f.unlink(missing_ok=True)
            except OSError:
                pass
            # Куки могут лежать не только здесь: путь бывает прописан в .env, и
            # его мы не трогаем. Раньше в обоих случаях бодро отвечали «удалены»,
            # а файл оставался на диске и продолжал уходить в yt-dlp — человек
            # считал доступ отозванным, хотя тот работал.
            left_all = downloader.all_jars()
            left = left_all[0][1] if left_all else None
            if left:
                await event.respond(
                    "🍪 Присланный файл удалён, <b>но куки ещё используются</b>: "
                    f"путь задан в <code>.env</code> — <code>{html.escape(str(left))}</code>.\n\n"
                    "Чтобы отключить совсем, убери оттуда строку "
                    "<code>YTDLP_COOKIES</code> и перезапусти "
                    "(<code>systemctl restart telegram-antidelete</code>), "
                    "либо удали сам файл.", parse_mode="html")
            else:
                await event.respond("🍪 Куки удалены с сервера.")
            return
        if have:
            import os.path as _op
            lines = []
            for label, path in jars:
                age = int((time.time() - _op.getmtime(path)) / 86400)
                lines.append(f"• <b>{html.escape(label)}</b> — {age} дн. назад")
            await event.respond(
                "🍪 <b>Куки на месте:</b>\n" + "\n".join(lines) +
                "\n\nДля каждой площадки свой файл. Перестало качаться — "
                "пришли свежий, остальные не тронутся.\n"
                "Убрать всё: <code>/cookies off</code>", parse_mode="html")
        else:
            await event.respond(
                "🍪 <b>Куков нет.</b> Нужны они только для <b>YouTube</b>: он не "
                "пускает серверные адреса. TikTok, Instagram, Pinterest и прямые "
                "ссылки качаются и без них.\n\n"
                "С куками YouTube качается <b>напрямую</b> — прокси не нужен, "
                "платный трафик не тратится. И только так открываются ролики "
                "с возрастным ограничением.\n\n"
                "Что сделать:\n"
                "1) заведи <b>отдельный</b> пустой аккаунт Google (не основной);\n"
                "2) войди им в YouTube в приватном окне;\n"
                "3) расширением «Get cookies.txt LOCALLY» выгрузи файл;\n"
                "4) закрой окно, <b>не нажимая «выйти»</b> — иначе куки протухнут;\n"
                "5) <b>пришли файл мне сюда</b>.",
                parse_mode="html")

    elif cmd == "export":
        if not arg:
            await show_people_picker(
                event, "export",
                "📄 <b>Какой чат выгрузить в файл?</b>\n"
                "Или руками: <code>/export иван</code>")
            return
        target, err = await resolve_chat(arg)
        if err:
            await event.respond(err, parse_mode="html")
            return
        spawn(do_export(target[0], target[1], event))

    elif cmd in ("help", "start"):
        if cmd == "start":
            await event.respond("👋 Кнопки внизу — частые действия. Полный список — /help",
                                buttons=main_keyboard())
        await reply_long(event, HELP_TEXT.format(gmax=config.GROUP_MAX_MEMBERS))


async def maybe_save_proxies(event, msg) -> bool:
    """Принять новый список прокси файлом.

    Трафик у прокси кончается, и менять список придётся регулярно. Заставлять
    ради этого лезть на сервер неправильно — принимаем файлом, как куки.
    """
    f = getattr(msg, "file", None)
    name = (getattr(f, "name", None) or "").lower()
    if "prox" not in name:
        return False
    if (getattr(f, "size", 0) or 0) > 2 * 1024 * 1024:
        await event.respond("Файл слишком большой для списка прокси — вряд ли это он.")
        return True
    tmp = config.BASE_DIR / "proxies.incoming"
    try:
        await msg.download_media(file=str(tmp))
        text = tmp.read_text(encoding="utf-8", errors="replace")
        good = [ln for ln in text.splitlines() if downloader._parse_proxy_line(ln)]
        if not good:
            tmp.unlink(missing_ok=True)
            await event.respond(
                "В файле не нашёл ни одного прокси. Нужен список по одному в "
                "строке: <code>адрес:порт:логин:пароль</code>", parse_mode="html")
            return True
        tmp.replace(downloader.PROXIES_PATH)
        os.chmod(downloader.PROXIES_PATH, 0o600)
        n = downloader.reload_proxies()
        downloader.clear_proxy_block()      # новый список — новый лимит
        await event.respond(
            f"🔀 <b>Список прокси принят</b> — адресов: {n}.\n"
            "Подхватил сразу, перезапуск не нужен. Попробуй ссылку с YouTube.",
            parse_mode="html")
        log.info("Принят новый список прокси: %s адресов", n)
        return True
    except Exception as e:
        log.exception("Не смог принять список прокси")
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        await event.respond(f"❌ Не смог сохранить: {alerts.esc(e)}", parse_mode="html")
        return True


async def maybe_save_cookies(event, msg) -> bool:
    """Принять cookies.txt, присланный боту файлом.

    Куки нужны ради YouTube: он не пускает серверные IP. Остальные площадки
    качаются и без них (TikTok идёт своим путём, через прямые ссылки).
    Просить лезть в .env ради этого неправильно: проще прислать файл боту.
    Возвращает True, если сообщение было попыткой прислать куки.
    """
    f = getattr(msg, "file", None)
    name = (getattr(f, "name", None) or "").lower()
    size = getattr(f, "size", 0) or 0
    if not (name.endswith(".txt") or "cookie" in name):
        return False
    if size > 2 * 1024 * 1024:
        await event.respond("Файл слишком большой для куков — вряд ли это они.")
        return True

    tmp = config.BASE_DIR / "cookies.incoming"
    try:
        await msg.download_media(file=str(tmp))
        text = tmp.read_text(encoding="utf-8", errors="replace")
        if not downloader.looks_like_cookies(text):
            tmp.unlink(missing_ok=True)
            await event.respond(
                "Это не похоже на cookies.txt. Нужен формат Netscape — его даёт "
                "расширение вроде «Get cookies.txt LOCALLY».")
            return True
        # Смотрим, чьи это куки, и кладём под свою площадку. Один общий файл
        # раньше означал, что куки Instagram затирали ютубовские и наоборот.
        doms = downloader.jar_domains(tmp)
        keys = {downloader.site_key("https://" + d) for d in doms}
        keys.discard("")
        if len(keys) == 1:
            key = keys.pop()
            dest = downloader.COOKIES_PATH.parent / f"cookies.{key}.txt"
            where = f"{key}.com"
        else:
            dest = downloader.COOKIES_PATH      # разнородный файл — общий
            where = "сайты: " + ", ".join(sorted(doms)[:4]) if doms else "неизвестно что"
        tmp.replace(dest)
        os.chmod(dest, 0o600)
        n = sum(1 for ln in text.splitlines() if ln.strip() and not ln.startswith("#"))
        await event.respond(
            f"🍪 <b>Куки приняты</b> — записей: {n}, для <b>{html.escape(where)}</b>.\n"
            "Кинь ссылку ещё раз — теперь войду под этим аккаунтом.\n\n"
            "<i>Куки других площадок не затронуты: для каждой держу свой файл.</i>\n\n"
            "<i>Файл лежит только на твоём сервере, права — только владельцу, "
            "в git и в поставку не попадает. Но это доступ к твоим аккаунтам на "
            "площадках: если куки утекут, меняй пароли. Убрать — /cookies off</i>",
            parse_mode="html")
        log.info("Приняты куки для загрузчика: %s записей", n)
        return True
    except Exception as e:
        log.exception("Не смог принять куки")
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        await event.respond(f"❌ Не смог сохранить: {alerts.esc(e)}", parse_mode="html")
        return True


def _is_audio_msg(msg) -> bool:
    """Голосовое, кружок или аудиофайл — то, что имеет смысл расшифровывать."""
    return bool(getattr(msg, "voice", None) or getattr(msg, "video_note", None)
                or getattr(msg, "audio", None))


async def on_bot_command(event):
    """Всё, что пришло боту в личку.

    Бота может найти и написать кто угодно, а команды дают доступ ко всему
    архиву переписки — поэтому отвечаем строго владельцу и никому больше.

    Кроме команд бот понимает два жеста без всякого синтаксиса:
      переслал голосовое  -> расшифрую
      кинул ссылку        -> скачаю
    Именно в личке бота, а не в Избранном: там ты кидаешь ссылки постоянно,
    и качать их все было бы дичью.
    """
    if event.sender_id != _me_id:
        log.warning("Чужой пишет боту (id %s) — игнорирую", event.sender_id)
        return

    msg = getattr(event, "message", None)
    if msg is not None and _is_audio_msg(msg):
        if not setting("transcribe"):
            await event.respond("Расшифровка выключена — включи в /menu.")
            return
        await transcribe_on_demand(event, msg)
        return

    # Прислали файл — возможно, это куки или список прокси для загрузчика
    if msg is not None and getattr(msg, "document", None):
        if await maybe_save_proxies(event, msg):
            return
        if await maybe_save_cookies(event, msg):
            return

    text = (event.raw_text or "").strip()
    if text and not text.startswith("/") and text not in KEYBOARD_ACTIONS:
        url = downloader.find_url(text)
        if url:
            if not setting("downloader"):
                await event.respond("Загрузчик выключен — включи в /menu.")
                return
            spawn(start_download(url, event))
            return

    await on_command(event)


def settings_keyboard():
    """Инлайн-клавиатура переключателей. Только у бота — у юзербота кнопок нет."""
    from telethon import Button
    rows = []
    for key, (label, _) in TOGGLES.items():
        mark = "✅" if setting(key) else "❌"
        rows.append([Button.inline(f"{mark} {label}", f"tog:{key}")])
    p = paused()
    rows.append([Button.inline(("▶️ Снять паузу" if p else "⏸ Пауза всех алертов"),
                               "tog:paused")])
    return rows


def _settings_text() -> str:
    """Текстовый вид настроек (когда кнопки недоступны — из лог-чата юзербота)."""
    lines = ["⚙️ <b>Настройки</b> (переключить: <code>/toggle ключ</code>)"]
    for key, (label, _) in TOGGLES.items():
        lines.append(f"{'✅' if setting(key) else '❌'} {label} — <code>/toggle {key}</code>")
    lines.append(f"{'⏸' if paused() else '▶️'} Пауза — <code>/toggle pause</code>")
    return "\n".join(lines)


async def _respond_buttons(event, text, buttons) -> bool:
    """Пробует ответить с кнопками. Кнопки умеет только бот; при неудаче — False."""
    # Нет кнопок — нет и сообщения. Иначе после каждой скачки mp3 прилетало
    # «Нужно иначе?» без единой кнопки: у звука вариантов качества не бывает,
    # result_buttons() возвращает None, а текст всё равно уходил.
    if not buttons:
        return False
    try:
        await event.respond(text, parse_mode="html", buttons=buttons)
        return True
    except Exception as e:
        log.debug("Кнопки не отрисовались (вероятно юзербот): %s", e)
        return False


# ---------------------------------------------------------------- экраны подписок

# Имена, показанные в карточках. Кнопка несёт только id (64 байта на всё), а
# db.peer_name для найденного по @username вернёт голое число: переписки с ним
# может не быть вовсе. Без этого после нажатия кнопки имя превращалось в id.
_person_titles = {}
PERSON_TITLES_KEEP = 200


def remember_title(peer_id: int, title: str) -> None:
    if not title or str(peer_id) == str(title):
        return
    _person_titles[peer_id] = title
    for old in list(_person_titles)[:-PERSON_TITLES_KEEP]:
        _person_titles.pop(old, None)


def peer_title(peer_id: int) -> str:
    """Имя человека: показанное недавно -> сохранённое в подписке -> из чатов
    -> из переписки. Числовой id — только если больше взять негде."""
    t = _person_titles.get(peer_id) or db.sub_title(peer_id)
    if t:
        return t
    row = db.get_chat(peer_id)
    if row and row["title"]:
        return row["title"]
    return db.peer_name(peer_id)


def person_subs_keyboard(peer_id: int):
    from telethon import Button
    return [[Button.inline(f"{'✅' if sub_has(f, peer_id) else '❌'} {emoji} {label}",
                           f"sub:{f}:{peer_id}")]
            for f, (emoji, label) in SUB_FEATURES.items()]


def people_keyboard(action: str, limit: int = 12):
    """Кнопки-люди из недавней переписки. Чтобы выбрать человека тыком,
    а не печатать его имя — это было главное неудобство подписок."""
    from telethon import Button
    rows = []
    for pid in db.active_peers(90, limit * 3):
        if pid == _me_id or pid <= 0:
            continue          # себя и группы в выборе людей не показываем
        name = peer_title(pid)
        rows.append([Button.inline(f"👤 {name}"[:60], f"pick:{action}:{pid}")])
        if len(rows) >= limit:
            break
    return rows


def _person_subs_text(peer_id: int) -> str:
    return "\n".join(
        f"{'✅' if sub_has(f, peer_id) else '❌'} {emoji} {label} — "
        f"<code>/sub {f} {peer_id}</code>"
        for f, (emoji, label) in SUB_FEATURES.items())


async def show_people_picker(event, action: str, head: str) -> None:
    """Показать список людей кнопками. Без кнопок (юзербот) — просто подсказка."""
    rows = people_keyboard(action)
    if rows and await _respond_buttons(event, head, rows):
        return
    await event.respond(head, parse_mode="html")


async def show_person_subs(event, peer_id: int, title: str) -> None:
    remember_title(peer_id, title)
    head = f"🔔 <b>{alerts.esc(title)}</b>\nЧто включить на этого человека:"
    if not await _respond_buttons(event, head, person_subs_keyboard(peer_id)):
        await event.respond(head + "\n" + _person_subs_text(peer_id), parse_mode="html")


SUBS_PAGE = 8


def all_subs_keyboard(offset: int = 0):
    """Подписки страницами: раньше список молча резался на 12-й, и снять
    подписку с тринадцатого человека было нечем."""
    from telethon import Button
    items = [(f, emoji, p["peer_id"], p["title"] or str(p["peer_id"]))
             for f, (emoji, _lbl) in SUB_FEATURES.items()
             for p in db.sub_list(f)]
    page = items[offset:offset + SUBS_PAGE]
    rows = [[Button.inline(f"{emoji} {name} · убрать"[:60], f"unsub:{feat}:{pid}")]
            for feat, emoji, pid, name in page]
    nav = []
    if offset > 0:
        nav.append(Button.inline("⬅️ Назад", f"subs:{max(0, offset - SUBS_PAGE)}"))
    if offset + SUBS_PAGE < len(items):
        nav.append(Button.inline("➡️ Ещё", f"subs:{offset + SUBS_PAGE}"))
    if nav:
        rows.append(nav)
    rows.append([Button.inline("➕ Добавить человека", "pickmenu:sub")])
    return rows


def subs_header(total: int, offset: int = 0) -> str:
    if not total:
        return ("🔔 <b>Подписок пока нет</b>\n"
                "Выбери человека кнопкой ниже — или <code>/sub @username</code>")
    tail = f" · показаны {offset + 1}-{min(offset + SUBS_PAGE, total)}" \
        if total > SUBS_PAGE else ""
    return f"🔔 <b>Подписки: {total}</b>{tail}\nТык по человеку — снять подписку."


async def show_all_subs(event, offset: int = 0) -> None:
    total = db.sub_count()
    head = subs_header(total, offset)
    if await _respond_buttons(event, head, all_subs_keyboard(offset)):
        return
    # Юзербот кнопок не умеет — тогда списком с командами
    lines = [head]
    for feat, (emoji, _lbl) in SUB_FEATURES.items():
        for p in db.sub_list(feat):
            lines.append(f"{emoji} {alerts.esc(p['title'] or p['peer_id'])} — "
                         f"снять: <code>/sub {feat} {p['peer_id']}</code>")
    if total == 0:
        lines.append("Добавить: <code>/sub @username</code>")
    await reply_long(event, "\n".join(lines))


# ---------------------------------------------------------------- экспорт чата


def _safe_name(title: str) -> str:
    """Имя файла без сюрпризов: слэш в названии чата увёл бы файл в другую папку."""
    keep = [c if (c.isalnum() or c in " -_") else "_" for c in (title or "chat")]
    return ("".join(keep).strip() or "chat")[:50]


async def do_export(chat_id: int, title: str, event) -> None:
    """Выгрузить переписку в HTML и отдать файлом."""
    config.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    # Имя уникальное: два одновременных /export по разным чатам иначе писали бы
    # в один файл и стирали его друг у друга в finally.
    path = (config.DOWNLOAD_DIR /
            f"Переписка_{_safe_name(title)}_{int(time.time() * 1000)}.html")
    try:
        await event.respond(f"📄 Собираю «{alerts.esc(title)}»…", parse_mode="html")
        # В отдельном потоке и на своём подключении к базе: выгрузка большого
        # чата иначе застопорила бы приём удалений на секунды.
        res = await asyncio.to_thread(export.write_html, chat_id, title, str(path))
        if not res["count"]:
            await event.respond("В архиве по этому чату пусто.")
            return
        scope = (f"последние {res['count']} из {res['total']}"
                 if res.get("truncated") else f"{res['count']} сообщений")
        await event.respond(
            f"📄 <b>{alerts.esc(title)}</b>\n"
            f"<i>{scope} · картинок вшито {res['images']} · "
            f"{res['bytes'] / 1024 / 1024:.1f} МБ</i>\n"
            "Открывается двойным кликом, интернет не нужен.",
            file=str(path), parse_mode="html")
        log.info("Экспорт «%s»: %s из %s сообщений", title, res["count"], res["total"])
    except Exception as e:
        log.exception("Экспорт упал")
        await event.respond(f"❌ Не смог выгрузить: {alerts.esc(e)}", parse_mode="html")
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


# Нижняя клавиатура: постоянные кнопки над полём ввода. Каждая присылает свой
# текст обычным сообщением — KEYBOARD_ACTIONS переводит его в команду.
KEYBOARD_ACTIONS = {
    "⚙️ Настройки": "/menu",
    "🔔 Подписки": "/subs",
    "👁 Истории": "/story",
    "🎧 Прослушать": "/voice",
    "🔎 Поиск": "/find",
    "💬 Чаты": "/chats",
    "📊 Статистика": "/stats",
    "❓ Помощь": "/help",
    "⏸ Пауза": "/pause",
    "▶️ Продолжить": "/resume",
}


def main_keyboard():
    """Нижняя клавиатура с частыми действиями — по два в ряд, чтобы не разъезжалось."""
    from telethon import Button
    b = lambda t: Button.text(t, resize=True)
    return [
        [b("⚙️ Настройки"), b("🔔 Подписки")],
        [b("👁 Истории"), b("🎧 Прослушать")],
        [b("🔎 Поиск"), b("💬 Чаты")],
        [b("📊 Статистика"), b("❓ Помощь")],
        [b("⏸ Пауза"), b("▶️ Продолжить")],
    ]


async def on_bot_callback(event):
    """Нажатия на кнопки настроек. Только владелец."""
    if event.sender_id != _me_id:
        await event.answer("Не для тебя.", alert=True)
        return
    data = event.data.decode() if isinstance(event.data, bytes) else event.data

    # Кнопки-люди из пикера историй и «сохранённые в профиле» (с пагинацией)
    if data.startswith(("story:", "pin:", "act:")):
        await event.answer("Смотрю…")
        parts = data.split(":")
        try:
            pid = int(parts[1])
        except (IndexError, ValueError):
            return
        title = peer_title(pid)
        offset = int(parts[2]) if len(parts) > 2 and parts[2].lstrip("-").isdigit() else 0
        if parts[0] == "story":
            spawn(view_stories(pid, title, event))
        elif parts[0] == "act":
            spawn(view_stories(pid, title, event, offset))
        else:
            spawn(view_pinned_stories(pid, title, event, offset))
        return

    # Подписки: sub — переключить и перерисовать карточку человека,
    # unsub — снять из общего списка и перерисовать список.
    if data.startswith("sub:") or data.startswith("unsub:"):
        try:
            prefix, feature, pid_raw = data.split(":", 2)
            pid = int(pid_raw)
        except ValueError:
            await event.answer("Кнопка не распознана.")
            return
        if feature not in SUB_FEATURES:
            await event.answer("Неизвестная функция.")
            return
        if prefix == "unsub":
            sub_set(feature, pid, False)
            await event.answer("Снято")
            try:
                await event.edit(subs_header(db.sub_count()), parse_mode="html",
                                 buttons=all_subs_keyboard())
            except Exception:
                pass
            return
        now_on = not sub_has(feature, pid)
        name = peer_title(pid)
        sub_set(feature, pid, now_on, name)
        await event.answer("Включено" if now_on else "Выключено")
        try:
            await event.edit(f"🔔 <b>{alerts.esc(name)}</b>\n"
                             "Что включить на этого человека:",
                             parse_mode="html", buttons=person_subs_keyboard(pid))
        except Exception:
            pass
        return

    # Листание списка подписок
    if data.startswith("subs:"):
        offset = int(data[5:]) if data[5:].isdigit() else 0
        await event.answer()
        try:
            await event.edit(subs_header(db.sub_count(), offset), parse_mode="html",
                             buttons=all_subs_keyboard(offset))
        except Exception:
            pass
        return

    # «➕ Добавить человека» и прочие выборы человека кнопкой
    if data.startswith("pickmenu:"):
        action = data.split(":", 1)[1]
        rows = people_keyboard(action)
        await event.answer()
        if not rows:
            await event.respond("Не из кого выбирать — в архиве пока нет личных "
                                "чатов. Можно по имени: <code>/sub @username</code>",
                                parse_mode="html")
            return
        titles = {"sub": "🔔 На кого подписаться?",
                  "voice": "🎧 Чьё голосовое прослушать скрытно?",
                  "read": "📖 Чей чат прочитать?",
                  "export": "📄 Какой чат выгрузить?"}
        try:
            await event.edit(titles.get(action, "Выбери человека:"),
                             parse_mode="html", buttons=rows)
        except Exception:
            await event.respond(titles.get(action, "Выбери человека:"),
                                parse_mode="html", buttons=rows)
        return

    if data.startswith("pick:"):
        try:
            _, action, pid_raw = data.split(":", 2)
            pid = int(pid_raw)
        except ValueError:
            await event.answer("Кнопка не распознана.")
            return
        name = peer_title(pid)
        await event.answer(name[:60])
        if action == "sub":
            await show_person_subs(event, pid, name)
        elif action == "voice":
            spawn(listen_secretly(pid, name, event, 1))
        elif action == "read":
            spawn(read_history(pid, name, config.READ_DEFAULT, event))
        elif action == "export":
            spawn(do_export(pid, name, event))
        return

    # Кнопка «Отмена» под шкалой скачивания
    if data.startswith("dlstop:"):
        raw = data[7:]
        ok = downloader.cancel(int(raw)) if raw.isdigit() else False
        await event.answer("Отменяю…" if ok else "Загрузка уже закончилась")
        return

    # Кнопки другого качества под скачанным видео
    if data.startswith("dlq:"):
        parts = data.split(":")
        url = _dl_links.get(int(parts[1])) if len(parts) > 2 and parts[1].isdigit() else None
        h = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
        if not url or not h:
            await event.answer("Ссылка уже забылась — пришли её заново.", alert=True)
            return
        await event.answer(f"Качаю в {h}p…")
        spawn(do_download(url, event, height=h))
        return

    # Кнопка «забрать mp3» под скачанным видео
    if data.startswith("dla:"):
        url = _dl_links.get(int(data[4:])) if data[4:].isdigit() else None
        if not url:
            await event.answer("Ссылка уже забылась — пришли её заново.", alert=True)
            return
        await event.answer("Качаю звук…")
        spawn(do_download(url, event, audio_only=True))
        return

    if not data.startswith("tog:"):
        return
    key = data[4:]
    if key == "paused":
        db.set_meta("paused", "0" if paused() else "1")
    elif key in TOGGLES:
        set_toggle(key, not setting(key))
    try:
        await event.edit("⚙️ <b>Настройки</b>\nЧто присылать:",
                         parse_mode="html", buttons=settings_keyboard())
    except Exception:
        pass
    await event.answer("Готово")


async def read_history(chat_id, title, n, event) -> None:
    """Скрытый просмотр: тянет последние n сообщений БЕЗ отметки «прочитано».

    Только iter_messages (GetHistory) — ни одного исходящего действия, read receipt
    не отправляется. Единственная неустранимая утечка — если ты сам откроешь этот
    чат в обычном клиенте, тот пошлёт «прочитано».
    """
    try:
        rows = []
        async for m in client.iter_messages(chat_id, limit=n):
            sender = await m.get_sender() if not m.out else None
            rows.append({
                "sender_name": "ты" if m.out else display_name(sender),
                "is_outgoing": bool(m.out),
                "sent_ts": int(m.date.timestamp()),
                "text": m.text or "",
                "media_type": media_kind(m) if (m.media and not
                    isinstance(m.media, types.MessageMediaWebPage)) else None,
            })
        rows.reverse()
        if not rows:
            await event.respond("Пусто — нечего показать.")
            return
        await reply_long(event, alerts.build_read_dump(title, rows))
        log.info("Скрытый просмотр: %s сообщений из «%s»", len(rows), title)
    except Exception as e:
        log.exception("read_history упал")
        await event.respond(f"Не смог прочитать: {alerts.esc(e)}", parse_mode="html")


async def listen_secretly(chat_id, title, event, count: int = 1) -> None:
    """Прислать последние голосовые/кружочки из чата, не отметив их прослушанными.

    Смысл ровно в этом: текст в Telegram можно подсмотреть, зажав чат в списке,
    а голосовое — нет, его надо открыть, и отправитель увидит, что ты слушал.
    Здесь идёт только чтение истории и скачивание файла — «прослушано»
    (readMessageContents) не отправляется, поэтому у него остаётся непрослушанным.
    """
    tmp = config.MEDIA_DIR / "listen"
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        found = []
        async for m in client.iter_messages(chat_id, limit=200):
            if m.out or not m.media:
                continue
            kind = media_kind(m)
            if is_voice_kind(kind):
                found.append((m, kind))
                if len(found) >= count:
                    break
        if not found:
            await event.respond(
                f"В «{alerts.esc(title)}» голосовых не нашёл — смотрел последние "
                "200 сообщений.", parse_mode="html")
            return

        for m, kind in found:
            path = None
            try:
                path = await m.download_media(file=str(tmp / f"{chat_id}_{m.id}"))
                head = (f"🎧 <b>{alerts.esc(title)}</b> · {esc_kind(kind)}\n"
                        f"<i>{alerts.fmt_time(int(m.date.timestamp()))} "
                        f"· он/она не увидит, что ты слушал</i>")
                vk = await alerts.video_kwargs(path)   # кружочек — тоже видео
                await event.respond(head, file=path, parse_mode="html", **vk)
                if setting("transcribe") and path:
                    text, err = await transcribe.transcribe(path)
                    await reply_long(event, "🎙 " + (alerts.esc(text) if text
                                     else f"<i>расшифровать не вышло: {alerts.esc(err)}</i>"))
            except Exception as e:
                log.exception("Скрытое прослушивание упало")
                await event.respond(f"❌ {alerts.esc(e)}", parse_mode="html")
            finally:
                if path:
                    try:
                        os.remove(path)
                    except OSError:
                        pass
        log.info("Скрыто прослушано %s голосовых из «%s»", len(found), title)
    except Exception as e:
        log.exception("listen_secretly упал")
        await event.respond(f"Не смог: {alerts.esc(e)}", parse_mode="html")


def esc_kind(kind: str) -> str:
    return alerts.esc("кружочек" if kind == "видеосообщение" else kind)


_story_tasks = set()   # активные выгрузки историй — чтобы можно было /stop


async def _send_story_items(chat_id, items, title, emoji, event) -> None:
    """Скачивает и отдаёт список историй. Просмотр НЕ отмечаем.

    Отменяемо через /stop (CancelledError не глушим). Между отправками пауза,
    чтобы пачка не словила FloodWait.
    """
    tmp = config.MEDIA_DIR / "stories"
    tmp.mkdir(parents=True, exist_ok=True)
    for i, it in enumerate(items):
        sid = getattr(it, "id", "?")
        media = getattr(it, "media", None)

        # В ответе API среди историй попадаются «заглушки»: StoryItemSkipped
        # (есть, но не отдана — например только для близких друзей) и
        # StoryItemDeleted. Поля media у них нет вовсе, и раньше на них
        # приходило бессмысленное «медиа не скачалось: нет атрибута media».
        if media is None:
            why = ("удалена" if type(it).__name__.endswith("Deleted")
                   else "недоступна — скорее всего, только для близких друзей")
            await notify(f"{emoji} <b>История #{sid}</b> — {why}.")
            await asyncio.sleep(0.3)
            continue

        cap = alerts.esc(getattr(it, "caption", None) or "")
        when = alerts.fmt_time(int(it.date.timestamp())) if getattr(it, "date", None) else ""
        kind = "видео" if getattr(media, "document", None) is not None else "фото"
        head = (f"{emoji} <b>История #{sid}</b> · {when} · {kind}"
                + (f"\n{cap}" if cap else ""))
        path = None
        try:
            # уникальное имя (chat_id, story_id, индекс) — два параллельных дампа
            # одного чата не перетрут файл друг друга
            path = await client.download_media(
                media, file=str(tmp / f"{chat_id}_{sid}_{i}"))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("История %s/%s не скачалась: %s", chat_id, sid, e)
            head += f"\n<i>медиа не скачалось: {alerts.esc(e)}</i>"
        if path and not os.path.getsize(path):
            # Пустой файл Telegram примет, но в чате он выглядит битым
            log.warning("История %s/%s скачалась пустой", chat_id, sid)
            head += "\n<i>файл пришёл пустым — Telegram его не отдал</i>"
            try:
                os.remove(path)
            except OSError:
                pass
            path = None
        try:
            await notify(head, file=path)
        finally:
            # Файл истории нигде не учтён в БД — чистим всегда, иначе утечёт
            # на диск мимо cleanup(). Не доехало — /story перезапросит.
            if path:
                try:
                    os.remove(path)
                except OSError:
                    pass
        await asyncio.sleep(0.5)


async def view_stories(chat_id, title, event, offset: int = 0) -> None:
    """Скрытый просмотр активных Stories + кнопка на сохранённые в профиле."""
    from telethon import Button
    try:
        entity = await client.get_input_entity(chat_id)
    except Exception as e:
        await event.respond(f"Не нашёл такого: {alerts.esc(e)}", parse_mode="html")
        return

    active, pinned_count = [], 0
    try:
        res = await client(GetPeerStoriesRequest(peer=entity))
        active = list(getattr(res.stories, "stories", []) or [])
    except Exception as e:
        log.debug("GetPeerStories %s: %s", chat_id, e)
    try:
        pin = await client(GetPinnedStoriesRequest(peer=entity, offset_id=0, limit=1))
        pinned_count = getattr(pin, "count", len(getattr(pin, "stories", [])))
    except Exception as e:
        log.debug("GetPinnedStories %s: %s", chat_id, e)

    rows = []
    # Кнопка «ещё» для активных: раньше их показывалось ровно STORY_BATCH,
    # и добраться до остальных было нечем — выглядело как «выгрузил не всё».
    if offset + config.STORY_BATCH < len(active):
        left = len(active) - offset - config.STORY_BATCH
        rows.append([Button.inline(f"➡️ Показать ещё ({left})",
                                   f"act:{chat_id}:{offset + config.STORY_BATCH}")])
    if pinned_count:
        rows.append([Button.inline(f"📌 Сохранённые в профиле ({pinned_count})",
                                   f"pin:{chat_id}:0")])
    btn = rows or None

    if not active:
        msg = f"У «{alerts.esc(title)}» сейчас нет активных историй."
        if pinned_count:
            msg += f" Но есть сохранённые в профиле: {pinned_count}."
        if not (btn and await _respond_buttons(event, msg, btn)):
            await event.respond(msg, parse_mode="html")
        return

    batch = active[offset: offset + config.STORY_BATCH]
    if not batch:
        await event.respond("Активных историй больше нет.")
        return
    shown_to = offset + len(batch)
    note = (f" (показываю {offset + 1}–{shown_to} из {len(active)}, /stop чтобы прервать)"
            if len(active) > len(batch) else "")
    head = (f"👁 <b>{alerts.esc(title)}</b> — активных историй: {len(active)}"
            f"{note} <i>(скрыто)</i>")
    if not (btn and await _respond_buttons(event, head, btn)):
        await event.respond(head, parse_mode="html")
    await _run_story_dump(_send_story_items(chat_id, batch, title, "🟢", event), event)
    log.info("Скрытый просмотр историй: %s активных у «%s»", len(batch), title)


async def view_pinned_stories(chat_id, title, event, offset_id=0) -> None:
    """Сохранённые в профиле («висящие») истории — постранично, скрыто."""
    from telethon import Button
    try:
        entity = await client.get_input_entity(chat_id)
        pin = await client(GetPinnedStoriesRequest(
            peer=entity, offset_id=offset_id, limit=config.STORY_BATCH))
        items = list(getattr(pin, "stories", []) or [])
        total = getattr(pin, "count", len(items))
    except Exception as e:
        await event.respond(f"Не смог получить сохранённые: {alerts.esc(e)}", parse_mode="html")
        return
    if not items:
        await event.respond("Сохранённых в профиле историй больше нет.")
        return

    last_id = items[-1].id
    # Полная порция — вероятно, есть ещё. Неполная — точно последняя.
    has_more = len(items) >= config.STORY_BATCH
    more_btn = ([[Button.inline("➡️ Показать ещё", f"pin:{chat_id}:{last_id}")]]
                if has_more else None)

    await event.respond(f"📌 <b>{alerts.esc(title)}</b> — сохранённые в профиле "
                        f"(всего {total}, порция {len(items)}) <i>(скрыто)</i>",
                        parse_mode="html")
    await _run_story_dump(_send_story_items(chat_id, items, title, "📌", event), event)
    if more_btn:
        await _respond_buttons(event, "Дальше?", more_btn)
    log.info("Скрытый просмотр сохранённых: порция %s у «%s»", len(items), title)


async def _run_story_dump(coro, event) -> None:
    """Запускает выгрузку историй как отменяемую задачу (для /stop)."""
    task = asyncio.current_task()
    _story_tasks.add(task)
    try:
        await coro
    except asyncio.CancelledError:
        try:
            await event.respond("⛔️ Остановлено.")
        except Exception:
            pass
        raise
    finally:
        _story_tasks.discard(task)


async def stories_picker(event) -> None:
    """Показывает кнопки-людей, у кого сейчас есть активные истории."""
    from telethon import Button
    try:
        allst = await client(GetAllStoriesRequest())
    except Exception as e:
        await event.respond(f"Не смог получить ленту историй: {alerts.esc(e)}",
                            parse_mode="html")
        return
    from telethon import utils as _tlutils
    names = {}
    for u in list(allst.users) + list(allst.chats):
        nm = display_name(u)
        if nm and nm != "без имени":
            names[u.id] = nm
    rows, found = [], []
    for ps in allst.peer_stories:
        # get_peer_id -> маркированный id (у канала -100…), совпадает с тем,
        # что ждут get_input_entity/get_chat. Сырой channel_id ломал бы кнопку.
        try:
            pid = _tlutils.get_peer_id(ps.peer)
        except Exception:
            continue
        raw = getattr(ps.peer, "user_id", None) or getattr(ps.peer, "channel_id", None) \
            or getattr(ps.peer, "chat_id", None)
        # имя: из ответа API (по сырому id) -> из нашего кэша чатов -> резолвом -> id
        name = names.get(raw)
        if not name:
            row = db.get_chat(pid)
            name = row["title"] if row else None
        if not name:
            name = str(pid)
        label = f"👤 {name} ({len(ps.stories)})"
        rows.append([Button.inline(label[:60], f"story:{pid}")])
        found.append((pid, name, len(ps.stories)))
    if not rows:
        await event.respond("Сейчас ни у кого в ленте нет активных историй.\n"
                            "Можно посмотреть у конкретного человека: "
                            "<code>/story @username</code>", parse_mode="html")
        return
    head = ("👁 <b>Чьи истории посмотреть?</b> "
            "<i>(просмотр скрытый)</i>\nИли: <code>/story @username</code>")
    if await _respond_buttons(event, head, rows):
        return
    # Из лог-чата кнопок нет — раньше здесь падало и ответа не приходило вообще
    lines = [head] + [f"👤 {alerts.esc(name)} ({n}) — <code>/story {pid}</code>"
                      for pid, name, n in found]
    await reply_long(event, "\n".join(lines))


HELP_TEXT = """<b>Команды</b> (кнопки внизу — то же, но быстрее)

<b>⚡️ Без команд — просто кинь боту</b>
ссылку → пришлю файл без водяных знаков
голосовое или кружок → пришлю расшифровку
файл <code>cookies.txt</code> → откроет YouTube (остальное и так качается)

<b>🕵️ Скрытно</b> <i>(без имени — покажу список кнопками)</i>
<code>/voice</code> — прослушать чужое ГС: пришлю файл и текст,
а у него оно останется непрослушанным
<code>/story</code> — Stories, и тебя нет в «кто смотрел»

<b>🔔 Слежка за человеком</b>
<code>/subs</code> — подписки: ➕ добавить, тык — снять
<code>/sub иван</code> — карточка одного человека
Три тумблера на каждого: расшифровывать все его ГС,
сообщать о смене профиля, сообщать о заходах в сеть

<b>🗄 Архив переписки</b>
<code>/find слово</code> — ищет сразу по ВСЕМ чатам и находит
удалённое, чего поиск Telegram уже не умеет
<code>/export</code> — вся переписка одним HTML-файлом
<code>/stats иван</code> — сводка по человеку

<b>👁 Где ловить удаления</b>
Личку и группы до {gmax} чел. беру сам, каналы и большие — нет.
<code>/chats</code> — список, за чем слежу (📌 = решено вручную)
<code>/watch чат</code> — ловить, даже если авто решило «нет»
<code>/unwatch чат</code> — не ловить · <code>/auto чат</code> — обратно на авто

<b>⚙️ Прочее</b>
<code>/menu</code> — что присылать · <code>/pause</code> · <code>/resume</code>
<code>/dl ссылка</code> — скачать явно (<code>mp3</code> в конце — только звук)
<code>/cookies</code> — куки для YouTube: с ними качает напрямую
<code>/proxy</code> — запасной путь к нему · <code>/stop</code> — прервать выгрузку

<i>Рабочие, но убраны из меню за редкостью: /read (текст чата),
/archive (старое медиа в склад-канал), /digest, /cleanup,
/keyboard, /toggle (тумблеры из /menu текстом).</i>"""


# Описания видны в меню Telegram — не больше строки, поэтому по сути.
# Видимое меню команд. Часть команд намеренно НЕ здесь — они рабочие, но нужны
# редко и только загромождали бы список: read, archive, digest, cleanup, keyboard,
# toggle. Полный перечень — в конце /help.
BOT_MENU = [
    ("menu", "Настройки: что присылать (кнопки)"),
    ("subs", "Мои подписки на людей"),
    ("sub", "Слежка за человеком: /sub иван"),
    ("voice", "Прослушать ГС скрытно, с расшифровкой"),
    ("story", "Смотреть истории скрыто: /story иван"),
    ("dl", "Скачать по ссылке без водяных знаков"),
    ("cookies", "Куки для YouTube (по желанию)"),
    ("proxy", "Прокси: состояние и обновление"),
    ("find", "Найти во ВСЕХ чатах, даже удалённое"),
    ("export", "Выгрузить переписку в файл: /export иван"),
    ("stats", "Архив целиком или чат: /stats иван"),
    ("chats", "За какими чатами слежу"),
    ("watch", "Ловить удаления в этом чате"),
    ("unwatch", "Не ловить удаления в этом чате"),
    ("auto", "Вернуть чат под автоправило"),
    ("pause", "Заглушить уведомления"),
    ("resume", "Вернуть уведомления"),
    ("stop", "Прервать выгрузку историй"),
    ("help", "Что умеет каждая команда"),
]


async def setup_bot() -> None:
    """Поднимает бота-уведомителя, если задан токен."""
    global bot, _bot_chat
    if not config.BOT_TOKEN:
        log.info("BOT_TOKEN не задан — алерты шлёт юзербот (без уведомлений)")
        return

    bot = TelegramClient(str(config.BASE_DIR / "antidelete-bot"),
                         config.API_ID, config.API_HASH, flood_sleep_threshold=120)
    await bot.start(bot_token=config.BOT_TOKEN)
    me_bot = await bot.get_me()
    _bot_chat = config.BOT_ALERT_CHAT_ID or _me_id

    bot.add_event_handler(on_bot_command, events.NewMessage(incoming=True))
    bot.add_event_handler(on_bot_callback, events.CallbackQuery())

    try:
        await bot(SetBotCommandsRequest(
            scope=types.BotCommandScopeDefault(), lang_code="",
            commands=[types.BotCommand(c, d) for c, d in BOT_MENU],
        ))
    except Exception as e:
        log.warning("Не смог поставить меню команд: %s", e)

    log.info("Бот-уведомитель @%s готов, шлёт в чат %s", me_bot.username, _bot_chat)


async def archive_media(chat_id: int, title: str, event) -> None:
    """Форвардит историческое медиа чата в склад-канал. Медленно и с потолком.

    Форвард идёт на стороне Telegram (файл не качается на VPS), но это исходящие
    действия — отсюда пауза между ними и жёсткий лимит за прогон.
    """
    # Прогресс живёт в meta, иначе каждый следующий запуск начинал бы с самого
    # свежего сообщения и пересылал те же файлы по кругу — а пачки исходящих
    # форвардов это ровно то, за что Telegram ограничивает аккаунт.
    #   archive:top    — до какого id чат уже пройден целиком
    #   archive:offset — курсор незаконченного прохода (идём от новых к старым)
    top_key, off_key = f"archive:top:{chat_id}", f"archive:offset:{chat_id}"
    ptop_key = f"archive:passtop:{chat_id}"
    top = db.get_int(top_key, 0)
    offset = db.get_int(off_key, 0)
    # Верх ТЕКУЩЕГО прохода запоминаем отдельно и на весь проход. Считать его
    # заново в каждом прогоне нельзя: последний прогон видит уже только хвост
    # старых сообщений, и граница «пройдено до сюда» уехала бы вниз — а значит
    # следующий запуск переслал бы всё заново.
    pass_top = db.get_int(ptop_key, 0)

    sent = failed = seen = 0
    limit_hit = False
    last_id = offset
    try:
        async for msg in client.iter_messages(chat_id, offset_id=offset, min_id=top):
            seen += 1
            if not pass_top:
                pass_top = msg.id      # первая строка прохода — она же самая свежая
                db.set_meta(ptop_key, pass_top)
            last_id = msg.id
            if sent >= config.ARCHIVE_MAX_PER_RUN:
                limit_hit = True
                break
            if msg.media is None or isinstance(msg.media, types.MessageMediaWebPage):
                continue
            try:
                await client.forward_messages(config.STORAGE_CHAT_ID, msg)
                sent += 1
                db.set_meta(off_key, last_id)   # позиция после каждого успеха
            except FloodWaitError as e:
                log.warning("Архив: FloodWait %s сек — жду", e.seconds)
                await asyncio.sleep(e.seconds + 2)
                continue
            except Exception as e:
                failed += 1
                log.debug("Архив: не форварднулось %s: %s", msg.id, e)
            finally:
                # Пауза всегда, а не только на успехе — иначе при потоке ошибок
                # цикл крутился бы без задержки и мог словить флуд.
                await asyncio.sleep(config.ARCHIVE_DELAY_SEC)
    except Exception:
        log.exception("Архивация чата %s упала", chat_id)
        db.set_meta(off_key, last_id)
        await event.respond(f"📦 «{alerts.esc(title)}»: прервалось на ошибке, "
                            f"сложено {sent}. Запусти ещё раз — продолжу с места.",
                            parse_mode="html")
        return

    if limit_hit:
        db.set_meta(off_key, last_id)
        tail = " Достигнут лимит за прогон — запусти ещё раз, продолжу с этого места."
    else:
        # Дошли до конца: помечаем верхнюю границу пройденного и сбрасываем курсор,
        # чтобы следующий прогон взял только то, что появилось с тех пор.
        db.set_meta(top_key, max(top, pass_top))
        db.set_meta(off_key, 0)
        db.set_meta(ptop_key, 0)
        tail = (" Чат пройден до конца — следующий запуск возьмёт только новое."
                if seen else " Нового с прошлого раза не появилось.")

    await event.respond(
        f"📦 Готово по «{alerts.esc(title)}»: сложено {sent}"
        + (f", не удалось {failed}" if failed else "") + "." + tail,
        parse_mode="html",
    )


# ---------------------------------------------------------------- загрузчик

# Callback-данные Telegram ограничены 64 байтами — ссылка туда не влезает.
# Поэтому кнопка «только звук» носит короткий номер, а сама ссылка ждёт здесь.
_dl_links = {}
# Отсчёт начинаем не с нуля: после рестарта словарь пуст, и нумерация с единицы
# означала бы, что старая кнопка «забрать mp3» указала бы на НОВУЮ ссылку.
_dl_seq = int(time.time()) % 100000
DL_LINKS_KEEP = 30


def remember_link(url: str) -> int:
    global _dl_seq
    _dl_seq += 1
    _dl_links[_dl_seq] = url
    for old in list(_dl_links)[:-DL_LINKS_KEEP]:
        _dl_links.pop(old, None)
    return _dl_seq


PROGRESS_EVERY = 4.0   # реже правим сообщение — иначе Telegram даст FloodWait


def _human(n) -> str:
    """Байты в человеческий вид."""
    if n is None:
        return "?"
    n = float(n)
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} ТБ"


def render_progress(p: dict) -> str:
    """Шкала скачивания. Размер бывает неизвестен — тогда показываем без процентов."""
    pct = p.get("percent")
    if pct is None:
        head, bar = "📥 <b>Качаю…</b>", "▱" * 10
    else:
        filled = max(0, min(10, int(pct / 10)))
        head, bar = f"📥 <b>Качаю — {pct:.0f}%</b>", "▰" * filled + "▱" * (10 - filled)
    bits = [f"<code>{bar}</code>"]
    if p.get("done"):
        bits.append(f"{_human(p['done'])} из {_human(p.get('total'))}")
    if p.get("speed"):
        bits.append(f"{_human(p['speed'])}/с")
    if p.get("eta"):
        bits.append(f"осталось ~{alerts.fmt_delta(p['eta'])}")
    return head + "\n" + " · ".join(bits)


def dl_cache_key(url: str, height, audio: bool) -> str:
    """Ключ кэша. Метки отслеживания и тайм-код выкидываем: на сам файл они не
    влияют, а без этого одна и та же ссылка с разными хвостами считалась бы
    разной и качалась заново."""
    from urllib.parse import parse_qsl, urlencode, urlsplit
    try:
        p = urlsplit(url)
        drop = {"si", "feature", "t", "start", "list", "index", "pp", "app"}
        q = [(k, v) for k, v in parse_qsl(p.query)
             if k.lower() not in drop and not k.lower().startswith("utm_")]
        norm = f"{p.netloc.lower()}{p.path.rstrip('/')}?{urlencode(sorted(q))}"
    except Exception:
        norm = url
    return f"{norm}|{'mp3' if audio else (height or config.DOWNLOAD_QUALITY)}"


def result_buttons(job_id: int, audio_only: bool, height):
    """Кнопки под результатом: другое качество и звук отдельно."""
    from telethon import Button
    if audio_only:
        return None
    cur = int(height or config.DOWNLOAD_QUALITY)
    alts = [q for q in (1080, 720, 480, 360) if q != cur][:3]
    rows = [[Button.inline(f"{q}p", f"dlq:{job_id}:{q}") for q in alts]]
    rows.append([Button.inline("🎵 Забрать mp3", f"dla:{job_id}")])
    return rows


async def send_from_cache(event, hit, job_id: int, audio_only: bool, height) -> bool:
    """Отправить уже скачанное по сохранённому идентификатору файла.

    Ничего не качается и не заливается — Telegram пересылает свой же файл.
    Это и есть та «мгновенность», которая есть у публичных ботов.
    """
    try:
        await event.respond(
            f"📥 <b>{alerts.esc(hit['title'])}</b>\n"
            f"<i>{(hit['size'] or 0) / 1024 / 1024:.1f} МБ · уже было скачано</i>",
            file=hit["file_id"], parse_mode="html")
    except Exception as e:
        # Файл мог протухнуть на стороне Telegram — тогда просто качаем заново
        log.info("Кэш не сработал (%s), качаю заново", e)
        return False
    await _respond_buttons(event, "Нужно иначе?",
                           result_buttons(job_id, audio_only, height))
    log.info("Отдано из кэша: %s", (hit["title"] or "")[:60])
    return True


async def start_download(url: str, event, audio_only: bool = False) -> None:
    """Единая точка входа по ссылке: сперва спросить качество, если есть выбор."""
    if audio_only or not setting("ask_quality"):
        await do_download(url, event, audio_only=audio_only)
        return
    if downloader.is_tiktok(url):
        # У TikTok вариант ровно один — спрашивать не о чем
        await do_download(url, event)
        return
    if not await offer_quality(url, event):
        await do_download(url, event)


async def offer_quality(url: str, event) -> bool:
    """Показать кнопки качества. False — выбирать не из чего, качаем как есть."""
    from telethon import Button
    note = None
    try:
        note = await event.respond("🔍 Смотрю, что доступно…")
    except Exception:
        pass
    info = await downloader.probe(url)
    quals = info.get("qualities") or []
    if info.get("error") or len(quals) < 2:
        # Разведка не удалась или выбора нет — молча уходим в обычную скачку,
        # там своя обработка ошибок и свои повторы с другого адреса
        await _drop_status(note)
        return False

    lid = remember_link(url)
    rows, row = [], []
    for q in quals:
        label = f"{q['height']}p"
        if q["size"]:
            label += f" · {_human(q['size'])}"
        row.append(Button.inline(label, f"dlq:{lid}:{q['step']}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([Button.inline("🎵 Только звук", f"dla:{lid}")])

    mins = int(info.get("duration") or 0) // 60
    head = (f"🎬 <b>{alerts.esc(info['title'])}</b>\n"
            + (f"<i>{mins} мин</i>\n" if mins else "")
            + "Какое качество?")
    if note is not None:
        try:
            await note.edit(head, parse_mode="html", buttons=rows)
            return True
        except Exception:
            pass
    return await _respond_buttons(event, head, rows)


async def do_download(url: str, event, audio_only: bool = False,
                      height: int = None) -> None:
    """Скачать по ссылке и отдать файл туда же, откуда попросили.

    Отвечаем через event.respond, а не notify: это ответ на команду, а не алерт.
    Иначе крупная заливка держала бы общий замок отправки и придерживала алерты
    об удалениях.
    """
    from telethon import Button
    what = "звук" if audio_only else "видео"
    job_id = remember_link(url)
    cancel_btn = [[Button.inline("⛔️ Отмена", f"dlstop:{job_id}")]]

    # Сначала смотрим, не качали ли это уже: тогда отдаём мгновенно и без
    # шкалы — качать и заливать заново нечего.
    key = dl_cache_key(url, height, audio_only)
    if config.DOWNLOAD_CACHE_DAYS > 0:
        hit = db.dl_get(key)
        if hit and await send_from_cache(event, hit, job_id, audio_only, height):
            return
        if hit:
            db.dl_forget(key)

    # Сообщение со шкалой правим на месте. Кнопок может не быть (из лог-чата) —
    # тогда просто качаем молча, без прогресса.
    status = None
    try:
        status = await event.respond(f"📥 Готовлюсь качать {what}…",
                                     parse_mode="html", buttons=cancel_btn)
    except Exception:
        log.debug("Прогресс без кнопок — отправляю как есть")
        try:
            status = await event.respond(f"📥 Качаю {what}…")
        except Exception:
            status = None

    state = {"last": 0.0, "text": ""}

    async def on_progress(p):
        if status is None:
            return
        now = time.time()
        if now - state["last"] < PROGRESS_EVERY:
            return
        text = render_progress(p)
        if text == state["text"]:
            return
        state["last"], state["text"] = now, text
        try:
            await status.edit(text, parse_mode="html", buttons=cancel_btn)
        except Exception:
            pass   # правку могли не принять (не изменилось / флуд) — не беда

    res = await downloader.fetch(url, audio_only=audio_only,
                                 on_progress=on_progress, job_id=job_id,
                                 height=height)

    if res.get("cancelled"):
        log.info("Загрузка отменена: %s", url[:100])
        await _finish_status(status, event, "⛔️ <b>Отменено.</b> Ничего не скачал.")
        return
    if res.get("error"):
        # Провалы тоже пишем в журнал: раньше в логах были только успехи, и
        # разбираться, почему у пользователя не скачалось, было не по чему.
        log.warning("Не скачалось (%s): %s", url[:80],
                    re.sub(r"<[^>]+>", "", res["error"])[:200])
        # Предупреждение о прокси — дело десятое, и падать из-за него нельзя:
        # если алерт не доставился (бота выкинули из лог-чата, адресат недоступен),
        # исключение уносило нас мимо отчёта об ошибке. Человек оставался со
        # шкалой «Готовлюсь качать…» и живой кнопкой «Отмена» навсегда.
        try:
            await warn_if_proxy_dead()
        except Exception:
            log.exception("Не смог предупредить про прокси")
        # Текст уже с разметкой и уже экранирован внутри — второй раз не трогаем
        await _finish_status(status, event, f"❌ {res['error']}")
        return

    paths, title, job = res["paths"], res["title"], res["job"]
    try:
        size_mb = sum(os.path.getsize(p) for p in paths) / 1024 / 1024
        note = f" · файлов: {len(paths)}" if len(paths) > 1 else ""
        if res.get("extra"):
            note += f" (ещё {res['extra']} не влезли)"
        # Заливка в Telegram — тоже небыстрая часть, о ней стоит сказать
        await _edit_status(status, f"📤 Скачал {size_mb:.1f} МБ, отправляю…")
        caption = (f"📥 <b>{alerts.esc(title)}</b>\n<i>{size_mb:.1f} МБ{note}</i>")
        audio_note = res.get("audio_note")
        if audio_note == downloader.NOTE_NO_AUDIO:
            caption += f"\n<i>🔇 {alerts.esc(audio_note)}</i>"
        elif audio_note:
            caption += f"\n<i>⚠️ {alerts.esc(audio_note)}</i>"
        # Одиночному видео проставляем размеры и длительность, иначе Telegram
        # покажет его файлом, а не роликом. Для альбома атрибуты общие — не лезем.
        vk = await alerts.video_kwargs(paths[0]) if len(paths) == 1 else {}
        # Несколько файлов (карусель Instagram) уходят альбомом, а не по одному
        sent = await event.respond(caption, file=paths if len(paths) > 1 else paths[0],
                                   parse_mode="html", **vk)
        log.info("Скачано по ссылке: %s (%s файл(ов), %.1f МБ)",
                 title, len(paths), size_mb)
        # Запоминаем, чтобы в следующий раз отдать мгновенно. Только одиночные
        # файлы: альбом собирается из нескольких, и одним идентификатором его
        # не переслать.
        if res.get("via_proxy"):
            # Считаем, сколько ушло через платный канал: увидеть это до того,
            # как трафик кончится, полезнее, чем узнать от клиента после
            db.incr("proxy_bytes", int(size_mb * 1024 * 1024))
        # Без звука по нашей вине не запоминаем: иначе повтор ссылки мгновенно
        # отдавал бы тот же немой файл, хотя следующая попытка могла бы справиться
        if config.DOWNLOAD_CACHE_DAYS > 0 and len(paths) == 1 and sent is not None \
                and res.get("audio_note") != downloader.NOTE_AUDIO_FAILED:
            try:
                from telethon import utils as _tlu
                fid = _tlu.pack_bot_file_id(sent.media)
                if fid:
                    db.dl_put(key, fid, title, int(size_mb * 1024 * 1024))
            except Exception as e:
                log.debug("Не смог запомнить файл в кэше: %s", e)
        await _drop_status(status)     # шкала отработала, в чате она лишняя
        await _respond_buttons(event, "Нужно иначе?",
                               result_buttons(job_id, audio_only, height))
    except Exception as e:
        log.exception("Не смог отдать скачанное")
        await _finish_status(status, event, f"❌ Скачалось, но не отправилось: {alerts.esc(e)}")
    finally:
        downloader.discard(job)


PROXY_WARN_EVERY = 6 * 3600   # не напоминать про кончившийся трафик чаще


async def warn_if_proxy_dead() -> None:
    """Сказать отдельным уведомлением, что прокси кончились.

    Иначе про это узнаёшь, только когда сам попробуешь скачать — а для бота с
    клиентами это значит «узнаю от них».
    """
    st = downloader.proxy_status()
    if not st["blocked_sec"]:
        return
    now = int(time.time())
    if now - db.get_int("proxy_warn_ts", 0) < PROXY_WARN_EVERY:
        return
    db.set_meta("proxy_warn_ts", now)
    used = db.get_int("proxy_bytes", 0)
    await notify(
        "🔀 <b>Прокси перестали пускать</b>\n"
        f"Скорее всего кончился трафик — через них ушло примерно "
        f"{_human(used)} с последнего сброса счётчика.\n"
        "—\n"
        "<i>YouTube может не качаться. Всё остальное работает как обычно — "
        "прокси нужны только ему. Пришли новый список файлом, и я подхвачу "
        "его сразу.</i>")


async def _edit_status(status, text: str) -> None:
    """Поправить сообщение-шкалу, если оно есть. Молча: правка могла не пройти."""
    if status is None:
        return
    try:
        await status.edit(text, parse_mode="html", buttons=None)
    except Exception:
        pass


async def _drop_status(status) -> None:
    if status is None:
        return
    try:
        await status.delete()
    except Exception:
        await _edit_status(status, "✅ Готово.")


async def _finish_status(status, event, text: str) -> None:
    """Финальное состояние: снимаем кнопку отмены, пишем итог.

    Если шкалы нет (её не удалось создать) — отвечаем обычным сообщением,
    иначе про ошибку никто бы не узнал.
    """
    if status is not None:
        try:
            await status.edit(text, parse_mode="html", buttons=None)
            return
        except Exception:
            log.debug("Не смог обновить сообщение-шкалу, отвечаю отдельно")
    try:
        await event.respond(text, parse_mode="html")
    except Exception:
        log.warning("Не смог сообщить результат загрузки")


async def transcribe_on_demand(event, msg) -> None:
    """Расшифровать голосовое/кружок, который прислали боту вручную."""
    tmp = config.MEDIA_DIR / "tr"
    tmp.mkdir(parents=True, exist_ok=True)
    path = None
    try:
        await event.respond("🎙 Слушаю…")
        path = await msg.download_media(file=str(tmp / f"ondemand_{msg.id}"))
        if not path:
            await event.respond("❌ Не смог скачать файл.")
            return
        text, err = await transcribe.transcribe(path)
        if not text:
            await event.respond(f"❌ Не расшифровалось: {alerts.esc(err)}",
                                parse_mode="html")
            return
        await reply_long(event, "🎙 <b>Расшифровка</b>\n—\n" + alerts.esc(text))
    except Exception as e:
        log.exception("Расшифровка по запросу упала")
        await event.respond(f"❌ Ошибка: {alerts.esc(e)}", parse_mode="html")
    finally:
        if path:
            try:
                os.remove(path)
            except OSError:
                pass


# ---------------------------------------------------------------- фоновые задачи


async def block_watcher() -> None:
    """Косвенная проверка блокировки. Протокольного события на это нет."""
    await asyncio.sleep(60)
    while True:
        if setting("block_check"):
            try:
                await _check_blocks()
            except Exception:
                log.exception("Проверка блокировок упала")
        # Небольшой разброс интервала, чтобы шаг не был машинно-ровным
        base = config.BLOCK_CHECK_INTERVAL_MIN * 60
        await asyncio.sleep(base + _rand.randint(0, max(1, base // 5)))


async def _check_blocks() -> None:
    for user_id in db.active_peers(config.BLOCK_CHECK_ACTIVE_DAYS,
                                   config.BLOCK_CHECK_MAX_PEERS):
        if user_id == _me_id or user_id <= 0:
            continue   # группы сюда не относятся, блокируют только люди
        try:
            full = await client(GetFullUserRequest(user_id))
            user = full.users[0]
        except FloodWaitError as e:
            log.warning("FloodWait %s сек в проверке блокировок", e.seconds)
            await asyncio.sleep(e.seconds + 2)
            continue
        except Exception as e:
            log.debug("GetFullUser %s: %s", user_id, e)
            continue

        has_photo = user.photo is not None and not isinstance(user.photo, types.UserProfilePhotoEmpty)
        has_status = user.status is not None and not isinstance(user.status, types.UserStatusEmpty)

        prev = db.get_peer_state(user_id)
        if prev is None:
            db.set_peer_state(user_id, has_photo, has_status, 0, False)
            await asyncio.sleep(1.2 + _rand.random())
            continue

        lost_photo = bool(prev["had_photo"]) and not has_photo
        lost_status = bool(prev["had_status"]) and not has_status
        suspicious = lost_photo and lost_status   # оба сразу — иначе слишком много ложных

        if suspicious:
            strikes = prev["strikes"] + 1
            if strikes >= config.BLOCK_CHECK_STRIKES and not prev["alerted"] and not paused():
                await notify(alerts.build_block_alert(
                    user_id, db.peer_name(user_id), lost_photo, lost_status))
                log.info("Подозрение на блокировку: %s", user_id)
                db.set_peer_state(user_id, prev["had_photo"], prev["had_status"], strikes, True)
            else:
                db.set_peer_state(user_id, prev["had_photo"], prev["had_status"], strikes, bool(prev["alerted"]))
        else:
            # признаки вернулись — сбрасываем, чтобы поймать следующий раз
            db.set_peer_state(user_id, has_photo, has_status, 0, False)

        await asyncio.sleep(1.2 + _rand.random())


async def heartbeat_loop() -> None:
    """Раз в HEARTBEAT_DAYS шлёт «жив», чтобы тишина не путалась с падением/разлогином."""
    if not config.HEARTBEAT_ENABLED:
        return
    while True:
        await asyncio.sleep(3600)
        try:
            if paused():
                continue   # на паузе не двигаем метку и не обнуляем счётчики
            last = db.get_int("heartbeat_ts", 0)
            now = int(time.time())
            if now - last < config.HEARTBEAT_DAYS * 86400:
                continue
            d, e, pu = (db.get_int("hb:deletes"), db.get_int("hb:edits"),
                        db.get_int("hb:purges"))
            st = db.stats()
            await notify(
                "💓 <b>Anti-delete жив</b>\n"
                f"За {config.HEARTBEAT_DAYS} дн: удалений {d}, правок {e}, чисток {pu}\n"
                f"В кэше: {st['messages']} сообщ., отслеживаем {db.count_monitored()} чатов",
            )
            db.set_meta("heartbeat_ts", now)
            for k in ("hb:deletes", "hb:edits", "hb:purges"):
                db.set_meta(k, "0")
        except Exception:
            log.exception("heartbeat_loop")


async def digest_loop() -> None:
    """Досылает накопленный дайджест раз в DIGEST_HOURS."""
    if config.DIGEST_MODE == "off":
        return
    period = max(1, config.DIGEST_HOURS)   # 0 давал бы busy-loop
    while True:
        await asyncio.sleep(min(period, 1) * 3600)
        try:
            last = db.get_int("digest_ts", 0)
            now = int(time.time())
            if now - last < period * 3600:
                continue
            await flush_digest()
            db.set_meta("digest_ts", now)
        except Exception:
            log.exception("digest_loop")


DIGEST_BATCH = 40        # событий в одном сообщении дайджеста
DIGEST_MAX_BATCHES = 5   # сколько сообщений готовы отправить за один досыл


async def flush_digest() -> None:
    """Досылает очередь порциями.

    Раньше вся очередь склеивалась в одну строку: если отправка сутки не
    удавалась, накопленное превращалось в текст на сотни килобайт, который
    уходил бы десятками сообщений подряд — прямой путь к FloodWait.
    """
    total = db.digest_count()
    if not total:
        return
    for _ in range(DIGEST_MAX_BATCHES):
        rows = db.peek_digest(DIGEST_BATCH)   # не удаляем, пока не отправим
        if not rows:
            return
        left = db.digest_count() - len(rows)
        header = (f"📨 <b>Дайджест: {len(rows)} событий</b>"
                  + (f" <i>(ещё {left} в очереди)</i>" if left else "") + "\n")
        body = "\n\n———\n\n".join(r["payload"] for r in rows)
        sent = await notify(header + "\n" + body)
        if not sent:
            log.warning("Дайджест не доставлен — оставляю в очереди (%s шт)", total)
            return
        db.delete_digest([r["id"] for r in rows])
    if db.digest_count():
        log.info("Дайджест: осталось %s событий, дошлю следующим заходом",
                 db.digest_count())


async def index_saved_messages() -> None:
    """Заносит id сообщений Избранного в игнор.

    Удаление в Избранном приходит без указания чата — как «неизвестное». Ты там
    всё удаляешь сам, поэтому такие id помечаем игнором, чтобы не слать пустой
    алерт. Живые сообщения Избранного игнорируются и так, эта задача закрывает
    исторические. Только чтение id, без пометки «прочитано».
    """
    if config.SAVED_INDEX_LIMIT <= 0:
        return
    # Водяной знак: не перечитывать Избранное целиком на каждом старте (это ~200
    # GetHistory при большом SAVED_INDEX_LIMIT). При крэш-лупе это иначе
    # превращалось бы в непрерывное вычитывание истории каждые 10 секунд.
    last_id = db.get_int("saved_index_max_id", 0)
    if last_id and time.time() - db.get_int("saved_index_ts", 0) < 20 * 3600:
        return
    try:
        batch, total, top = [], 0, last_id
        async for msg in client.iter_messages(_me_id, limit=config.SAVED_INDEX_LIMIT,
                                              min_id=last_id):
            batch.append((msg.id, _me_id))
            top = max(top, msg.id)
            if len(batch) >= 500:
                total += db.note_ignored_bulk(batch); batch = []
        if batch:
            total += db.note_ignored_bulk(batch)
        db.set_meta("saved_index_max_id", top)
        db.set_meta("saved_index_ts", int(time.time()))
        log.info("Избранное: %s новых id занесено в игнор", total)
    except FloodWaitError as e:
        log.warning("Индексация Избранного: FloodWait %s сек — отложу", e.seconds)
    except Exception:
        log.exception("Индексация Избранного не удалась")


async def cleanup_loop() -> None:
    while True:
        try:
            s = db.cleanup()
            if any(s.values()):
                log.info("Чистка диска: %s", s)
            # Оборванные загрузки и осиротевшие копии для расшифровки: в БД они
            # никак не учтены, поэтому сами по себе никогда бы не удалились
            downloader.sweep()
            n = _sweep_tmp_dirs()
            if n:
                log.info("Подмёл %s служебных файлов", n)
        except Exception:
            log.exception("Чистка упала")
        await asyncio.sleep(6 * 3600)


def _sweep_tmp_dirs() -> int:
    """Служебные файлы, которых нет в базе: копии для расшифровки и выгруженные
    истории. Ни срок хранения, ни лимит места их не видят — они идут по строкам
    messages, а этих файлов там нет. Без этого они копились бы вечно."""
    cutoff = time.time() - 6 * 3600
    n = 0
    for d in (config.MEDIA_DIR / "tr", config.MEDIA_DIR / "stories"):
        if not d.exists():
            continue
        for f in d.iterdir():
            try:
                if f.is_file() and f.stat().st_mtime < cutoff:
                    f.unlink()
                    n += 1
            except OSError:
                pass
    return n


# ---------------------------------------------------------------- запуск


def security_selfcheck() -> None:
    """Секретные файлы должны быть доступны только владельцу. Ужимаем, если нет."""
    import stat as _stat
    targets = {
        config.SESSION_PATH + ".session": "файл сессии (ключи от аккаунта!)",
        str(config.BASE_DIR / "antidelete-bot.session"): "сессия бота",
        str(config.BASE_DIR / ".env"): ".env с API-ключами",
        config.DB_PATH: "база с перепиской",
        config.DB_PATH + "-wal": "WAL базы (там текст сообщений)",
        config.DB_PATH + "-shm": "shm базы",
    }
    for path, what in targets.items():
        try:
            mode = os.stat(path).st_mode
        except FileNotFoundError:
            continue
        if mode & (_stat.S_IRWXG | _stat.S_IRWXO):
            os.chmod(path, 0o600)
            log.warning("Права на %s были открыты для других — ужал до 600 (%s)",
                        os.path.basename(path), what)


async def main() -> None:
    global _me_id
    # Никакого интерактивного логина: под systemd вводить код некуда.
    await client.connect()
    if not await client.is_user_authorized():
        raise SystemExit(
            "Сессия не авторизована. Запусти интерактивно: python3 setup.py"
        )
    me = await client.get_me()
    _me_id = me.id
    security_selfcheck()
    log.info("Залогинен как %s (@%s, id %s)", display_name(me), me.username, me.id)

    config.MEDIA_DIR.mkdir(exist_ok=True)
    config.DOWNLOAD_DIR.mkdir(exist_ok=True)
    reload_subs()
    # Сервис мог упасть посреди скачивания — подметаем брошенное
    dropped = downloader.sweep()
    if dropped:
        log.info("Убрал %s брошенных загрузок", dropped)

    await setup_bot()
    asyncio.create_task(cleanup_loop())
    asyncio.create_task(block_watcher())
    asyncio.create_task(heartbeat_loop())
    asyncio.create_task(digest_loop())
    asyncio.create_task(profile_watcher())
    asyncio.create_task(index_saved_messages())

    # При крэш-лупе systemd перезапускает раз в 10 сек — без этой отсечки
    # лог-чат завалило бы сообщениями о старте.
    now = int(time.time())
    last_start = int(db.get_meta("last_start_ts", "0") or 0)
    db.set_meta("last_start_ts", now)

    if now - last_start < 300:
        log.warning("Перезапуск через %s сек после прошлого — стартовый алерт пропущен",
                    now - last_start)
    else:
        s = db.stats()
        monitored = db.count_monitored()
        await notify(
            "▶️ <b>Anti-delete запущен</b>\n"
            f"Аккаунт: {alerts.esc(display_name(me))} (id {me.id})\n"
            f"В кэше: {s['messages']} сообщ., {s['media_bytes'] / 1024 / 1024:.1f} МБ медиа\n"
            f"Отслеживаем чатов: {monitored}\n"
            f"Чистка: {config.PURGE_MIN_COUNT}+ удалений за {config.PURGE_WINDOW_SEC} сек\n"
            f"Правки: {'да' if setting('track_edits') else 'нет'} · "
            f"Огоньки: {'да' if setting('view_once') else 'нет'} · "
            f"Блокировки: {'да' if setting('block_check') else 'нет'} · "
            f"Группы: {'до ' + str(config.GROUP_MAX_MEMBERS) if config.MONITOR_GROUPS else 'нет'}\n"
            f"Расшифровка: {'да' if setting('transcribe') and transcribe.installed() else 'нет'} · "
            f"Загрузчик: {'да' if setting('downloader') and downloader.installed() else 'нет'} · "
            f"Подписок на людей: {sum(len(v) for v in _subs.values())}\n"
            "⚙️ /menu — настройки · 🔔 /subs — подписки"
            + ("\n⏸ <b>Сейчас на паузе</b> — /resume" if paused() else ""),
        )

    # Ловим SIGTERM от systemd, чтобы досылать то, что копится в дебаунсе,
    # а не терять последние секунды удалений при рестарте.
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    runner = asyncio.create_task(client.run_until_disconnected())
    waiter = asyncio.create_task(stop.wait())
    await asyncio.wait([runner, waiter], return_when=asyncio.FIRST_COMPLETED)

    # Связь могла оборваться не по нашей воле. Самый неприятный случай —
    # сессию отозвали («Завершить все сеансы» в настройках Telegram): тогда
    # runner падает с исключением, и без этой проверки бот молча выходил бы
    # с нулевым кодом, а владелец неделю думал бы, что всё работает.
    died = None
    if runner.done() and not runner.cancelled():
        died = runner.exception()
    if died is not None:
        log.error("Связь с Telegram разорвана: %s: %s",
                  type(died).__name__, died, exc_info=died)
        # Сообщение идёт через бота — он живёт на своём токене и продолжает
        # работать, даже когда аккаунт уже не пускают. Отправка не должна
        # помешать корректно погасить остальное, поэтому под try.
        try:
            if _auth_revoked(died):
                await notify(
                    "🔴 <b>Сессия отозвана</b>\n\n"
                    "Telegram больше не принимает вход этого аккаунта — обычно так "
                    "бывает после «Завершить все сеансы» в настройках.\n\n"
                    "Бот остановлен. Чтобы поднять снова: <code>./venv/bin/python3 setup.py</code> "
                    "и заново войти, потом <code>systemctl restart telegram-antidelete</code>.")
            else:
                await notify(
                    "🔴 <b>Бот остановился</b>\n\nСвязь с Telegram разорвана: "
                    f"<code>{alerts.esc(type(died).__name__)}</code>. "
                    "Перезапуск произойдёт сам; если сообщение повторяется — загляни в журнал.")
        except Exception:
            log.exception("Не смог сообщить об обрыве связи")

    log.info("Останавливаюсь, досылаю накопленное…")
    await flush_pending()
    # Дождаться отправки, которая уже в полёте: буфер мог уйти из _buffers в
    # _emit до SIGTERM, и flush_pending его не увидит. notify держит _send_lock
    # на время отправки — берём его, чтобы не оборвать disconnect'ом.
    try:
        await asyncio.wait_for(_send_lock.acquire(), timeout=15)
        _send_lock.release()
    except (asyncio.TimeoutError, RuntimeError):
        pass
    for task in (runner, waiter):
        task.cancel()
    # Гасим пул расшифровки не дожидаясь: whisper может считать минуты, а
    # systemd даёт на остановку 30 секунд — иначе рестарт кончался бы SIGKILL.
    transcribe.shutdown()
    if bot is not None:
        await bot.disconnect()
    await client.disconnect()
    log.info("Остановлен")

    # Код возврата решает, поднимет ли systemd бота обратно: в юните стоит
    # Restart=on-failure, и на нулевой выход он не реагирует. Раньше сюда
    # приходили и штатная остановка, и обрыв связи — во втором случае бот
    # молча оставался лежать, хотя сам же сообщал «перезапуск произойдёт сам».
    if died is not None and not _auth_revoked(died):
        return 1
    # Отозванная сессия — исключение: перезапуск ничего не даст, а с
    # RestartSec=10 обернулся бы вечным циклом падений. Владелец уже уведомлён.
    return 0


def _auth_revoked(exc) -> bool:
    """Сессию отозвали? Тогда перезапуск бессмысленен — нужен повторный вход."""
    name = type(exc).__name__
    return "AuthKey" in name or "Unauthorized" in name


async def flush_pending() -> None:
    """Досылает буферы удалений, не дождавшиеся своего дебаунса."""
    for key in list(_buffers):
        buf = _buffers.pop(key, None)
        if not buf:
            continue
        if buf["task"]:
            buf["task"].cancel()
        try:
            # systemd даёт на остановку 30 сек — файлы ждём совсем недолго
            await _emit(key, buf, wait_files=5)
        except Exception:
            log.exception("Не смог дослать буфер %s при остановке", key)


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()) or 0)
    except KeyboardInterrupt:
        log.info("Остановлен вручную")
