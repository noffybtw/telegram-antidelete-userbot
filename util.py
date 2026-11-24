"""Мелкие хелперы, общие для main.py и backfill.py."""

# То, что имеет смысл расшифровывать: голосовые и кружочки-видеосообщения.
VOICE_KINDS = ("голосовое", "видеосообщение")


def is_voice_kind(kind) -> bool:
    return kind in VOICE_KINDS


def media_kind(msg) -> str:
    """Человекочитаемый тип вложения. Порядок важен: gif и стикер — тоже документы.

    Для view-once сначала смотрим флаги на самом МЕДИА: после сгорания
    document/photo уже нет, и msg.voice/msg.video_note вернут пусто.
    """
    from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
    m = getattr(msg, "media", None)
    if isinstance(m, MessageMediaDocument):
        if getattr(m, "round", False):
            return "видеосообщение"
        if getattr(m, "voice", False):
            return "голосовое"
        if getattr(m, "video", False):
            return "видео"
    if isinstance(m, MessageMediaPhoto):
        return "фото"
    if msg.photo:
        return "фото"
    if msg.sticker:
        return "стикер"
    if msg.gif:
        return "gif"
    if msg.video_note:
        return "видеосообщение"
    if msg.voice:
        return "голосовое"
    if msg.video:
        return "видео"
    if msg.audio:
        return "аудио"
    if msg.document:
        return "файл"
    if msg.contact:
        return "контакт"
    if msg.geo:
        return "геолокация"
    if msg.poll:
        return "опрос"
    return "медиа"


VIDEO_EXT = {".mp4", ".mov", ".mkv", ".webm", ".m4v", ".avi", ".3gp"}


def video_meta(path: str):
    """(длительность, ширина, высота) видео через ffprobe. None — если не видео.

    Нужно, потому что сам Telethon размеры читать не умеет (для этого ему нужен
    отдельный пакет hachoir) и подставляет 1x1 и 0 секунд. Telegram по таким
    атрибутам рисует превью в один пиксель и отдаёт ролик файлом, который нельзя
    посмотреть, не скачав целиком. ffprobe у нас и так есть — ставится с ffmpeg.
    """
    import json
    import os.path
    import subprocess
    if not path or os.path.splitext(path)[1].lower() not in VIDEO_EXT:
        return None
    try:
        res = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height:format=duration",
             "-of", "json", path],
            capture_output=True, timeout=30, text=True)
        if res.returncode != 0:
            return None
        data = json.loads(res.stdout or "{}")
        stream = (data.get("streams") or [{}])[0]
        w, h = stream.get("width"), stream.get("height")
        if not w or not h:
            return None
        dur = float((data.get("format") or {}).get("duration") or 0)
        return int(dur), int(w), int(h)
    except Exception:
        return None


def display_name(entity) -> str:
    if entity is None:
        return "неизвестно"
    parts = [getattr(entity, "first_name", None), getattr(entity, "last_name", None)]
    name = " ".join(p for p in parts if p)
    return (name or getattr(entity, "username", None)
            or getattr(entity, "title", None) or "без имени")


async def classify_chat(client, entity, me_id: int):
    """Решает, следить ли за чатом. Возвращает (kind, members, monitored, title).

    Правила:
      личка с человеком      -> следим
      группа до N участников -> следим (общие чаты с друзьями)
      группа больше N        -> нет
      канал / бот / себя     -> нет
    """
    from telethon.tl.types import Channel, Chat, User
    from telethon.tl.functions.channels import GetFullChannelRequest

    import config

    title = display_name(entity)

    if isinstance(entity, User):
        if entity.bot:
            return "bot", None, False, title
        if entity.deleted:
            return "deleted", None, False, title
        if entity.id == me_id:
            return "self", None, False, title
        return "private", None, True, title

    if not config.MONITOR_GROUPS:
        return "group", None, False, title

    if isinstance(entity, Chat):
        members = getattr(entity, "participants_count", None)
        if getattr(entity, "deactivated", False) or getattr(entity, "left", False):
            return "group", members, False, title
        ok = members is not None and members <= config.GROUP_MAX_MEMBERS
        return "group", members, ok, title

    if isinstance(entity, Channel):
        if not entity.megagroup:
            return "channel", None, False, title   # вещательный канал — мимо
        members = getattr(entity, "participants_count", None)
        if members is None:
            try:
                full = await client(GetFullChannelRequest(entity))
                members = full.full_chat.participants_count
            except Exception:
                return "supergroup", None, False, title   # не смогли узнать — не следим
        ok = members is not None and members <= config.GROUP_MAX_MEMBERS
        return "supergroup", members, ok, title

    return "other", None, False, title
