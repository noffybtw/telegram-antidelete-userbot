"""Конфигурация: читает .env, приводит типы, проверяет обязательное."""

import os
from pathlib import Path

from dotenv import load_dotenv

# umask ставим здесь, а не в main.py: config импортируют ВСЕ точки входа
# (setup.py, backfill.py, cache.py), и все они создают чувствительные файлы
# (сессия, база, -wal с текстом). Иначе под системным umask 022 они легли бы 644.
os.umask(0o077)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def _int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if raw in ("1", "true", "yes", "on", "да"):
        return True
    if raw in ("0", "false", "no", "off", "нет"):
        return False
    return default


def _str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name) or "").strip())
    except ValueError:
        return default


# --- Telegram ---
API_ID = _int("API_ID", 0)
API_HASH = _str("API_HASH")
PHONE = _str("PHONE")
LOG_CHAT_ID = _int("LOG_CHAT_ID", 0)

SESSION_PATH = str(BASE_DIR / "antidelete")
DB_PATH = str(BASE_DIR / "antidelete.db")
MEDIA_DIR = BASE_DIR / "media"

# --- Детект чистки переписки ---
PURGE_MIN_COUNT = _int("PURGE_MIN_COUNT", 5)
PURGE_WINDOW_SEC = _int("PURGE_WINDOW_SEC", 30)
PURGE_MAX_MEDIA = _int("PURGE_MAX_MEDIA", 10)
# Сколько ждать после последнего удаления, прежде чем решать "пачка или одиночка"
DEBOUNCE_SEC = 3

# --- Детект блокировки ---
BLOCK_CHECK_ENABLED = _bool("BLOCK_CHECK_ENABLED", True)
BLOCK_CHECK_INTERVAL_MIN = _int("BLOCK_CHECK_INTERVAL_MIN", 120)
BLOCK_CHECK_ACTIVE_DAYS = _int("BLOCK_CHECK_ACTIVE_DAYS", 7)
# Сколько проверок подряд признак должен держаться, прежде чем слать алерт
BLOCK_CHECK_STRIKES = _int("BLOCK_CHECK_STRIKES", 2)
# Потолок собеседников за цикл: страховка, чтобы проверка не превратилась
# в тысячи запросов к Telegram, если активных чатов вдруг станет очень много
BLOCK_CHECK_MAX_PEERS = _int("BLOCK_CHECK_MAX_PEERS", 20)

# --- Кэш ---
MEDIA_RETENTION_DAYS = _int("MEDIA_RETENTION_DAYS", 7)
MEDIA_MAX_TOTAL_MB = _int("MEDIA_MAX_TOTAL_MB", 5000)
MEDIA_MAX_FILE_MB = _int("MEDIA_MAX_FILE_MB", 200)
# 0 = хранить историю вечно. Именно это и нужно по умолчанию: текст почти
# ничего не весит, а срезание по сроку уничтожало бы залитую бэкфиллом историю.
DB_RETENTION_DAYS = _int("DB_RETENTION_DAYS", 0)

# --- Мониторинг групп ---
# Кроме личек следить за групповыми чатами до GROUP_MAX_MEMBERS участников
# (общие чаты с друзьями). Большие группы и каналы игнорируются.
MONITOR_GROUPS = _bool("MONITOR_GROUPS", True)
GROUP_MAX_MEMBERS = _int("GROUP_MAX_MEMBERS", 30)
# Как часто перепроверять размер/тип чата (группа могла разрастись за лимит)
CHAT_RECHECK_HOURS = _int("CHAT_RECHECK_HOURS", 24)

# Порог маркированного id. Не выдуман: Telethon кодирует каналы как
# -(1000000000000 + channel_id) — см. utils.get_peer_id / resolve_id.
# Значит личные чаты (id > 0) и старые группы (-id) всегда выше порога,
# а каналы и супергруппы — ниже.
SUPERGROUP_ID_THRESHOLD = -1_000_000_000_000

# --- Поведение ---
TRACK_EDITS = _bool("TRACK_EDITS", True)
IGNORE_BOTS = _bool("IGNORE_BOTS", True)
ALERT_UNKNOWN_DELETES = _bool("ALERT_UNKNOWN_DELETES", True)
# Алертить и на удаление СВОИХ сообщений (их может снести собеседник,
# но отличить это от собственного удаления нечем — будет шумно)
ALERT_OWN_DELETES = _bool("ALERT_OWN_DELETES", True)
TIMEZONE = _str("TIMEZONE")  # пусто = локальное время машины


# --- Бот-уведомитель (опционально) ---
# Юзербот шлёт алерты от твоего же имени, а Telegram не уведомляет тебя о твоих
# сообщениях — поэтому они приходят уже прочитанными. Бот решает это: сообщения
# от него приходят как обычные входящие, с уведомлением и непрочитанными.
# Слежку ботом заменить нельзя: Bot API не отдаёт события удаления.
BOT_TOKEN = _str("BOT_TOKEN")
# Куда бот шлёт алерты. 0 = в личку тебе (нужно один раз нажать Start у бота).
BOT_ALERT_CHAT_ID = _int("BOT_ALERT_CHAT_ID", 0)

# --- Heartbeat: периодический сигнал «жив» ---
HEARTBEAT_ENABLED = _bool("HEARTBEAT_ENABLED", True)
HEARTBEAT_DAYS = _int("HEARTBEAT_DAYS", 7)

# --- Дайджест: копить алерты и слать пачкой, а не мгновенно ---
# off  — мгновенные алерты (по умолчанию)
# all  — вообще всё в дайджест
# purge— мгновенные одиночные, но чистки собирать в дайджест
DIGEST_MODE = _str("DIGEST_MODE", "off").lower()
DIGEST_HOURS = _int("DIGEST_HOURS", 24)

# --- Архив исторического медиа в отдельный канал (ручной, осторожный) ---
# Форвард старых файлов на стороне Telegram — это исходящие действия, риск флуда.
# Поэтому по умолчанию выключено, запускается вручную командой /archive,
# с паузой между файлами и жёстким потолком за один вызов.
STORAGE_CHAT_ID = _int("STORAGE_CHAT_ID", 0)
ARCHIVE_DELAY_SEC = _float("ARCHIVE_DELAY_SEC", 3.0)
ARCHIVE_MAX_PER_RUN = _int("ARCHIVE_MAX_PER_RUN", 200)

# --- Поиск по архиву ---
SEARCH_LIMIT = _int("SEARCH_LIMIT", 15)

# --- Избранное ---
# Удаления в Избранном ты делаешь сам — алерты не нужны. Их id заносятся
# в игнор при старте. 0 = не индексировать (тогда удаления старых
# сохранёнок могут разово приходить как «неизвестные»).
SAVED_INDEX_LIMIT = _int("SAVED_INDEX_LIMIT", 20000)

# --- View-once ("огонёк") ---
# Сохранять самоуничтожающиеся фото/видео/голосовые сразу при получении.
# ВАЖНО: это перезаливка чужого контента на серверы Telegram — юридически
# и по модерации на твою ответственность. По умолчанию включено (ты просил).
CAPTURE_VIEW_ONCE = _bool("CAPTURE_VIEW_ONCE", True)

# --- Скрытый просмотр (/read) ---
# Сколько сообщений показывать по умолчанию и максимум за раз
READ_DEFAULT = _int("READ_DEFAULT", 20)
READ_MAX = _int("READ_MAX", 100)

# --- Истории ---
# Сколько историй отдавать за раз (остальные — по кнопке «показать ещё»),
# чтобы 100+ сохранённых не завалили чат одним потоком.
STORY_BATCH = _int("STORY_BATCH", 5)

# --- Расшифровка голосовых и кружочков (локальный Whisper) ---
# Считается ЗДЕСЬ, на этой машине: аудио никуда не уходит. Telegram Premium не
# нужен и не помог бы — его transcribeAudio работает только по живому сообщению
# на серверах Telegram, а нам надо расшифровывать уже удалённое, из своего кэша.
TRANSCRIBE_ENABLED = _bool("TRANSCRIBE_ENABLED", True)
# Расшифровывать голосовые, которые удалили (и «огоньки»)
TRANSCRIBE_DELETED = _bool("TRANSCRIBE_DELETED", True)
# tiny / base / small / medium. small — разумный баланс: ~1.5x быстрее реального
# времени на 4 ядрах, русский держит хорошо. base вдвое быстрее и заметно хуже.
WHISPER_MODEL = _str("WHISPER_MODEL", "small")
WHISPER_LANG = _str("WHISPER_LANG")        # пусто = определять язык самому
# 0 = ядра минус одно (одно оставляем event loop'у, чтобы бот не подтормаживал)
WHISPER_THREADS = _int("WHISPER_THREADS", 0)
# Модель лежит в папке проекта, а не в ~/.cache: systemd монтирует home read-only
WHISPER_DIR = str(BASE_DIR / "models")
# Потолок длины: часовой подкаст занял бы ядра на 40 минут
TRANSCRIBE_MAX_SEC = _int("TRANSCRIBE_MAX_SEC", 600)
# Больше этого в очереди не копим — лучше честно сказать «занят», чем копить час
TRANSCRIBE_QUEUE_MAX = _int("TRANSCRIBE_QUEUE_MAX", 10)

# --- Загрузчик медиа по ссылке (yt-dlp) ---
DOWNLOADER_ENABLED = _bool("DOWNLOADER_ENABLED", True)
# TikTok блокирует серверные IP, и куки от этого не спасают: своим экстрактором
# yt-dlp получает мусор вместо ответа. Поэтому прямую ссылку (сразу без
# вотермарки) спрашиваем у открытого API tikwm.com, а качаем уже сами.
# ВАЖНО: этому сервису видно, какие ролики ты качаешь. Не устраивает — ставь
# false, но тогда TikTok не будет работать вовсе.
TIKTOK_VIA_API = _bool("TIKTOK_VIA_API", True)
# Прокси для загрузчика: http://user:pass@host:port или socks5://...
# Нужен для YouTube: он отдаёт 403 адресу, который пометил. «Жилой» IP при этом
# не обязателен — подходит любой, которого YouTube ещё не знает (проверено на
# датацентровых прокси Webshare, качает без куков). Прокси не единственный путь
# и не первый: см. _plan() в downloader.py — на помеченном адресе первыми идут
# куки напрямую (бесплатно), прокси следом, голый прямой заход последним.
# Уходят прокси только на YouTube-хосты (PROXY_HOSTS), остальным площадкам они
# не нужны и трафик на них не тратится.
DOWNLOAD_PROXY = _str("DOWNLOAD_PROXY")
# Файл со списком прокси — по одному в строке. Понимает и формат Webshare
# (host:port:user:pass), и обычный URL. Пусто = ищем proxies.txt рядом с ботом.
# Список лучше одного адреса: при отказе бот сам пробует следующий.
PROXY_FILE = _str("PROXY_FILE")
# Разрешать ли последнюю попытку С КУКАМИ, когда через прокси не вышло.
# Нужно для роликов, закрытых до входа (возрастное ограничение). Куки при этом
# уходят с адреса прокси — для Google это вход из другой страны, поэтому под
# это должен быть ОТДЕЛЬНЫЙ аккаунт, а не основной.
COOKIES_WITH_PROXY = _bool("COOKIES_WITH_PROXY", True)
# Качество по умолчанию (высота кадра). Замерено на трёхминутном клипе:
# 1080p — 23 сек и 81 МБ, 720p — 11 сек и 29 МБ. На телефоне разницы почти не
# видно, а ждать вдвое меньше. 1080p доступно кнопкой под результатом.
DOWNLOAD_QUALITY = _int("DOWNLOAD_QUALITY", 720)
# Спрашивать качество ДО скачивания. Стоит лишних ~9 секунд на разведку
# (площадка всё равно требует их, прежде чем что-то рассказать), зато не
# качается лишнее. Выключишь — сразу качает в DOWNLOAD_QUALITY.
ASK_QUALITY = _bool("ASK_QUALITY", True)
# Сколько дней помнить уже скачанное. Повторная ссылка тогда отправляется
# мгновенно — Telegram пересылает свой же файл, ничего не качая заново.
# 0 = не кэшировать.
DOWNLOAD_CACHE_DAYS = _int("DOWNLOAD_CACHE_DAYS", 90)
DOWNLOAD_MAX_MB = _int("DOWNLOAD_MAX_MB", 300)
DOWNLOAD_TIMEOUT_SEC = _int("DOWNLOAD_TIMEOUT_SEC", 600)
DOWNLOAD_DIR = BASE_DIR / "downloads"
# Путь к cookies.txt. YouTube и Instagram с серверного IP просят подтвердить,
# что ты не бот; куки из браузера это снимают, и на помеченном адресе именно
# они пробуются первыми. TikTok (идёт через API), Pinterest и прямые ссылки
# работают без куков; YouTube без них качается только через прокси или пока
# адрес сервера не помечен.
YTDLP_COOKIES = _str("YTDLP_COOKIES")

# --- Слежка за сменой профиля ---
PROFILE_WATCH_ENABLED = _bool("PROFILE_WATCH_ENABLED", True)
# Плановый опрос — это подстраховка. Основную работу делает мгновенная реакция
# на апдейт от Telegram (UpdateUser / UpdateUserName), поэтому часто дёргать
# сеть незачем, но час — слишком редко, чтобы функция ощущалась живой.
PROFILE_CHECK_INTERVAL_MIN = _int("PROFILE_CHECK_INTERVAL_MIN", 30)
# Потолок проверяемых за прогон. Подписок может быть много, а каждая — это
# отдельный запрос к Telegram; кого не успели, возьмём в следующий заход.
PROFILE_CHECK_MAX_PEERS = _int("PROFILE_CHECK_MAX_PEERS", 30)

# --- Онлайн-трекинг ---
# По умолчанию выключено: это самая шумная и самая «сталкерская» функция.
ONLINE_WATCH_ENABLED = _bool("ONLINE_WATCH_ENABLED", False)
# Не слать «зашёл» чаще этого — клиент мигает статусом при каждом открытии
ONLINE_MIN_GAP_SEC = _int("ONLINE_MIN_GAP_SEC", 300)

# --- Экспорт переписки в файл ---
EXPORT_MAX_MESSAGES = _int("EXPORT_MAX_MESSAGES", 50000)


def require_credentials() -> None:
    missing = [n for n, v in (("API_ID", API_ID), ("API_HASH", API_HASH)) if not v]
    if missing:
        raise SystemExit(
            "В .env не заполнено: " + ", ".join(missing) + "\n"
            "Возьми API_ID/API_HASH на https://my.telegram.org -> API development tools"
        )


def require_log_chat() -> None:
    if not LOG_CHAT_ID:
        raise SystemExit(
            "В .env не заполнен LOG_CHAT_ID.\n"
            "Запусти сначала: python3 setup.py — он создаст лог-чат и пропишет ID сам."
        )
