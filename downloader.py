"""Скачивание видео и фото по ссылке через yt-dlp.

Кидаешь ссылку — получаешь готовый файл. Файлы качаются на эту машину прямо с
площадки. Единственный посредник — TikTok: за прямой ссылкой на ролик ходим в
публичный сторонний API tikwm.com (TIKTOK_API, отключается TIKTOK_VIA_API).
Туда уходит только сама ссылка — ни аккаунта, ни куков; ролик потом качаем
сами. Всё остальное посредников не знает.

Про «водяные знаки»: убрать их постфактум нельзя, они вжигаются в кадр. Приём
другой — брать у площадки исходник, который вотермаркой ещё не залит. Для
TikTok это работает (tikwm отдаёт play-ссылку, а не залитую вотермаркой), у
YouTube/Pinterest/Instagram вотермарок нет и так.

yt-dlp запускаем отдельным процессом, а не библиотекой: его сетевая работа не
блокирует event loop, а процессу можно жёстко ограничить время жизни.
"""

import asyncio
import html
import ipaddress
import json
import logging
import os
import re
import shutil
import signal
import socket
import time
from pathlib import Path
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

import config

log = logging.getLogger("downloader")

YTDLP = str(config.BASE_DIR / "venv" / "bin" / "yt-dlp")
# Генератор PO-токенов для YouTube. Без него YouTube не отдаёт ссылки даже
# при живых куках: «Sign in to confirm you're not a bot». Ставится install.sh.
POT_SCRIPT = config.BASE_DIR / "pot-provider" / "server" / "build" / "generate_once.js"
# Не больше двух качалок разом: иначе пачка ссылок забьёт и канал, и диск
_gate = asyncio.Semaphore(2)
# Разведка (узнать доступные качества) поднимает те же yt-dlp и node, поэтому
# её тоже надо ограничивать. Семафор отдельный: иначе опрос отбирал бы слоты
# у настоящих скачиваний, а на пачке ссылок мог занять оба и заблокировать себя.
_probe_gate = asyncio.Semaphore(2)

URL_RE = re.compile(r"https?://[^\s<>\"']+")
# Потолок на пост: карусель Instagram бывает на 10-20 картинок, но слать
# бесконечную пачку одним ответом всё равно нельзя.
MAX_FILES = 10
# Куда кладём куки, присланные боту файлом
COOKIES_PATH = config.BASE_DIR / "cookies.txt"

# Прогресс скачивания. Своя метка в начале строки, чтобы отличать эти строки от
# обычного вывода yt-dlp; «NA» приходит, когда размер ещё неизвестен.
PROGRESS_TAG = "@@PROG@@"
PROGRESS_TEMPLATE = (
    "download:" + PROGRESS_TAG +
    "%(progress.downloaded_bytes)s|%(progress.total_bytes)s|"
    "%(progress.total_bytes_estimate)s|%(progress.speed)s|%(progress.eta)s"
)

# Идущие сейчас загрузки: по номеру можно убить процесс кнопкой «Отмена».
# _jobs — только пока жив сам процесс yt-dlp; _active — всё время работы fetch(),
# включая ожидание очереди, резолв DNS и опрос TikTok. Кнопка «Отмена» показана
# с самого начала, поэтому ориентироваться на один _jobs нельзя: раньше нажатие
# в эти окна молча терялось, а человеку отвечали «загрузка уже закончилась» —
# и файл всё равно приезжал, потратив платный трафик.
_jobs = {}
_active = set()
_cancelled = set()


def cancel(job_id: int) -> bool:
    """Отменить загрузку. True, если было что отменять.

    Процесса может ещё не быть — тогда просто запоминаем намерение, и fetch()
    сам остановится на ближайшей проверке.
    """
    if job_id not in _active and job_id not in _jobs:
        return False            # такой загрузки нет — врать не о чем
    _cancelled.add(job_id)
    proc = _jobs.get(job_id)
    if proc is not None and proc.returncode is None:
        _kill(proc)
    return True


# --- TikTok ---
# yt-dlp с серверного IP TikTok не отдаёт вообще: своим экстрактором он получает
# «Unexpected response», и куки тут не помогают — блокируется сам IP. Обходим так
# же, как это делают публичные боты-загрузчики: спрашиваем прямую ссылку у
# открытого API, а качаем её уже сами. Ссылка приходит БЕЗ вотермарки.
TIKTOK_API = "https://tikwm.com/api/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36"


def is_tiktok(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host == "tiktok.com" or host.endswith(".tiktok.com")


def _tiktok_lookup_blocking(url: str):
    """Спросить у API прямые ссылки. Блокирующая — звать только из потока."""
    req = Request(TIKTOK_API + "?url=" + quote(url, safe=""),
                  headers={"User-Agent": UA})
    with urlopen(req, timeout=30) as resp:
        if resp.status != 200:
            return None, f"сервис ответил {resp.status}"
        body = resp.read(2_000_000)
    try:
        data = json.loads(body)
    except ValueError:
        return None, "сервис ответил не по формату"
    if data.get("code") != 0:
        return None, str(data.get("msg") or "сервис не отдал видео")
    d = data.get("data") or {}
    if d.get("images"):
        urls = [u for u in d["images"] if isinstance(u, str)][:MAX_FILES]
    else:
        one = d.get("hdplay") or d.get("play")
        urls = [one] if isinstance(one, str) else []
    if not urls:
        return None, "в ответе нет медиа"
    return {"urls": urls, "title": (d.get("title") or "TikTok").strip()}, None


async def tiktok_direct(url: str):
    """(данные, ошибка). Ссылки из ответа проверяем как любые чужие —
    сервис посторонний, доверять его выдаче вслепую нельзя."""
    try:
        res, err = await asyncio.to_thread(_tiktok_lookup_blocking, url)
    except Exception as e:
        return None, f"не достучался до сервиса TikTok: {e}"
    if err:
        return None, err
    safe = []
    for u in res["urls"]:
        bad = await check_url_resolved(u)
        if bad:
            log.warning("TikTok-сервис вернул подозрительную ссылку: %s", bad)
            continue
        safe.append(u)
    if not safe:
        return None, "сервис вернул ссылки, которым я не доверяю"
    res["urls"] = safe
    return res, None


def _num(s):
    """Число из шаблона прогресса. 'NA' и пустое — значит неизвестно."""
    if not s or s in ("NA", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_progress(line: str):
    body = line.split(PROGRESS_TAG, 1)[1]
    parts = body.split("|")
    while len(parts) < 5:
        parts.append("")
    done, total, est, speed, eta = (_num(p) for p in parts[:5])
    total = total or est
    return {"done": done, "total": total, "speed": speed, "eta": eta,
            "percent": (done / total * 100) if (done and total) else None}


def jar_domains(path) -> set:
    """Домены, которые покрывает файл кук. Пустое множество — прочитать не вышло."""
    out = set()
    try:
        for line in Path(path).read_text(errors="replace").splitlines():
            if line.startswith("#") or not line.strip():
                continue
            dom = line.split("\t")[0].strip().lstrip(".").lower()
            if dom:
                out.add(dom)
    except OSError:
        pass
    return out


def jar_covers(path, url: str) -> bool:
    """Годится ли этот файл кук для такого адреса."""
    host = (urlparse(url or "").hostname or "").lower()
    if not host:
        return True            # не знаем площадку — не мешаем
    doms = jar_domains(path)
    if not doms:
        return True            # файл нечитаем: пусть решает сам yt-dlp
    return any(host == d or host.endswith("." + d) for d in doms)


SITE_ALIASES = {"youtu": "youtube", "ytimg": "youtube",
                "instagr": "instagram", "cdninstagram": "instagram",
                "vt": "tiktok", "vm": "tiktok", "pin": "pinterest"}


def site_key(url: str) -> str:
    """Короткое имя площадки для имени файла кук: instagram, youtube, tiktok."""
    host = (urlparse(url or "").hostname or "").lower()
    host = host[4:] if host.startswith("www.") else host
    parts = [p for p in host.split(".") if p]
    # Берём предпоследнюю часть: www.instagram.com -> instagram
    key = parts[-2] if len(parts) >= 2 else (parts[0] if parts else "")
    # Короткие адреса ведут туда же, куда длинные: иначе куки, сохранённые
    # под youtube, не нашлись бы для ссылки youtu.be.
    return SITE_ALIASES.get(key, key)


def site_cookies_path(url: str):
    """Файл кук именно для этой площадки, если он заведён."""
    key = site_key(url)
    if not key:
        return None
    # Рядом с общим файлом кук, а не от config.BASE_DIR: так подмена COOKIES_PATH
    # (в тестах) уводит и файлы площадок, и они не пишутся в рабочую папку.
    p = COOKIES_PATH.parent / f"cookies.{key}.txt"
    return p if p.exists() else None


def cookies_file(url: str = None):
    """Путь к кукам под конкретный адрес.

    Раньше файл был один на всё. Из-за этого куки YouTube подставлялись
    Instagram-у, тот отвечал «нужен вход», а бот советовал выгрузить куки
    заново — и человек выгружал ютубовские по второму разу. Теперь для каждой
    площадки свой файл: cookies.instagram.txt, cookies.youtube.txt. Общий
    cookies.txt берётся, только если он действительно покрывает этот адрес.
    """
    own = site_cookies_path(url) if url else None
    if own:
        return str(own)
    # Своего файла нет — отдаём общий. Не отбираем его, даже если домены не
    # совпали: лишние куки yt-dlp просто не отправит, а вот отказ их дать
    # сломал бы уже работающие установки. За правдивость сообщения отвечает
    # cookies_fit() ниже, а не этот выбор.
    if config.YTDLP_COOKIES and Path(config.YTDLP_COOKIES).exists():
        return config.YTDLP_COOKIES
    if COOKIES_PATH.exists():
        return str(COOKIES_PATH)
    return None


def all_jars() -> list:
    """Все файлы кук, которые у нас есть: (площадка, путь)."""
    out = []
    if config.YTDLP_COOKIES and Path(config.YTDLP_COOKIES).exists():
        out.append(("из .env", config.YTDLP_COOKIES))
    if COOKIES_PATH.exists():
        doms = sorted(jar_domains(COOKIES_PATH))
        out.append((", ".join(doms[:3]) if doms else "общий", str(COOKIES_PATH)))
    for f in sorted(COOKIES_PATH.parent.glob("cookies.*.txt")):
        if f.name == COOKIES_PATH.name:
            continue
        out.append((f.name[len("cookies."):-len(".txt")], str(f)))
    return out


def cookies_fit(url: str) -> bool:
    """Есть ли у нас куки, которые реально годятся для этого адреса.

    Именно это и надо знать сообщению об ошибке: файл может лежать, но быть
    от совсем другой площадки.
    """
    if site_cookies_path(url):
        return True
    jar = cookies_file(url)
    return bool(jar) and jar_covers(jar, url)


def looks_like_cookies(text: str) -> bool:
    """Проверка формата Netscape cookies.txt — чтобы не принять что попало."""
    head = (text or "")[:4000].lstrip()
    if head.startswith("# Netscape HTTP Cookie File") or head.startswith("# HTTP Cookie File"):
        return True
    # Иногда шапки нет — тогда судим по строкам: домен, флаги, срок, имя, значение
    good = 0
    for line in head.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        if len(line.split("\t")) >= 6:
            good += 1
        if good >= 2:
            return True
    return False


def installed() -> bool:
    return Path(YTDLP).exists()


def find_url(text: str):
    """Первая ссылка в тексте, если она там есть."""
    m = URL_RE.search(text or "")
    return m.group(0).rstrip(".,;)]}") if m else None


INTERNAL_NAMES = ("localhost", "metadata", "metadata.google.internal")


def _ip_is_internal(text: str) -> bool:
    try:
        ip = ipaddress.ip_address(text)
    except ValueError:
        return False
    return (ip.is_private or ip.is_loopback or ip.is_link_local
            or ip.is_reserved or ip.is_multicast)


def check_url(url: str):
    """Проверка без обращения к сети. None — годная ссылка, иначе причина."""
    try:
        p = urlparse(url)
    except Exception:
        return "это не похоже на ссылку"
    if p.scheme not in ("http", "https"):
        return "поддерживаются только ссылки http/https"
    if not p.netloc:
        return "в ссылке нет адреса сайта"
    host = (p.hostname or "").strip("[]").lower()
    if not host or host in INTERNAL_NAMES or _ip_is_internal(host):
        return "ссылка ведёт во внутреннюю сеть — не пойду туда"
    return None


async def check_url_resolved(url: str):
    """То же плюс проверка, куда имя резолвится. Резолв — через loop, не напрямую:
    socket.getaddrinfo блокирующий и подвесил бы бота на время DNS-запроса.

    Зачем вообще: иначе присланная кем-то ссылка вида http://169.254.169.254/…
    заставила бы сервер сходить к своему же метадата-сервису хостинга и принести
    оттуда ключи. От редиректа внутрь это не спасает, но прямой случай закрывает.
    """
    bad = check_url(url)
    if bad:
        return bad
    host = (urlparse(url).hostname or "").strip("[]").lower()
    try:
        loop = asyncio.get_running_loop()
        infos = await asyncio.wait_for(
            loop.getaddrinfo(host, None, proto=socket.IPPROTO_TCP), timeout=10)
    except Exception as e:
        # Отказ, а не пропуск. Раньше тут стоял «return None» (то есть «ссылка
        # годная») — и любой сбой резолва снимал проверку целиком: достаточно
        # было подгадать таймаут, чтобы адрес во внутренней сети проехал мимо.
        log.info("Не проверил адрес %s: %s", host, e)
        return "не смог проверить, куда ведёт адрес — не пойду туда"
    if not infos:
        return "адрес никуда не резолвится"
    for info in infos:
        if _ip_is_internal(info[4][0]):
            return "ссылка ведёт во внутреннюю сеть — не пойду туда"
    return None


# --- Пул прокси ---
# Один адрес рано или поздно попадёт под раздачу, поэтому держим список и
# ходим по кругу, а при отказе пробуем следующий.
PROXIES_PATH = config.BASE_DIR / "proxies.txt"
_proxy_pool = None
_proxy_pos = 0
# Сколько адресов пробуем, прежде чем сдаться. Не жадность: YouTube временами
# отвечает «LOGIN_REQUIRED» на конкретный ролик с конкретного адреса, и с
# другого тот же ролик отдаётся без вопросов. Одна неудачная попытка стоит
# пару секунд, так что перебрать половину списка дешевле, чем отказать зря.
PROXY_TRIES = 8
_last_good = None    # индекс адреса, который сработал последним


def _parse_proxy_line(line: str):
    """Строка списка -> URL прокси. Понимает формат Webshare host:port:user:pass."""
    # ﻿ — метка порядка байтов, которую блокнот Windows дописывает в начало
    # файла. Без её удаления первый прокси в списке молча не работал бы.
    line = (line or "").strip().lstrip("﻿").strip()
    if not line or line.startswith("#"):
        return None
    if "://" in line:
        return line
    parts = line.split(":")
    if len(parts) == 4:
        host, port, user, pwd = parts
        # Логин и пароль экранируем: в них попадаются символы, ломающие URL
        return f"http://{quote(user, safe='')}:{quote(pwd, safe='')}@{host}:{port}"
    if len(parts) == 2:
        return f"http://{line}"
    return None


def proxy_pool() -> list:
    global _proxy_pool
    if _proxy_pool is not None:
        return _proxy_pool
    pool = []
    if config.DOWNLOAD_PROXY:
        pool.append(config.DOWNLOAD_PROXY)
    path = Path(config.PROXY_FILE) if config.PROXY_FILE else PROXIES_PATH
    try:
        if path.exists():
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                p = _parse_proxy_line(line)
                if p and p not in pool:
                    pool.append(p)
    except OSError as e:
        log.warning("Не смог прочитать список прокси: %s", e)
    _proxy_pool = pool
    if pool:
        log.info("Прокси в пуле: %s", len(pool))
    return pool


def reload_proxies() -> int:
    global _proxy_pool, _last_good
    _proxy_pool = None
    _last_good = None
    return len(proxy_pool())


def clear_proxy_block() -> None:
    """Снять паузу после отказа по лимиту — например, когда прислали новый список."""
    globals()["_proxy_blocked_until"] = 0.0


def _candidates() -> list:
    """Какие прокси пробовать по очереди. Пустой пул -> идём напрямую.

    Начинаем с того, что сработал в прошлый раз: YouTube отвечает по-разному
    с разных адресов, и найденный рабочий обычно рабочий и дальше. Без этого
    каждая скачка снова угадывала бы адрес с нуля.
    """
    global _proxy_pos
    pool = proxy_pool()
    if not pool:
        return [None]
    start = _last_good if _last_good is not None else _proxy_pos
    out = [pool[(start + i) % len(pool)] for i in range(min(PROXY_TRIES, len(pool)))]
    _proxy_pos = (_proxy_pos + 1) % len(pool)
    return out


def mark_good(proxy) -> None:
    """Запомнить сработавший адрес, чтобы в следующий раз начать с него."""
    global _last_good
    if proxy is None:
        return
    pool = proxy_pool()
    if proxy in pool:
        _last_good = pool.index(proxy)


def hide_proxy(text: str) -> str:
    """Убрать логин и пароль прокси из текста, который может уехать в чат или лог."""
    return re.sub(r"//[^/@\s]+:[^/@\s]+@", "//***:***@", text or "")


# Ошибки, при которых имеет смысл повторить с другого адреса. Всё остальное
# (нет такого видео, файл больше лимита) от смены прокси не изменится.
RETRIABLE = ("403", "not a bot", "sign in to confirm", "unexpected response",
             "unable to download video data", "timed out", "timeout",
             "connection", "temporarily", "429")


def _worth_retry(tail: str) -> bool:
    low = (tail or "").lower()
    return any(k in low for k in RETRIABLE)


# Кому чужой адрес действительно нужен. Всё остальное прекрасно качается с
# адреса сервера, и гонять его через прокси — значит впустую жечь платный
# трафик. Именно на этом он в первый раз и кончился: TikTok с Instagram
# выбрали лимит, хотя им прокси не нужен вовсе.
PROXY_HOSTS = ("youtube.com", "youtu.be", "googlevideo.com", "music.youtube.com")

# Прокси отвечает так, когда кончился трафик или неверен логин. Перебирать
# остальные адреса бессмысленно: лимит один на весь аккаунт.
QUOTA_MARKERS = ("402 payment required", "bandwidthlimit", "x-webshare-error",
                 "407 proxy authentication", "proxy authentication required")
PROXY_COOLDOWN = 1800          # полчаса не трогаем прокси после отказа по лимиту
_proxy_blocked_until = 0.0


def needs_proxy(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return any(host == h or host.endswith("." + h) for h in PROXY_HOSTS)


def _proxy_exhausted(tail: str) -> bool:
    low = (tail or "").lower()
    return any(m in low for m in QUOTA_MARKERS)


def proxies_usable() -> bool:
    return bool(proxy_pool()) and time.time() >= _proxy_blocked_until


def proxy_status() -> dict:
    left = max(0, int(_proxy_blocked_until - time.time()))
    return {"count": len(proxy_pool()), "blocked_sec": left}


# Прямой доступ к YouTube то работает, то нет — зависит от того, помечен ли
# сейчас адрес сервера. Запоминаем, чем кончилась последняя попытка, и
# начинаем с того, что сработало: когда напрямую получается, платный трафик
# прокси не тратится вовсе.
_direct_ok_ts = 0.0
DIRECT_TRUST_SEC = 3600


def note_direct_result(ok: bool) -> None:
    globals()["_direct_ok_ts"] = time.time() if ok else 0.0


def direct_first() -> bool:
    return time.time() - _direct_ok_ts < DIRECT_TRUST_SEC


def _plan(url: str, have_cookies: bool) -> list:
    """Порядок попыток: список пар (прокси или None, слать ли куки).

    Ключевой факт: куки на ПРЯМОМ соединении обходят проверку «подтвердите, что
    вы не робот» даже с помеченного адреса — и не тратят платный трафик прокси.
    Поэтому, когда адрес похоже помечен, куки-напрямую идут первым делом, а не
    последней попыткой через прокси (как было раньше — тогда при кончившемся
    трафике куки вообще не доходили до дела, и валидные куки выдавались за
    протухшие).
    """
    if needs_proxy(url):
        proxies = [(p, False) for p in _candidates()] if proxies_usable() else []
        if direct_first():
            # Наш адрес недавно пускало напрямую — начинаем с бесплатного пути.
            steps = [(None, False)]
            if have_cookies:
                steps.append((None, True))    # вдруг ролик закрыт до входа
            steps += proxies
        else:
            # Адрес, похоже, помечен. Куки-напрямую — самый надёжный и бесплатный
            # обход; прокси следом; голый прямой заход — последней попыткой.
            steps = ([(None, True)] if have_cookies else []) + proxies + [(None, False)]
        # Совсем закрытые ролики (гео + возраст) добираем куками через прокси,
        # но только если это явно разрешено (для Google это вход с чужого адреса).
        if have_cookies and config.COOKIES_WITH_PROXY and proxies:
            steps.append((proxies[0][0], True))
    else:
        # Здесь прокси не нужен. Оставляем его запасным путём на случай, если
        # площадка вдруг заблокирует адрес сервера.
        steps = [(None, False)]
        # Instagram и прочие тоже прячут часть контента за входом. Куки для них
        # раньше не отправлялись вовсе — при том что текст ошибки предлагал их
        # прислать. Обещание без исполнения; теперь шаг с куками есть и здесь.
        if have_cookies:
            steps.append((None, True))
        if proxies_usable():
            steps += [(p, False) for p in _candidates()[:2]]
    # Убираем дубли, сохраняя порядок: без этого один и тот же заход мог
    # повториться (например, (None, False) из direct_first и из хвоста).
    out, seen = [], set()
    for s in steps:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out or [(None, False)]


def _build_args(urls, out_dir: Path, meta: Path, audio_only: bool,
                proxy: str = None, with_cookies: bool = None,
                height: int = None, fmt: str = None) -> list:
    args = [
        YTDLP,
        "--ignore-config",      # не подхватывать чужой ~/.config/yt-dlp
        "--no-playlist",        # ссылка на плейлист не должна тянуть 200 роликов
        "--newline",            # каждое обновление прогресса — своей строкой
        "--progress-template", PROGRESS_TEMPLATE,
        "--no-warnings",
        "--no-mtime",
        "--restrict-filenames",  # имя файла без пробелов и юникод-сюрпризов
        "--retries", "3",
        "--socket-timeout", "30",
        "--max-filesize", f"{config.DOWNLOAD_MAX_MB}m",
        # Движок JS: YouTube отдаёт ссылки только тому, кто решил его
        # js-задачу («n challenge»). По умолчанию yt-dlp ищет deno, а у нас node.
        "--js-runtimes", "node",
        "-P", f"home:{out_dir}",
        # Порядковый номер обязателен: у карусели все элементы носят одно
        # название, и без него они писались бы в один файл, затирая друг друга.
        # Именно номер, а не %(id)s: у прямой CDN-ссылки id — это вся строка
        # запроса, и имя файла не влезало в лимит файловой системы.
        "-o", "%(autonumber)03d-%(title).60B.%(ext)s",
        "--print-to-file", "after_move:%(title)s", str(meta),
    ]
    if audio_only:
        args += ["-f", "ba/b", "-x", "--audio-format", "mp3"]
    else:
        # h264 в приоритете: AV1 у Telegram проигрывается не везде, ролик
        # приходил бы файлом вместо видео. Высота ограничена: вдвое меньше
        # ждать и втрое меньше весит, а на телефоне разница почти не видна.
        h = int(height or config.DOWNLOAD_QUALITY)
        plat = site_key(urls[0]) if urls else ""
        if plat in ("instagram", "pinterest", "facebook"):
            # Эти площадки отдают ЦЕЛЬНЫЙ файл, где видео и звук уже вместе и в
            # совместимых с Telegram кодеках. Их РАЗДЕЛЬНАЯ дорожка DASH иногда
            # несёт звук в opus, который в mp4 ложится криво: видео играет, а
            # звука нет. Поэтому сперва цельный формат, и только если его нет —
            # раздельные со слиянием.
            # Последним — видео без звука: у поста, где звука нет в оригинале
            # (has_audio=False), ни «b», ни «+ba» не находятся, и yt-dlp падал
            # «Requested format is not available». Есть ли звук на самом деле,
            # потом проверяет _ensure_audio по самому файлу.
            fmt_auto = f"b[height<={h}]/bv*[height<={h}]+ba/b/bv*+ba/bv*[height<={h}]/bv*"
        else:
            # YouTube: цельный формат максимум 360p, поэтому наоборот — сперва
            # раздельные дорожки со слиянием.
            fmt_auto = f"bv*[height<={h}]+ba/b[height<={h}]/bv*+ba/b"
        args += ["-f", fmt or fmt_auto,
                 "--merge-output-format", "mp4",
                 # aac перед opus: opus в mp4 Telegram проигрывает без звука.
                 "-S", f"res:{h},vcodec:h264,acodec:aac,ext:mp4:m4a"]
    if POT_SCRIPT.exists():
        args += ["--extractor-args",
                 f"youtubepot-bgutilscript:script_path={POT_SCRIPT}"]
    if proxy:
        args += ["--proxy", proxy]
    # Куки берём по первой ссылке: в одном задании они всегда с одной площадки.
    ck = cookies_file(urls[0] if urls else None)
    # По умолчанию через прокси куки НЕ отправляем: для Google это вход в
    # аккаунт с чужого адреса в другой стране — прямой путь к блокировке по
    # «подозрительной активности». И они не нужны: обычные ролики с прокси
    # качаются без всякого входа.
    # Исключение — with_cookies=True: так мы добираем ролики, которые YouTube
    # закрыл до входа (возрастное ограничение). Тогда куки нужны по существу.
    use_ck = (with_cookies if with_cookies is not None else not proxy)
    if ck and use_ck:
        args += ["--cookies", ck]
    # "--" обязателен: без него ссылка, начинающаяся с дефиса, была бы
    # разобрана как ключ командной строки.
    args += ["--"] + list(urls)
    return args


LOG_TAIL_LINES = 60

# Ступени качества, которые предлагаем на выбор
QUALITY_STEPS = (360, 480, 720, 1080)


def qualities_from(info: dict) -> list:
    """Какие качества реально доступны и сколько примерно весят.

    Размер у отдельной видеодорожки — только видео, поэтому к нему прибавляем
    звук: иначе на кнопке было бы вдвое меньше правды.
    """
    formats = info.get("formats") or []
    audio_sizes = [f.get("filesize") or f.get("filesize_approx") or 0
                   for f in formats
                   if f.get("vcodec") == "none" and f.get("acodec") not in (None, "none")]
    audio = min([a for a in audio_sizes if a], default=0)

    videos = [f for f in formats if f.get("height")]
    out, seen = [], set()
    for step in QUALITY_STEPS:
        fit = [f for f in videos if f["height"] <= step]
        if not fit:
            continue
        top = max(f["height"] for f in fit)
        if top in seen:
            continue     # эта ступень не добавляет выбора — та же дорожка
        seen.add(top)
        cands = [f for f in fit if f["height"] == top]
        # Оцениваем по h264: именно его выбирает скачивание (-S vcodec:h264).
        # По VP9/AV1 вышло бы вдвое меньше правды — они легче при том же кадре.
        h264 = [c for c in cands if "avc" in (c.get("vcodec") or "").lower()]
        pick = h264 or cands
        sizes = [f.get("filesize") or f.get("filesize_approx") or 0 for f in pick]
        size = max([s for s in sizes if s], default=0)
        muxed = any(c.get("acodec") not in (None, "none") for c in pick)
        if size and not muxed:
            size += audio
        out.append({"step": step, "height": top, "size": size})
    return out


async def probe(url: str) -> dict:
    """Узнать название и доступные качества, ничего не скачивая.

    Стоит тех же ~7 секунд, что и начало обычной скачки: YouTube всё равно
    требует решить свою js-задачу, прежде чем что-то рассказать.
    """
    if not installed():
        return {"error": "yt-dlp не установлен"}
    bad = await check_url_resolved(url)
    if bad:
        return {"error": bad}

    # Тот же план, что и у скачки: иначе на помеченном адресе опрос упирался бы
    # в «ты не робот» ещё до выбора качества, хотя куки его обходят.
    ck = cookies_file(url)
    last_tail, quota = "", False
    async with _probe_gate:
        for proxy, with_ck in _plan(url, bool(ck)):
            # Лимит один на весь аккаунт: после отказа по трафику остальные адреса
            # ответят тем же. Пропускаем их, но прямые шаги ещё имеют смысл.
            if proxy and quota:
                continue
            head = [YTDLP, "--ignore-config", "--no-playlist", "--no-warnings",
                    "--socket-timeout", "30", "--js-runtimes", "node", "-J"]
            if POT_SCRIPT.exists():
                head += ["--extractor-args",
                         f"youtubepot-bgutilscript:script_path={POT_SCRIPT}"]
            if proxy:
                head += ["--proxy", proxy]
            use_ck = with_ck if with_ck is not None else not proxy
            if ck and use_ck:
                head += ["--cookies", ck]
            try:
                proc = await asyncio.create_subprocess_exec(
                    *head, "--", url,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                    start_new_session=True)   # своя группа — чтобы гасить с потомками
                out, err = await asyncio.wait_for(proc.communicate(), timeout=120)
            except asyncio.TimeoutError:
                await _kill_and_reap(proc)
                continue
            except Exception as e:
                return {"error": html.escape(hide_proxy(str(e)))}
            if proc.returncode == 0 and out:
                try:
                    info = json.loads(out)
                except ValueError:
                    continue
                mark_good(proxy)
                return {"title": (info.get("title") or "видео").strip(),
                        "duration": info.get("duration") or 0,
                        "qualities": qualities_from(info), "error": None}
            tail = (err or b"").decode("utf-8", "replace")
            last_tail = tail or last_tail
            # Прокси ответил «кончился трафик» — остальные адреса из того же аккаунта
            # ответят так же. Ставим паузу и не гоняем впустую весь пул.
            if proxy and _proxy_exhausted(tail):
                quota = True
                globals()["_proxy_blocked_until"] = time.time() + PROXY_COOLDOWN
                continue
            if not _worth_retry(tail):
                return {"error": _explain(tail, used_proxy=bool(proxy), url=url,
                                          hit_quota=quota), "tail": tail}
        return {"error": _explain(last_tail, used_proxy=bool(proxy_pool()), url=url,
                                  hit_quota=quota), "tail": last_tail}


_STREAM_PART_RE = re.compile(r"\.f\d+\.[a-z0-9]+$", re.I)
# Чисто звуковые расширения: если просили видео, а пришло только это —
# видеодорожка не скачалась, и отдавать такое нельзя
AUDIO_EXT = {".m4a", ".mp3", ".opus", ".ogg", ".oga", ".aac", ".flac", ".wav", ".weba"}


def _is_stream_leftover(name: str) -> bool:
    """Недосклеенный кусок: yt-dlp называет отдельные дорожки «...f137.mp4»,
    «...f140.m4a». Если склейка не дошла, они остаются лежать, и раньше бот
    отправлял такой обрывок как готовый файл — приходила одна звуковая дорожка
    вместо видео."""
    return bool(_STREAM_PART_RE.search(name or ""))


async def _pump(proc, on_progress, log_tail) -> None:
    """Вычитывает вывод процесса: прогресс отдаёт наружу, остальное копит хвостом."""
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        s = line.decode("utf-8", "replace").rstrip()
        if PROGRESS_TAG in s:
            if on_progress is not None:
                try:
                    await on_progress(_parse_progress(s))
                except Exception:
                    log.debug("Обработчик прогресса упал", exc_info=True)
            continue
        if s:
            log_tail.append(s)
            if len(log_tail) > LOG_TAIL_LINES * 2:
                del log_tail[:LOG_TAIL_LINES]
    await proc.wait()


async def fetch(url: str, audio_only: bool = False, on_progress=None,
                job_id: int = None, height: int = None) -> dict:
    """Скачать по ссылке. Возвращает {'paths','title','job','error'}.

    on_progress — корутина, ей отдаются словари с ходом скачивания.
    job_id — номер, по которому загрузку можно прибить через cancel().

    Вызывающий обязан позвать discard(job) после отправки — иначе файлы
    останутся лежать на диске.
    """
    if not installed():
        return {"error": "yt-dlp не установлен"}
    # Отмечаемся работающими сразу: кнопка «Отмена» уже показана, и нажатие в
    # ближайшие секунды (резолв DNS, очередь, опрос TikTok) должно сработать.
    if job_id is not None:
        _active.add(job_id)
    try:
        return await _fetch(url, audio_only, on_progress, job_id, height)
    finally:
        if job_id is not None:
            _active.discard(job_id)
            _cancelled.discard(job_id)


def _cancel_pending(job_id) -> bool:
    return job_id is not None and job_id in _cancelled


async def _fetch(url: str, audio_only: bool = False, on_progress=None,
                 job_id: int = None, height: int = None) -> dict:
    bad = await check_url_resolved(url)
    if bad:
        return {"error": bad}
    if _cancel_pending(job_id):
        return {"error": "отменено", "cancelled": True}

    job = config.DOWNLOAD_DIR / f"job-{int(time.time() * 1000)}-{abs(hash(url)) % 10000}"
    out_dir, meta = job / "out", job / "title.txt"
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        return {"error": f"не смог создать папку для загрузки: {e}"}

    # TikTok идёт в обход: своим экстрактором yt-dlp его с сервера не получает,
    # поэтому сперва узнаём прямые ссылки, а качаем уже их.
    dl_urls, forced_title = [url], None
    if config.TIKTOK_VIA_API and is_tiktok(url):
        info, err = await tiktok_direct(url)
        if err:
            discard(job)
            return {"error": f"TikTok: {html.escape(err)}"}
        dl_urls, forced_title = info["urls"], info["title"]
        if _cancel_pending(job_id):     # опрос TikTok идёт до 30 секунд
            discard(job)
            return {"error": "отменено", "cancelled": True}

    # Пробуем по очереди несколько адресов: один может оказаться забаненным
    # площадкой, и тогда следующий обычно проходит. Если куки есть — они идут
    # на прямом соединении первыми (или сразу после бесплатного прямого захода):
    # так обходится проверка «ты не робот» с помеченного адреса, не тратя прокси.
    steps = _plan(url, bool(cookies_file(url)))
    tail, files, tried_cookies, used_proxy = "", [], False, False
    hit_quota = False
    attempt, i = 0, 0
    while i < len(steps):
        proxy, with_ck = steps[i]
        i += 1
        attempt += 1
        used_proxy = used_proxy or bool(proxy)
        log_tail = []
        tried_cookies = tried_cookies or with_ck
        if with_ck:
            log.info("Пробую с куками — ролик закрыт до входа")
        args = _build_args(dl_urls, out_dir, meta, audio_only,
                           proxy=proxy, with_cookies=with_ck, height=height)
        async with _gate:
            # В очереди можно простоять сколько угодно (две качалки разом), и
            # отмена чаще всего прилетает именно сюда. Проверяем, уже получив
            # слот, — иначе запустили бы то, что человек уже отменил.
            if _cancel_pending(job_id):
                discard(job)
                return {"error": "отменено", "cancelled": True}
            proc = None
            try:
                proc = await asyncio.create_subprocess_exec(
                    *args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    # Своя сессия: yt-dlp порождает ffmpeg и node, и убивать их
                    # надо всей группой — иначе они переживают отмену.
                    start_new_session=True,
                )
                if job_id is not None:
                    _jobs[job_id] = proc
                # Читаем построчно, а не communicate(): так видно прогресс по ходу
                # дела, а обычный вывод копится только хвостом — на длинном ролике
                # yt-dlp иначе выдаёт мегабайты текста.
                await asyncio.wait_for(_pump(proc, on_progress, log_tail),
                                       timeout=config.DOWNLOAD_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                await _kill_and_reap(proc)
                discard(job)
                return {"error": f"не уложился в {config.DOWNLOAD_TIMEOUT_SEC} сек"}
            except asyncio.CancelledError:
                await _kill_and_reap(proc)
                discard(job)
                raise
            except Exception as e:
                await _kill_and_reap(proc)
                discard(job)
                return {"error": html.escape(hide_proxy(str(e)))}
            finally:
                if job_id is not None:
                    _jobs.pop(job_id, None)

        if job_id is not None and job_id in _cancelled:
            _cancelled.discard(job_id)
            discard(job)
            return {"error": "отменено", "cancelled": True}

        tail = "\n".join(log_tail)
        rc = proc.returncode if proc is not None else 1
        files = [p for p in out_dir.iterdir()
                 if p.is_file() and p.suffix not in (".part", ".ytdl")
                 and not _is_stream_leftover(p.name)] \
            if out_dir.exists() else []
        # Код возврата обязателен. Без него частичная неудача считалась успехом:
        # видео не скачалось, звук скачался — и уезжала одна звуковая дорожка
        # вместо ролика.
        if rc != 0:
            log.info("yt-dlp вышел с кодом %s — считаю попытку неудачной", rc)
            files = []
        # Просили видео, а лежит один звук — значит видеодорожка не скачалась.
        # Раньше такое уезжало пользователю как готовый файл.
        if files and not audio_only and \
                all(f.suffix.lower() in AUDIO_EXT for f in files):
            log.info("Скачался только звук, а просили видео — это неудача")
            files, tail = [], tail + "\nвидеодорожка не скачалась"
        if files:
            mark_good(proxy)
            if needs_proxy(url):
                # Успех засчитываем «прямому пути» только если он был ГОЛЫЙ.
                # Если получилось благодаря кукам, адрес всё ещё помечен, и
                # начинать следующий раз с голого захода — терять попытку зря.
                note_direct_result(proxy is None and not with_ck)
            break

        # Прокси отвечает «кончился трафик» — перебирать остальные бессмысленно,
        # лимит один на весь аккаунт. Выкидываем их из плана и идём напрямую.
        if proxy and _proxy_exhausted(tail):
            hit_quota = True    # иначе правду затрёт ошибка следующей попытки
            globals()["_proxy_blocked_until"] = time.time() + PROXY_COOLDOWN
            log.warning("Прокси не пускают (трафик или логин) — "
                        "следующие %s мин работаю без них", PROXY_COOLDOWN // 60)
            # Только ещё не пройденные прямые шаги. Пустой остаток означает, что
            # прямой заход уже был и повторять его незачем — раньше сюда
            # подставлялся [(None, False)] и попытка гонялась второй раз.
            steps = [s for s in steps[i:] if s[0] is None]
            i = 0
            _clean(out_dir)     # мусор от неудачной попытки — раньше тут терялся
            if not steps:
                break
            continue
        # Голый прямой заход не пустили — значит адрес помечен. Но только если
        # причина вообще в адресе: «видео удалено» или «файл больше лимита» к
        # нему отношения не имеют, а сбрасывали флаг ровно так же — и следующая
        # скачка зря лезла в платный прокси с чистого адреса.
        if proxy is None and needs_proxy(url) and _worth_retry(tail):
            note_direct_result(False)
        if not _worth_retry(tail):
            break     # причина не в адресе — менять его бессмысленно
        log.info("Попытка %s не удалась, пробую следующий адрес", attempt)
        _clean(out_dir)

    if not files:
        discard(job)
        return {"error": _explain(tail, used_proxy=used_proxy,
                                  tried_cookies=tried_cookies,
                                  url=url, hit_quota=hit_quota)}

    # Видео без звука (Instagram отдаёт «цельный» формат, где звука на деле
    # нет): проверяем ffprobe и при нужде перекачиваем со звуком.
    files.sort(key=lambda p: p.name)    # порядок карусели — по номеру в имени
    audio_note = None
    if not audio_only and dl_urls == [url]:
        try:
            files, audio_note = await _ensure_audio(
                url, job, files, proxy, with_ck, height, job_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Проверка звука упала — отдаю как скачалось")
        if _cancel_pending(job_id):
            discard(job)
            return {"error": "отменено", "cancelled": True}

    # ВСЕ файлы, а не самый большой. В посте Instagram может быть десяток фото —
    # раньше отдавалась только одна, самая тяжёлая, а остальные молча удалялись
    # вместе с рабочей папкой.
    paths = [str(p) for p in files[:MAX_FILES]]
    extra = len(files) - len(paths)
    title = forced_title or ""
    if not title:
        try:
            title = meta.read_text(encoding="utf-8", errors="replace").strip().splitlines()[0]
        except (OSError, IndexError):
            title = files[0].stem
    return {"paths": paths, "title": title or files[0].stem, "job": job,
            "extra": extra, "via_proxy": used_proxy, "error": None,
            "audio_note": audio_note}


# ---------- звук в видео ----------
# Instagram в video_versions не всегда говорит, есть ли звук (has_audio), и
# yt-dlp считает такой формат цельным: «b» берёт его первым, а в файле одна
# картинка. Поэтому после скачивания смотрим в сам файл, а не в метаданные.

VIDEO_FILE_EXT = {".mp4", ".mov", ".mkv", ".webm", ".m4v", ".avi", ".3gp"}
AUDIO_RETRY_MAX = 4            # сколько перекачек ради звука, не больше
NOTE_NO_AUDIO = "в оригинале без звука"
NOTE_AUDIO_FAILED = "звук не скачался"


def _has_audio_blocking(path) -> object:
    """True/False — есть ли в файле звуковая дорожка; None — не смогли узнать."""
    import subprocess
    try:
        res = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a",
             "-show_entries", "stream=index", "-of", "csv=p=0", str(path)],
            capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    if res.returncode != 0:
        return None
    return bool(res.stdout.strip())


async def has_audio(path):
    return await asyncio.to_thread(_has_audio_blocking, path)


def _real_codec(c) -> bool:
    return c not in (None, "none", "")


def _all_formats(info) -> list:
    if not isinstance(info, dict):
        return []
    out = list(info.get("formats") or [])
    for e in info.get("entries") or []:
        if isinstance(e, dict):
            out += list(e.get("formats") or [])
    return out


def _audio_chains(info, height) -> list:
    """Что пробовать, чтобы был звук: сперва склейка лучшего видео с лучшим
    звуком (нужен ffmpeg и отдельная звуковая дорожка), затем каждый формат
    с картинкой, который yt-dlp перечислил и который МОЖЕТ нести звук."""
    h = int(height or config.DOWNLOAD_QUALITY)
    fmts = _all_formats(info)
    chains = []
    audio_only = [f for f in fmts if f.get("vcodec") == "none" and _real_codec(f.get("acodec"))]
    if audio_only and shutil.which("ffmpeg"):
        chains.append(f"bv*[height<={h}]+ba/bv*+ba")
    if info.get("entries"):
        return chains           # у карусели свои format_id у каждого элемента
    cands = [f for f in fmts if f.get("vcodec") != "none" and f.get("acodec") != "none"
             and f.get("format_id")]
    cands.sort(key=lambda f: (not _real_codec(f.get("acodec")),
                              -(f.get("height") or 0)))
    seen = set()
    for f in cands:
        if f["format_id"] not in seen:
            seen.add(f["format_id"])
            chains.append(f["format_id"])
    return chains


async def _ytdlp_run(args, timeout: int, job_id=None):
    """Запустить yt-dlp и дождаться. (код, stdout) или (None, b"") по таймауту."""
    async with _gate:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            start_new_session=True)
        if job_id is not None:
            _jobs[job_id] = proc
        try:
            out, _err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            await _kill_and_reap(proc)
            return None, b""
        except asyncio.CancelledError:
            await _kill_and_reap(proc)
            raise
        finally:
            if job_id is not None:
                _jobs.pop(job_id, None)
    return proc.returncode, out or b""


def _info_args(url, proxy, with_ck) -> list:
    head = [YTDLP, "--ignore-config", "--no-playlist", "--no-warnings",
            "--socket-timeout", "30", "--js-runtimes", "node", "-J"]
    if POT_SCRIPT.exists():           # как в probe(): без него YouTube не ответит
        head += ["--extractor-args",
                 f"youtubepot-bgutilscript:script_path={POT_SCRIPT}"]
    if proxy:
        head += ["--proxy", proxy]
    ck = cookies_file(url)
    use_ck = with_ck if with_ck is not None else not proxy
    if ck and use_ck:
        head += ["--cookies", ck]
    return head + ["--", url]


async def _ensure_audio(url, job: Path, files, proxy, with_ck, height, job_id):
    """Вернуть (files, пометка). Пометка — None, если со звуком всё в порядке."""
    videos = [f for f in files if f.suffix.lower() in VIDEO_FILE_EXT]
    if not videos or not shutil.which("ffprobe"):
        return files, None
    silent = [f for f in videos if await has_audio(f) is False]
    if not silent:
        return files, None
    log.info("Скачалось видео без звука (%s шт) — разбираюсь", len(silent))

    rc, out = await _ytdlp_run(_info_args(url, proxy, with_ck), 120, job_id)
    info = None
    if rc == 0 and out:
        try:
            info = json.loads(out)
        except ValueError:
            info = None
    fmts = _all_formats(info)
    if fmts and all(f.get("acodec") == "none" for f in fmts):
        log.info("Ни в одном формате нет звука — в оригинале без звука")
        return files, NOTE_NO_AUDIO

    files = list(files)
    for k, fmt in enumerate(_audio_chains(info or {}, height)[:AUDIO_RETRY_MAX]):
        if _cancel_pending(job_id):
            return files, None
        rdir = job / f"audio-{k}"
        rdir.mkdir(parents=True, exist_ok=True)
        args = _build_args([url], rdir, rdir / "title.txt", False, proxy=proxy,
                           with_cookies=with_ck, height=height, fmt=fmt)
        rc, _ = await _ytdlp_run(args, config.DOWNLOAD_TIMEOUT_SEC, job_id)
        got = sorted((p for p in rdir.iterdir() if p.is_file()
                      and p.suffix not in (".part", ".ytdl") and p.name != "title.txt"
                      and not _is_stream_leftover(p.name)), key=lambda p: p.name) \
            if rc == 0 else []
        if not got:
            log.info("Формат %s не скачался", fmt)
            continue
        # Карусель перекачивается целиком: файлы сопоставляем по порядку
        pairs = zip(range(len(files)), got) if len(got) == len(files) else \
            ([(files.index(silent[0]), got[0])] if len(silent) == 1 else [])
        for i, g in pairs:
            if files[i] in silent and await has_audio(g):
                log.info("Звук нашёлся в формате %s", fmt)
                files[i] = g
        silent = [f for f in silent if f in files]
        if not silent:
            return files, None

    if fmts and not any(_real_codec(f.get("acodec")) for f in fmts):
        # Звук нигде явно не заявлен, а все форматы, которые могли его нести,
        # перепробованы и пусты — значит, его нет и в оригинале
        return files, NOTE_NO_AUDIO
    log.warning("Звук так и не скачался: %s", url[:100])
    return files, NOTE_AUDIO_FAILED


async def _kill_and_reap(proc) -> None:
    """Убить ветку процессов и дождаться, пока она действительно умрёт.

    Без ожидания уборка гналась наперегонки с ffmpeg: он мог создать файл уже
    после того, как каталог перечислили, и rmtree падал на «каталог не пуст»
    (ошибку глотал ignore_errors), оставляя мусор до следующей чистки.
    """
    _kill(proc)
    if proc is None:
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=10)
    except (asyncio.TimeoutError, ProcessLookupError):
        pass


def _clean(out_dir: Path) -> None:
    """Убрать недокачанное перед следующей попыткой. Молча: файлов может не быть."""
    for junk in (out_dir.iterdir() if out_dir.exists() else []):
        try:
            junk.unlink()
        except OSError:
            pass


def _kill(proc) -> None:
    """Убить всю ветку процессов, а не только yt-dlp.

    yt-dlp сам запускает ffmpeg (склейка видео со звуком) и node (js-задача
    YouTube). SIGKILL одному лишь родителю оставлял их сиротами: после «отменено»
    ffmpeg ещё минуты дожимал 300-мегабайтный ролик, держал открытыми уже
    удалённые файлы (место на диске не возвращалось) и мог создать новый файл
    прямо во время уборки — тогда папка задания оставалась лежать до ближайшей
    чистки. Поэтому процессы запускаются своей группой и гасятся группой.
    """
    if proc is None or proc.returncode is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        # Группы может уже не быть — тогда хотя бы сам процесс
        try:
            proc.kill()
        except ProcessLookupError:
            pass


def _explain(stderr_tail: str, used_proxy: bool = False,
             tried_cookies: bool = False, url: str = None,
             hit_quota: bool = False) -> str:
    """Перевод типовых отказов yt-dlp на человеческий.

    Порядок проверок важен. Раньше здесь ловилось «confirm you», а yt-dlp этой же
    фразой советует обновиться («Confirm you are on the latest version») — и
    блокировка по IP выдавалась за «требует вход», уводя не туда.

    url нужен, чтобы не выдавать советы про YouTube на чужую беду: экстрактор
    Instagram отвечает «rate-limit reached or login required», и без адреса это
    попадало в ветку бот-чека — человеку сообщали, что виноват YouTube, и тут же
    уверяли, что «Instagram работает без этого». То есть про сломанное говорили,
    что оно работает.

    hit_quota — прокси отказал по трафику где-то по ходу перебора. Без этого
    флага правда терялась: после 402 делался ещё один прямой заход, его ошибка
    затирала хвост, и вместо «кончился трафик» человек читал «этот ролик закрыт
    до входа, обычные качаются нормально» — хотя не качалось решительно ничего.
    """
    low = (stderr_tail or "").lower()
    # Важно не «есть ли файл», а «годится ли он сюда»: с ютубовскими куками
    # Instagram всё равно откажет, и советовать «выгрузи заново» бессмысленно.
    have_cookies = cookies_fit(url) if url else (cookies_file() is not None)
    # Про какую площадку речь. Без адреса считаем, что про YouTube: почти все
    # эти отказы приходят от него.
    site_is_yt = True if not url else needs_proxy(url)
    # Какую площадку называть в подсказке. Без этого бот советовал «выгрузи куки
    # заново» человеку, у которого куки были только от YouTube, а отказал
    # Instagram: тот выгружал ютубовские по второму разу, и ничего не менялось.
    site = site_key(url) if url else ""
    where = f"{site}.com" if site else "сайт"
    if not have_cookies:
        hint = (f"\n\n<b>Как починить:</b> открой {where} в браузере под своим "
                "аккаунтом, выгрузи куки расширением вроде «Get cookies.txt» "
                "и <b>пришли мне этот файл</b>. Куки от других площадок тут "
                "не подойдут — для каждой нужны свои, я их храню отдельно.")
    else:
        hint = (f"\n\nКуки для {where} у меня есть, но они не подошли — похоже, "
                "протухли. Выгрузи заново и пришли файл ещё раз.")

    if _proxy_exhausted(low) or hit_quota:
        ck_line = ("" if have_cookies else
                   "Либо, чтобы вообще не зависеть от прокси, пришли "
                   "<code>cookies.txt</code> с отдельного Google-аккаунта — "
                   "с куками YouTube качается напрямую, без платного трафика.\n\n")
        return ("<b>на прокси кончился трафик.</b>\n\n"
                "YouTube без чужого адреса видео не отдаёт, поэтому он сейчас "
                "недоступен. TikTok, Instagram и прямые ссылки работают как "
                "обычно — им прокси не нужен.\n\n"
                + ck_line +
                "Лечится так: продли тариф у провайдера прокси либо пришли мне "
                "новый файл <code>proxies.txt</code> — подхвачу без перезапуска.")
    if "видеодорожка не скачалась" in (stderr_tail or ""):
        return ("скачалась только звуковая дорожка, видео не пришло. "
                "Это бывает, когда площадка отдаёт поток кусками и часть их "
                "теряется. Попробуй ещё раз или выбери качество пониже.")
    if "403" in low and ("video data" in low or "fragment" in low):
        # Метаданные достались, а само видео сервер отдавать отказался. У YouTube
        # это про адрес: его видео-серверы режут датацентровые IP независимо от
        # куков и PO-токена. Куки тут не при чём, и советовать их — врать.
        return ("видео-серверы площадки отказали этому адресу (403).\n\n"
                "Так YouTube отсекает серверные IP — куки и токены на это не "
                "влияют. Лечится только прокси с «жилым» адресом: пропиши его в "
                "<code>DOWNLOAD_PROXY</code>. TikTok и Instagram работают и так.")
    if "unexpected response from webpage" in low:
        # Тут блокируется именно адрес, а не отсутствие входа, поэтому обещать
        # починку куками — врать: раньше сюда подклеивалась подсказка «пришли
        # cookies.txt, дальше заработает», хотя они на это не влияют.
        return ("площадка не пускает адрес сервера — для неё это робот.\n\n"
                "Куки тут не помогут: блокируется сам адрес. Помогает прокси — "
                "пришли мне файл <code>proxies.txt</code>.")
    if ("not a bot" in low or "sign in to confirm" in low
            or "login_required" in low or "captcha" in low or "rate-limit" in low):
        # Не YouTube — значит и советы не про него. Эти же маркеры сыплет,
        # например, Instagram («rate-limit reached or login required»), и
        # рассказывать в ответ про серверные IP YouTube было бы не по адресу.
        if not site_is_yt:
            if have_cookies:
                return ("площадка не отдала контент: он виден только после "
                        "входа, а мои куки ей не подошли. Выгрузи куки "
                        "<b>этой площадки</b> и пришли файл ещё раз.")
            return ("площадка отдаёт это только после входа в аккаунт "
                    "(либо просит подождать — она ограничивает частые "
                    "запросы).\n\n<b>Как починить:</b> открой <b>этот же "
                    "сайт</b> в браузере, выгрузи куки расширением вроде "
                    "«Get cookies.txt LOCALLY» и <b>пришли мне файл</b>.")
        # Через прокси эта же фраза значит совсем другое. Если адреса рабочие
        # (а мы их только что перебрали), дело не в нас, а в самом ролике:
        # YouTube закрыл его до входа — обычно из-за возрастного ограничения.
        if used_proxy:
            if tried_cookies:
                return ("YouTube отдаёт этот ролик только по входу, и куки, "
                        "которые у меня есть, ему не подошли — скорее всего "
                        "протухли или на том аккаунте нет доступа (возрастное "
                        "ограничение). Пришли свежий <code>cookies.txt</code>.")
            return ("YouTube закрыл именно этот ролик до входа в аккаунт — так "
                    "он помечает, например, возрастные ограничения.\n\n"
                    "Обычные ролики качаются нормально. Чтобы брать и такие, "
                    "пришли мне <code>cookies.txt</code> — только заведи под это "
                    "<b>отдельный</b> аккаунт Google, не основной.")
        # Прямой заход без куков и без рабочего прокси. Для YouTube это самый
        # частый случай: адрес сервера помечен как робот. Честно даём ОБА рабочих
        # пути, а не только куки — и напоминаем, что остального это не касается.
        if not have_cookies:
            return ("YouTube не пускает адрес сервера — считает его роботом. "
                    "Это не поломка: так площадка отсекает серверные IP.\n\n"
                    "<b>Обойти можно двумя способами, любой на выбор:</b>\n"
                    "1) прислать мне <code>cookies.txt</code> с <b>отдельного</b> "
                    "Google-аккаунта (бесплатно, обновлять раз в пару месяцев);\n"
                    "2) прислать рабочий <code>proxies.txt</code> с непомеченным "
                    "адресом.\n\n"
                    "TikTok, Instagram, Pinterest и прямые ссылки работают и без "
                    "этого — им ничего не нужно.")
        return "площадка просит подтвердить, что запрос не от робота." + hint
    if "login required" in low or "requires authentication" in low or "--cookies" in low:
        return "контент виден только после входа в аккаунт." + hint
    if "max-filesize" in low or "larger than" in low:
        return f"файл больше лимита {config.DOWNLOAD_MAX_MB} МБ"
    if "unsupported url" in low or "no video" in low:
        return "с этого сайта скачивать не умею"
    if "private" in low or "is not available" in low:
        return "контент приватный или скрыт настройками"
    if "unavailable" in low or "not exist" in low or "404" in low:
        return "по ссылке ничего нет (удалено или опечатка)"
    tail = stderr_tail.strip().splitlines()[-1] if stderr_tail.strip() else ""
    # Хвост от yt-dlp экранируем: он идёт в сообщение с HTML-разметкой, а внутри
    # запросто попадаются угловые скобки из ссылок. И обязательно прогоняем
    # через hide_proxy: yt-dlp печатает адрес прокси целиком, вместе с логином
    # и паролем от платного сервиса.
    return html.escape(hide_proxy(tail)[:300]) or "не скачалось"


def discard(job) -> None:
    """Убрать за собой. Молча: файлов может уже не быть."""
    try:
        shutil.rmtree(job, ignore_errors=True)
    except Exception:
        pass


def sweep() -> int:
    """Подмести брошенные задания (падение сервиса посреди скачивания)."""
    n = 0
    root = config.DOWNLOAD_DIR
    if not root.exists():
        return 0
    cutoff = time.time() - 6 * 3600
    for d in root.iterdir():
        try:
            if d.is_dir() and d.stat().st_mtime < cutoff:
                shutil.rmtree(d, ignore_errors=True)
                n += 1
        except OSError:
            pass
    return n
