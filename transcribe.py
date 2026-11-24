"""Локальная расшифровка голосовых и кружочков.

Считается на этой машине через faster-whisper: аудио никуда не уходит — для
приватного бота это принципиально. Telegram Premium тут не помог бы: его
transcribeAudio работает только по ЖИВОМУ сообщению, лежащему на серверах
Telegram, а нам надо разбирать в том числе уже удалённое — из своего кэша.

Модель грузится лениво, при первом запросе: иначе каждый рестарт сервиса стоил
бы полугигабайта памяти даже тем, кому расшифровка не нужна.
"""

import asyncio
import concurrent.futures
import importlib.util
import logging
import os
import time

import config

log = logging.getLogger("transcribe")

_model = None
_fail_ts = 0.0        # когда последний раз не смогли загрузить модель
_lock = asyncio.Lock()  # модель не потокобезопасна: по одной расшифровке за раз
_waiting = 0            # сколько задач ждёт очереди
_installed = None       # результат проверки пакета, считаем один раз
_pool = None            # свой пул под whisper, не дефолтный executor

RETRY_LOAD_SEC = 600    # первая загрузка тянет модель из сети — даём второй шанс
BEAM = 5                # качество/скорость; 1 быстрее, но заметно хуже на русском


def installed() -> bool:
    """Стоит ли пакет. Через find_spec, а не import: сам импорт faster_whisper
    тянет ctranslate2 и onnxruntime — это сотни миллисекунд, и делать их прямо
    в event loop (installed() зовётся из обработчиков) нельзя."""
    global _installed
    if _installed is None:
        try:
            _installed = importlib.util.find_spec("faster_whisper") is not None
        except (ImportError, ValueError):
            _installed = False
    return _installed


def _get_pool():
    """Свой однопоточный пул вместо дефолтного executor.

    Дефолтный на выходе из asyncio.run джойнится с потолком в 300 секунд, а
    systemd даёт на остановку 30 — то есть рестарт во время расшифровки
    гарантированно заканчивался бы SIGKILL. Свой пул мы гасим не дожидаясь.
    """
    global _pool
    if _pool is None:
        _pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="whisper")
    return _pool


def shutdown() -> None:
    """Погасить пул, не дожидаясь текущей расшифровки."""
    global _pool
    if _pool is not None:
        _pool.shutdown(wait=False, cancel_futures=True)
        _pool = None


def _threads() -> int:
    if config.WHISPER_THREADS > 0:
        return config.WHISPER_THREADS
    # Одно ядро оставляем event loop'у, иначе бот подтормаживает во время работы
    return max(1, (os.cpu_count() or 2) - 1)


def _have_local_model() -> bool:
    """Лежит ли модель уже на диске (папка от прошлой загрузки непустая)."""
    d = os.path.join(config.WHISPER_DIR, f"models--Systran--faster-whisper-{config.WHISPER_MODEL}")
    if os.path.isdir(d):
        return True
    # На случай другой раскладки кэша HuggingFace — ищем любой .bin рядом
    try:
        for root, _dirs, files in os.walk(config.WHISPER_DIR):
            if any(f.endswith(".bin") for f in files):
                return True
    except OSError:
        pass
    return False


def _load():
    """Блокирующая загрузка модели — вызывать только из отдельного потока.

    Если модель уже скачана, читаем ТОЛЬКО с диска. Иначе faster-whisper на
    каждой загрузке ходит на HuggingFace сверить версию: это и лишняя задержка,
    и наш IP светится там, где для приватного инструмента незачем.
    """
    from faster_whisper import WhisperModel
    kw = dict(device="cpu", compute_type="int8",
              download_root=config.WHISPER_DIR, cpu_threads=_threads())
    if _have_local_model():
        try:
            return WhisperModel(config.WHISPER_MODEL, local_files_only=True, **kw)
        except Exception:
            log.info("Локальная копия модели не подошла — качаю заново")
    return WhisperModel(config.WHISPER_MODEL, **kw)


async def _ensure_model():
    global _model, _fail_ts
    if _model is not None:
        return _model
    if _fail_ts and time.time() - _fail_ts < RETRY_LOAD_SEC:
        return None   # недавно не смогли — не долбимся в сеть на каждом голосовом
    try:
        t0 = time.time()
        loop = asyncio.get_running_loop()
        _model = await loop.run_in_executor(_get_pool(), _load)
        log.info("Модель «%s» загружена за %.1f сек (%s потоков)",
                 config.WHISPER_MODEL, time.time() - t0, _threads())
        _fail_ts = 0.0
    except Exception:
        _fail_ts = time.time()
        log.exception("Не смог загрузить модель распознавания")
        return None
    return _model


def probe_duration(path: str):
    """Длительность аудио в секундах через ffprobe. None — если не вышло.

    Нужна отдельно и ДО расшифровки: model.transcribe() сначала декодирует
    файл целиком в память и прогоняет по нему VAD, и только потом отдаёт
    info.duration. То есть проверка длины по info срабатывает уже после того,
    как многочасовое аудио съело память и процессор. ffprobe читает только
    заголовок и стоит миллисекунды.
    """
    import subprocess
    try:
        res = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", "--", path],
            capture_output=True, timeout=30, text=True)
        if res.returncode != 0:
            return None
        return float((res.stdout or "").strip())
    except (ValueError, OSError, subprocess.SubprocessError):
        return None


def _run(model, path: str):
    """Блокирующая расшифровка. Возвращает (текст, длительность)."""
    segments, info = model.transcribe(
        path,
        language=config.WHISPER_LANG or None,
        vad_filter=True,          # отсекает тишину: быстрее и без выдуманных слов
        beam_size=BEAM,
    )
    # Страховка на случай, если ffprobe промолчал: сюда мы попадаем уже с
    # декодированным файлом, но хотя бы не тратим процессор на распознавание.
    if info.duration > config.TRANSCRIBE_MAX_SEC:
        return None, info.duration
    text = " ".join(s.text.strip() for s in segments).strip()
    return text, info.duration


async def transcribe(path: str):
    """Расшифровать файл. Возвращает (текст, ошибка) — ровно одно из двух."""
    global _waiting
    if not installed():
        return None, "пакет faster-whisper не установлен"
    if not path or not os.path.exists(path):
        return None, "файла уже нет на диске"
    if _waiting >= config.TRANSCRIBE_QUEUE_MAX:
        # Честный отказ лучше молчаливой очереди на час вперёд
        return None, f"очередь занята ({_waiting} в ожидании), попробуй позже"

    # Отсекаем длинное ДО того, как файл попадёт в whisper: тот декодирует
    # его целиком в память. Голосовое на много часов иначе съело бы гигабайты
    # и заняло ядра, а прислать его может кто угодно из собеседников.
    loop = asyncio.get_running_loop()
    dur = await loop.run_in_executor(_get_pool(), probe_duration, path)
    if dur is not None and dur > config.TRANSCRIBE_MAX_SEC:
        return None, (f"аудио длиннее потолка: {int(dur)} сек "
                      f"при лимите {config.TRANSCRIBE_MAX_SEC}")
    if dur is None:
        # ffprobe не смог прочитать заголовок — не рискуем памятью и судим
        # по размеру: 64 КБ/с заведомо выше битрейта голосовых Telegram.
        try:
            if os.path.getsize(path) > config.TRANSCRIBE_MAX_SEC * 64 * 1024:
                return None, "файл слишком большой для расшифровки"
        except OSError:
            return None, "файла уже нет на диске"

    _waiting += 1
    try:
        async with _lock:
            model = await _ensure_model()
            if model is None:
                return None, "модель распознавания недоступна"
            text, dur = await loop.run_in_executor(_get_pool(), _run, model, path)
        if text is None:
            return None, (f"аудио длиннее потолка: {int(dur)} сек "
                          f"при лимите {config.TRANSCRIBE_MAX_SEC}")
        if not text:
            return None, "речи не распознано (тишина или музыка)"
        return text, None
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.exception("Расшифровка %s упала", path)
        return None, str(e)
    finally:
        _waiting -= 1


def queue_len() -> int:
    return _waiting
