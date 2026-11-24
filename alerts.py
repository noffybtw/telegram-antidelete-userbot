"""Формирование и отправка алертов в лог-чат."""

import asyncio
import html
import logging
import os
import re
from datetime import datetime, timezone

from telethon.errors import FloodWaitError, RPCError

import config

log = logging.getLogger("alerts")

MAX_TEXT = 4000      # лимит Telegram 4096, берём с запасом на разметку
MAX_CAPTION = 900    # лимит подписи 1024
MAX_PREVIEW = 300    # длина текста ОДНОЙ строки в сводке о чистке
RETRY_SLEEP = 5      # база бэкоффа при сетевых ошибках
# Потолок строк в сводке о чистке. Без него снос 2000 сообщений разом давал бы
# текст на сотню килобайт, который уходил бы десятками сообщений подряд —
# это и минуты молчания по остальным алертам, и явный спам-паттерн от аккаунта.
MAX_PURGE_LINES = 60
# Страховка на самой отправке: сколько кусков готовы слать за один вызов
MAX_CHUNKS = 6

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo(config.TIMEZONE) if config.TIMEZONE else None
except Exception:
    log.warning("Не распознан TIMEZONE=%r, беру локальное время", config.TIMEZONE)
    TZ = None


def fmt_dt(ts) -> str:
    if not ts:
        return "неизвестно"
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return (dt.astimezone(TZ) if TZ else dt.astimezone()).strftime("%Y-%m-%d %H:%M:%S")


def fmt_time(ts) -> str:
    """Только время для сегодняшнего, дата+время для старого — чтобы не шуметь."""
    if not ts:
        return "неизвестно"
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(TZ) if TZ \
        else datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone()
    today = datetime.now(dt.tzinfo).date()
    if dt.date() == today:
        return dt.strftime("%H:%M")
    if dt.year == today.year:
        return dt.strftime("%d.%m %H:%M")
    return dt.strftime("%d.%m.%Y %H:%M")


def fmt_delta(seconds) -> str:
    s = int(seconds or 0)
    if s < 0:
        return "мгновенно"
    if s < 60:
        return f"{s} сек"
    if s < 3600:
        return f"{s // 60} мин {s % 60} сек"
    if s < 86400:
        return f"{s // 3600} ч {(s % 3600) // 60} мин"
    return f"{s // 86400} дн {(s % 86400) // 3600} ч"


def esc(text) -> str:
    return html.escape(str(text or ""))


def sender_line(row) -> str:
    """Имя и @username. Числовой id намеренно не показываем — он шумит,
    а найти человека можно через /chats."""
    name = f"<b>{esc(row['sender_name'] or 'без имени')}</b>"
    if row["sender_username"]:
        return f"{name} @{esc(row['sender_username'])}"
    return name


def person_ref(user_id=None, name=None, username=None) -> str:
    """Как назвать человека, чтобы его всегда можно было найти.

    @username есть — имя и @username. Нет — имя ссылкой tg://user?id=… (по ней
    Telegram открывает профиль) и рядом числовой id: ссылка в некоторых
    клиентах не кликается, а по id человек находится всегда. Имени нет —
    ссылкой служит сам «id N».
    """
    name = (name or "").strip() or None
    if username:
        return (f"<b>{esc(name)}</b> @{esc(username)}" if name
                else f"@{esc(username)}")
    try:
        uid = int(user_id) if user_id is not None else None
    except (TypeError, ValueError):
        uid = None
    if uid:
        if name:
            return f'<a href="tg://user?id={uid}">{esc(name)}</a> · id {uid}'
        return f'<a href="tg://user?id={uid}">id {uid}</a>'
    return f"<b>{esc(name or 'неизвестно')}</b>"


def _purge_peer(rows, peer=None) -> dict:
    """Собеседник в личке. Входящих может не быть совсем (снёс всю переписку,
    остались только твои) — тогда его id это id чата, а имя — название чата."""
    info = {"id": None, "name": None, "username": None}
    for r in rows:
        if not r["is_outgoing"]:
            info = {"id": r["sender_id"] or r["chat_id"], "name": r["sender_name"],
                    "username": r["sender_username"]}
            break
    if info["id"] is None and rows:
        info["id"] = rows[0]["chat_id"]
        info["name"] = rows[0]["chat_title"]
    for k, v in (peer or {}).items():
        if v:
            info[k] = v
    return info


def is_group_row(row) -> bool:
    return row["chat_id"] is not None and row["chat_id"] < 0


def head_line(row) -> str:
    """Кто и где. В личке чат — это тот же человек, поэтому его не дублируем."""
    line = sender_line(row)
    if is_group_row(row):
        line += f" · <i>{esc(row['chat_title'] or row['chat_id'])}</i>"
    return line


def _field(row, key):
    """Колонка, которой может не быть: строки из старых баз и тестовые заглушки."""
    try:
        return row[key]
    except (IndexError, KeyError, TypeError):
        return None


MEDIA_FAIL_MARK = "⚠️ не сохранён"
# Виды вложений, у которых есть файл: только для них «нет файла» — это сбой
FILE_KINDS = {"фото", "видео", "голосовое", "видеосообщение", "стикер", "gif",
              "аудио", "файл"}


def media_fail_note(tries: str, reason: str) -> str:
    """media_note для файла, который так и не скачался."""
    return f"{MEDIA_FAIL_MARK} {tries}: {reason}"


def media_fail_line(row):
    """Строка «Файл не сохранился: почему» — или None, если с файлом всё в порядке
    или его и не должно было быть (твоё сообщение, геолокация)."""
    note = row["media_note"] or ""
    if row["media_path"] or not row["media_type"]:
        return None
    if note.startswith(MEDIA_FAIL_MARK):
        why = note[len(MEDIA_FAIL_MARK):].strip()
        return f"⚠️ <b>Файл не сохранился</b> {esc(why)}"
    if note.startswith("не скачалось"):
        return f"⚠️ <b>Файл не сохранился</b>: {esc(note[len('не скачалось'):].lstrip(' :'))}"
    if not note and not row["is_outgoing"] and row["media_type"] in FILE_KINDS:
        return "⚠️ <b>Файл не сохранился</b>: не успел скачаться до удаления"
    return None


def _body(row) -> str:
    """Текст сообщения + пометка о медиа + расшифровка, если она уже есть."""
    chunks = []
    if row["text"]:
        chunks.append(esc(row["text"]))
    if row["media_type"]:
        fail = media_fail_line(row)
        if fail:
            chunks.append(f"<i>[{esc(row['media_type'])}]</i>\n{fail}")
        else:
            note = f" — {esc(row['media_note'])}" if row["media_note"] else ""
            chunks.append(f"<i>[{esc(row['media_type'])}{note}]</i>")
    tr = _field(row, "transcript")
    if tr:
        # Голосовое уже расшифровано вживую — показываем текст прямо в алерте,
        # чтобы не гонять вторым сообщением
        chunks.append(f"🎙 <i>{esc(tr)}</i>")
    if not chunks:
        chunks.append("<i>(пустое сообщение)</i>")
    return "\n".join(chunks)


def chat_line(row) -> str:
    """В группе автор и чат — разные сущности, в личке совпадают."""
    title = row["chat_title"] or row["sender_name"] or row["chat_id"]
    if row["chat_id"] is not None and row["chat_id"] < 0:
        return f"{esc(title)} <i>(групповой чат)</i>"
    return esc(title)


def build_delete_alert(row, deleted_ts: int, peer=None) -> str:
    delta = fmt_delta(deleted_ts - row["sent_ts"])
    edited = ""
    if row["edit_count"]:
        edited = f"\n<i>(правилось {row['edit_count']} раз(а), ниже последняя версия)</i>"
    if row["is_outgoing"]:
        # Собеседник может снести твоё сообщение — в личке это разрешено.
        # Но кто именно нажал «удалить», Telegram не сообщает: в событии нет
        # автора удаления. Поэтому помечаем отдельно и не выдаём догадку за факт.
        if is_group_row(row):
            where = esc(row["chat_title"] or row["chat_id"])
        else:
            # В личке удалить твоё мог только ты сам или собеседник — называем
            # его так, чтобы можно было найти, даже без @username
            p = _purge_peer([row], peer)
            where = person_ref(p["id"], p["name"], p["username"])
        return (
            f"🗑 <b>Удалено твоё сообщение</b> · {where}\n"
            f"<i>{fmt_time(row['sent_ts'])} → удалено через {delta} "
            f"· кто удалил, Telegram не сообщает</i>"
            f"{edited}\n"
            "—\n" + _body(row)
        )

    return (
        f"🗑 {head_line(row)}\n"
        f"<i>{fmt_time(row['sent_ts'])} → удалено через {delta}</i>"
        f"{edited}\n"
        "—\n" + _body(row)
    )


def build_unknown_alert(msg_ids, deleted_ts: int, chat_title=None) -> str:
    ids = ", ".join(str(i) for i in msg_ids[:20])
    more = f" и ещё {len(msg_ids) - 20}" if len(msg_ids) > 20 else ""
    if chat_title:
        where = f"Чат: {esc(chat_title)}\n"
        why = ("<i>Сообщение удалили, но его не было в кэше — скрипт не работал "
               "в момент отправки. Содержимое восстановить нечем.</i>")
    else:
        where = ""
        why = ("<i>Сообщения не было в кэше: скрипт не работал в момент отправки, "
               "либо это была обычная группа, а не личка. Чат и автор неизвестны — "
               "Telegram их в событии удаления не передаёт.</i>")
    return (
        "🗑 <b>Удалено сообщение — содержимое не восстановлено</b>\n"
        f"{where}"
        f"Удалено: {fmt_time(deleted_ts)}\n"
        f"ID: {ids}{more}\n"
        "—\n" + why
    )


def build_purge_alert(chat_title: str, rows, unknown_ids, deleted_ts: int, window_sec: int,
                      peer=None, people=None, chat_id=None) -> str:
    """Сводка о чистке. Человек назван ВСЕГДА: @username, а без него — ссылка
    tg://user и id (см. person_ref).

    peer — {"id","name","username"} собеседника в личке, people — то же по
    sender_id для авторов в группе: main дотягивает недостающее из базы и
    Telegram. Без подсказок всё равно выйдет ссылка по id из самих строк.
    """
    incoming = [r for r in rows if not r["is_outgoing"]]
    outgoing = [r for r in rows if r["is_outgoing"]]
    total = len(rows) + len(unknown_ids)
    people = people or {}

    if chat_id is not None:
        is_group = chat_id < 0
    else:
        is_group = bool(rows) and rows[0]["chat_id"] is not None and rows[0]["chat_id"] < 0

    if is_group:
        where = f"{esc(chat_title)} <i>(групповой)</i>"
    else:
        p = _purge_peer(rows, peer)
        # peer_name без имени отдаёт голое число — именем его не считаем
        title_name = chat_title if str(chat_title or "") != str(p["id"]) else None
        where = (person_ref(p["id"], p["name"] or title_name, p["username"])
                 if p["id"] else esc(chat_title))
    head = (
        "🧹 <b>Похоже на чистку переписки</b>\n"
        f"Чат: {where}\n"
        f"Удалено разом: {total} сообщ. за {window_sec} сек\n"
    )

    if is_group:
        counts, refs = {}, {}
        for r in incoming:
            sid = r["sender_id"]
            key = sid if sid is not None else ("name", r["sender_name"])
            counts[key] = counts.get(key, 0) + 1
            if key not in refs:
                hint = people.get(sid) or {}
                refs[key] = person_ref(sid, hint.get("name") or r["sender_name"],
                                       hint.get("username") or r["sender_username"])
        parts = [f"{refs[k]}: {c}" for k, c in sorted(counts.items(), key=lambda x: -x[1])]
        if outgoing:
            parts.append(f"твоих: {len(outgoing)}")
        head += "Авторы: " + ", ".join(parts) if parts else "Авторы: неизвестно"
    else:
        head += f"Из них его/её: {len(incoming)}, твоих: {len(outgoing)}"
    if unknown_ids:
        head += f", не было в кэше: {len(unknown_ids)}"
    head += f"\nУдалено: {fmt_dt(deleted_ts)}\n—"

    ordered = sorted(rows, key=lambda x: x["sent_ts"])
    shown = ordered[:MAX_PURGE_LINES]
    lines = []
    for r in shown:
        if r["is_outgoing"]:
            who = "ты"
        else:
            if is_group:
                hint = people.get(r["sender_id"]) or {}
                nm = hint.get("name") or r["sender_name"]
                who = esc(nm) if nm else (f"id {r['sender_id']}" if r["sender_id"] else "он/она")
            else:
                who = "он/она"
        text = (r["text"] or "").strip().replace("\n", " ")
        if len(text) > MAX_PREVIEW:
            text = text[:MAX_PREVIEW] + "…"
        media = f" [{r['media_type']}]" if r["media_type"] else ""
        if r["media_type"] and media_fail_line(r):
            media = f" [{r['media_type']} — ⚠️ файл не сохранился]"
        lines.append(f"<b>{fmt_dt(r['sent_ts'])[11:]}</b> {who}:{esc(media)} {esc(text)}")

    hidden = len(ordered) - len(shown)
    if hidden:
        lines.append(f"\n<i>…и ещё {hidden} сообщ. — целиком их видно "
                     f"в <code>/find</code> или <code>/export</code>, "
                     f"здесь не показываю, чтобы не слать простыню.</i>")

    if outgoing and not incoming and not is_group:
        lines.append("\n<i>Удалены только твои сообщения — похоже, собеседник снёс всю переписку целиком.</i>")

    return head + "\n" + "\n".join(lines)


def build_viewonce_alert(row, kind: str, once: bool = True) -> str:
    """Алерт о сохранённом «огоньке» — сразу при получении, до сгорания.

    Только суть: кто, когда, что за файл и подпись. Пояснения вроде «файл
    ниже» и «просмотр один раз» владелец просил убрать — он это и так знает.
    once оставлен ради совместимости вызова, в текст больше не идёт.
    """
    text = (
        f"🔥 <b>Сохранён огонёк</b> · {head_line(row)}\n"
        f"<i>{fmt_time(row['sent_ts'])} · {esc(kind)}</i>"
    )
    if row["text"]:
        text += "\n—\n" + esc(row["text"])
    fail = media_fail_line(row)
    if fail:
        text += "\n" + fail
    return text


def build_read_dump(chat_title, rows, hidden=True) -> str:
    """Рендер истории для /read (скрытый просмотр)."""
    head = (f"👁 <b>{esc(chat_title)}</b> — последние {len(rows)}"
            + (" <i>(без отметки «прочитано»)</i>" if hidden else "") + "\n—")
    lines = []
    for r in rows:
        who = "ты" if r["is_outgoing"] else esc(r["sender_name"] or "он/она")
        t = fmt_time(r["sent_ts"])
        text = (r["text"] or "").strip()
        media = f" [{esc(r['media_type'])}]" if r["media_type"] else ""
        lines.append(f"<b>{t}</b> {who}:{media} {esc(text)}")
    return head + "\n" + "\n".join(lines)


# ---------- правки: пословный дифф ----------

# Токены: слово (с буквами-комбинаторами), пробелы, один знак препинания или
# эмодзи целиком — с ZWJ-склейками, модификаторами тона и вариантами.
# Флаг (пара региональных индикаторов) — тоже одним токеном. Любой символ
# попадает хотя бы в одну ветку, поэтому склейка токенов = исходный текст.
_EMOJI_TAIL = "\ufe0f\u20e3\U0001F3FB-\U0001F3FF"
_EDIT_TOKEN_RE = re.compile(
    r"[\U0001F1E6-\U0001F1FF]{2}"
    r"|[\w\u0300-\u036f]+"
    r"|\s+"
    rf"|[^\w\s](?:[{_EMOJI_TAIL}]|\u200d[^\w\s])*"
)
EDIT_CONTEXT_WORDS = 8     # слов контекста вокруг правки в длинном тексте
EDIT_COMPACT_FROM = 300    # с какой длины показываем только места правок
EDIT_REWRITE_BELOW = 0.4   # меньше общего — считаем, что текст переписан целиком


def _edit_tokens(text: str) -> list:
    return _EDIT_TOKEN_RE.findall(text or "")


def _is_word(tok: str) -> bool:
    return bool(tok) and (tok[0].isalnum() or tok[0] == "_")


def _visible_len(html_text: str) -> int:
    """Сколько насчитает Telegram: текст без тегов, сущности раскрыты, UTF-16."""
    plain = html.unescape(re.sub(r"<[^>]+>", "", html_text))
    return len(plain.encode("utf-16-le")) // 2


def _fits(html_text: str) -> bool:
    return len(html_text) <= MAX_TEXT and _visible_len(html_text) <= MAX_TEXT


def _cut(raw: str, limit) -> str:
    """Урезать СЫРОЙ текст до экранирования: так не разорвать ни &amp;, ни тег."""
    if limit is None or len(raw) <= limit:
        return raw
    return raw[:max(1, limit)].rstrip() + "…"


def _mark(tag: str, toks, limit=None) -> str:
    """Изменённый кусок в теге. Пробелы по краям — снаружи тега, и каждая строка
    закрывает свой тег сама: split_text режет по переводу строки, и тег,
    протянутый через него, сломал бы разметку обоих кусков."""
    i, j = 0, len(toks)
    while i < j and toks[i].isspace():
        i += 1
    while j > i and toks[j - 1].isspace():
        j -= 1
    core = _cut("".join(toks[i:j]), limit)
    inner = "\n".join(f"<{tag}>{esc(line)}</{tag}>" if line.strip() else esc(line)
                      for line in core.split("\n"))
    return esc("".join(toks[:i])) + inner + esc("".join(toks[j:]))


def _ctx(toks, first: bool, last: bool, words) -> str:
    """Неизменённый кусок между правками. words=None — показать целиком."""
    if words is None:
        return esc("".join(toks))
    idx = [k for k, t in enumerate(toks) if _is_word(t)]
    if first and last:
        return esc("".join(toks))
    if first:
        if len(idx) <= words:
            return esc("".join(toks))
        return "…" + esc("".join(toks[idx[-words]:]))
    if last:
        if len(idx) <= words:
            return esc("".join(toks))
        return esc("".join(toks[:idx[words - 1] + 1])) + "…"
    if len(idx) <= 2 * words:
        return esc("".join(toks))
    return (esc("".join(toks[:idx[words - 1] + 1])) + " … "
            + esc("".join(toks[idx[-words]:])))


def _diff_sides(a, b, ops, words=None, limit=None):
    old, new = [], []
    for k, (tag, i1, i2, j1, j2) in enumerate(ops):
        if tag == "equal":
            piece = _ctx(a[i1:i2], k == 0, k == len(ops) - 1, words)
            old.append(piece)
            new.append(piece)
            continue
        if i2 > i1:
            old.append(_mark("s", a[i1:i2], limit))
        if j2 > j1:
            new.append(_mark("b", b[j1:j2], limit))
    return "".join(old), "".join(new)


def _similarity(a, b, ops) -> float:
    """Доля общего по непробельным символам."""
    def nonspace(toks):
        return sum(len(t) for t in toks if not t.isspace())
    same = sum(nonspace(a[i1:i2]) for tag, i1, i2, _, _ in ops if tag == "equal")
    total = max(nonspace(a), nonspace(b)) or 1
    return same / total


def _rewrite_sides(old_text: str, new_text: str, limit=None):
    """Старый стиль: старое зачёркнуто целиком, новое как есть."""
    o = _mark("s", _edit_tokens(_cut(old_text, limit))) if old_text else "<s>(пусто)</s>"
    n = esc(_cut(new_text, limit)) if new_text else "<i>(пусто)</i>"
    return o, n


# Выше этого произведения длин difflib без autojunk уходит в квадрат на
# повторяющихся токенах (пробелы, «и», «а») — секунды и минуты на одну правку
EDIT_EXACT_MAX = 1_500_000


def _opcodes(a, b) -> list:
    """Опкоды difflib, но быстро: общее начало и конец отрезаем сразу (правка
    почти всегда локальная), а огромную середину сравниваем с autojunk."""
    import difflib
    p, n = 0, min(len(a), len(b))
    while p < n and a[p] == b[p]:
        p += 1
    q = 0
    while q < n - p and a[len(a) - 1 - q] == b[len(b) - 1 - q]:
        q += 1
    ma, mb = a[p:len(a) - q], b[p:len(b) - q]
    sm = difflib.SequenceMatcher(None, ma, mb,
                                 autojunk=len(ma) * len(mb) > EDIT_EXACT_MAX)
    ops = [] if not p else [("equal", 0, p, 0, p)]
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        ops.append((tag, i1 + p, i2 + p, j1 + p, j2 + p))
    if q:
        ops.append(("equal", len(a) - q, len(a), len(b) - q, len(b)))
    # Склеить соседние equal (на стыке среза и середины)
    out = []
    for op in ops:
        if out and op[0] == "equal" and out[-1][0] == "equal":
            out[-1] = ("equal", out[-1][1], op[2], out[-1][3], op[4])
        else:
            out.append(op)
    return out


def edit_diff(old_text: str, new_text: str, budget: int = MAX_TEXT):
    """(старое, новое) в HTML: в старом зачёркнуто только убранное и
    изменённое, в новом выделено только новое. Длинный текст — лишь места
    правок с контекстом, «…» между ними. Текст, переписанный почти целиком,
    показываем как раньше. Результат всегда влезает в budget."""
    old_text, new_text = old_text or "", new_text or ""
    a, b = _edit_tokens(old_text), _edit_tokens(new_text)
    ops = _opcodes(a, b) if old_text and new_text else []

    def ok(o, n):
        return _fits_budget(o + "\n↓\n" + n, budget)

    if ops and _similarity(a, b, ops) >= EDIT_REWRITE_BELOW:
        long_text = max(len(old_text), len(new_text)) > EDIT_COMPACT_FROM
        words = EDIT_CONTEXT_WORDS if long_text else None
        tries = [(words, None)]
        tries += [(EDIT_CONTEXT_WORDS, None), (3, None), (3, 300), (2, 60), (1, 20)]
        for w, lim in tries:
            o, n = _diff_sides(a, b, ops, w, lim)
            if ok(o, n):
                return o, n
    # Переписано целиком (или правок столько, что по месту не уложить)
    limit = None
    while True:
        o, n = _rewrite_sides(old_text, new_text, limit)
        if ok(o, n) or (limit is not None and limit <= 20):
            return o, n
        limit = (max(len(old_text), len(new_text)) if limit is None else limit) * 3 // 4


def _fits_budget(html_text: str, budget: int) -> bool:
    return len(html_text) <= budget and _visible_len(html_text) <= budget


def build_edit_alert(row, old_text: str, new_text: str, edited_ts: int) -> str:
    head = (
        f"✏️ {head_line(row)}\n"
        f"<i>{fmt_time(row['sent_ts'])} → правка через "
        f"{fmt_delta(edited_ts - row['sent_ts'])}</i>\n"
        "—\n"
    )
    old, new = edit_diff(old_text, new_text, budget=MAX_TEXT - len(head) - 10)
    return head + old + "\n↓\n" + new


def build_block_alert(user_id: int, name: str, lost_photo: bool, lost_status: bool) -> str:
    signs = []
    if lost_photo:
        signs.append("пропало фото профиля")
    if lost_status:
        signs.append("пропал last seen")
    return (
        "⛔️ <b>Предположительно заблокировал(а)</b>\n"
        f"Кто: {esc(name)} (id {user_id})\n"
        f"Признаки: {esc(', '.join(signs))}\n"
        "—\n"
        "<i>Протокольного события «тебя заблокировали» в Telegram нет — это косвенная "
        "догадка. Тот же эффект даёт ручное закручивание приватности. "
        "Проверять фактом, а не верить на слово.</i>"
    )


def build_transcript_alert(row, text: str, kind: str = "голосовое") -> str:
    """Расшифровка голосового/кружочка — отдельным сообщением, следом за алертом."""
    return (
        f"🎙 <b>Расшифровка</b> · {head_line(row)}\n"
        f"<i>{fmt_time(row['sent_ts'])} · {esc(kind)}</i>\n"
        "—\n" + esc(text)
    )


def build_profile_alert(name: str, user_id: int, changes: list) -> str:
    """Смена @username / имени / фото или удаление аккаунта."""
    lines = []
    for what, was, now in changes:
        if was and now:
            lines.append(f"{esc(what)}: <s>{esc(was)}</s> → <b>{esc(now)}</b>")
        elif now:
            lines.append(f"{esc(what)}: появилось <b>{esc(now)}</b>")
        else:
            lines.append(f"{esc(what)}: <s>{esc(was)}</s> → убрано")
    return (
        f"👤 <b>Профиль изменился</b> · {esc(name)}\n"
        f"<i>id {user_id}</i>\n"
        "—\n" + "\n".join(lines)
    )


def build_deleted_account_alert(name: str, user_id: int) -> str:
    return (
        f"💀 <b>Аккаунт удалён</b> · {esc(name)}\n"
        f"<i>id {user_id}</i>\n"
        "—\n"
        "<i>Человек удалил аккаунт из Telegram. Переписка с ним в архиве осталась.</i>"
    )


def build_online_alert(name: str, online: bool, session_sec: int = 0) -> str:
    """Алерт мгновенный, поэтому время не пишем — оно и так на самом сообщении."""
    if online:
        return f"🟢 <b>{esc(name)}</b> зашёл(ла) в сеть"
    tail = f" · пробыл(а) в сети {fmt_delta(session_sec)}" if session_sec > 0 else ""
    return f"⚪️ <b>{esc(name)}</b> вышел(ла){tail}"


def build_chat_stats(title: str, st: dict) -> str:
    """Сводка по одному чату: сколько, когда, чего."""
    if not st["total"]:
        return f"📊 <b>{esc(title)}</b>\nВ архиве по этому чату пусто."
    days = max(1, (st["last_ts"] - st["first_ts"]) // 86400)
    hours = ", ".join(f"{h:02d}:00 ({n})" for h, n in st["hours"]) or "—"
    types = ", ".join(f"{esc(t)} {n}" for t, n in st["types"]) or "нет вложений"
    return (
        f"📊 <b>{esc(title)}</b>\n"
        f"Всего сообщений: <b>{st['total']}</b> "
        f"(твоих {st['mine']}, его/её {st['theirs']})\n"
        f"Период: {fmt_time(st['first_ts'])} — {fmt_time(st['last_ts'])} "
        f"(~{days} дн, {st['total'] // days} в день)\n"
        f"Написано символов: {st['chars']}\n"
        f"Вложений: {st['media']} · {types}\n"
        f"Правилось сообщений: {st['edited']}\n"
        f"Активнее всего: {hours}"
    )


# ---------- отправка ----------


async def _call(coro_factory, attempts: int = 3):
    """Уважаем FloodWait: не глушим, а ждём столько, сколько сказал Telegram."""
    for i in range(attempts):
        try:
            return await coro_factory()
        except FloodWaitError as e:
            wait = e.seconds + 2
            log.warning("FloodWait %s сек — жду", e.seconds)
            await asyncio.sleep(wait)
        except (OSError, ConnectionError) as e:
            log.warning("Сеть отвалилась (%s), попытка %s/%s", e, i + 1, attempts)
            await asyncio.sleep(RETRY_SLEEP * (i + 1))
    log.error("Не удалось отправить после %s попыток", attempts)
    return None


def split_text(text: str):
    """Режем по границам строк. Индексами, а не откусыванием головы: старый
    вариант делал text = text[cut:] на каждом шаге, то есть копировал весь
    остаток — на сотне килобайт это квадрат."""
    start, n = 0, len(text)
    while n - start > MAX_TEXT:
        cut = text.rfind("\n", start, start + MAX_TEXT)
        if cut - start < MAX_TEXT // 2:      # -1 (не нашли) сюда же попадает
            cut = start + MAX_TEXT
        yield text[start:cut]
        start = cut
        while start < n and text[start] == "\n":
            start += 1
    yield text[start:]


async def video_kwargs(path: str) -> dict:
    """Атрибуты для видеофайла: длительность, размеры, поддержка стриминга.

    Без них Telegram считает ролик обычным документом — превью 1x1, смотреть
    можно только скачав целиком. Особенно заметно на историях: они все видео.
    """
    from util import video_meta
    meta = await asyncio.to_thread(video_meta, path)
    if not meta:
        return {}
    from telethon.tl.types import DocumentAttributeVideo
    dur, w, h = meta
    return {
        "supports_streaming": True,
        "attributes": [DocumentAttributeVideo(duration=dur, w=w, h=h,
                                              supports_streaming=True)],
    }


_USER_LINK_RE = re.compile(r'<a href="tg://user\?id=\d+">(.*?)</a>', re.S)


def strip_user_links(text: str) -> str:
    """Ссылки tg://user → просто текст. id рядом остаётся, найти человека можно."""
    return _USER_LINK_RE.sub(r"\1", text or "")


async def send(client, text: str, file: str = None, chat=None):
    """Отправить алерт; ссылки на людей не должны его ронять.

    Telethon превращает <a href="tg://user?id=…"> в упоминание и для этого
    ищет пользователя. Не нашёл в кэше — ссылку молча выкидывает, но бот ещё
    и спрашивает Telegram (users.getUsers), и ошибка этого запроса уронила бы
    отправку целиком. Тогда шлём тот же текст без ссылок: имя и id остаются.
    """
    try:
        return await _send(client, text, file, chat)
    except RPCError as e:
        if "tg://user?id=" not in (text or ""):
            raise
        log.warning("Не ушло со ссылкой на человека (%s) — шлю без ссылки", e)
        return await _send(client, strip_user_links(text), file, chat)


async def _send(client, text: str, file: str = None, chat=None):
    """Отправить алерт. Файл идёт подписью, если текст влезает, иначе отдельно.

    Если файл был запрошен, результат отражает судьбу ИМЕННО файла: вызывающий
    по нему решает, стирать ли локальную копию. Вернуть успех из-за доехавшего
    текста нельзя — иначе медиа удалилось бы с диска, так и не доехав.
    """
    chat = config.LOG_CHAT_ID if chat is None else chat
    chunks = list(split_text(text))
    # Страховка от пачки: сколько бы текста ни собралось выше, отправка не
    # должна превращаться в очередь из десятков сообщений подряд — за такое
    # Telegram выдаёт FloodWait, а на юзерботе это выглядит как спам.
    if len(chunks) > MAX_CHUNKS:
        dropped = len(chunks) - (MAX_CHUNKS - 1)
        chunks = chunks[:MAX_CHUNKS - 1]
        chunks.append(f"<i>…и ещё {dropped} фрагм. не отправлено: слишком "
                      f"длинно для одного алерта. Целиком — в /find или /export.</i>")
        log.warning("Алерт урезан: не отправлено %s фрагментов", dropped)
    want_file = bool(file and os.path.exists(file))
    # Для видео добавляем размеры и длительность — иначе Telegram отдаёт его
    # неиграбельным документом с превью в один пиксель
    vk = await video_kwargs(file) if want_file and isinstance(file, str) else {}

    # Короткий текст с вложением — одной отправкой, подписью к файлу
    if want_file and len(chunks) == 1 and len(text) <= MAX_CAPTION:
        sent = await _call(lambda: client.send_file(
            chat, file, caption=text, parse_mode="html", **vk))
        if sent:
            return sent
        log.warning("Не ушло вложением, пробую текстом и файлом раздельно")

    text_sent = None
    for chunk in chunks:
        text_sent = await _call(lambda c=chunk: client.send_message(
            chat, c, parse_mode="html", link_preview=False))

    if not want_file:
        return text_sent

    return await _call(lambda: client.send_file(chat, file, **vk))
