"""Экспорт переписки из кэша в один HTML-файл.

Файл самодостаточный: открывается двойным кликом, интернет не нужен, картинки
вшиты прямо внутрь. Расшифровки голосовых, если они есть, попадают туда же.

Работает в отдельном потоке и на СВОЁМ подключении к базе: делить sqlite-соединение
с основным циклом нельзя — оно не рассчитано на одновременное использование.
"""

import base64
import html
import mimetypes
import os
import sqlite3
from datetime import datetime, timezone

import alerts
import config

# Картинки вшиваем прямо в файл, но осторожно: чат на 50 тысяч сообщений с
# гигабайтом фото иначе превратился бы в неоткрываемый HTML.
EMBED_MAX_FILE = 500 * 1024
EMBED_MAX_TOTAL = 30 * 1024 * 1024

CSS = """
:root { color-scheme: light dark; }
body { margin:0; padding:24px; font:15px/1.5 -apple-system,Segoe UI,Roboto,sans-serif;
       background:#f4f5f7; color:#111; }
@media (prefers-color-scheme: dark) { body { background:#17181c; color:#e8e8ea; } }
.wrap { max-width:760px; margin:0 auto; }
h1 { font-size:20px; margin:0 0 4px; }
.meta { opacity:.6; font-size:13px; margin-bottom:20px; }
.day { text-align:center; margin:22px 0 10px; font-size:12px; opacity:.55; }
.m { max-width:78%; margin:6px 0; padding:8px 12px; border-radius:14px;
     background:#fff; box-shadow:0 1px 2px rgba(0,0,0,.08); word-wrap:break-word; }
@media (prefers-color-scheme: dark) { .m { background:#25262b; box-shadow:none; } }
.m.out { margin-left:auto; background:#d7f0d0; }
@media (prefers-color-scheme: dark) { .m.out { background:#2b4a33; } }
.who { font-size:12px; font-weight:600; opacity:.75; margin-bottom:2px; }
.t { font-size:11px; opacity:.5; float:right; margin:3px 0 0 10px; }
.tag { font-size:12px; opacity:.7; font-style:italic; }
.tr { margin-top:5px; padding-left:8px; border-left:2px solid currentColor;
      opacity:.8; font-size:14px; }
img { max-width:100%; border-radius:9px; margin-top:5px; display:block; }
"""


def _local(ts) -> datetime:
    """Метка времени в часовом поясе из настроек, а не в поясе сервера.

    Сервер обычно живёт в UTC, и без этого выгруженная переписка выезжала
    со временем, не совпадающим ни с уведомлениями бота, ни с самим Telegram.
    """
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return dt.astimezone(alerts.TZ) if alerts.TZ else dt.astimezone()


def _now_local() -> datetime:
    return _local(datetime.now(tz=timezone.utc).timestamp())


def _esc(s) -> str:
    return html.escape(str(s or ""))


def _embed(path: str, budget: list):
    """Картинку — в data:URI. Возвращает готовый тег или пустую строку."""
    if not path or budget[0] <= 0:
        return ""
    try:
        size = os.path.getsize(path)
        if size > EMBED_MAX_FILE or size > budget[0]:
            return ""
        mime = mimetypes.guess_type(path)[0] or ""
        if not mime.startswith("image/"):
            return ""
        with open(path, "rb") as fh:
            data = base64.b64encode(fh.read()).decode("ascii")
        budget[0] -= size
        return f'<img src="data:{mime};base64,{data}">'
    except OSError:
        return ""


def write_html(chat_id: int, title: str, out_path: str, limit: int = 0) -> dict:
    """Выгрузить чат в HTML. Возвращает {'count', 'images', 'bytes'}."""
    # 0 в настройках означает «без потолка». Раньше он доезжал до SQL как
    # LIMIT 0, и выгрузка приходила пустой с бодрым «в архиве пусто».
    limit = limit or config.EXPORT_MAX_MESSAGES
    if limit <= 0:
        limit = -1          # у SQLite отрицательный LIMIT = без ограничения
    con = sqlite3.connect(f"file:{config.DB_PATH}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    budget = [EMBED_MAX_TOTAL]
    count = images = 0
    last_day = None

    try:
        total = con.execute(
            "SELECT COUNT(*) FROM messages WHERE chat_id=?", (chat_id,)
        ).fetchone()[0]
        # Берём ПОСЛЕДНИЕ limit сообщений, а не первые: наивный
        # "ORDER BY sent_ts LIMIT n" резал бы хвост, то есть выбрасывал всё
        # свежее и молча отдавал переписку многолетней давности.
        cur = con.execute(
            "SELECT * FROM (SELECT * FROM messages WHERE chat_id=? "
            "               ORDER BY sent_ts DESC, msg_id DESC LIMIT ?) "
            "ORDER BY sent_ts, msg_id",
            (chat_id, limit),
        )
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(f"<!doctype html><meta charset=utf-8>"
                     f"<title>{_esc(title)}</title><style>{CSS}</style>"
                     f'<div class=wrap><h1>{_esc(title)}</h1>'
                     f'<div class=meta>Выгружено из локального архива '
                     f'{_now_local().strftime("%d.%m.%Y %H:%M")}</div>')
            for row in cur:
                dt = _local(row["sent_ts"])
                day = dt.strftime("%d.%m.%Y")
                if day != last_day:
                    fh.write(f'<div class=day>{_esc(day)}</div>')
                    last_day = day

                cls = "m out" if row["is_outgoing"] else "m"
                who = "Ты" if row["is_outgoing"] else (row["sender_name"] or "он/она")
                parts = [f'<div class="{cls}"><span class=t>{dt.strftime("%H:%M")}</span>'
                         f'<div class=who>{_esc(who)}</div>']
                if row["text"]:
                    parts.append(_esc(row["text"]).replace("\n", "<br>"))
                if row["media_type"]:
                    note = f" — {_esc(row['media_note'])}" if row["media_note"] else ""
                    parts.append(f'<div class=tag>[{_esc(row["media_type"])}{note}]</div>')
                    tag = _embed(row["media_path"], budget)
                    if tag:
                        parts.append(tag)
                        images += 1
                if row["transcript"]:
                    parts.append(f'<div class=tr>🎙 {_esc(row["transcript"])}</div>')
                parts.append("</div>")
                fh.write("".join(parts))
                count += 1
            tail = (f'Показаны последние {count} из {total} — '
                    f'потолок EXPORT_MAX_MESSAGES'
                    if total > count else f'Всего сообщений: {count}')
            fh.write(f'<div class=meta>{_esc(tail)}</div></div>')
    finally:
        con.close()

    return {"count": count, "total": total, "truncated": total > count,
            "images": images,
            "bytes": os.path.getsize(out_path) if os.path.exists(out_path) else 0}
