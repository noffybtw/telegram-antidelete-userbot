"""SQLite-кэш сообщений.

Telegram в событии удаления присылает только ID сообщений — ни текста, ни чата.
Поэтому каждое входящее сообщение кладётся сюда заранее, а при удалении
поднимается по ID.

Про ID: в личных чатах и старых группах message_id берутся из одной общей
последовательности твоего аккаунта, поэтому поиск по одному только msg_id
однозначен — этого достаточно, чтобы восстановить чат, который Telegram
в событии удаления не сообщает.
"""

import os
import sqlite3
import time
from pathlib import Path

import config

IGNORED_RETENTION_DAYS = 180
# Срезание истории по сроку идёт порциями: одним DELETE на миллионе строк это
# секунды удержания записи прямо в цикле обработки событий.
CLEANUP_CHUNK = 5000
CLEANUP_MAX_ROWS = 100_000   # потолок за один прогон, остальное — в следующий

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    chat_id         INTEGER NOT NULL,
    msg_id          INTEGER NOT NULL,
    sender_id       INTEGER,
    sender_name     TEXT,
    sender_username TEXT,
    chat_title      TEXT,
    text            TEXT,
    media_path      TEXT,
    media_type      TEXT,
    media_note      TEXT,
    is_outgoing     INTEGER NOT NULL DEFAULT 0,
    sent_ts         INTEGER NOT NULL,
    edit_count      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chat_id, msg_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_msg_id  ON messages(msg_id);
CREATE INDEX IF NOT EXISTS idx_messages_sent_ts ON messages(sent_ts);
CREATE INDEX IF NOT EXISTS idx_messages_media   ON messages(media_path);
-- (chat_id, sent_ts): peer_name/active_peers сортировали сотни тысяч строк чата
CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages(chat_id, sent_ts);
-- частичный по медиа: cleanup находит десятки файлов, не читая всю таблицу
CREATE INDEX IF NOT EXISTS idx_messages_media_ts ON messages(sent_ts) WHERE media_path IS NOT NULL;

CREATE TABLE IF NOT EXISTS edits (
    chat_id  INTEGER NOT NULL,
    msg_id   INTEGER NOT NULL,
    old_text TEXT,
    new_text TEXT,
    ts       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_edits_ts ON edits(ts);

CREATE TABLE IF NOT EXISTS peer_state (
    user_id     INTEGER PRIMARY KEY,
    had_photo   INTEGER NOT NULL DEFAULT 0,
    had_status  INTEGER NOT NULL DEFAULT 0,
    strikes     INTEGER NOT NULL DEFAULT 0,
    alerted     INTEGER NOT NULL DEFAULT 0,
    last_check  INTEGER
);

CREATE TABLE IF NOT EXISTS ignored_ids (
    msg_id  INTEGER PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    ts      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ignored_ts ON ignored_ids(ts);

CREATE TABLE IF NOT EXISTS chats (
    chat_id    INTEGER PRIMARY KEY,
    kind       TEXT,
    title      TEXT,
    members    INTEGER,
    monitored  INTEGER NOT NULL DEFAULT 1,
    checked_ts INTEGER
);

CREATE TABLE IF NOT EXISTS digest_queue (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    kind    TEXT NOT NULL,
    chat_id INTEGER,
    payload TEXT NOT NULL,
    file    TEXT,
    ts      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

-- Подписки на конкретного человека: одна таблица на все функции сразу.
-- feature: voice (расшифровывать его ГС всегда), profile (следить за сменой
-- профиля), online (уведомлять о заходах). Добавить фичу = добавить значение,
-- а не таблицу.
CREATE TABLE IF NOT EXISTS subs (
    feature  TEXT NOT NULL,
    peer_id  INTEGER NOT NULL,
    title    TEXT,
    added_ts INTEGER NOT NULL,
    PRIMARY KEY (feature, peer_id)
);

-- Снимок профиля: с чем сравнивать, чтобы заметить смену @username/имени/фото
CREATE TABLE IF NOT EXISTS profile_state (
    user_id    INTEGER PRIMARY KEY,
    username   TEXT,
    name       TEXT,
    photo_id   INTEGER,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    checked_ts INTEGER
);

-- Уже скачанное по ссылке. Храним не файл, а идентификатор, который Telegram
-- выдал при отправке: по нему то же самое отправляется повторно мгновенно,
-- без скачивания и без заливки. Именно так «мгновенные» боты и работают.
CREATE TABLE IF NOT EXISTS dl_cache (
    key      TEXT PRIMARY KEY,
    file_id  TEXT NOT NULL,
    title    TEXT,
    size     INTEGER,
    ts       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dl_cache_ts ON dl_cache(ts);

-- Онлайн: since_ts — когда зашёл (чтобы посчитать, сколько пробыл),
-- notified_ts — когда последний раз уведомляли (антиспам при мигании статуса)
CREATE TABLE IF NOT EXISTS online_state (
    user_id     INTEGER PRIMARY KEY,
    online      INTEGER NOT NULL DEFAULT 0,
    since_ts    INTEGER,
    notified_ts INTEGER
);
"""


class Cache:
    def __init__(self, path: str = None):
        self.db = sqlite3.connect(path or config.DB_PATH, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        # REPLACE не срабатывает триггер удаления FTS без этого — иначе сироты в индексе
        self.db.execute("PRAGMA recursive_triggers=ON")
        self.db.execute("PRAGMA cache_size=-32000")      # 32 МБ страничного кеша
        self.db.execute("PRAGMA mmap_size=268435456")    # 256 МБ mmap
        # Экспорт и статистика открывают СВОИ подключения. По умолчанию busy_timeout
        # нулевой — то есть любое пересечение по блокировке сразу дало бы
        # «database is locked» прямо в обработчике события, а не подождало бы.
        self.db.execute("PRAGMA busy_timeout=10000")
        self.db.executescript(SCHEMA)
        self._migrate()
        self._ensure_fts()
        self.db.commit()
        self._maybe_analyze()
        # Счётчик занятого медиа: None = ещё ни разу не считали
        self._media_bytes = None
        self._media_bytes_ts = 0.0

    def _maybe_analyze(self) -> None:
        """Разовый ANALYZE — без статистики планировщик выбирает индексы вслепую."""
        if self.get_meta("analyzed_v1") == "1":
            return
        try:
            self.db.execute("ANALYZE")
            self.set_meta("analyzed_v1", "1")
        except sqlite3.Error:
            pass

    def _migrate(self) -> None:
        """Мелкие ALTER-ы для баз, созданных ранними версиями."""
        cols = {r["name"] for r in self.db.execute("PRAGMA table_info(chats)")}
        if "override" not in cols:
            # NULL = решать автоматически, 1 = следить принудительно, 0 = не следить
            self.db.execute("ALTER TABLE chats ADD COLUMN override INTEGER")
        mcols = {r["name"] for r in self.db.execute("PRAGMA table_info(messages)")}
        if "ttl_captured" not in mcols:
            # 1 = это был «огонёк», уже сохранён сразу при получении; его
            # последующее самосгорание не нужно слать вторым алертом
            self.db.execute("ALTER TABLE messages ADD COLUMN ttl_captured INTEGER NOT NULL DEFAULT 0")
        if "transcript" not in mcols:
            # Расшифровка голосового/кружочка. Храним, чтобы не считать заново:
            # при удалении алерт переиспользует уже готовый текст.
            self.db.execute("ALTER TABLE messages ADD COLUMN transcript TEXT")

    def _ensure_fts(self) -> None:
        """Полнотекстовый индекс по тексту сообщений (для /find).

        External-content: сам текст не дублируется, FTS ссылается на messages
        по rowid, а триггеры держат индекс в синхроне. Для уже накопленных строк
        индекс наполняется отдельно — rebuild_fts().
        """
        self.db.executescript("""
            CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
                text, content='messages', content_rowid='rowid', tokenize='unicode61'
            );
            CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
                INSERT INTO messages_fts(rowid, text) VALUES (new.rowid, new.text);
            END;
            CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
                INSERT INTO messages_fts(messages_fts, rowid, text)
                    VALUES('delete', old.rowid, old.text);
            END;
            CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
                INSERT INTO messages_fts(messages_fts, rowid, text)
                    VALUES('delete', old.rowid, old.text);
                INSERT INTO messages_fts(rowid, text) VALUES (new.rowid, new.text);
            END;
        """)

    def rebuild_fts(self) -> int:
        """Наполнить полнотекстовый индекс из уже существующих строк."""
        self.db.execute("INSERT INTO messages_fts(messages_fts) VALUES('rebuild')")
        self.db.commit()
        return self.db.execute("SELECT COUNT(*) FROM messages_fts").fetchone()[0]

    @staticmethod
    def _fts_query(query: str) -> str:
        """Пользовательский запрос -> безопасный префиксный FTS-запрос.

        Каждое слово ищется как префикс («сейф» находит «сейфа», «сейфов») —
        для языков со склонениями это важно. Кавычки внутри экранируются.
        """
        terms = [t.replace('"', '""') for t in query.split() if t.strip()]
        return " ".join(f'"{t}"*' for t in terms)

    def search(self, query: str, limit: int = 15) -> list:
        fts = self._fts_query(query)
        if not fts:
            return []
        return self.db.execute(
            # Маркеры подсветки — управляющие символы \x01/\x02, которых в тексте
            # быть не может. Раньше стояли «», и реальные ёлочки в сообщении ломали
            # HTML-разметку ответа (в базе 800+ таких сообщений).
            """SELECT m.*, snippet(messages_fts, 0, char(1), char(2), '…', 12) AS snip
               FROM messages_fts f JOIN messages m ON m.rowid = f.rowid
               WHERE messages_fts MATCH ? ORDER BY rank LIMIT ?""",
            (fts, limit),
        ).fetchall()

    # ---------- сообщения ----------

    def add_message(self, **kw) -> None:
        self.db.execute(
            """INSERT OR REPLACE INTO messages
               (chat_id, msg_id, sender_id, sender_name, sender_username, chat_title,
                text, media_path, media_type, media_note, is_outgoing, sent_ts)
               VALUES (:chat_id, :msg_id, :sender_id, :sender_name, :sender_username,
                       :chat_title, :text, :media_path, :media_type, :media_note,
                       :is_outgoing, :sent_ts)""",
            {
                "media_path": None, "media_type": None, "media_note": None,
                "is_outgoing": 0, "text": None, "sender_id": None,
                "sender_name": None, "sender_username": None, "chat_title": None,
                **kw,
            },
        )
        self.db.commit()

    def add_history_message(self, **kw) -> bool:
        """Вставка из бэкфилла: НЕ трогает уже существующую строку.

        Живая запись могла успеть обзавестись скачанным медиа — перезаписывать её
        историческим текстом нельзя, потеряется media_path.
        """
        cur = self.db.execute(
            """INSERT OR IGNORE INTO messages
               (chat_id, msg_id, sender_id, sender_name, sender_username, chat_title,
                text, media_path, media_type, media_note, is_outgoing, sent_ts)
               VALUES (:chat_id, :msg_id, :sender_id, :sender_name, :sender_username,
                       :chat_title, :text, NULL, :media_type, :media_note,
                       :is_outgoing, :sent_ts)""",
            {"media_type": None, "media_note": None, "is_outgoing": 0, "text": None,
             "sender_id": None, "sender_name": None, "sender_username": None,
             "chat_title": None, **kw},
        )
        return cur.rowcount > 0

    def get(self, msg_id: int, chat_id: int = None):
        """Найти сообщение.

        Если чат известен — ищем только точным совпадением. Отката на поиск по
        одному msg_id тут быть не должно: в супергруппах ID нумеруются внутри
        канала и легко совпадут с чужим сообщением из лички.
        """
        if chat_id is not None:
            return self.db.execute(
                "SELECT * FROM messages WHERE chat_id=? AND msg_id=?", (chat_id, msg_id)
            ).fetchone()
        # Событие updateDeleteMessages (chat_id=None) приходит только для личек и
        # старых групп — супергруппы идут отдельным событием со своим chat_id.
        # Их per-channel id мелкие и могут случайно совпасть, поэтому отсекаем.
        rows = self.db.execute(
            "SELECT * FROM messages WHERE msg_id=? AND chat_id > ? ORDER BY sent_ts DESC",
            (msg_id, config.SUPERGROUP_ID_THRESHOLD),
        ).fetchall()
        return rows[0] if rows else None

    def update_text(self, chat_id: int, msg_id: int, new_text: str) -> None:
        self.db.execute(
            "UPDATE messages SET text=?, edit_count=edit_count+1 WHERE chat_id=? AND msg_id=?",
            (new_text, chat_id, msg_id),
        )
        self.db.commit()

    def add_edit(self, chat_id: int, msg_id: int, old_text: str, new_text: str) -> None:
        self.db.execute(
            "INSERT INTO edits (chat_id, msg_id, old_text, new_text, ts) VALUES (?,?,?,?,?)",
            (chat_id, msg_id, old_text, new_text, int(time.time())),
        )
        self.db.commit()

    # ---------- медиа ----------

    def mark_ttl_captured(self, chat_id: int, msg_id: int) -> None:
        self.db.execute(
            "UPDATE messages SET ttl_captured=1 WHERE chat_id=? AND msg_id=?",
            (chat_id, msg_id),
        )
        self.db.commit()

    def set_media_path(self, chat_id: int, msg_id: int, path: str = None, note: str = None) -> None:
        """Дозапись после скачивания: строка уже вставлена, файл подъехал позже."""
        self.db.execute(
            "UPDATE messages SET media_path=?, media_note=? WHERE chat_id=? AND msg_id=?",
            (path, note, chat_id, msg_id),
        )
        self.db.commit()
        if path:
            try:
                self.note_media_bytes(os.path.getsize(path))
            except OSError:
                pass

    def drop_media_file(self, chat_id: int, msg_id: int, note: str = "отправлено в лог-чат") -> None:
        """Файл ушёл в Telegram — локальную копию больше не держим."""
        row = self.db.execute(
            "SELECT media_path FROM messages WHERE chat_id=? AND msg_id=?", (chat_id, msg_id)
        ).fetchone()
        if row and row["media_path"]:
            try:
                self.note_media_bytes(-os.path.getsize(row["media_path"]))
            except OSError:
                pass
            _unlink(row["media_path"])
        self.db.execute(
            "UPDATE messages SET media_path=NULL, media_note=? WHERE chat_id=? AND msg_id=?",
            (note, chat_id, msg_id),
        )
        self.db.commit()

    def media_total_bytes(self, max_age_sec: int = 0) -> int:
        """Сколько занимают сохранённые файлы.

        Точный ответ стоит по одному stat() на файл, а спрашивать надо перед
        КАЖДЫМ скачиванием. Поэтому держим счётчик: пересчитываем целиком не
        чаще, чем раз в max_age_sec, а между пересчётами правим на лету.
        """
        now = time.time()
        if (max_age_sec and self._media_bytes is not None
                and now - self._media_bytes_ts < max_age_sec):
            return self._media_bytes
        total = 0
        for row in self.db.execute(
            "SELECT media_path FROM messages WHERE media_path IS NOT NULL"
        ):
            try:
                total += os.path.getsize(row["media_path"])
            except OSError:
                pass
        self._media_bytes, self._media_bytes_ts = total, now
        return total

    def note_media_bytes(self, delta: int) -> None:
        """Поправить счётчик занятого места, не пересчитывая всё заново."""
        if self._media_bytes is not None:
            self._media_bytes = max(0, self._media_bytes + delta)

    def cleanup(self) -> dict:
        """Скользящее окно: чистим старые файлы, давим переполнение, режем старые строки."""
        now = int(time.time())
        stats = {"files_by_age": 0, "files_by_size": 0, "rows": 0, "edits": 0, "ignored": 0}

        cutoff = now - config.MEDIA_RETENTION_DAYS * 86400
        for row in self.db.execute(
            "SELECT chat_id, msg_id, media_path FROM messages "
            "WHERE media_path IS NOT NULL AND sent_ts < ?",
            (cutoff,),
        ).fetchall():
            _unlink(row["media_path"])
            self.db.execute(
                "UPDATE messages SET media_path=NULL, media_note=? WHERE chat_id=? AND msg_id=?",
                ("файл вычищен по сроку хранения", row["chat_id"], row["msg_id"]),
            )
            stats["files_by_age"] += 1

        limit = config.MEDIA_MAX_TOTAL_MB * 1024 * 1024
        total = self.media_total_bytes()
        if total > limit:
            for row in self.db.execute(
                "SELECT chat_id, msg_id, media_path FROM messages "
                "WHERE media_path IS NOT NULL ORDER BY sent_ts ASC"
            ).fetchall():
                if total <= limit:
                    break
                try:
                    total -= os.path.getsize(row["media_path"])
                except OSError:
                    pass
                _unlink(row["media_path"])
                self.db.execute(
                    "UPDATE messages SET media_path=NULL, media_note=? WHERE chat_id=? AND msg_id=?",
                    ("файл вычищен: превышен лимит места", row["chat_id"], row["msg_id"]),
                )
                stats["files_by_size"] += 1

        # DB_RETENTION_DAYS=0 — история не срезается. Иначе бэкфилл сам себя стирал бы:
        # залитые старые сообщения тут же попадали под срок.
        if config.DB_RETENTION_DAYS > 0:
            row_cutoff = now - config.DB_RETENTION_DAYS * 86400
            # Порциями и с потолком за прогон. Одним DELETE это на миллионной
            # базе выливается в секунды удержания записи (на каждой строке ещё
            # срабатывает FTS-триггер), а чистка идёт в том же цикле, что и приём
            # удалений. Не дочищенное подберёт следующий прогон через 6 часов.
            while stats["rows"] < CLEANUP_MAX_ROWS:
                chunk = self.db.execute(
                    "DELETE FROM messages WHERE rowid IN ("
                    "  SELECT rowid FROM messages"
                    "  WHERE sent_ts < ? AND media_path IS NULL LIMIT ?)",
                    (row_cutoff, CLEANUP_CHUNK),
                ).rowcount
                stats["rows"] += chunk
                self.db.commit()
                if chunk < CLEANUP_CHUNK:
                    break
            stats["edits"] = self.db.execute(
                "DELETE FROM edits WHERE ts < ?", (row_cutoff,)
            ).rowcount

        # Пометки игнорируемых чатов нужны недолго и растут — их режем всегда.
        stats["ignored"] = self.db.execute(
            "DELETE FROM ignored_ids WHERE ts < ?", (now - IGNORED_RETENTION_DAYS * 86400,)
        ).rowcount
        self.db.commit()
        _prune_empty_dirs(config.MEDIA_DIR)
        # Файлы удаляли — счётчик занятого места пересчитываем начисто
        self._media_bytes = None
        self.media_total_bytes()
        return stats

    # ---------- собеседники ----------

    def active_peers(self, days: int, limit: int = 0) -> list:
        """Личные чаты с активностью за N дней, самые свежие первыми."""
        cutoff = int(time.time()) - days * 86400
        sql = ("SELECT chat_id FROM messages WHERE sent_ts >= ? AND chat_id > 0 "
               "GROUP BY chat_id ORDER BY MAX(sent_ts) DESC")
        params = [cutoff]
        if limit > 0:
            sql += " LIMIT ?"
            params.append(limit)
        return [r["chat_id"] for r in self.db.execute(sql, params)]

    def person_hint(self, chat_id: int, user_id: int):
        """Последние известные имя и @username человека в этом чате — по его
        входящим. Ищем внутри чата: так работает первичный ключ (chat_id, …),
        а не полный проход по всей базе."""
        return self.db.execute(
            "SELECT sender_name, sender_username FROM messages "
            "WHERE chat_id=? AND sender_id=? AND is_outgoing=0 "
            "AND (sender_username IS NOT NULL OR sender_name IS NOT NULL) "
            "ORDER BY sent_ts DESC LIMIT 1",
            (chat_id, user_id),
        ).fetchone()

    def peer_name(self, chat_id: int) -> str:
        row = self.db.execute(
            # Имя автора — только со входящих: у твоих сообщений автор ты сам,
            # и личка, где остались одни твои, звалась бы твоим же именем
            "SELECT chat_title, CASE WHEN is_outgoing=0 THEN sender_name END AS sender_name "
            "FROM messages WHERE chat_id=? ORDER BY sent_ts DESC LIMIT 1",
            (chat_id,),
        ).fetchone()
        if not row:
            return str(chat_id)
        return row["chat_title"] or row["sender_name"] or str(chat_id)

    def get_peer_state(self, user_id: int):
        return self.db.execute(
            "SELECT * FROM peer_state WHERE user_id=?", (user_id,)
        ).fetchone()

    def set_peer_state(self, user_id: int, had_photo: bool, had_status: bool,
                       strikes: int, alerted: bool) -> None:
        self.db.execute(
            """INSERT INTO peer_state (user_id, had_photo, had_status, strikes, alerted, last_check)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 had_photo=excluded.had_photo, had_status=excluded.had_status,
                 strikes=excluded.strikes, alerted=excluded.alerted,
                 last_check=excluded.last_check""",
            (user_id, int(had_photo), int(had_status), strikes, int(alerted), int(time.time())),
        )
        self.db.commit()

    def note_ignored(self, msg_id: int, chat_id: int) -> None:
        """Запомнить ID из неотслеживаемой лички/старой группы.

        Само сообщение не храним — только факт, что этот ID принадлежит чату,
        который нас не интересует. Иначе его удаление приедет как «неизвестное»:
        чат в событии не передаётся, и отличить его от пропущенного было бы нечем.
        """
        self.db.execute(
            "INSERT OR REPLACE INTO ignored_ids (msg_id, chat_id, ts) VALUES (?,?,?)",
            (msg_id, chat_id, int(time.time())),
        )
        self.db.commit()

    def note_ignored_bulk(self, pairs) -> int:
        """Пакетно: [(msg_id, chat_id), ...] за один коммит."""
        now = int(time.time())
        rows = [(m, c, now) for m, c in pairs]
        self.db.executemany(
            "INSERT OR REPLACE INTO ignored_ids (msg_id, chat_id, ts) VALUES (?,?,?)", rows
        )
        self.db.commit()
        return len(rows)

    def is_ignored(self, msg_id: int) -> bool:
        return self.db.execute(
            "SELECT 1 FROM ignored_ids WHERE msg_id=?", (msg_id,)
        ).fetchone() is not None

    # ---------- решения о мониторинге чатов ----------

    def get_chat(self, chat_id: int):
        return self.db.execute(
            "SELECT * FROM chats WHERE chat_id=?", (chat_id,)
        ).fetchone()

    def set_chat(self, chat_id: int, kind: str, title: str,
                 members, monitored: bool) -> None:
        self.db.execute(
            """INSERT INTO chats (chat_id, kind, title, members, monitored, checked_ts)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET
                 kind=excluded.kind, title=excluded.title, members=excluded.members,
                 monitored=excluded.monitored, checked_ts=excluded.checked_ts""",
            (chat_id, kind, title, members, int(monitored), int(time.time())),
        )
        self.db.commit()

    def count_monitored(self) -> int:
        return self.db.execute(
            "SELECT COUNT(*) FROM chats WHERE COALESCE(override, monitored)=1"
        ).fetchone()[0]

    def is_monitored(self, chat_id: int) -> bool:
        """Известен ли чат как отслеживаемый. Незнакомый чат = нет.

        Ручной override сильнее авторешения — иначе после /watch сообщения
        кэшировались бы, а удаления в этом чате игнорировались.
        """
        row = self.db.execute(
            "SELECT COALESCE(override, monitored) AS m FROM chats WHERE chat_id=?",
            (chat_id,),
        ).fetchone()
        return bool(row and row["m"])

    # ---------- ручное переопределение слежки ----------

    def set_override(self, chat_id: int, value) -> None:
        """value: 1 — следить принудительно, 0 — не следить, None — снова авто."""
        # monitored=0 для новой строки: если потом вернут «авто», незнакомый чат
        # не должен молча оказаться отслеживаемым из-за DEFAULT 1.
        self.db.execute(
            "INSERT INTO chats (chat_id, override, monitored) VALUES (?,?,0) "
            "ON CONFLICT(chat_id) DO UPDATE SET override=excluded.override",
            (chat_id, value),
        )
        self.db.commit()

    def get_override(self, chat_id: int):
        row = self.db.execute(
            "SELECT override FROM chats WHERE chat_id=?", (chat_id,)
        ).fetchone()
        return row["override"] if row else None

    def find_chats(self, needle: str, limit: int = 20) -> list:
        """Поиск чатов по подстроке названия — для /watch, /unwatch, /chats.

        Фильтр в Python: SQLite LIKE регистронезависим только для латиницы,
        а названия чаще кириллические.
        """
        key = needle.casefold()
        rows = self.db.execute(
            "SELECT chat_id, kind, title, members, monitored, override FROM chats "
            "ORDER BY monitored DESC, title"
        ).fetchall()
        hits = [r for r in rows if key in (r["title"] or "").casefold()]
        return hits[:limit]

    def monitored_chats(self, limit: int = 100) -> list:
        # COALESCE: ручной /watch (override=1) должен попадать, /unwatch (override=0) — нет
        return self.db.execute(
            "SELECT chat_id, kind, title, members, monitored, override FROM chats "
            "WHERE COALESCE(override, monitored)=1 ORDER BY kind, title LIMIT ?",
            (limit,),
        ).fetchall()

    # ---------- дайджест ----------

    def enqueue_digest(self, kind: str, chat_id, payload: str, file: str = None) -> None:
        self.db.execute(
            "INSERT INTO digest_queue (kind, chat_id, payload, file, ts) VALUES (?,?,?,?,?)",
            (kind, chat_id, payload, file, int(time.time())),
        )
        self.db.commit()

    def peek_digest(self, limit: int = 0) -> list:
        """Читает очередь НЕ удаляя — удалять только после успешной отправки."""
        sql = "SELECT * FROM digest_queue ORDER BY ts, id"
        if limit > 0:
            return self.db.execute(sql + " LIMIT ?", (limit,)).fetchall()
        return self.db.execute(sql).fetchall()

    def delete_digest(self, ids) -> None:
        if not ids:
            return
        self.db.executemany("DELETE FROM digest_queue WHERE id=?", [(i,) for i in ids])
        self.db.commit()

    def digest_count(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM digest_queue").fetchone()[0]

    # ---------- подписки на людей ----------

    def sub_add(self, feature: str, peer_id: int, title: str = None) -> None:
        self.db.execute(
            "INSERT INTO subs (feature, peer_id, title, added_ts) VALUES (?,?,?,?) "
            "ON CONFLICT(feature, peer_id) DO UPDATE SET title=excluded.title",
            (feature, peer_id, title, int(time.time())),
        )
        self.db.commit()

    def sub_del(self, feature: str, peer_id: int) -> bool:
        cur = self.db.execute(
            "DELETE FROM subs WHERE feature=? AND peer_id=?", (feature, peer_id)
        )
        self.db.commit()
        return cur.rowcount > 0

    def sub_has(self, feature: str, peer_id: int) -> bool:
        return self.db.execute(
            "SELECT 1 FROM subs WHERE feature=? AND peer_id=?", (feature, peer_id)
        ).fetchone() is not None

    def sub_list(self, feature: str = None) -> list:
        if feature:
            return self.db.execute(
                "SELECT * FROM subs WHERE feature=? ORDER BY title, peer_id", (feature,)
            ).fetchall()
        return self.db.execute("SELECT * FROM subs ORDER BY feature, title").fetchall()

    def sub_all(self) -> dict:
        """Все подписки разом: {feature: {peer_id, ...}} — для кэша в памяти."""
        out = {}
        for r in self.db.execute("SELECT feature, peer_id FROM subs"):
            out.setdefault(r["feature"], set()).add(r["peer_id"])
        return out

    def sub_title(self, peer_id: int):
        """Имя, сохранённое при подписке. Нужно для тех, кого нашли по @username:
        в переписке их может не быть вовсе, и восстановить имя больше неоткуда."""
        row = self.db.execute(
            "SELECT title FROM subs WHERE peer_id=? AND title IS NOT NULL LIMIT 1",
            (peer_id,),
        ).fetchone()
        return row["title"] if row else None

    def sub_count(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM subs").fetchone()[0]

    def sub_features_for(self, peer_id: int) -> set:
        return {r["feature"] for r in self.db.execute(
            "SELECT feature FROM subs WHERE peer_id=?", (peer_id,))}

    # ---------- кэш скачанного по ссылке ----------

    def dl_get(self, key: str):
        row = self.db.execute(
            "SELECT * FROM dl_cache WHERE key=?", (key,)).fetchone()
        if row is None:
            return None
        # Просроченное не отдаём: контент могли снять с публикации, и повторная
        # отправка старого файла выглядела бы как «бот врёт»
        if config.DOWNLOAD_CACHE_DAYS > 0 and \
                time.time() - row["ts"] > config.DOWNLOAD_CACHE_DAYS * 86400:
            return None
        return row

    def dl_put(self, key: str, file_id: str, title: str, size: int) -> None:
        self.db.execute(
            "INSERT INTO dl_cache (key, file_id, title, size, ts) VALUES (?,?,?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET file_id=excluded.file_id, "
            "  title=excluded.title, size=excluded.size, ts=excluded.ts",
            (key, file_id, title, size, int(time.time())),
        )
        self.db.commit()

    def dl_forget(self, key: str) -> None:
        """Убрать запись — например, если Telegram больше не отдаёт этот файл."""
        self.db.execute("DELETE FROM dl_cache WHERE key=?", (key,))
        self.db.commit()

    def dl_stats(self) -> dict:
        row = self.db.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(size),0) AS bytes FROM dl_cache"
        ).fetchone()
        return {"count": row["n"], "bytes": row["bytes"]}

    # ---------- расшифровки ----------

    def set_transcript(self, chat_id: int, msg_id: int, text: str) -> None:
        self.db.execute(
            "UPDATE messages SET transcript=? WHERE chat_id=? AND msg_id=?",
            (text, chat_id, msg_id),
        )
        self.db.commit()

    def get_transcript(self, chat_id: int, msg_id: int):
        row = self.db.execute(
            "SELECT transcript FROM messages WHERE chat_id=? AND msg_id=?",
            (chat_id, msg_id),
        ).fetchone()
        return row["transcript"] if row else None

    # ---------- профиль собеседника ----------

    def get_profile_state(self, user_id: int):
        return self.db.execute(
            "SELECT * FROM profile_state WHERE user_id=?", (user_id,)
        ).fetchone()

    def set_profile_state(self, user_id: int, username, name, photo_id,
                          is_deleted: bool = False) -> None:
        self.db.execute(
            """INSERT INTO profile_state (user_id, username, name, photo_id,
                                          is_deleted, checked_ts)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 username=excluded.username, name=excluded.name,
                 photo_id=excluded.photo_id, is_deleted=excluded.is_deleted,
                 checked_ts=excluded.checked_ts""",
            (user_id, username, name, photo_id, int(is_deleted), int(time.time())),
        )
        self.db.commit()

    # ---------- онлайн ----------

    def get_online_state(self, user_id: int):
        return self.db.execute(
            "SELECT * FROM online_state WHERE user_id=?", (user_id,)
        ).fetchone()

    def set_online_state(self, user_id: int, online: bool, since_ts=None,
                         notified_ts=None) -> None:
        self.db.execute(
            """INSERT INTO online_state (user_id, online, since_ts, notified_ts)
               VALUES (?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 online=excluded.online, since_ts=excluded.since_ts,
                 notified_ts=excluded.notified_ts""",
            (user_id, int(online), since_ts, notified_ts),
        )
        self.db.commit()

    # ---------- выборки по одному чату (экспорт, статистика) ----------

    def chat_messages(self, chat_id: int, limit: int = 0):
        """Вся переписка с одним человеком по возрастанию времени.

        Курсором, а не fetchall: экспорт большого чата иначе поднял бы сотни
        тысяч строк в память разом.
        """
        sql = ("SELECT * FROM messages WHERE chat_id=? ORDER BY sent_ts, msg_id")
        params = [chat_id]
        if limit > 0:
            sql += " LIMIT ?"
            params.append(limit)
        return self.db.execute(sql, params)

    def chat_stats(self, chat_id: int) -> dict:
        return _chat_stats_on(self.db, chat_id)

    # ---------- meta ----------

    def incr(self, key: str, by: int = 1) -> None:
        self.db.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = CAST(value AS INTEGER) + ?",
            (key, str(by), by),
        )
        self.db.commit()

    def get_int(self, key: str, default: int = 0) -> int:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        try:
            return int(row["value"]) if row else default
        except (ValueError, TypeError):
            return default

    def get_meta(self, key: str, default: str = None):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        self.db.execute(
            "INSERT INTO meta (key, value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        self.db.commit()

    # ---------- статистика ----------

    def stats(self) -> dict:
        day_ago = int(time.time()) - 86400
        q = lambda sql, *a: self.db.execute(sql, a).fetchone()[0]
        return {
            "messages": q("SELECT COUNT(*) FROM messages"),
            "messages_24h": q("SELECT COUNT(*) FROM messages WHERE sent_ts >= ?", day_ago),
            "chats": q("SELECT COUNT(DISTINCT chat_id) FROM messages"),
            "files": q("SELECT COUNT(*) FROM messages WHERE media_path IS NOT NULL"),
            "media_bytes": self.media_total_bytes(),
            "edits_24h": q("SELECT COUNT(*) FROM edits WHERE ts >= ?", day_ago),
        }


def _chat_stats_on(con, chat_id: int) -> dict:
    """Сводка по чату. Вынесена из класса, чтобы её можно было посчитать
    на ОТДЕЛЬНОМ подключении в потоке: на крупном чате (сотни тысяч сообщений)
    эти агрегаты занимают около секунды, а секунда в event loop — это секунда,
    когда не обрабатываются удаления."""
    row = con.execute(
        """SELECT COUNT(*) AS total,
                  SUM(is_outgoing) AS mine,
                  MIN(sent_ts) AS first_ts,
                  MAX(sent_ts) AS last_ts,
                  SUM(CASE WHEN media_type IS NOT NULL THEN 1 ELSE 0 END) AS media,
                  SUM(CASE WHEN edit_count > 0 THEN 1 ELSE 0 END) AS edited,
                  SUM(LENGTH(COALESCE(text,''))) AS chars
           FROM messages WHERE chat_id=?""",
        (chat_id,),
    ).fetchone()
    out = {k: (row[k] or 0) for k in
           ("total", "mine", "first_ts", "last_ts", "media", "edited", "chars")}
    out["theirs"] = out["total"] - out["mine"]
    # Час суток в поясе из настроек. 'localtime' у SQLite — это пояс СЕРВЕРА,
    # а он почти всегда UTC: без поправки «активнее всего в 21:00» уезжало на
    # пару часов от того, что человек видит у себя в Telegram.
    out["hours"] = [(r["h"], r["n"]) for r in con.execute(
        f"SELECT CAST(strftime('%H', sent_ts + {_tz_offset_sec()}, 'unixepoch') AS INTEGER) AS h,"
        " COUNT(*) AS n FROM messages WHERE chat_id=? GROUP BY h ORDER BY n DESC LIMIT 3",
        (chat_id,))]
    out["types"] = [(r["media_type"], r["n"]) for r in con.execute(
        "SELECT media_type, COUNT(*) AS n FROM messages "
        "WHERE chat_id=? AND media_type IS NOT NULL "
        "GROUP BY media_type ORDER BY n DESC LIMIT 6", (chat_id,))]
    return out


def chat_stats_ro(chat_id: int) -> dict:
    """Та же сводка на своём read-only подключении — безопасно звать из потока.
    Делить sqlite-соединение с event loop нельзя: оно не рассчитано на
    одновременное использование."""
    con = sqlite3.connect(f"file:{config.DB_PATH}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        return _chat_stats_on(con, chat_id)
    finally:
        con.close()


def _tz_offset_sec() -> int:
    """Сдвиг настроенного пояса от UTC в секундах, на текущий момент.

    Целое число, а не строка от пользователя: оно подставляется в SQL, и
    брать туда что-либо кроме int нельзя.
    """
    import datetime as _dt
    try:
        import alerts
        tz = alerts.TZ
    except Exception:
        tz = None
    now = _dt.datetime.now(tz=_dt.timezone.utc)
    off = (now.astimezone(tz) if tz else now.astimezone()).utcoffset()
    return int(off.total_seconds()) if off else 0


def _unlink(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def _prune_empty_dirs(root: Path) -> None:
    if not root.exists():
        return
    for d in sorted(root.iterdir(), reverse=True):
        if d.is_dir():
            try:
                d.rmdir()
            except OSError:
                pass
