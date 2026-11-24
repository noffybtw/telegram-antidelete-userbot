"""Офлайн-прогон конвейера: без сети, без Telegram, на подставном клиенте.

Проверяет ровно то, что в бою проверить трудно: что при удалении алерт
собирается правильно, пачка схлопывается в сводку, а медиафайл не стирается
с диска, если отправка не прошла.

Запуск: ./venv/bin/python test_pipeline.py
"""

import asyncio
import os
import sys
import tempfile
import time
from pathlib import Path

TMP = Path(tempfile.mkdtemp(prefix="antidelete-test-"))
os.environ["LOG_CHAT_ID"] = "-1009999999999"
os.environ["API_ID"] = os.environ.get("API_ID") or "1"
os.environ["API_HASH"] = os.environ.get("API_HASH") or "x"

import config

config.DB_PATH = str(TMP / "test.db")
config.SESSION_PATH = str(TMP / "test-session")
config.MEDIA_DIR = TMP / "media"
config.DEBOUNCE_SEC = 0.1
config.PURGE_WINDOW_SEC = 3

import alerts
import main

alerts.RETRY_SLEEP = 0

FAILED = []


def check(name: str, condition: bool, hint: str = "") -> None:
    print(("  ✅ " if condition else "  ❌ ") + name + ("" if condition else f"  <- {hint}"))
    if not condition:
        FAILED.append(name)


class FakeClient:
    """Подставной клиент: вместо отправки в Telegram складывает в список."""

    def __init__(self, fail_times: int = 0, exc=None):
        self.sent = []
        self.fail_times = fail_times
        self.exc = exc or ConnectionError("сеть отвалилась")

    def _maybe_fail(self):
        if self.fail_times:
            self.fail_times -= 1
            raise self.exc

    async def send_message(self, chat, text, **kw):
        self._maybe_fail()
        self.sent.append({"kind": "text", "text": text, "file": None})
        return object()

    async def send_file(self, chat, file, caption=None, **kw):
        self._maybe_fail()
        self.sent.append({"kind": "file", "text": caption, "file": file})
        return object()


class CommandEvent:
    """Сообщение в лог-чате: команда. По умолчанию — от владельца (out=True)."""
    def __init__(self, text, forward=None, chat_id=None, sender_id=None, out=True):
        self.chat_id = int(os.environ["LOG_CHAT_ID"]) if chat_id is None else chat_id
        self.raw_text = text
        self.forward = forward
        self.sender_id = sender_id
        self.out = out
        self.replies = []
        self.buttons = []
        self.files = []
        self.message = None
        self.sent_msgs = []
    async def respond(self, text, **kw):
        self.replies.append(text)
        if kw.get("buttons"):
            self.buttons = kw["buttons"]
        if kw.get("file"):
            self.files.append(kw["file"])
        m = SentMsg(self)
        self.sent_msgs.append(m)
        return m


class SentMsg:
    """Отправленное ботом сообщение: его правят (шкала) и удаляют."""
    def __init__(self, owner):
        self.owner = owner
        self.edits = []
        self.deleted = False
    async def edit(self, text, **kw):
        self.edits.append(text)
        # Кнопки могут появиться именно при правке (выбор качества рисуется
        # поверх сообщения «смотрю, что доступно»)
        if kw.get("buttons"):
            self.owner.buttons = kw["buttons"]
        return self
    async def delete(self):
        self.deleted = True


class CallbackEvent:
    """Нажатие инлайн-кнопки бота."""
    def __init__(self, data, sender_id=None):
        self.data = data.encode() if isinstance(data, str) else data
        self.sender_id = sender_id
        self.answered = []
        self.replies = []
    async def answer(self, text="", **kw):
        self.answered.append(text)
    async def respond(self, text, **kw):
        self.replies.append(text)
    async def edit(self, text, **kw):
        self.replies.append(text)


class DeleteEvent:
    def __init__(self, ids, chat_id=None):
        self.deleted_ids = ids
        self.chat_id = chat_id


class EditEvent:
    class _Msg:
        def __init__(self, mid, text):
            self.id = mid
            self.text = text

    def __init__(self, chat_id, mid, text):
        self.chat_id = chat_id
        self.message = self._Msg(mid, text)
        self.is_private = True


def mark_monitored(chat_id, kind="private", title="Иван Петров", members=None, ok=True):
    main.db.set_chat(chat_id, kind, title, members, ok)


def add(msg_id, chat_id=777, text="сообщение", outgoing=False, ago=60, **kw):
    main.db.add_message(
        chat_id=chat_id, msg_id=msg_id, sender_id=chat_id, sender_name="Иван Петров",
        sender_username="ivan", chat_title="Иван Петров", text=text,
        is_outgoing=int(outgoing), sent_ts=int(time.time()) - ago, **kw
    )


async def settle():
    await asyncio.sleep(0.45)


async def wait_for(cond, timeout=5.0):
    """Ждать условие, а не «наверное, успеет». Фиксированные паузы делали
    проверки загрузчика плавающими: код стал делать на шаг больше и не
    укладывался в захардкоженные 0.3 секунды."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        await asyncio.sleep(0.02)
    return False


async def run():
    mark_monitored(777)
    mark_monitored(-1005550000, "supergroup", "Кентухи", 12)
    mark_monitored(-1006660000, "supergroup", "Чат города", 5000, ok=False)

    print("\n1. Одиночное удаление входящего")
    fake = main.client = FakeClient()
    add(101, text="секретное сообщение")
    await main.on_deleted(DeleteEvent([101]))
    await settle()
    body = fake.sent[0]["text"] if fake.sent else ""
    check("пришёл ровно один алерт", len(fake.sent) == 1, f"пришло {len(fake.sent)}")
    check("текст сообщения восстановлен", "секретное сообщение" in body)
    check("указан автор", "Иван Петров" in body and "@ivan" in body)
    check("посчитано время до удаления", "через 1 мин" in body, body[:200])

    print("\n2. Удалили ТВОЁ сообщение — тоже сообщаем")
    import config as _cfg
    _cfg.ALERT_OWN_DELETES = True
    fake = main.client = FakeClient()
    add(102, text="это писал я", outgoing=True)
    await main.on_deleted(DeleteEvent([102]))
    await settle()
    body = fake.sent[0]["text"] if fake.sent else ""
    check("алерт пришёл", len(fake.sent) == 1, f"пришло {len(fake.sent)}")
    check("помечено как твоё сообщение", "твоё сообщение" in body, body[:150])
    check("текст восстановлен", "это писал я" in body)
    check("честно сказано, что автор удаления неизвестен",
          "не сообщает" in body, body[:200])

    print("\n2б. Флагом это отключается")
    _cfg.ALERT_OWN_DELETES = False
    fake = main.client = FakeClient()
    add(1102, text="и это писал я", outgoing=True)
    await main.on_deleted(DeleteEvent([1102]))
    await settle()
    check("с выключенным флагом молчит", not fake.sent, f"пришло {len(fake.sent)}")
    _cfg.ALERT_OWN_DELETES = True

    print("\n3. Удаления в супергруппе/канале — игнор")
    fake = main.client = FakeClient()
    add(103, text="из группы")
    await main.on_deleted(DeleteEvent([103], chat_id=-1001111111111))
    await settle()
    check("алертов нет", not fake.sent, f"пришло {len(fake.sent)}")

    print("\n4. Пачка удалений схлопывается в одну сводку")
    fake = main.client = FakeClient()
    for i in range(6):
        add(200 + i, text=f"строка {i}", outgoing=(i % 2 == 0))
    await main.on_deleted(DeleteEvent([200 + i for i in range(6)]))
    await settle()
    body = fake.sent[0]["text"] if fake.sent else ""
    check("одно сообщение вместо шести", len(fake.sent) == 1, f"пришло {len(fake.sent)}")
    check("помечено как чистка", "чистку переписки" in body)
    check("свои и чужие посчитаны раздельно", "Из них его/её: 3, твоих: 3" in body, body[:300])
    check("содержимое в сводке", "строка 5" in body)

    print("\n5. Удалено то, чего не было в кэше")
    fake = main.client = FakeClient()
    await main.on_deleted(DeleteEvent([999001]))
    await settle()
    body = fake.sent[0]["text"] if fake.sent else ""
    check("алерт всё равно пришёл", len(fake.sent) == 1, f"пришло {len(fake.sent)}")
    check("честно сказано, что содержимого нет", "не восстановлено" in body)

    print("\n6. Медиа: уходит файлом и стирается с диска")
    fake = main.client = FakeClient()
    media = config.MEDIA_DIR / "777"
    media.mkdir(parents=True, exist_ok=True)
    path = media / "301.jpg"
    path.write_bytes(b"\xff\xd8\xff" + b"0" * 100)
    add(301, text="фотка", media_type="фото", media_path=str(path))
    await main.on_deleted(DeleteEvent([301]))
    await settle()
    check("алерт ушёл вложением", fake.sent and fake.sent[0]["kind"] == "file",
          str(fake.sent[:1]))
    check("локальная копия удалена", not path.exists())
    check("в БД помечено, что файл уехал",
          (main.db.get(301)["media_note"] or "") == "отправлено в лог-чат")

    print("\n7. Отправка не прошла — файл на диске сохраняется")
    fake = main.client = FakeClient(fail_times=99)
    path2 = media / "302.jpg"
    path2.write_bytes(b"\xff\xd8\xff" + b"0" * 100)
    add(302, text="вторая фотка", media_type="фото", media_path=str(path2))
    await main.on_deleted(DeleteEvent([302]))
    await settle()
    check("файл не потерян", path2.exists(), "медиа стёрли, не доставив")
    check("путь в БД остался", main.db.get(302)["media_path"] == str(path2))

    print("\n8. FloodWait не глушится, а пережидается")
    from telethon.errors import FloodWaitError
    flood = FloodWaitError(request=None)
    flood.seconds = 0
    fake = main.client = FakeClient(fail_times=1, exc=flood)
    add(303, text="после флудвейта")
    await main.on_deleted(DeleteEvent([303]))
    await asyncio.sleep(2.8)   # _call ждёт e.seconds + 2, поэтому settle() тут мало
    check("алерт доставлен после паузы", len(fake.sent) == 1, f"пришло {len(fake.sent)}")

    print("\n9. Правка сообщения: было -> стало")
    fake = main.client = FakeClient()
    add(401, text="я приду в 19:00")
    await main.on_edited(EditEvent(777, 401, "я не приду"))
    await settle()
    body = fake.sent[0]["text"] if fake.sent else ""
    check("алерт о правке пришёл", len(fake.sent) == 1, f"пришло {len(fake.sent)}")
    # Правка теперь пословная: внутри текста теги <s>/<b>, сверяем видимое
    import re as _re9
    seen = _re9.sub(r"<[^>]+>", "", body)
    check("старая версия видна", "я приду в 19:00" in seen, seen)
    check("новая версия видна", "я не приду" in seen, seen)
    check("кэш обновлён", main.db.get(401)["text"] == "я не приду")

    print("\n10. Правка без изменения текста (превью ссылки) — молчим")
    fake = main.client = FakeClient()
    add(402, text="одинаковый текст")
    await main.on_edited(EditEvent(777, 402, "одинаковый текст"))
    await settle()
    check("алертов нет", not fake.sent, f"пришло {len(fake.sent)}")

    print("\n11. Пауза: кэш пишется, алерты молчат")
    fake = main.client = FakeClient()
    main.db.set_meta("paused", "1")
    add(501, text="во время паузы")
    await main.on_deleted(DeleteEvent([501]))
    await settle()
    check("алертов нет", not fake.sent, f"пришло {len(fake.sent)}")
    main.db.set_meta("paused", "0")

    print("\n12. Чистка диска по сроку хранения")
    old = media / "601.jpg"
    old.write_bytes(b"0" * 50)
    add(601, text="старое", media_type="фото", media_path=str(old),
        ago=(config.MEDIA_RETENTION_DAYS + 1) * 86400)
    stats = main.db.cleanup()
    check("старый файл вычищен", not old.exists() and stats["files_by_age"] == 1, str(stats))
    check("запись в БД осталась с пометкой",
          "по сроку хранения" in (main.db.get(601)["media_note"] or ""))

    print("\n13. Лимит места на диске")
    config.MEDIA_MAX_TOTAL_MB = 0
    big = media / "701.bin"
    big.write_bytes(b"0" * 5000)
    add(701, text="жирный файл", media_type="файл", media_path=str(big), ago=10)
    stats = main.db.cleanup()
    # заодно подметается недоставленный файл из п.7 — потому >=, а не ==
    check("файл вычищен по лимиту", not big.exists() and stats["files_by_size"] >= 1, str(stats))


    print("\n14. Удаление в маленькой группе — ловим")
    fake = main.client = FakeClient()
    add(801, chat_id=-1005550000, text="в группе кентухи")
    main.db.db.execute("UPDATE messages SET chat_title='Кентухи', sender_name='Петя' WHERE msg_id=801")
    main.db.db.commit()
    await main.on_deleted(DeleteEvent([801], chat_id=-1005550000))
    await settle()
    body = fake.sent[0]["text"] if fake.sent else ""
    check("алерт пришёл", len(fake.sent) == 1, f"пришло {len(fake.sent)}")
    check("текст восстановлен", "в группе кентухи" in body)
    check("в группе показан и автор, и чат", "Петя" in body and "Кентухи" in body, body[:200])

    print("\n15. Удаление в большой группе — игнор")
    fake = main.client = FakeClient()
    add(802, chat_id=-1006660000, text="в чате города")
    await main.on_deleted(DeleteEvent([802], chat_id=-1006660000))
    await settle()
    check("алертов нет", not fake.sent, f"пришло {len(fake.sent)}")

    print("\n16. Незнакомая супергруппа — игнор")
    fake = main.client = FakeClient()
    await main.on_deleted(DeleteEvent([803], chat_id=-1007770000))
    await settle()
    check("алертов нет", not fake.sent, f"пришло {len(fake.sent)}")

    print("\n17. ID супергруппы не путается с личкой")
    fake = main.client = FakeClient()
    add(900, chat_id=777, text="ЭТО ИЗ ЛИЧКИ")
    await main.on_deleted(DeleteEvent([900], chat_id=-1005550000))
    await settle()
    body = fake.sent[0]["text"] if fake.sent else ""
    check("личное сообщение не подставилось", "ЭТО ИЗ ЛИЧКИ" not in body, body[:200])
    check("честно сказано, что содержимого нет", not fake.sent or "не восстановлено" in body)

    print("\n18. Удаление в чате, за которым не следим (личка)")
    fake = main.client = FakeClient()
    mark_monitored(888, "private", "Бот-помойка", ok=False)
    add(901, chat_id=888, text="от бота")
    await main.on_deleted(DeleteEvent([901]))
    await settle()
    check("алертов нет", not fake.sent, f"пришло {len(fake.sent)}")

    print("\n19. Правка в группе")
    fake = main.client = FakeClient()
    add(950, chat_id=-1005550000, text="приду в 8")
    await main.on_edited(EditEvent(-1005550000, 950, "не приду"))
    await settle()
    check("алерт о правке пришёл", len(fake.sent) == 1, f"пришло {len(fake.sent)}")

    print("\n20. Правка в неотслеживаемом чате — молчим")
    fake = main.client = FakeClient()
    add(951, chat_id=888, text="было")
    await main.on_edited(EditEvent(888, 951, "стало"))
    await settle()
    check("алертов нет", not fake.sent, f"пришло {len(fake.sent)}")


    print("\n21. Удаление в игнорируемом чате не шумит «неизвестным»")
    fake = main.client = FakeClient()
    main.db.note_ignored(970, 888)      # так помечается сообщение бота/большой группы
    await main.on_deleted(DeleteEvent([970]))
    await settle()
    check("алертов нет", not fake.sent, f"пришло {len(fake.sent)}")

    print("\n22. Настоящее пропущенное удаление всё ещё сообщается")
    fake = main.client = FakeClient()
    await main.on_deleted(DeleteEvent([971]))
    await settle()
    check("алерт пришёл", len(fake.sent) == 1, f"пришло {len(fake.sent)}")
    check("сказано, что не восстановлено",
          fake.sent and "не восстановлено" in fake.sent[0]["text"])

    print("\n23. Остановка досылает то, что висит в дебаунсе")
    fake = main.client = FakeClient()
    add(980, text="поймано перед выключением")
    await main.on_deleted(DeleteEvent([980]))
    await asyncio.sleep(0.02)           # намеренно не ждём дебаунс
    check("буфер ещё не отправлен", not fake.sent, "отправилось слишком рано")
    await main.flush_pending()
    check("после остановки алерт ушёл", len(fake.sent) == 1, f"пришло {len(fake.sent)}")
    check("содержимое на месте",
          fake.sent and "поймано перед выключением" in fake.sent[0]["text"])

    print("\n24. Счётчик отслеживаемых чатов")
    n = main.db.count_monitored()
    # в фикстуре отслеживаемых два: личка 777 и группа «Кентухи»
    check("считает только отслеживаемые", n == 2, f"насчитал {n} (ждали 2)")


    print("\n25. Бессрочное хранение не стирает историю (регрессия)")
    import config as cfg
    cfg.DB_RETENTION_DAYS = 0
    add(990, text="сообщение из 2021", ago=1800 * 86400)
    before = main.db.stats()["messages"]
    main.db.cleanup()
    after = main.db.stats()["messages"]
    check("старое сообщение выжило", main.db.get(990) is not None,
          "чистка снесла историю, ради которой делался бэкфилл")
    check("ничего не удалено", before == after, f"было {before}, стало {after}")

    print("\n26. Со сроком хранения старое всё же режется")
    cfg.DB_RETENTION_DAYS = 90
    main.db.cleanup()
    check("удалено по сроку", main.db.get(990) is None)
    cfg.DB_RETENTION_DAYS = 0


    print("\n27. Файл не доехал, текст доехал — копия не стирается (регрессия)")

    class OnlyTextClient(FakeClient):
        """Текст уходит, send_file всегда падает."""
        async def send_file(self, chat, file, caption=None, **kw):
            raise ConnectionError("файл не пролез")

    fake = main.client = OnlyTextClient()
    media.mkdir(parents=True, exist_ok=True)   # её могла снести чистка в п.12-13
    path3 = media / "303.jpg"
    path3.write_bytes(b"\xff\xd8\xff" + b"0" * 100)
    add(303 + 10000, text="фотка с длинным текстом " + "я" * 1200,
        media_type="фото", media_path=str(path3))
    await main.on_deleted(DeleteEvent([10303]))
    await settle()
    check("текст всё же доставлен", any(x["kind"] == "text" for x in fake.sent),
          str(fake.sent[:1]))
    check("файл НЕ стёрт с диска", path3.exists(),
          "медиа удалено при недоставленном файле")
    check("путь в БД сохранён", main.db.get(10303)["media_path"] == str(path3))


    print("\n28. Полнотекстовый поиск по истории")
    main.db.rebuild_fts()
    add(1200, text="секретный пароль от сейфа 4417")
    add(1201, chat_id=-1005550000, text="встречаемся у сейфа в шесть")
    main.db.rebuild_fts()
    hits = main.db.search("сейф")
    check("поиск нашёл оба совпадения", len(hits) == 2, f"нашёл {len(hits)}")
    check("сниппет подсвечивает слово", any("сейф" in (h["snip"] or "").lower() for h in hits))

    print("\n29. Команда /find")
    fake = main.client = FakeClient()
    ev = CommandEvent("/find пароль")
    await main.on_command(ev)
    await settle()
    text = " ".join(x["text"] for x in fake.sent) + " ".join(ev.replies)
    check("нашлось и отправлено", "4417" in text, text[:120])

    print("\n30. Пересланное сообщение командой не считается")
    fake = main.client = FakeClient()
    ev = CommandEvent("/pause", forward=object())
    await main.on_command(ev)
    check("на пересланное не среагировал", not ev.replies and main.db.get_meta("paused","0")=="0")

    print("\n31. Ручной override: watch маленького бота, unwatch большой группы")
    main.db.set_chat(-1006660000, "supergroup", "Чат города", 5000, False)  # авто: нет
    main.db.set_override(-1006660000, 1)                                      # но следим
    check("override включил слежку", main.db.is_monitored(-1006660000) or main.db.get_override(-1006660000)==1)
    fake = main.client = FakeClient()
    ev = CommandEvent("/watch Кентухи")
    await main.on_command(ev)
    check("watch подтверждён", ev.replies and "принудительно" in ev.replies[0], str(ev.replies))
    check("override записан", main.db.get_override(-1005550000) == 1)

    print("\n32. should_monitor уважает override")
    class FakeChatEvent:
        def __init__(self, cid): self.chat_id = cid
        async def get_chat(self): raise AssertionError("не должен переклассифицировать")
    main.db.set_chat(-1008880000, "supergroup", "Большая", 900, False)
    main.db.set_override(-1008880000, 1)
    main._monitor_cache.clear()
    ok = await main.should_monitor(FakeChatEvent(-1008880000))
    check("override заставляет следить без переклассификации", ok is True)

    print("\n33. Дайджест: чистки копятся, не шлются мгновенно")
    import config as cfg
    cfg.DIGEST_MODE = "purge"
    fake = main.client = FakeClient()
    for i in range(6):
        add(1300 + i, chat_id=777, text=f"чистка {i}")
    await main.on_deleted(DeleteEvent([1300 + i for i in range(6)]))
    await settle()
    check("мгновенного алерта о чистке нет", not fake.sent, f"пришло {len(fake.sent)}")
    check("чистка в очереди дайджеста", main.db.digest_count() >= 1, f"в очереди {main.db.digest_count()}")
    await main.flush_digest()
    check("дайджест досылается", len(fake.sent) >= 1)
    cfg.DIGEST_MODE = "off"

    print("\n34. Медиа-алерт идёт мгновенно даже в дайджесте")
    cfg.DIGEST_MODE = "all"
    fake = main.client = FakeClient()
    media.mkdir(parents=True, exist_ok=True)
    pth = media / "1400.jpg"; pth.write_bytes(b"\xff\xd8\xff" + b"0"*80)
    add(1400, chat_id=777, text="фото которое удалили", media_type="фото", media_path=str(pth))
    await main.on_deleted(DeleteEvent([1400]))
    await settle()
    check("медиа не ушло в дайджест, а отправлено", fake.sent and fake.sent[0]["kind"]=="file",
          str(fake.sent[:1]))
    cfg.DIGEST_MODE = "off"

    print("\n35. Счётчики heartbeat растут на реальных событиях")
    before = main.db.get_int("hb:deletes")
    fake = main.client = FakeClient()
    add(1500, chat_id=777, text="разовое удаление")
    await main.on_deleted(DeleteEvent([1500]))
    await settle()
    check("счётчик удалений увеличился", main.db.get_int("hb:deletes") == before + 1,
          f"было {before}, стало {main.db.get_int('hb:deletes')}")


    print("\n36. /watch включает и приём удалений, не только кэш (регрессия)")
    BIG = -1009990000
    main.db.set_chat(BIG, "supergroup", "Большая группа", 900, False)
    fake = main.client = FakeClient()
    add(1600, chat_id=BIG, text="до watch")
    await main.on_deleted(DeleteEvent([1600], chat_id=BIG))
    await settle()
    check("без watch удаление игнорируется", not fake.sent, f"пришло {len(fake.sent)}")

    main.db.set_override(BIG, 1)
    main._monitor_cache.pop(BIG, None)
    fake = main.client = FakeClient()
    add(1601, chat_id=BIG, text="после watch")
    await main.on_deleted(DeleteEvent([1601], chat_id=BIG))
    await settle()
    check("после watch удаление ловится", len(fake.sent) == 1, f"пришло {len(fake.sent)}")
    check("содержимое восстановлено",
          fake.sent and "после watch" in fake.sent[0]["text"])

    print("\n37. Поиск не падает на кривых запросах")
    for q in ['"', 'OR', 'NEAR(', '*', '()', '   ', 'a AND']:
        try:
            main.db.search(q)
        except Exception as e:
            check(f"запрос {q!r} не роняет поиск", False, f"{type(e).__name__}: {e}")
            break
    else:
        check("все кривые запросы обработаны", True)


    print("\n38. Бот отвечает только владельцу")
    OWNER = 100000001
    main._me_id = OWNER
    fake = main.client = FakeClient()
    stranger = CommandEvent("/find пароль", chat_id=OWNER, sender_id=999999)
    await main.on_bot_command(stranger)
    await settle()
    check("чужому не ответил", not stranger.replies and not fake.sent,
          f"ответов {len(stranger.replies)}, отправок {len(fake.sent)}")

    owner_ev = CommandEvent("/stats", chat_id=OWNER, sender_id=OWNER)
    await main.on_bot_command(owner_ev)
    await settle()
    check("владельцу ответил", len(owner_ev.replies) == 1, f"ответов {len(owner_ev.replies)}")

    print("\n39. Меню команд заполнено")
    check("в меню есть команды", len(main.BOT_MENU) >= 10, f"{len(main.BOT_MENU)}")
    names = [c for c, _ in main.BOT_MENU]
    check("описания непустые", all(d.strip() for _, d in main.BOT_MENU))
    check("нет дублей", len(names) == len(set(names)))


    print("\n40. Все алерты идут через одну воронку (регрессия маршрутизации)")
    import ast, pathlib as _pl
    tree = ast.parse(_pl.Path("main.py").read_text())
    # ВСЕ alerts.send должны быть внутри _send_one — единственной точки, которая
    # знает про приоритет бота. notify/notify_group только берут замок вокруг неё.
    span = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_send_one":
            span = (node.lineno, node.end_lineno)
    check("воронка _send_one существует", span is not None)
    stray = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "send"
                and getattr(node.func.value, "id", "") == "alerts"):
            if not (span and span[0] <= node.lineno <= span[1]):
                stray.append(node.lineno)
    check("все alerts.send только внутри _send_one()", not stray,
          f"вне воронки на строках {stray} — алерт в обход бота не получит уведомления")

    print("\n40б. Замок отправки не реентерабельный — внутри группы только _send_one")
    # notify внутри notify_group означал бы вечный дедлок всей отправки
    grp = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "notify_group":
            grp = (node.lineno, node.end_lineno)
    bad = [n.lineno for n in ast.walk(tree)
           if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "notify"
           and grp and grp[0] <= n.lineno <= grp[1]]
    check("notify_group не зовёт notify (иначе дедлок)", not bad, f"строки {bad}")
    # и сама воронка не должна брать замок повторно
    src_lines = _pl.Path("main.py").read_text().splitlines()
    body = "\n".join(src_lines[span[0]:span[1]]) if span else ""
    check("_send_one не берёт замок сам", "_send_lock" not in body,
          "двойной захват = дедлок")

    print("\n41. notify предпочитает бота, но падает на юзербота при сбое")
    class DeadBot(FakeClient):
        async def send_message(self, *a, **k): raise ConnectionError("бот лёг")
        async def send_file(self, *a, **k): raise ConnectionError("бот лёг")
    user_cli = FakeClient()
    main.client = user_cli
    main.bot = FakeClient(); main._bot_chat = 12345
    await main.notify("тест через бота")
    check("ушло через бота", len(main.bot.sent) == 1 and not user_cli.sent,
          f"бот {len(main.bot.sent)}, юзербот {len(user_cli.sent)}")

    main.bot = DeadBot()
    user_cli = main.client = FakeClient()
    await main.notify("бот лежит")
    check("при сбое бота ушло юзерботом", len(user_cli.sent) == 1,
          f"юзербот {len(user_cli.sent)}")
    main.bot = None


    print("\n42. Команда /chats не падает (регрессия несовпадения колонок)")
    main.db.set_chat(-1005550000, "supergroup", "Кентухи", 12, True)
    main.db.set_chat(777, "private", "Аня", None, True)
    main.db.set_chat(-1009990000, "supergroup", "Большая", 900, False)
    main.db.set_override(-1009990000, 1)

    ev = CommandEvent("/chats")
    await main.on_command(ev)
    await settle()
    body = " ".join(ev.replies)
    check("ответ пришёл", len(ev.replies) >= 1, f"ответов {len(ev.replies)}")
    check("чаты перечислены", "Кентухи" in body, body[:150])

    ev2 = CommandEvent("/chats кент")
    await main.on_command(ev2)
    await settle()
    check("поиск по названию работает", ev2.replies and "Кентухи" in ev2.replies[0])

    print("\n43. Обе выборки чатов дают одинаковые колонки")
    a = main.db.monitored_chats(1)
    b = main.db.find_chats("", 1)
    ka = set(a[0].keys()) if a else set()
    kb = set(b[0].keys()) if b else set()
    need = {"chat_id", "kind", "title", "members", "monitored", "override"}
    check("monitored_chats отдаёт всё нужное", need <= ka, f"не хватает {need - ka}")
    check("find_chats отдаёт всё нужное", need <= kb, f"не хватает {need - kb}")


    print("\n44. Удаление в Избранном не шумит «неизвестным»")
    OWNER2 = int(os.environ["LOG_CHAT_ID"])  # неважно, важен механизм игнора
    # имитируем индексацию Избранного: id занесён в игнор
    main.db.note_ignored_bulk([(70001, 100000001), (70002, 100000001)])
    fake = main.client = FakeClient()
    await main.on_deleted(DeleteEvent([70001, 70002]))
    await settle()
    check("алертов нет", not fake.sent, f"пришло {len(fake.sent)}")

    print("\n45. Пакетный игнор действительно помечает")
    check("оба id в игноре", main.db.is_ignored(70001) and main.db.is_ignored(70002))
    check("посторонний id не задет", not main.db.is_ignored(70003))

    print("\n46. Переключатели: meta перекрывает дефолт .env")
    import config as _c
    _c.TRACK_EDITS = True
    check("дефолт из .env", main.setting("track_edits") is True)
    main.set_toggle("track_edits", False)
    check("meta перекрывает", main.setting("track_edits") is False)
    main.db.db.execute("DELETE FROM meta WHERE key='set:track_edits'"); main.db.db.commit()
    check("снятие meta возвращает дефолт", main.setting("track_edits") is True)

    print("\n47. Клавиатура настроек строится")
    kb = main.settings_keyboard()
    check("есть кнопка на каждый переключатель + пауза", len(kb) == len(main.TOGGLES) + 1,
          f"кнопок {len(kb)}")

    print("\n48. Огонёк уже сохранён — его сгорание не даёт второй алерт")
    fake = main.client = FakeClient()
    add(2100, text="секрет")
    main.db.mark_ttl_captured(777, 2100)
    await main.on_deleted(DeleteEvent([2100]))
    await settle()
    check("повторного алерта об удалении нет", not fake.sent, f"пришло {len(fake.sent)}")

    print("\n49. media_kind распознаёт сгоревший огонёк по флагам медиа")
    from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
    class M:
        def __init__(self, media): self.media = media; self.photo=None; self.document=None
        sticker=gif=video_note=voice=video=audio=None
        contact=geo=poll=None
    doc_voice = MessageMediaDocument(); doc_voice.voice=True; doc_voice.round=False; doc_voice.video=False
    from util import media_kind
    check("голосовой огонёк -> голосовое", media_kind(M(doc_voice)) == "голосовое",
          media_kind(M(doc_voice)))
    photo = MessageMediaPhoto()
    check("фото-огонёк -> фото", media_kind(M(photo)) == "фото", media_kind(M(photo)))

    print("\n50. Настройка view_once выключает перехват")
    _c.CAPTURE_VIEW_ONCE = True
    main.db.db.execute("DELETE FROM meta WHERE key='set:view_once'"); main.db.db.commit()
    check("по умолчанию включено", main.setting("view_once") is True)
    main.set_toggle("view_once", False)
    check("выключается", main.setting("view_once") is False)
    main.set_toggle("view_once", True)

    print("\n51. Нижняя клавиатура строится и её кнопки маппятся в команды")
    kb = main.main_keyboard()
    check("клавиатура непустая", len(kb) >= 3, f"рядов {len(kb)}")
    check("«Настройки» -> /menu", main.KEYBOARD_ACTIONS.get("⚙️ Настройки") == "/menu")
    check("«Истории» -> /story", main.KEYBOARD_ACTIONS.get("👁 Истории") == "/story")
    check("«Прослушать» -> /voice", main.KEYBOARD_ACTIONS.get("🎧 Прослушать") == "/voice")
    check("«Статистика» -> /stats", main.KEYBOARD_ACTIONS.get("📊 Статистика") == "/stats")

    print("\n52. Кнопка присылает текст — обработчик понимает как команду")
    fake = main.client = FakeClient()
    ev = CommandEvent("📊 Статистика")   # обычный текст, не /команда
    await main.on_command(ev)
    await settle()
    check("статистика ответила на нажатие кнопки", len(ev.replies) == 1,
          f"ответов {len(ev.replies)}")

    print("\n53. /story без чата открывает пикер, а не ошибку")
    called = {}
    orig_picker = main.stories_picker
    async def fake_picker(ev): called["picker"] = True
    main.stories_picker = fake_picker
    ev = CommandEvent("/story")
    await main.on_command(ev)
    check("вызван пикер людей", called.get("picker") is True)
    main.stories_picker = orig_picker

    print("\n54. Нажатие кнопки-человека и кнопки «сохранённые» роутится верно")
    OWNER = main._me_id
    seen = {}
    orig_vs, orig_vp = main.view_stories, main.view_pinned_stories
    async def fake_vs(cid, title, ev): seen["story"] = cid
    async def fake_vp(cid, title, ev): seen["pin"] = cid
    async def fake_vp2(cid, title, ev, offset=0): seen["pin"] = (cid, offset)
    main.view_stories, main.view_pinned_stories = fake_vs, fake_vp2
    await main.on_bot_callback(CallbackEvent("story:12345", sender_id=OWNER))
    await settle()
    check("story:<id> -> view_stories(id)", seen.get("story") == 12345, str(seen))
    await main.on_bot_callback(CallbackEvent("pin:678", sender_id=OWNER))
    await settle()
    check("pin:<id> -> view_pinned(id, offset 0)", seen.get("pin") == (678, 0), str(seen))
    await main.on_bot_callback(CallbackEvent("pin:678:900", sender_id=OWNER))
    await settle()
    check("pin:<id>:<off> -> пагинация с offset", seen.get("pin") == (678, 900), str(seen))
    main.view_stories, main.view_pinned_stories = orig_vs, orig_vp

    print("\n55. Чужой не может дёргать кнопки историй")
    seen.clear()
    main.view_stories = fake_vs
    cb = CallbackEvent("story:999", sender_id=999999)
    await main.on_bot_callback(cb)
    await settle()
    check("чужому отказ, функция не вызвана", "story" not in seen and cb.answered,
          str(seen))
    main.view_stories = orig_vs

    print("\n56. resolve_chat понимает @username (через get_entity)")
    from telethon.tl.types import User as _TgUser
    ent = _TgUser(id=555000, first_name="Дуров", access_hash=0, username="durov")
    class StoryClient(FakeClient):
        async def get_entity(self, arg): return ent
    main.client = StoryClient()
    target, err = await main.resolve_chat("@durov")
    check("@username резолвится в (id, имя)", target == (555000, "Дуров") and err is None,
          f"{target} {err}")

    print("\n57. /stop отменяет идущую выгрузку историй")
    started = asyncio.Event()
    async def long_dump():
        started.set()
        await asyncio.sleep(30)          # долгая «выгрузка»
    async def wrapped():
        await main._run_story_dump(long_dump(), CommandEvent("x"))
    task = asyncio.create_task(wrapped())
    await started.wait()
    check("задача учтена в _story_tasks", len(main._story_tasks) == 1,
          f"{len(main._story_tasks)}")
    ev = CommandEvent("/stop")
    await main.on_command(ev)
    await asyncio.sleep(0.05)
    check("/stop сообщил об отмене", ev.replies and "рерв" in ev.replies[0],
          str(ev.replies))
    check("задача отменена", task.cancelled() or task.done())
    check("список выгрузок очистился", len(main._story_tasks) == 0,
          f"{len(main._story_tasks)}")

    print("\n58. /stop без активных выгрузок отвечает мягко")
    ev = CommandEvent("/stop")
    await main.on_command(ev)
    check("нечего останавливать", ev.replies and "нечего" in ev.replies[0].lower(),
          str(ev.replies))

    print("\n59. Команду в лог-чате выполняет только владелец (security)")
    OWNER = main._me_id
    ev = CommandEvent("/find пароль", sender_id=999999, out=False)   # чужой участник
    await main.on_command(ev)
    check("чужому в лог-чате отказ (пусто)", not ev.replies, str(ev.replies))
    ev = CommandEvent("/stats", sender_id=OWNER, out=False)          # владелец по id
    await main.on_command(ev)
    check("владельцу по sender_id — ответ", len(ev.replies) == 1, str(len(ev.replies)))

    print("\n60. Дайджест не теряется при сбое отправки (регрессия)")
    import config as dc
    main.db.db.execute("DELETE FROM digest_queue"); main.db.db.commit()
    main.db.enqueue_digest("delete", 1, "<b>важное событие</b>")
    class DeadClient(FakeClient):
        async def send_message(self, *a, **k): raise ConnectionError("нет сети")
        async def send_file(self, *a, **k): raise ConnectionError("нет сети")
    main.bot = None
    main.client = DeadClient()
    await main.flush_digest()
    check("при сбое дайджест остался в очереди", main.db.digest_count() == 1,
          f"в очереди {main.db.digest_count()}")
    main.client = FakeClient()
    await main.flush_digest()
    check("после восстановления связи очередь очистилась", main.db.digest_count() == 0)

    print("\n61. Огонёк не помечается captured, если отправка не прошла (регрессия)")
    add(3300, text="секретное фото")
    main.db.set_media_path(777, 3300, None, None)
    row = main.db.get(3300, 777)
    # эмулируем ветку notify-провала внутри capture: пометка только при sent
    main.bot = None
    main.client = DeadClient()
    sent = await main.notify("огонёк", file=None, sensitive=True)
    if sent:
        main.db.mark_ttl_captured(777, 3300)
    check("не помечен при неудаче", main.db.get(3300, 777)["ttl_captured"] == 0,
          "помечен, хотя не отправлено — огонёк пропал бы совсем")
    main.client = FakeClient()

    print("\n62. /find не ломается на сообщениях с ёлочками «»")
    add(3400, text="верни «сейф» немедленно")
    main.db.rebuild_fts()
    fake = main.client = FakeClient()
    ev = CommandEvent("/find сейф", sender_id=OWNER)
    await main.on_command(ev)
    await settle()
    blob = " ".join(x["text"] for x in fake.sent) + " ".join(ev.replies)
    check("результат с ёлочками отправлен", "сейф" in blob, blob[:120])
    check("нет сырых перевёрнутых тегов от ёлочек", "</b>сейф<b>" not in blob)

    print("\n63. monitored_chats уважает override (/watch виден, /unwatch скрыт)")
    main.db.set_chat(-100777001, "supergroup", "Ватч-группа", 900, False)
    main.db.set_override(-100777001, 1)      # /watch на большую группу
    main.db.set_chat(555111, "private", "Анватч", None, True)
    main.db.set_override(555111, 0)          # /unwatch на личку
    titles = [r["title"] for r in main.db.monitored_chats()]
    check("watch-чат виден в списке", "Ватч-группа" in titles, str(titles)[:120])
    check("unwatch-чат скрыт из списка", "Анватч" not in titles, str(titles)[:120])


    # =============== новые функции ===============
    import transcribe as _tr
    import downloader as _dl
    import export as _ex

    # Тесты работают с ФАЙЛАМИ проекта, поэтому уводим пути в песочницу:
    # иначе проверка «/cookies off» стирала настоящие куки на боевом сервере,
    # а список прокси подхватывался бы реальный.
    _dl.COOKIES_PATH = TMP / "cookies.txt"
    _dl.PROXIES_PATH = TMP / "proxies.txt"
    _dl._proxy_pool = []
    # Проверки прямой скачки не должны упираться в разведку качеств: она ходит
    # в сеть. Сам выбор качества проверяется отдельно, с подставным probe.
    main.set_toggle("ask_quality", False)

    OWNER = main._me_id
    # Ранние тесты переименовывали чат 777 — возвращаем известное имя, чтобы
    # поиск по части названия («/sub Иван») был предсказуем.
    main.db.set_chat(777, "private", "Иван Петров", None, True)

    def fake_transcribe(result="я завтра не приду", err=None, calls=None):
        async def _f(path):
            if calls is not None:
                calls.append(path)
            return (result, err)
        return _f

    print("\n64. Подписка на человека: включается, снимается, живёт в кэше")
    main.db.db.execute("DELETE FROM subs"); main.db.db.commit()
    main.reload_subs()
    check("сначала пусто", not main.sub_has("voice", 777))
    main.sub_set("voice", 777, True, "Иван Петров")
    check("включилась", main.sub_has("voice", 777))
    check("кэш в памяти обновился", 777 in main._subs.get("voice", set()))
    check("в базе тоже", main.db.sub_has("voice", 777))
    main.sub_set("voice", 777, False)
    check("снялась", not main.sub_has("voice", 777) and not main.db.sub_has("voice", 777))

    print("\n65. /sub имя — карточка человека с кнопками")
    ev = CommandEvent("/sub Иван", sender_id=OWNER)
    await main.on_command(ev)
    await settle()
    check("карточка показана", ev.replies and "Иван" in ev.replies[0], str(ev.replies)[:120])
    check("кнопка на каждую функцию", len(ev.buttons) == len(main.SUB_FEATURES),
          f"кнопок {len(ev.buttons)}")

    print("\n66. /sub voice имя — переключение текстом (когда кнопок нет)")
    ev = CommandEvent("/sub voice Иван", sender_id=OWNER)
    await main.on_command(ev)
    check("включено текстом", main.sub_has("voice", 777) and "Включил" in ev.replies[0],
          str(ev.replies)[:120])
    ev = CommandEvent("/sub voice Иван", sender_id=OWNER)
    await main.on_command(ev)
    check("повторно — выключено", not main.sub_has("voice", 777))

    print("\n67. Кнопка подписки переключает, чужому — отказ")
    cb = CallbackEvent("sub:voice:777", sender_id=OWNER)
    await main.on_bot_callback(cb)
    check("владелец включил кнопкой", main.sub_has("voice", 777), str(cb.answered))
    cb = CallbackEvent("sub:profile:777", sender_id=999999)
    await main.on_bot_callback(cb)
    check("чужой не смог", not main.sub_has("profile", 777), str(cb.answered))
    cb = CallbackEvent("unsub:voice:777", sender_id=OWNER)
    await main.on_bot_callback(cb)
    check("unsub снимает", not main.sub_has("voice", 777))

    print("\n68. Битые callback-данные не роняют обработчик")
    for bad in ("sub:voice", "sub:voice:абв", "sub:чушь:777", "unsub::1"):
        cb = CallbackEvent(bad, sender_id=OWNER)
        try:
            await main.on_bot_callback(cb)
        except Exception as e:
            check(f"{bad!r} не роняет", False, f"{type(e).__name__}: {e}")
            break
    else:
        check("все битые варианты обработаны", True)

    print("\n69. Удалили голосовое -> приходит расшифровка")
    _cfg.TRANSCRIBE_ENABLED = True
    _cfg.TRANSCRIBE_DELETED = True
    main.db.db.execute("DELETE FROM meta WHERE key='set:transcribe'"); main.db.db.commit()
    orig_tr = _tr.transcribe
    calls = []
    _tr.transcribe = fake_transcribe("я завтра не приду", calls=calls)
    fake = main.client = FakeClient()
    media.mkdir(parents=True, exist_ok=True)
    voice = media / "4001.ogg"; voice.write_bytes(b"OggS" + b"0" * 200)
    add(4001, text=None, media_type="голосовое", media_path=str(voice))
    await main.on_deleted(DeleteEvent([4001]))
    await settle()
    await asyncio.sleep(0.3)
    blob = " ".join(x["text"] or "" for x in fake.sent)
    check("алерт об удалении пришёл", any("голосовое" in (x["text"] or "") for x in fake.sent),
          blob[:150])
    check("расшифровка прислана", "я завтра не приду" in blob, blob[:200])
    check("транскрипт сохранён в базу",
          main.db.get_transcript(777, 4001) == "я завтра не приду")
    check("расшифровывали КОПИЮ, а не оригинал",
          calls and calls[0] != str(voice), str(calls))
    check("копия за собой убрана", not any(os.path.exists(p) for p in calls), str(calls))

    print("\n70. Уже расшифрованное голосовое показывает текст прямо в алерте")
    fake = main.client = FakeClient()
    voice2 = media / "4002.ogg"; voice2.write_bytes(b"OggS" + b"0" * 200)
    add(4002, text=None, media_type="голосовое", media_path=str(voice2))
    main.db.set_transcript(777, 4002, "текст из живой расшифровки")
    await main.on_deleted(DeleteEvent([4002]))
    await settle()
    blob = " ".join(x["text"] or "" for x in fake.sent)
    check("текст в самом алерте", "текст из живой расшифровки" in blob, blob[:200])
    check("вторым сообщением не дублируется",
          blob.count("текст из живой расшифровки") == 1, blob[:250])

    print("\n71. Расшифровка выключена — не трогаем")
    main.set_toggle("transcribe", False)
    fake = main.client = FakeClient()
    voice3 = media / "4003.ogg"; voice3.write_bytes(b"OggS" + b"0" * 200)
    add(4003, text=None, media_type="голосовое", media_path=str(voice3))
    check("копия не снимается", main.stash_voice(main.db.get(4003, 777)) is None)
    main.set_toggle("transcribe", True)

    print("\n72. Не-голосовое не расшифровываем")
    pic = media / "4004.jpg"; pic.write_bytes(b"\xff\xd8\xff" + b"0" * 80)
    add(4004, text="фотка", media_type="фото", media_path=str(pic))
    check("фото мимо расшифровки", main.stash_voice(main.db.get(4004, 777)) is None)

    print("\n73. Проверка ссылок: внутренняя сеть и чужие схемы отбиты")
    check("метадата облака заблокирована",
          _dl.check_url("http://169.254.169.254/latest/meta-data/") is not None)
    check("localhost заблокирован", _dl.check_url("http://localhost:8080/x") is not None)
    check("127.0.0.1 заблокирован", _dl.check_url("http://127.0.0.1/x") is not None)
    check("внутренняя сеть заблокирована", _dl.check_url("http://192.168.1.1/x") is not None)
    check("file:// отбит", _dl.check_url("file:///etc/passwd") is not None)
    check("обычная ссылка проходит", _dl.check_url("https://vm.tiktok.com/ZS123/") is None)
    check("ссылка из текста вынимается",
          _dl.find_url("глянь https://vm.tiktok.com/ZS1/ ржака") == "https://vm.tiktok.com/ZS1/")
    check("без ссылки — None", _dl.find_url("просто текст") is None)

    print("\n74. Ссылка с дефисом не уедет в ключ командной строки")
    args = _dl._build_args(["https://x.com/-rf"], TMP, TMP / "m.txt", False)
    check("перед ссылкой стоит --", args[-2] == "--" and args[-1] == "https://x.com/-rf",
          str(args[-3:]))
    check("конфиг пользователя не подхватывается", "--ignore-config" in args)
    check("плейлист не тянется целиком", "--no-playlist" in args)
    check("есть потолок размера", "--max-filesize" in args)

    print("\n75. /dl без ссылки — подсказка, а не ошибка")
    ev = CommandEvent("/dl", sender_id=OWNER)
    await main.on_command(ev)
    check("подсказка выдана", ev.replies and "ссылк" in ev.replies[0].lower(),
          str(ev.replies)[:120])

    print("\n76. /dl со ссылкой -> файл уходит, за собой прибрано")
    orig_fetch = _dl.fetch
    orig_discard = _dl.discard
    dlfile = TMP / "video.mp4"; dlfile.write_bytes(b"0" * 1000)
    jobdir = TMP / "job1"; jobdir.mkdir(exist_ok=True)
    discarded = []
    async def fake_fetch(url, audio_only=False, on_progress=None, job_id=None, height=None):
        return {"paths": [str(dlfile)], "title": "Смешное видео", "job": jobdir,
                "extra": 0, "error": None}
    _dl.fetch = fake_fetch
    _dl.discard = lambda j: discarded.append(j)
    ev = CommandEvent("/dl https://vm.tiktok.com/ZS1/", sender_id=OWNER)
    await main.on_command(ev)
    await wait_for(lambda: str(dlfile) in ev.files)
    check("файл отправлен", str(dlfile) in ev.files, str(ev.files))
    check("название в подписи", any("Смешное видео" in r for r in ev.replies),
          str(ev.replies)[:150])
    check("временная папка убрана", jobdir in discarded, str(discarded))

    print("\n77. Ошибка скачивания объясняется по-человечески")
    async def fail_fetch(url, audio_only=False, on_progress=None, job_id=None, height=None):
        return {"error": "площадка требует вход"}
    _dl.fetch = fail_fetch
    ev = CommandEvent("/dl https://youtube.com/watch?v=x", sender_id=OWNER)
    await main.on_command(ev)
    await wait_for(lambda: any(m.edits for m in ev.sent_msgs))
    # Ошибка теперь правит сообщение-шкалу, а не шлёт новое: смотрим и правки
    said = list(ev.replies) + [e for m in ev.sent_msgs for e in m.edits]
    check("причина названа", any("требует вход" in r for r in said), str(said)[:200])
    check("кнопка отмены со шкалы снята", all(m.edits for m in ev.sent_msgs if m.edits),
          "итог должен приходить без кнопки «Отмена»")
    # регистр не важен: текст подсказки разный, смотря есть ли куки на сервере
    check("объяснение yt-dlp переводится",
          "cookies.txt" in _dl._explain("ERROR: Sign in to confirm you're not a bot"))
    check("лимит размера объясняется",
          "МБ" in _dl._explain("File is larger than max-filesize"))
    _dl.fetch = orig_fetch

    print("\n78. Загрузчик выключен — не качает")
    main.set_toggle("downloader", False)
    ev = CommandEvent("/dl https://vm.tiktok.com/ZS1/", sender_id=OWNER)
    await main.on_command(ev)
    check("сказано, что выключен", ev.replies and "выключен" in ev.replies[0],
          str(ev.replies)[:120])
    main.set_toggle("downloader", True)

    print("\n79. Боту кинули голосовое -> расшифровка без команд")
    class FakeMsg:
        def __init__(self, voice=False, mid=1):
            self.voice = voice
            self.video_note = None
            self.audio = None
            self.id = mid
            self.downloaded = None
        async def download_media(self, file=None):
            Path(file).parent.mkdir(parents=True, exist_ok=True)
            Path(file).write_bytes(b"OggS" + b"0" * 50)
            return file

    class BotEvent(CommandEvent):
        def __init__(self, text="", msg=None, sender_id=None):
            super().__init__(text, chat_id=OWNER, sender_id=sender_id, out=False)
            self.message = msg

    _tr.transcribe = fake_transcribe("привет как дела")
    ev = BotEvent("", FakeMsg(voice=True), sender_id=OWNER)
    await main.on_bot_command(ev)
    await asyncio.sleep(0.2)
    check("расшифровка прислана", any("привет как дела" in r for r in ev.replies),
          str(ev.replies)[:150])

    print("\n80. Боту кинули ссылку без команды -> качает")
    _dl.fetch = fake_fetch
    ev = BotEvent("https://vm.tiktok.com/ZS9/", None, sender_id=OWNER)
    await main.on_bot_command(ev)
    await wait_for(lambda: bool(ev.files))
    check("скачал по голой ссылке", str(dlfile) in ev.files, str(ev.files))

    print("\n81. Ссылка в ЛОГ-ЧАТЕ сама по себе не качает (иначе алерты зациклятся)")
    ev = CommandEvent("посмотри https://vm.tiktok.com/ZS9/", sender_id=OWNER)
    await main.on_command(ev)
    await asyncio.sleep(0.2)
    check("ничего не скачано", not ev.files, str(ev.files))
    _dl.fetch = orig_fetch

    print("\n82. Чужой не может ни скачать, ни расшифровать через бота")
    ev = BotEvent("https://vm.tiktok.com/ZS9/", None, sender_id=999999)
    await main.on_bot_command(ev)
    check("чужому по ссылке отказ", not ev.replies and not ev.files, str(ev.replies))
    ev = BotEvent("", FakeMsg(voice=True), sender_id=999999)
    await main.on_bot_command(ev)
    check("чужому по голосовому отказ", not ev.replies, str(ev.replies))

    print("\n83. Онлайн-трекинг: только по подписке и без спама")
    class StatusEvent:
        def __init__(self, uid, online):
            self.user_id = uid
            self.online = online

    _cfg.ONLINE_WATCH_ENABLED = True
    main.db.db.execute("DELETE FROM meta WHERE key='set:online_watch'")
    main.db.db.execute("DELETE FROM online_state"); main.db.db.commit()
    fake = main.client = FakeClient()
    await main.on_user_update(StatusEvent(777, True))
    check("без подписки молчит", not fake.sent, f"пришло {len(fake.sent)}")

    main.sub_set("online", 777, True, "Иван Петров")
    fake = main.client = FakeClient()
    await main.on_user_update(StatusEvent(777, True))
    check("по подписке сообщает о заходе",
          fake.sent and "в сеть" in fake.sent[0]["text"], str(fake.sent)[:150])

    fake = main.client = FakeClient()
    await main.on_user_update(StatusEvent(777, True))
    check("повтор того же статуса не дублируется", not fake.sent, f"пришло {len(fake.sent)}")

    fake = main.client = FakeClient()
    await main.on_user_update(StatusEvent(777, False))
    check("о выходе тоже сообщает",
          fake.sent and "ышел" in fake.sent[0]["text"], str(fake.sent)[:150])

    fake = main.client = FakeClient()
    await main.on_user_update(StatusEvent(777, True))
    check("повторный заход в окне антиспама молчит", not fake.sent,
          f"пришло {len(fake.sent)} (ONLINE_MIN_GAP_SEC={_cfg.ONLINE_MIN_GAP_SEC})")

    print("\n84. Событие «печатает» онлайн-алертом не считается")
    fake = main.client = FakeClient()
    await main.on_user_update(StatusEvent(777, None))
    check("None-статус игнорируется", not fake.sent, f"пришло {len(fake.sent)}")
    main.sub_set("online", 777, False)

    print("\n85. Смена профиля: первый прогон молчит, второй — сообщает")
    from telethon.tl.types import User as _U
    class PhotoStub:
        def __init__(self, pid): self.photo_id = pid
    class ProfClient(FakeClient):
        def __init__(self, ent): super().__init__(); self.ent = ent
        async def get_entity(self, uid): return self.ent

    main.db.db.execute("DELETE FROM profile_state"); main.db.db.commit()
    main.sub_set("profile", 777, True, "Иван Петров")
    _cfg.PROFILE_WATCH_ENABLED = True
    main.db.db.execute("DELETE FROM meta WHERE key='set:profile_watch'"); main.db.db.commit()

    u1 = _U(id=777, first_name="Иван", last_name="Петров", username="ivan", access_hash=0)
    u1.photo = PhotoStub(111)
    cli = main.client = ProfClient(u1)
    await main._check_profiles()
    check("первый прогон только запоминает", not cli.sent, f"пришло {len(cli.sent)}")
    check("снимок записан", main.db.get_profile_state(777)["username"] == "ivan")

    u2 = _U(id=777, first_name="Иван", last_name="Петров", username="ivan_new", access_hash=0)
    u2.photo = PhotoStub(222)
    cli = main.client = ProfClient(u2)
    await main._check_profiles()
    body = cli.sent[0]["text"] if cli.sent else ""
    check("смена username поймана", "ivan_new" in body, body[:200])
    check("старый username показан", "ivan" in body)
    check("смена фото поймана", "Фото" in body, body[:200])

    print("\n86. Удаление аккаунта — отдельный алерт")
    u3 = _U(id=777, first_name="", access_hash=0, deleted=True)
    u3.photo = None
    cli = main.client = ProfClient(u3)
    await main._check_profiles()
    body = cli.sent[0]["text"] if cli.sent else ""
    check("сказано, что аккаунт удалён", "удалён" in body.lower(), body[:200])
    check("больше не повторяется", True)
    cli = main.client = ProfClient(u3)
    await main._check_profiles()
    check("повторного алерта нет", not cli.sent, f"пришло {len(cli.sent)}")
    main.sub_set("profile", 777, False)

    print("\n87. Экспорт чата в HTML")
    outp = TMP / "exp.html"
    add(5001, text="первое сообщение", ago=7200)
    add(5002, text="<script>alert(1)</script>", ago=3600)
    res = _ex.write_html(777, "Иван Петров", str(outp))
    htm = outp.read_text(encoding="utf-8")
    check("файл собрался", res["count"] > 0 and outp.exists(), str(res))
    check("сообщения внутри", "первое сообщение" in htm)
    check("HTML экранирован (нет XSS)", "<script>alert(1)" not in htm
          and "&lt;script&gt;" in htm)
    check("расшифровка попала в файл", "текст из живой расшифровки" in htm)
    check("заголовок на месте", "Иван Петров" in htm)

    print("\n88. Экспорт командой отдаёт файл")
    ev = CommandEvent("/export Иван", sender_id=OWNER)
    await main.on_command(ev)
    await asyncio.sleep(0.5)
    check("файл прислан", ev.files, str(ev.replies)[:200])
    check("временный файл убран",
          not any(os.path.exists(f) for f in ev.files), str(ev.files))

    print("\n89. Имя файла экспорта безопасно")
    check("слэш не уводит в другую папку", "/" not in main._safe_name("а/б/../в"))
    check("пустое имя не ломает", main._safe_name("") == "chat")
    check("длина ограничена", len(main._safe_name("я" * 300)) <= 50)

    print("\n90. Статистика по одному чату")
    ev = CommandEvent("/stats Иван", sender_id=OWNER)
    await main.on_command(ev)
    await settle()
    body = " ".join(ev.replies)
    check("сводка пришла", "Иван" in body and "Всего сообщений" in body, body[:200])
    check("разделены свои и чужие", "твоих" in body, body[:200])

    print("\n91. Статистика без аргумента — по всему архиву (регрессия)")
    ev = CommandEvent("/stats", sender_id=OWNER)
    await main.on_command(ev)
    await settle()
    check("общая статистика работает",
          ev.replies and "В кэше" in ev.replies[0], str(ev.replies)[:150])

    print("\n92. Новые переключатели видны в меню и работают")
    kb = main.settings_keyboard()
    check("кнопка на каждый переключатель + пауза", len(kb) == len(main.TOGGLES) + 1,
          f"кнопок {len(kb)}")
    for key in ("transcribe", "downloader", "profile_watch", "online_watch"):
        check(f"переключатель {key} есть", key in main.TOGGLES)
    txt = main._settings_text()
    check("текстовый вид тоже полный",
          all(k in txt for k in main.TOGGLES), txt[:200])

    print("\n93. Кнопка «Подписки» маппится в команду")
    check("«🔔 Подписки» -> /subs", main.KEYBOARD_ACTIONS.get("🔔 Подписки") == "/subs")
    # ярлык лежит в b.button.text: сам b.text — это метод класса Button
    labels = [b.button.text for row in main.main_keyboard() for b in row]
    missing = [t for t in labels if not main.KEYBOARD_ACTIONS.get(t)]
    check("все кнопки клавиатуры имеют команду", not missing, f"без команды: {missing}")

    print("\n94. Меню команд без дублей и с описаниями")
    names = [c for c, _ in main.BOT_MENU]
    check("нет дублей", len(names) == len(set(names)), str([n for n in names if names.count(n) > 1]))
    check("все описания непустые и короткие",
          all(d.strip() and len(d) <= 60 for _, d in main.BOT_MENU))
    check("новые команды в меню",
          all(c in names for c in ("sub", "subs", "dl", "export")), str(names))

    print("\n95. Справка не разрослась в стену текста")
    lines = main.HELP_TEXT.splitlines()
    check("справка компактная", len(lines) <= 40, f"строк {len(lines)}")
    check("сказано про жесты без команд",
          "кинь боту" in main.HELP_TEXT.lower() or "без команд" in main.HELP_TEXT.lower())

    print("\n96. Расшифровка не блокирует приём: очередь имеет потолок")
    _tr.transcribe = orig_tr
    real = TMP / "queue-probe.ogg"; real.write_bytes(b"OggS" + b"0" * 100)
    _cfg.TRANSCRIBE_QUEUE_MAX = 0
    text, err = await _tr.transcribe(str(real))
    check("переполненная очередь отвечает отказом, а не виснет",
          text is None and "очеред" in (err or ""), f"{text} / {err}")
    _cfg.TRANSCRIBE_QUEUE_MAX = 10
    text, err = await _tr.transcribe("/нет/такого/файла.ogg")
    check("пропавший файл — понятная ошибка", text is None and "нет" in (err or ""),
          f"{text} / {err}")
    _tr.transcribe = orig_tr


    # =============== регрессии по находкам аудита ===============

    print("\n97. Экспорт берёт САМЫЕ СВЕЖИЕ сообщения, а не самые старые (регрессия)")
    EXP = 990001
    main.db.set_chat(EXP, "private", "Экспортный", None, True)
    for i in range(10):
        add(60000 + i, chat_id=EXP, text=f"сообщение номер {i}", ago=(20 - i) * 3600)
    outp2 = TMP / "exp2.html"
    res = _ex.write_html(EXP, "Экспортный", str(outp2), limit=3)
    htm = outp2.read_text(encoding="utf-8")
    check("взяты последние три", all(f"сообщение номер {i}" in htm for i in (7, 8, 9)),
          "взялись старые вместо свежих")
    check("самые старые не попали", "сообщение номер 0" not in htm)
    check("порядок хронологический",
          htm.index("номер 7") < htm.index("номер 8") < htm.index("номер 9"))
    check("усечение честно помечено", res["truncated"] and res["total"] == 10,
          str(res))
    check("в самом файле написано, что показана часть", "из 10" in htm, htm[-300:])

    print("\n98. Сводка о чистке имеет потолок строк (защита от FloodWait)")
    rows_many = [main.db.get(200 + (i % 6)) for i in range(6)]
    rows_many = [r for r in rows_many if r]
    big = rows_many * 40           # 240 «удалённых» строк
    body = alerts.build_purge_alert("Чат", big, [], int(time.time()), 30)
    n_lines = body.count("\n")
    check("строк не больше потолка + служебные",
          n_lines <= alerts.MAX_PURGE_LINES + 12, f"строк {n_lines}")
    check("сказано, сколько скрыто", "и ещё" in body, body[-300:])
    check("общее число всё равно верное", "Удалено разом: 240" in body, body[:200])

    print("\n99. Отправка не разваливается на десятки сообщений")
    fake = main.client = FakeClient()
    main.bot = None
    await main.notify("строка\n" * 20000)     # ~140 КБ
    check("отправок не больше потолка", len(fake.sent) <= alerts.MAX_CHUNKS,
          f"отправок {len(fake.sent)} (потолок {alerts.MAX_CHUNKS})")
    check("предупреждение об урезании есть",
          any("не отправлено" in (x["text"] or "") for x in fake.sent))

    print("\n100. split_text режет верно и не теряет текст")
    src = "\n".join(f"строка {i}" for i in range(2000))
    parts = list(alerts.split_text(src))
    check("склейка даёт исходный текст", "\n".join(parts) == src,
          f"было {len(src)}, стало {len(chr(10).join(parts))}")
    check("каждый кусок в пределах лимита",
          all(len(p) <= alerts.MAX_TEXT for p in parts), str([len(p) for p in parts]))
    check("короткий текст не режется", list(alerts.split_text("привет")) == ["привет"])

    print("\n101. /archive помнит позицию и не шлёт одно и то же дважды")
    class AMsg:
        def __init__(self, mid): self.id = mid; self.media = object()
    class ArchiveClient(FakeClient):
        def __init__(self, msgs):
            super().__init__(); self.msgs = msgs; self.forwarded = []
        def iter_messages(self, chat_id, **kw):
            off, lo = kw.get("offset_id") or 0, kw.get("min_id") or 0
            sel = sorted([m for m in self.msgs
                          if (off == 0 or m.id < off) and m.id > lo],
                         key=lambda m: -m.id)
            async def gen():
                for m in sel:
                    yield m
            return gen()
        async def forward_messages(self, dest, msg):
            self.forwarded.append(msg.id)

    ARCH = 991001
    _cfg.ARCHIVE_DELAY_SEC = 0
    _cfg.ARCHIVE_MAX_PER_RUN = 3
    _cfg.STORAGE_CHAT_ID = -100555
    main.db.db.execute("DELETE FROM meta WHERE key LIKE 'archive:%'"); main.db.db.commit()
    msgs = [AMsg(i) for i in range(1, 11)]
    cli = main.client = ArchiveClient(msgs)
    ev = CommandEvent("x", sender_id=OWNER)
    await main.archive_media(ARCH, "Архивный", ev)
    first = list(cli.forwarded)
    check("первый прогон взял свою порцию", len(first) == 3, str(first))
    check("сказано про продолжение", any("продолж" in r for r in ev.replies),
          str(ev.replies)[:150])

    cli = main.client = ArchiveClient(msgs)
    ev = CommandEvent("x", sender_id=OWNER)
    await main.archive_media(ARCH, "Архивный", ev)
    second = list(cli.forwarded)
    check("второй прогон продолжил, а не повторил",
          not (set(first) & set(second)), f"пересечение: {set(first) & set(second)}")
    check("порция снова получена", len(second) == 3, str(second))

    # домотаем до конца
    for _ in range(4):
        main.client = ArchiveClient(msgs)
        await main.archive_media(ARCH, "Архивный", CommandEvent("x", sender_id=OWNER))
    cli = main.client = ArchiveClient(msgs)
    ev = CommandEvent("x", sender_id=OWNER)
    await main.archive_media(ARCH, "Архивный", ev)
    check("дойдя до конца, повторно ничего не шлёт", not cli.forwarded,
          f"переслал {cli.forwarded}")
    _cfg.STORAGE_CHAT_ID = 0

    print("\n102. Двойная расшифровка одного сообщения не происходит")
    calls2 = []
    async def slow_tr(path):
        calls2.append(path)
        await asyncio.sleep(0.15)
        return ("текст", None)
    _tr.transcribe = slow_tr
    fake = main.client = FakeClient()
    v = media / "7001.ogg"; v.write_bytes(b"OggS" + b"0" * 100)
    add(7001, text=None, media_type="голосовое", media_path=str(v))
    r = main.db.get(7001, 777)
    t1 = main.spawn(main.run_transcript(r, "голосовое"))
    t2 = main.spawn(main.run_transcript(r, "голосовое"))
    await asyncio.gather(t1, t2)
    check("расшифровка запущена один раз", len(calls2) == 1, f"запусков {len(calls2)}")
    check("алерт с текстом один",
          sum("текст" in (x["text"] or "") for x in fake.sent) == 1, str(len(fake.sent)))

    print("\n103. Готовый транскрипт не пересчитывается")
    calls3 = []
    async def tr3(path):
        calls3.append(path); return ("новый", None)
    _tr.transcribe = tr3
    r = main.db.get(7001, 777)
    await main.run_transcript(r, "голосовое")
    check("повторно не считали", not calls3, f"вызовов {len(calls3)}")
    _tr.transcribe = orig_tr

    print("\n104. Проверка ссылки закрывается при сбое DNS, а не открывается")
    # Без сети: резолвер подменён. Публичный адрес для живого хоста, сбой DNS
    # для .invalid — ровно то, что ответил бы настоящий резолвер.
    import socket as _sock
    _loop = asyncio.get_running_loop()
    _orig_gai = _loop.getaddrinfo
    async def _fake_gai(host, *a, **kw):
        if host.endswith(".invalid"):
            raise _sock.gaierror(-2, "Name or service not known")
        return [(_sock.AF_INET, _sock.SOCK_STREAM, 6, "", ("93.184.215.14", 0))]
    _loop.getaddrinfo = _fake_gai
    try:
        bad = await _dl.check_url_resolved("https://совершенно-несуществующий-хост.invalid/x")
        check("несуществующий хост отбит", bad is not None, str(bad))
        ok = await _dl.check_url_resolved("https://example.com/video")
        check("живой хост проходит", ok is None, str(ok))
    finally:
        _loop.getaddrinfo = _orig_gai

    print("\n105. Дайджест шлётся порциями, а не одной простынёй")
    main.db.db.execute("DELETE FROM digest_queue"); main.db.db.commit()
    for i in range(95):
        main.db.enqueue_digest("delete", 777, f"<b>событие {i}</b>")
    fake = main.client = FakeClient()
    await main.flush_digest()
    check("отправлено несколькими порциями", 2 <= len(fake.sent) <= main.DIGEST_MAX_BATCHES,
          f"отправок {len(fake.sent)}")
    check("очередь очищена", main.db.digest_count() == 0,
          f"осталось {main.db.digest_count()}")
    check("в шапке видно, сколько ещё ждёт",
          any("в очереди" in (x["text"] or "") for x in fake.sent))

    print("\n106. Онлайн: «вышел» без парного «зашёл» не шлётся")
    main.db.db.execute("DELETE FROM online_state"); main.db.db.commit()
    main.sub_set("online", 777, True, "Иван Петров")
    fake = main.client = FakeClient()
    await main.on_user_update(StatusEvent(777, True))       # первый заход — сообщаем
    check("о заходе сообщили", len(fake.sent) == 1, f"{len(fake.sent)}")
    await main.on_user_update(StatusEvent(777, False))      # и о выходе
    check("о выходе сообщили", len(fake.sent) == 2, f"{len(fake.sent)}")
    fake = main.client = FakeClient()
    await main.on_user_update(StatusEvent(777, True))       # заход подавлен антиспамом
    await main.on_user_update(StatusEvent(777, False))
    check("подавленный заход не даёт «вышел»", not fake.sent,
          f"пришло {len(fake.sent)} — «вышел» без «зашёл»")
    main.sub_set("online", 777, False)

    print("\n107. На паузе смена профиля не теряется")
    main.db.db.execute("DELETE FROM profile_state"); main.db.db.commit()
    main.sub_set("profile", 777, True, "Иван Петров")
    u_a = _U(id=777, first_name="Иван", username="ivan", access_hash=0)
    u_a.photo = PhotoStub(1)
    main.client = ProfClient(u_a)
    await main._check_profiles()                       # снимок
    main.db.set_meta("paused", "1")
    u_b = _U(id=777, first_name="Иван", username="ivan_paused", access_hash=0)
    u_b.photo = PhotoStub(1)
    cli = main.client = ProfClient(u_b)
    await main._check_profiles()
    check("на паузе молчим", not cli.sent, f"пришло {len(cli.sent)}")
    check("снимок НЕ перезаписан на паузе",
          main.db.get_profile_state(777)["username"] == "ivan",
          "снимок обновился молча — изменение потеряно навсегда")
    main.db.set_meta("paused", "0")
    cli = main.client = ProfClient(u_b)
    await main._check_profiles()
    check("после снятия паузы изменение всё-таки приехало",
          cli.sent and "ivan_paused" in cli.sent[0]["text"], str(cli.sent)[:150])
    main.sub_set("profile", 777, False)

    print("\n108. Подписки листаются, счётчик честный")
    main.db.db.execute("DELETE FROM subs"); main.db.db.commit()
    for i in range(11):
        main.db.sub_add("voice", 800000 + i, f"Человек {i}")
    main.reload_subs()
    check("счётчик = реальное число", main.db.sub_count() == 11, str(main.db.sub_count()))
    kb1 = main.all_subs_keyboard(0)
    lbl = lambda b: getattr(b, 'button', b).text   # inline — сразу объект, text — обёртка
    labels1 = [lbl(b) for row in kb1 for b in row]
    check("на странице не больше SUBS_PAGE людей",
          sum("убрать" in l for l in labels1) == main.SUBS_PAGE, str(len(labels1)))
    check("есть кнопка «ещё»", any("Ещё" in l for l in labels1), str(labels1))
    check("есть кнопка добавления", any("Добавить" in l for l in labels1), str(labels1))
    kb2 = main.all_subs_keyboard(main.SUBS_PAGE)
    labels2 = [lbl(b) for row in kb2 for b in row]
    check("вторая страница показывает остальных",
          sum("убрать" in l for l in labels2) == 3, str(labels2))
    check("на второй странице есть «назад»", any("Назад" in l for l in labels2))
    check("в шапке настоящее число", "11" in main.subs_header(11, 0),
          main.subs_header(11, 0))
    main.db.db.execute("DELETE FROM subs"); main.db.db.commit()
    main.reload_subs()

    print("\n109. Имя человека не превращается в число после нажатия кнопки")
    main.db.sub_add("voice", 555000, "Дуров")
    main.reload_subs()
    check("имя берётся из подписки", main.peer_title(555000) == "Дуров",
          main.peer_title(555000))
    main.remember_title(556000, "Аня из пикера")
    check("имя из показанной карточки помнится",
          main.peer_title(556000) == "Аня из пикера", main.peer_title(556000))
    check("id не подставляется как имя", main.peer_title(777) == "Иван Петров",
          main.peer_title(777))
    main.db.db.execute("DELETE FROM subs"); main.db.db.commit()
    main.reload_subs()

    print("\n110. /read понимает имя из нескольких слов")
    main.db.set_chat(992001, "private", "Вася Петров", None, True)
    ev = CommandEvent("/read Вася Петров 5", sender_id=OWNER)
    seen_read = {}
    orig_rh = main.read_history
    async def fake_rh(cid, title, n, e): seen_read.update(cid=cid, n=n)
    main.read_history = fake_rh
    await main.on_command(ev)
    await settle()
    check("нашёлся чат по полному имени", seen_read.get("cid") == 992001,
          f"{seen_read} / {ev.replies}")
    check("число прочитано отдельно от имени", seen_read.get("n") == 5, str(seen_read))
    main.read_history = orig_rh

    print("\n111. Пикер людей: /read и /sub без аргумента дают кнопки")
    ev = CommandEvent("/read", sender_id=OWNER)
    await main.on_command(ev)
    check("кнопки-люди показаны", ev.buttons, str(ev.replies)[:120])
    picks = [b.data.decode() for row in ev.buttons for b in row]
    check("кнопки ведут на выбор человека",
          all(p.startswith("pick:read:") for p in picks), str(picks[:3]))

    seen_pick = {}
    async def fake_rh2(cid, title, n, e): seen_pick.update(cid=cid, title=title)
    main.read_history = fake_rh2
    cb = CallbackEvent(picks[0], sender_id=OWNER)
    await main.on_bot_callback(cb)
    await settle()
    check("нажатие кнопки запускает чтение", "cid" in seen_pick, str(seen_pick))
    check("имя передано, а не число", not str(seen_pick.get("title","")).lstrip("-").isdigit(),
          str(seen_pick))
    main.read_history = orig_rh

    print("\n112. Чистка режет историю порциями, а не одним ударом")
    import cache as _cache
    check("порция задана и разумна", 0 < _cache.CLEANUP_CHUNK <= 20000,
          str(_cache.CLEANUP_CHUNK))
    check("есть потолок за прогон", _cache.CLEANUP_MAX_ROWS > 0)
    cfg.DB_RETENTION_DAYS = 1
    for i in range(25):
        add(75000 + i, chat_id=EXP, text=f"древнее {i}", ago=100 * 86400)
    old_chunk = _cache.CLEANUP_CHUNK
    _cache.CLEANUP_CHUNK = 10          # мелкая порция, чтобы проверить цикл
    st = main.db.cleanup()
    check("удалило всё, что попало под срок, за несколько порций",
          st["rows"] >= 25, str(st))
    check("свежее не тронуто", main.db.get(60009, EXP) is not None)
    _cache.CLEANUP_CHUNK = old_chunk
    cfg.DB_RETENTION_DAYS = 0

    print("\n113. Фоновая задача не проглатывает исключение")
    import logging as _lg
    caught = []
    class Grab(_lg.Handler):
        def emit(self, rec): caught.append(rec.getMessage())
    h = Grab(); main.log.addHandler(h)
    async def boom(): raise ValueError("нарочно")
    t = main.spawn(boom())
    await asyncio.sleep(0.05)
    main.log.removeHandler(h)
    check("падение задачи попало в лог",
          any("упала" in m for m in caught), str(caught))

    print("\n114. Служебные файлы подметаются")
    trd = config.MEDIA_DIR / "tr"; trd.mkdir(parents=True, exist_ok=True)
    std = config.MEDIA_DIR / "stories"; std.mkdir(parents=True, exist_ok=True)
    old_tr = trd / "старый.ogg"; old_tr.write_bytes(b"x")
    old_st = std / "старая.jpg"; old_st.write_bytes(b"x")
    fresh_tr = trd / "свежий.ogg"; fresh_tr.write_bytes(b"x")
    past = time.time() - 10 * 3600
    os.utime(old_tr, (past, past)); os.utime(old_st, (past, past))
    n = main._sweep_tmp_dirs()
    check("старые копии для расшифровки убраны", not old_tr.exists())
    check("старые файлы историй тоже убраны (раньше копились вечно)",
          not old_st.exists())
    check("свежие не тронуты", fresh_tr.exists())
    check("посчитано верно", n == 2, str(n))


    # =============== вторая волна: отчёт пользователя ===============

    print("\n115. Пост из нескольких фото уходит целиком, а не одной картинкой")
    manyjob = TMP / "job-many"; manyjob.mkdir(exist_ok=True)
    pics = []
    for i in range(4):
        f = TMP / f"pic{i}.jpg"
        f.write_bytes(b"\xff\xd8\xff" + b"0" * (100 * (i + 1)))   # разного размера
        pics.append(str(f))
    async def multi_fetch(url, audio_only=False, on_progress=None, job_id=None, height=None):
        return {"paths": pics, "title": "Пост с каруселью", "job": manyjob,
                "extra": 0, "error": None}
    _dl.fetch = multi_fetch
    ev = CommandEvent("/dl https://instagram.com/p/ABC/", sender_id=OWNER)
    await main.on_command(ev)
    await wait_for(lambda: bool(ev.files))
    sent_files = ev.files[0] if ev.files else []
    check("ушли ВСЕ файлы, а не самый большой",
          isinstance(sent_files, list) and len(sent_files) == 4, str(ev.files))
    check("в подписи указано количество",
          any("файлов: 4" in r for r in ev.replies), str(ev.replies)[:200])
    _dl.fetch = orig_fetch

    print("\n116. Один файл по-прежнему уходит одним файлом, не списком")
    _dl.fetch = fake_fetch
    ev = CommandEvent("/dl https://vm.tiktok.com/ZS1/", sender_id=OWNER)
    await main.on_command(ev)
    await wait_for(lambda: bool(ev.files))
    check("одиночный файл — строкой, а не списком",
          ev.files and isinstance(ev.files[0], str), str(ev.files))
    _dl.fetch = orig_fetch

    print("\n117. Ошибка TikTok больше не врёт про «требует вход»")
    tiktok_err = ("ERROR: [TikTok] 123: Unexpected response from webpage request; "
                  "Confirm you are on the latest version using yt-dlp -U")
    msg117 = _dl._explain(tiktok_err)
    check("названа настоящая причина — блокировка адреса",
          "не пускает адрес сервера" in msg117, msg117[:160])
    check("сказано, что делать — прокси", "proxies.txt" in msg117, msg117[:200])
    # Блокируется адрес, а не отсутствие входа: обещать починку куками — врать
    check("куки не обещают как лечение",
          "не помогут" in msg117, msg117[:200])
    check("совет обновиться не принят за требование входа",
          "требует вход" not in msg117, msg117[:160])
    yt_err = "ERROR: Sign in to confirm you're not a bot. Use --cookies"
    check("капча YouTube распознана",
          "робот" in _dl._explain(yt_err), _dl._explain(yt_err)[:120])
    check("размер объясняется отдельно",
          "МБ" in _dl._explain("File is larger than max-filesize"))

    print("\n118. Куки: принимаются, проверяются на формат")
    NETSCAPE = ("# Netscape HTTP Cookie File\n"
                ".tiktok.com\tTRUE\t/\tTRUE\t1900000000\tsessionid\tabc123\n"
                ".tiktok.com\tTRUE\t/\tTRUE\t1900000000\tttwid\txyz789\n")
    check("формат Netscape распознан", _dl.looks_like_cookies(NETSCAPE))
    check("шапки нет, но табы есть — тоже принимаем",
          _dl.looks_like_cookies(
              ".a.com\tTRUE\t/\tTRUE\t1\tk\tv\n.b.com\tTRUE\t/\tTRUE\t1\tk2\tv2\n"))
    check("посторонний текст отбит", not _dl.looks_like_cookies("привет, как дела"))
    check("HTML-страница отбита", not _dl.looks_like_cookies("<html><body>hi</body></html>"))

    class FileMsg:
        class _F:
            def __init__(self, name, size): self.name = name; self.size = size
        def __init__(self, name, body, size=None):
            self.file = self._F(name, size if size is not None else len(body))
            self.document = object()
            self.voice = self.video_note = self.audio = None
            self.id = 1
            self._body = body
        async def download_media(self, file=None):
            Path(file).write_text(self._body, encoding="utf-8")
            return file

    ev = BotEvent("", FileMsg("cookies.txt", NETSCAPE), sender_id=OWNER)
    await main.on_bot_command(ev)
    # Файл tiktok-овский, значит и лечь должен под tiktok, а не в общий cookies.txt
    tj = _dl.COOKIES_PATH.parent / "cookies.tiktok.txt"
    check("куки приняты", tj.exists(), str(ev.replies)[:150])
    check("бот назвал площадку", any("tiktok.com" in r for r in ev.replies),
          str(ev.replies)[:200])
    check("сказано, сколько записей", any("записей: 2" in r for r in ev.replies),
          str(ev.replies)[:200])
    check("предупредили, что это доступ к аккаунтам",
          any("пароли" in r for r in ev.replies), str(ev.replies)[:300])
    check("права только владельцу",
          oct(os.stat(tj).st_mode)[-3:] == "600", oct(os.stat(tj).st_mode)[-3:])
    check("загрузчик их подхватил для TikTok",
          _dl.cookies_file("https://www.tiktok.com/@a/video/1") == str(tj))
    check("для другой площадки они не подставляются",
          _dl.cookies_file("https://www.instagram.com/p/a/") is None)

    ev = BotEvent("", FileMsg("cookies.txt", "это точно не куки"), sender_id=OWNER)
    await main.on_bot_command(ev)
    check("мусор вместо куков отбит",
          any("не похоже" in r for r in ev.replies), str(ev.replies)[:150])
    check("старые куки при этом не затёрты", tj.exists())

    ev = BotEvent("", FileMsg("cookies.txt", NETSCAPE), sender_id=999999)
    await main.on_bot_command(ev)
    check("чужому куки не принять", not ev.replies, str(ev.replies))

    ev = CommandEvent("/cookies", sender_id=OWNER)
    await main.on_command(ev)
    check("/cookies показывает, что они есть",
          ev.replies and "на месте" in ev.replies[0], str(ev.replies)[:150])
    ev = CommandEvent("/cookies off", sender_id=OWNER)
    await main.on_command(ev)
    check("/cookies off удаляет", not _dl.COOKIES_PATH.exists(), str(ev.replies))
    check("и файлы площадок тоже", not tj.exists(), str(list(tj.parent.glob("cookies*"))))

    print("\n119. /voice — скрытое прослушивание с расшифровкой")
    class VMsg:
        def __init__(self, mid, out=False):
            self.id = mid; self.out = out
            self.media = object()
            self.date = __import__("datetime").datetime.now(
                __import__("datetime").timezone.utc)
        async def download_media(self, file=None):
            Path(file).parent.mkdir(parents=True, exist_ok=True)
            Path(file).write_bytes(b"OggS" + b"0" * 60)
            return file

    class VoiceClient(FakeClient):
        def __init__(self, msgs): super().__init__(); self.msgs = msgs; self.read_marks = []
        def iter_messages(self, chat_id, limit=None):
            async def gen():
                for m in self.msgs: yield m
            return gen()
        async def send_read_acknowledge(self, *a, **k):
            self.read_marks.append(a)   # если позовут — тест это заметит

    orig_mk = main.media_kind
    main.media_kind = lambda m: "голосовое"
    _tr.transcribe = fake_transcribe("привет это голосовое")
    cli = main.client = VoiceClient([VMsg(9001), VMsg(9002)])
    ev = CommandEvent("x", sender_id=OWNER)
    await main.listen_secretly(777, "Иван Петров", ev, count=1)
    blob = " ".join(ev.replies)
    check("файл прислан", ev.files, str(ev.replies)[:150])
    check("расшифровка приложена", "привет это голосовое" in blob, blob[:200])
    check("сказано, что он не узнает", "не увидит" in blob, blob[:200])
    check("«прослушано» НЕ отправлено", not cli.read_marks, str(cli.read_marks))
    check("временный файл убран",
          not any(os.path.exists(f) for f in ev.files if isinstance(f, str)),
          str(ev.files))

    ev = CommandEvent("x", sender_id=OWNER)
    main.client = VoiceClient([])
    await main.listen_secretly(777, "Иван Петров", ev, count=1)
    check("если голосовых нет — понятный ответ",
          ev.replies and "не нашёл" in ev.replies[0], str(ev.replies))
    main.media_kind = orig_mk
    _tr.transcribe = orig_tr

    print("\n120. Подписка на профиль сразу снимает точку отсчёта")
    main.db.db.execute("DELETE FROM profile_state"); main.db.db.commit()
    u_snap = _U(id=777, first_name="Иван", username="ivan_snap", access_hash=0)
    u_snap.photo = PhotoStub(777001)
    main.client = ProfClient(u_snap)
    main.sub_set("profile", 777, True, "Иван Петров")
    await asyncio.sleep(0.1)          # snapshot_profile идёт фоновой задачей
    st = main.db.get_profile_state(777)
    check("снимок снят сразу при подписке, а не через час",
          st is not None and st["username"] == "ivan_snap",
          "без этого смена профиля до первого опроса терялась навсегда")
    check("id фото тоже запомнен", st and st["photo_id"] == 777001, str(dict(st) if st else None))

    print("\n121. Апдейт от Telegram проверяет профиль мгновенно")
    class RawEv:
        def __init__(self, uid): self.user_id = uid
    u_chg = _U(id=777, first_name="Иван", username="ivan_changed", access_hash=0)
    u_chg.photo = PhotoStub(777002)
    cli = main.client = ProfClient(u_chg)
    await main.on_profile_hint(RawEv(777))
    await asyncio.sleep(0.15)
    body = cli.sent[0]["text"] if cli.sent else ""
    check("изменение поймано без ожидания опроса", "ivan_changed" in body, body[:200])
    check("показано и старое значение", "ivan_snap" in body, body[:200])

    cli = main.client = ProfClient(u_chg)
    await main.on_profile_hint(RawEv(999888))     # не подписан
    check("на неподписанного не реагируем", not cli.sent, str(cli.sent))
    main.sub_set("profile", 777, False)

    print("\n122. Убранные из меню команды продолжают работать")
    menu_names = [c for c, _ in main.BOT_MENU]
    for gone in ("read", "archive", "digest", "cleanup"):
        check(f"/{gone} убран из меню", gone not in menu_names, str(menu_names))
    check("/voice в меню есть", "voice" in menu_names)
    check("/cookies в меню есть", "cookies" in menu_names)
    # но обработчики на месте
    main.read_history = fake_rh2
    ev = CommandEvent("/read Иван 5", sender_id=OWNER)
    await main.on_command(ev)
    await settle()
    check("/read всё ещё выполняется", "cid" in seen_pick, str(seen_pick))
    main.read_history = orig_rh
    ev = CommandEvent("/cleanup", sender_id=OWNER)
    await main.on_command(ev)
    check("/cleanup всё ещё выполняется",
          ev.replies and "ычищено" in ev.replies[0], str(ev.replies))
    check("в справке сказано, что они остались рабочими",
          "убраны из меню" in main.HELP_TEXT, main.HELP_TEXT[-250:])


    print("\n123. Шкала скачивания рисуется и считает проценты")
    r = main.render_progress({"done": 5_000_000, "total": 10_000_000,
                              "speed": 2_000_000, "eta": 3, "percent": 50.0})
    check("процент показан", "50%" in r, r)
    check("шкала наполовину", r.count("▰") == 5 and r.count("▱") == 5, r)
    check("объём человеческий", "4.8 МБ" in r and "9.5 МБ" in r, r)
    check("скорость показана", "/с" in r, r)
    check("осталось времени показано", "осталось" in r, r)
    r0 = main.render_progress({"done": 1000, "total": None, "speed": None,
                               "eta": None, "percent": None})
    check("размер неизвестен — без процентов, но без падения",
          "▱" in r0 and "%" not in r0, r0)
    check("человеческий размер: байты", main._human(512) == "512 Б", main._human(512))
    check("человеческий размер: гигабайты",
          main._human(3 * 1024**3).endswith("ГБ"), main._human(3 * 1024**3))
    check("неизвестный размер не роняет", main._human(None) == "?")

    print("\n124. Отмена скачивания: кнопка, коллбэк, результат")
    prog_seen = []
    async def slow_fetch(url, audio_only=False, on_progress=None, job_id=None, height=None):
        for pct in (10, 40, 70):
            if on_progress:
                await on_progress({"done": pct * 1000, "total": 100000,
                                   "speed": 500000, "eta": 5, "percent": float(pct)})
            await asyncio.sleep(0.02)
        prog_seen.append(job_id)
        return {"error": "отменено", "cancelled": True}
    _dl.fetch = slow_fetch
    main.PROGRESS_EVERY = 0        # в тесте не троттлим
    ev = CommandEvent("/dl https://example.com/big.mp4", sender_id=OWNER)
    await main.on_command(ev)
    await asyncio.sleep(0.5)
    check("сообщение-шкала создано", ev.sent_msgs, str(ev.replies))
    check("под ним кнопка «Отмена»",
          any("Отмена" in getattr(b, "text", "") for row in ev.buttons for b in row),
          str(ev.buttons))
    edits = [e for m in ev.sent_msgs for e in m.edits]
    check("шкала обновлялась по ходу", any("Качаю" in e for e in edits), str(edits)[:200])
    check("итог — «отменено»", any("Отменено" in e for e in edits), str(edits)[:200])
    main.PROGRESS_EVERY = 4.0
    _dl.fetch = orig_fetch

    print("\n125. Коллбэк отмены прибивает нужную загрузку")
    killed = []
    orig_cancel = _dl.cancel
    _dl.cancel = lambda jid: (killed.append(jid), True)[1]
    cb = CallbackEvent("dlstop:777123", sender_id=OWNER)
    await main.on_bot_callback(cb)
    check("отменили именно тот номер", killed == [777123], str(killed))
    check("пользователю ответили", cb.answered and "тмен" in cb.answered[0],
          str(cb.answered))
    _dl.cancel = lambda jid: False
    cb = CallbackEvent("dlstop:999", sender_id=OWNER)
    await main.on_bot_callback(cb)
    check("уже завершённую — мягкий ответ",
          cb.answered and "закончилась" in cb.answered[0], str(cb.answered))
    cb = CallbackEvent("dlstop:абв", sender_id=OWNER)
    await main.on_bot_callback(cb)
    check("битый номер не роняет", cb.answered, str(cb.answered))
    killed.clear()
    cb = CallbackEvent("dlstop:1", sender_id=999999)
    await main.on_bot_callback(cb)
    check("чужой отменить не может", not killed, str(killed))
    _dl.cancel = orig_cancel

    print("\n124б. Отмена работает и до запуска процесса (очередь, DNS, TikTok)")
    # Кнопка «Отмена» показана сразу, а процесс yt-dlp появляется секунды спустя:
    # сперва резолв DNS, опрос TikTok, ожидание слота в очереди. Раньше нажатие
    # в это окно молча терялось — cancel() смотрел только на живой процесс,
    # отвечал «загрузка уже закончилась», и файл всё равно доезжал.
    _dl._active.clear(); _dl._cancelled.clear(); _dl._jobs.clear()
    check("незнакомый номер честно отвергается", _dl.cancel(4242) is False)
    _dl._active.add(4242)                      # fetch стартовал, процесса ещё нет
    check("стоящую в очереди отменить МОЖНО", _dl.cancel(4242) is True)
    check("намерение записано — fetch остановится сам", 4242 in _dl._cancelled)
    check("после отмены процесса так и не появилось", not _dl._jobs)
    _dl._active.clear(); _dl._cancelled.clear()

    # И то же самое сквозь настоящий fetch(): отменяем до всякого процесса
    async def never_exec(*a, **kw):
        raise AssertionError("процесс не должен был запуститься после отмены")
    orig_exec = _dl.asyncio.create_subprocess_exec
    _dl.asyncio.create_subprocess_exec = never_exec
    _dl._active.add(4343); _dl._cancelled.add(4343)
    _orig_cur = _dl.check_url_resolved       # без сети: DNS тут не проверяем
    async def _no_dns(u):
        return _dl.check_url(u)
    _dl.check_url_resolved = _no_dns
    try:
        res_c = await _dl.fetch("https://youtu.be/x", job_id=4343)
    finally:
        _dl.check_url_resolved = _orig_cur
    _dl.asyncio.create_subprocess_exec = orig_exec
    check("fetch увидел отмену и не полез качать", res_c.get("cancelled") is True,
          str(res_c)[:160])
    check("за собой прибрано", not _dl._active and 4343 not in _dl._cancelled)

    print("\n126. Истории-заглушки не выдают техническую ошибку")
    class StoryStub:
        """StoryItemSkipped/Deleted: поля media у них нет вовсе."""
        def __init__(self, sid, deleted=False):
            self.id = sid
            self.__class__.__name__ = "StoryItemDeleted" if deleted else "StoryItemSkipped"
    class RealStory:
        def __init__(self, sid, empty=False):
            self.id = sid
            self.media = object()
            self.caption = None
            self.date = __import__("datetime").datetime.now(
                __import__("datetime").timezone.utc)
            self.empty = empty
    class StoryClient2(FakeClient):
        def __init__(self, empty=False): super().__init__(); self.empty = empty
        async def download_media(self, media, file=None):
            Path(file).parent.mkdir(parents=True, exist_ok=True)
            Path(file + ".jpg").write_bytes(b"" if self.empty else b"\xff\xd8\xff" + b"0" * 50)
            return file + ".jpg"

    main.bot = None
    fake = main.client = StoryClient2()
    skipped = StoryStub(7, deleted=False)
    await main._send_story_items(555, [skipped, RealStory(8)], "Кто-то", "📌",
                                 CommandEvent("x", sender_id=OWNER))
    blob = " ".join(x["text"] or "" for x in fake.sent)
    check("про заглушку сказано по-человечески",
          "недоступна" in blob and "close_friends" not in blob and "attribute" not in blob,
          blob[:250])
    check("номер истории показан", "#7" in blob and "#8" in blob, blob[:250])
    check("нормальная история всё равно ушла файлом",
          any(x["kind"] == "file" for x in fake.sent), str(fake.sent)[:200])

    deleted_stub = StoryStub(9, deleted=True)
    fake = main.client = StoryClient2()
    await main._send_story_items(555, [deleted_stub], "Кто-то", "📌",
                                 CommandEvent("x", sender_id=OWNER))
    check("удалённая помечена как удалённая",
          "удалена" in (fake.sent[0]["text"] or ""), str(fake.sent)[:200])

    print("\n127. Пустой файл истории не уходит как битый")
    fake = main.client = StoryClient2(empty=True)
    await main._send_story_items(555, [RealStory(10)], "Кто-то", "📌",
                                 CommandEvent("x", sender_id=OWNER))
    check("вместо битого файла — объяснение",
          fake.sent and "пустым" in (fake.sent[0]["text"] or ""), str(fake.sent)[:250])
    check("файл при этом не отправлен",
          all(x["kind"] != "file" for x in fake.sent), str(fake.sent)[:200])
    check("на диске мусор не остался",
          not list((config.MEDIA_DIR / "stories").glob("555_10_*")),
          str(list((config.MEDIA_DIR / "stories").glob("555_*"))))

    print("\n128. Видео уходит видео, а не файлом с превью 1x1 (регрессия)")
    vid = TMP / "clip.mp4"
    import subprocess as _sp
    _sp.run(["ffmpeg", "-f", "lavfi", "-i", "testsrc=size=480x854:duration=2",
             "-c:v", "libx264", "-pix_fmt", "yuv420p", "-y", str(vid)],
            capture_output=True)
    if vid.exists():
        from util import video_meta as _vm
        meta = _vm(str(vid))
        check("ffprobe достаёт размеры и длительность",
              meta and meta[1] == 480 and meta[2] == 854, str(meta))
        vk = await alerts.video_kwargs(str(vid))
        check("отдаём Telegram supports_streaming", vk.get("supports_streaming") is True,
              str(vk))
        a = vk["attributes"][0]
        check("размеры не 1x1", (a.w, a.h) == (480, 854), f"{a.w}x{a.h}")
        check("длительность не нулевая", a.duration >= 1, str(a.duration))
        check("сам Telethon без нас этого не умеет",
              __import__("telethon").utils.get_attributes(str(vid))[0][-1].w == 1,
              "если Telethon научился — проверка больше не нужна")
    else:
        check("ffmpeg доступен для теста", False, "не смог собрать тестовый ролик")

    check("не-видео не получает видео-атрибутов",
          await alerts.video_kwargs(str(TMP / "queue-probe.ogg")) == {})
    check("пропавший файл не роняет", await alerts.video_kwargs("/нет/файла.mp4") == {})

    print("\n129. Ссылка TikTok узнаётся, ответ API разбирается")
    for u, want in (("https://www.tiktok.com/@a/video/123", True),
                    ("https://vm.tiktok.com/ZS1/", True),
                    ("https://tiktok.com/@a/video/1", True),
                    ("https://nottiktok.com/x", False),
                    ("https://youtube.com/watch?v=1", False)):
        check(f"{u[:40]} -> {'TikTok' if want else 'не TikTok'}",
              _dl.is_tiktok(u) is want)
    check("в аргументы попадает движок JS для YouTube",
          "--js-runtimes" in _dl._build_args(["https://x.com/v"], TMP, TMP / "m", False))
    check("h264 в приоритете над AV1",
          any("h264" in str(a) for a in
              _dl._build_args(["https://x.com/v"], TMP, TMP / "m", False)))
    # Номер, а не id: у прямой CDN-ссылки id — это вся строка запроса,
    # и имя файла не влезало в лимит файловой системы
    _args = _dl._build_args(["https://x.com/v"], TMP, TMP / "m", False)
    check("в имени файла есть нумерация — карусель не затирает сама себя",
          any("autonumber" in str(a) for a in _args), str(_args))
    check("длина имени ограничена", any(".60B" in str(a) for a in _args), str(_args))
    check("PO-токен подключается, если генератор собран",
          _dl.POT_SCRIPT.exists() ==
          any("bgutilscript" in str(a) for a in _args),
          f"генератор есть: {_dl.POT_SCRIPT.exists()}")

    print("\n130. Отказ видео-серверов объясняется адресом, а не куками")
    m403 = _dl._explain("ERROR: unable to download video data: HTTP Error 403: Forbidden")
    check("сказано про адрес", "адрес" in m403.lower(), m403[:150])
    check("названа настоящая лечилка — прокси", "DOWNLOAD_PROXY" in m403, m403[:200])
    check("НЕ советует куки (они тут ни при чём)",
          "пришли мне этот файл" not in m403, m403[:200])
    check("сказано, что другие площадки работают",
          "TikTok" in m403, m403[:200])

    print("\n131. Пул прокси: разбор списка, ротация, маскировка")
    (TMP / "proxies.txt").write_text(
        "# комментарий\n"
        "1.1.1.1:6001:user:p@ss\n"          # формат Webshare, пароль со спецсимволом
        "2.2.2.2:6002:u2:p2\n"
        "3.3.3.3:6003:u3:p3\n"
        "socks5://4.4.4.4:1080\n"           # готовый URL
        "\n", encoding="utf-8")
    _cfg.DOWNLOAD_PROXY = ""
    _dl._proxy_pool = None
    pool = _dl.proxy_pool()
    check("разобраны все строки, мусор пропущен", len(pool) == 4, str(len(pool)))
    check("формат Webshare превращён в URL",
          pool[0].startswith("http://user:") and "@1.1.1.1:6001" in pool[0], pool[0])
    check("спецсимвол в пароле экранирован", "p%40ss" in pool[0], pool[0])
    check("готовый URL взят как есть", "socks5://4.4.4.4:1080" in pool, str(pool))

    _dl._proxy_pos = 0
    _dl._last_good = None
    first = _dl._candidates()
    second = _dl._candidates()
    check("даётся столько адресов, сколько есть (но не больше потолка)",
          len(first) == min(_dl.PROXY_TRIES, len(pool)), str(len(first)))
    check("ротация сдвигает начало", first[0] != second[0], f"{first[0]} / {second[0]}")

    # Сработавший адрес запоминается: иначе каждая скачка снова угадывала бы
    # рабочий с нуля, а YouTube с разных адресов отвечает по-разному
    _dl.mark_good(pool[2])
    after = _dl._candidates()
    check("следующая попытка начинается с удачного адреса", after[0] == pool[2],
          f"{after[0]} вместо {pool[2]}")
    _dl._last_good = None

    check("логин и пароль маскируются",
          _dl.hide_proxy("http://user:secret@1.2.3.4:6000") == "http://***:***@1.2.3.4:6000")
    check("в маскированном нет пароля",
          "secret" not in _dl.hide_proxy("сбой http://user:secret@1.2.3.4:6000 тут"))

    print("\n132. Через прокси куки не отправляются (защита аккаунта)")
    (TMP / "cookies.txt").write_text(
        "# Netscape HTTP Cookie File\n.youtube.com\tTRUE\t/\tTRUE\t1\tSID\tx\n",
        encoding="utf-8")
    a_proxy = _dl._build_args(["https://x.com/v"], TMP, TMP / "m", False,
                              proxy="http://u:p@1.1.1.1:6001")
    check("прокси прокинут в yt-dlp",
          "--proxy" in a_proxy and "http://u:p@1.1.1.1:6001" in a_proxy, str(a_proxy[-6:]))
    check("куки при этом НЕ идут — иначе Google увидит вход из другой страны",
          "--cookies" not in a_proxy, str(a_proxy))
    a_direct = _dl._build_args(["https://x.com/v"], TMP, TMP / "m", False)
    check("без прокси куки используются", "--cookies" in a_direct, str(a_direct))
    check("без прокси и ключа --proxy нет", "--proxy" not in a_direct)
    (TMP / "cookies.txt").unlink()

    print("\n132б. Смысл ошибки зависит от того, были ли прокси")
    boterr = "ERROR: Sign in to confirm you are not a bot"
    m_direct = _dl._explain(boterr, used_proxy=False)
    m_proxy = _dl._explain(boterr, used_proxy=True)
    m_ck = _dl._explain(boterr, used_proxy=True, tried_cookies=True)
    check("без прокси — виноват наш адрес, даём ОБА пути (куки и прокси)",
          "адрес сервера" in m_direct and "cookies.txt" in m_direct
          and "proxies.txt" in m_direct, m_direct[:220])
    check("с прокси — виноват ролик, а не адрес",
          "именно этот ролик" in m_proxy, m_proxy[:140])
    check("с прокси предупреждаем про ОТДЕЛЬНЫЙ аккаунт",
          "отдельный" in m_proxy.lower(), m_proxy[:200])
    check("куки не подошли — говорим прямо",
          "не подошли" in m_ck, m_ck[:140])
    check("LOGIN_REQUIRED распознаётся как та же ситуация",
          "ролик" in _dl._explain("playability status: LOGIN_REQUIRED", used_proxy=True))

    print("\n133. Повтор с другого адреса — только когда он может помочь")
    for err, want in (("HTTP Error 403: Forbidden", True),
                      ("Sign in to confirm you're not a bot", True),
                      ("Unexpected response from webpage request", True),
                      ("HTTP Error 429", True),
                      ("Video unavailable", False),
                      ("File is larger than max-filesize", False),
                      ("Unsupported URL", False)):
        check(f"{err[:38]:40} -> {'повтор' if want else 'без повтора'}",
              _dl._worth_retry(err) is want)

    print("\n133б. Кэш скачанного: ключ, хранение, срок")
    k1 = main.dl_cache_key("https://youtu.be/abc?si=XYZ&t=42&utm_source=tg", None, False)
    k2 = main.dl_cache_key("https://youtu.be/abc", None, False)
    check("мусорные хвосты ссылки не делают её другой", k1 == k2, f"{k1} / {k2}")
    check("mp3 хранится отдельно от видео",
          main.dl_cache_key("https://youtu.be/abc", None, True) != k2)
    check("другое качество — другой ключ",
          main.dl_cache_key("https://youtu.be/abc", 1080, False) != k2)
    check("кривая ссылка не роняет ключ", bool(main.dl_cache_key("не ссылка", None, False)))

    main.db.db.execute("DELETE FROM dl_cache"); main.db.db.commit()
    check("пустой кэш ничего не отдаёт", main.db.dl_get(k2) is None)
    main.db.dl_put(k2, "BAADFILEID", "Смешное видео", 5_000_000)
    hit = main.db.dl_get(k2)
    check("записалось и читается", hit and hit["file_id"] == "BAADFILEID", str(dict(hit) if hit else None))
    check("имя и размер сохранены",
          hit["title"] == "Смешное видео" and hit["size"] == 5_000_000)
    st = main.db.dl_stats()
    check("статистика считает", st["count"] == 1 and st["bytes"] == 5_000_000, str(st))
    main.db.dl_forget(k2)
    check("забывание работает", main.db.dl_get(k2) is None)

    # Протухшее не отдаём: контент могли снять с публикации
    main.db.dl_put(k2, "OLD", "Старое", 1)
    main.db.db.execute("UPDATE dl_cache SET ts=? WHERE key=?",
                       (int(time.time()) - 200 * 86400, k2))
    main.db.db.commit()
    _cfg.DOWNLOAD_CACHE_DAYS = 90
    check("просроченное не отдаётся", main.db.dl_get(k2) is None)
    _cfg.DOWNLOAD_CACHE_DAYS = 0
    check("при выключенном кэше срок не проверяется — но и не используется",
          main.db.dl_get(k2) is not None)
    _cfg.DOWNLOAD_CACHE_DAYS = 90
    main.db.db.execute("DELETE FROM dl_cache"); main.db.db.commit()

    print("\n133в. Повторная ссылка отдаётся мгновенно, без скачивания")
    downloads = []
    async def counting_fetch(url, audio_only=False, on_progress=None, job_id=None, height=None):
        downloads.append(url)
        return {"paths": [str(dlfile)], "title": "Кэшируемое", "job": jobdir,
                "extra": 0, "error": None}
    _dl.fetch = counting_fetch
    _dl.discard = lambda j: None

    KEY = main.dl_cache_key("https://youtu.be/cached1", None, False)
    main.db.dl_put(KEY, "BAADCACHED", "Кэшируемое", 3_000_000)
    ev = CommandEvent("/dl https://youtu.be/cached1", sender_id=OWNER)
    await main.on_command(ev)
    await wait_for(lambda: bool(ev.files))
    check("файл отправлен из кэша", ev.files == ["BAADCACHED"], str(ev.files))
    check("НИЧЕГО не качалось", not downloads, str(downloads))
    check("человеку сказано, что это уже было",
          any("уже было скачано" in r for r in ev.replies), str(ev.replies)[:150])
    check("шкала прогресса не показывалась",
          not any("Качаю" in r or "Готовлюсь" in r for r in ev.replies),
          str(ev.replies)[:150])

    # Битый идентификатор — не тупик: чистим запись и качаем заново
    class BadFileEvent(CommandEvent):
        async def respond(self, text, **kw):
            if kw.get("file") == "BAADBROKEN":
                raise ValueError("файл больше не существует")
            return await CommandEvent.respond(self, text, **kw)
    main.db.dl_put(KEY, "BAADBROKEN", "Битое", 1000)
    downloads.clear()
    ev = BadFileEvent("/dl https://youtu.be/cached1", sender_id=OWNER)
    await main.on_command(ev)
    await wait_for(lambda: bool(downloads))
    check("на битом кэше скачали заново", len(downloads) == 1, str(downloads))
    check("битая запись убрана", main.db.dl_get(KEY) is None)
    _dl.fetch = orig_fetch
    _dl.discard = orig_discard

    print("\n133г. Кнопки качества под результатом")
    rows = main.result_buttons(5, False, 720)
    labels = [b.text for row in rows for b in row]
    check("предложены другие качества", "1080p" in labels and "360p" in labels, str(labels))
    check("текущее качество не предлагается снова", "720p" not in labels, str(labels))
    check("кнопка mp3 на месте", any("mp3" in l for l in labels), str(labels))
    check("под аудио кнопок нет", main.result_buttons(5, True, 720) is None)

    picked = []
    async def note_dl(url, ev, audio_only=False, height=None):
        picked.append((url, height, audio_only))
    orig_do = main.do_download
    main.do_download = note_dl
    lid = main.remember_link("https://youtu.be/qq")
    await main.on_bot_callback(CallbackEvent(f"dlq:{lid}:1080", sender_id=OWNER))
    await settle()
    check("кнопка 1080p качает в 1080p", picked and picked[0][1] == 1080, str(picked))
    picked.clear()
    await main.on_bot_callback(CallbackEvent(f"dla:{lid}", sender_id=OWNER))
    await settle()
    check("кнопка mp3 качает звук", picked and picked[0][2] is True, str(picked))
    cb = CallbackEvent("dlq:99999999:1080", sender_id=OWNER)
    await main.on_bot_callback(cb)
    check("забытая ссылка — мягкий отказ", cb.answered and "забыл" in cb.answered[0],
          str(cb.answered))
    cb = CallbackEvent(f"dlq:{lid}:1080", sender_id=999999)
    picked.clear()
    await main.on_bot_callback(cb)
    check("чужой не может дёргать качество", not picked, str(picked))
    main.do_download = orig_do

    print("\n133д. Качество прокидывается в yt-dlp")
    a720 = _dl._build_args(["u"], TMP, TMP / "m", False, height=720)
    a360 = _dl._build_args(["u"], TMP, TMP / "m", False, height=360)
    check("720 попало в выбор формата", any("height<=720" in str(x) for x in a720))
    check("360 попало в выбор формата", any("height<=360" in str(x) for x in a360))
    adef = _dl._build_args(["u"], TMP, TMP / "m", False)
    check("без указания берётся значение по умолчанию",
          any(f"height<={_cfg.DOWNLOAD_QUALITY}" in str(x) for x in adef),
          f"ожидали {_cfg.DOWNLOAD_QUALITY}")
    check("для звука качество не навязывается",
          not any("height" in str(x) for x in
                  _dl._build_args(["u"], TMP, TMP / "m", True)))

    print("\n133е. Обрывок дорожки не выдаётся за готовое видео (регрессия)")
    check("f140.m4a опознан как обрывок", _dl._is_stream_leftover("001-_.f140.m4a"))
    check("f137.mp4 тоже", _dl._is_stream_leftover("Ролик.f137.mp4"))
    check("нормальное имя не трогаем", not _dl._is_stream_leftover("Ролик.mp4"))
    check("mp3 не обрывок", not _dl._is_stream_leftover("Песня.mp3"))
    check("цифры в названии не путают",
          not _dl._is_stream_leftover("Топ 140 моментов.mp4"))

    print("\n133ж. Выбор качества до скачивания")
    async def fake_probe(url):
        return {"title": "Тестовый ролик", "duration": 213, "error": None,
                "qualities": [{"step": 360, "height": 360, "size": 11_000_000},
                              {"step": 480, "height": 480, "size": 15_000_000},
                              {"step": 720, "height": 720, "size": 26_000_000},
                              {"step": 1080, "height": 1080, "size": 78_000_000}]}
    orig_probe = _dl.probe
    _dl.probe = fake_probe
    main.set_toggle("ask_quality", True)
    started = []
    async def note_start(url, ev, audio_only=False, height=None):
        started.append((url, height, audio_only))
    orig_do2 = main.do_download
    main.do_download = note_start

    ev = BotEvent("https://youtu.be/pickme", None, sender_id=OWNER)
    await main.on_bot_command(ev)
    await wait_for(lambda: bool(ev.buttons))
    labels = [b.text for row in ev.buttons for b in row]
    check("показаны все качества", sum(p in " ".join(labels) for p in
          ("360p", "480p", "720p", "1080p")) == 4, str(labels))
    check("на кнопках есть размер", any("МБ" in l for l in labels), str(labels))
    check("есть кнопка «только звук»", any("звук" in l for l in labels), str(labels))
    said = list(ev.replies) + [e for m in ev.sent_msgs for e in m.edits]
    check("название и длительность показаны",
          any("Тестовый ролик" in r and "3 мин" in r for r in said), str(said)[:200])
    check("до выбора ничего не качалось", not started, str(started))

    data = [b.data.decode() for row in ev.buttons for b in row if b.data.decode().startswith("dlq:")]
    await main.on_bot_callback(CallbackEvent(data[0], sender_id=OWNER))
    await settle()
    check("тык по кнопке запускает скачивание", started and started[0][1] == 360,
          str(started))

    print("\n133з. Когда спрашивать не о чем — не спрашиваем")
    started.clear()
    async def one_quality(url):
        return {"title": "Один вариант", "duration": 10, "error": None,
                "qualities": [{"step": 720, "height": 720, "size": 1000}]}
    _dl.probe = one_quality
    ev = BotEvent("https://youtu.be/single", None, sender_id=OWNER)
    await main.on_bot_command(ev)
    await wait_for(lambda: bool(started))
    check("единственное качество — качаем сразу", len(started) == 1, str(started))

    started.clear()
    _dl.probe = fake_probe
    ev = BotEvent("https://www.tiktok.com/@a/video/1", None, sender_id=OWNER)
    await main.on_bot_command(ev)
    await wait_for(lambda: bool(started))
    check("у TikTok выбора нет — не спрашиваем", len(started) == 1, str(started))

    started.clear()
    async def broken_probe(url):
        return {"error": "разведка не удалась"}
    _dl.probe = broken_probe
    ev = BotEvent("https://youtu.be/broken", None, sender_id=OWNER)
    await main.on_bot_command(ev)
    await wait_for(lambda: bool(started))
    check("разведка сломалась — всё равно качаем, а не сдаёмся",
          len(started) == 1, str(started))

    started.clear()
    _dl.probe = fake_probe
    main.set_toggle("ask_quality", False)
    ev = BotEvent("https://youtu.be/noask", None, sender_id=OWNER)
    await main.on_bot_command(ev)
    await wait_for(lambda: bool(started))
    check("переключатель выключает вопрос", len(started) == 1, str(started))
    check("переключатель виден в меню", "ask_quality" in main.TOGGLES)

    _dl.probe = orig_probe
    main.do_download = orig_do2

    print("\n133и. Оценка размеров берётся по h264 — как и качаем")
    info = {"formats": [
        {"height": 1080, "vcodec": "avc1.640028", "acodec": "none", "filesize": 78_000_000},
        {"height": 1080, "vcodec": "vp09.00.40.08", "acodec": "none", "filesize": 30_000_000},
        {"height": 720, "vcodec": "avc1.4d401f", "acodec": "none", "filesize": 26_000_000},
        {"height": None, "vcodec": "none", "acodec": "mp4a.40.2", "filesize": 3_000_000},
    ]}
    qs = {q["height"]: q["size"] for q in _dl.qualities_from(info)}
    check("1080p посчитан по h264, а не по лёгкому vp9",
          qs.get(1080) == 78_000_000 + 3_000_000, str(qs))
    check("звук прибавлен к отдельной видеодорожке",
          qs.get(720) == 26_000_000 + 3_000_000, str(qs))
    check("пустой ответ не роняет", _dl.qualities_from({}) == [])

    print("\n133к. Прокси тратится только там, где он нужен")
    _dl._proxy_pool = None
    (TMP / "proxies.txt").write_text("1.1.1.1:6001:u:p\n2.2.2.2:6002:u:p\n", "utf-8")
    _cfg.DOWNLOAD_PROXY = ""
    _dl.reload_proxies()
    _dl.clear_proxy_block()
    for u, want_proxy in (("https://www.youtube.com/watch?v=x", True),
                          ("https://youtu.be/x", True),
                          ("https://music.youtube.com/watch?v=x", True),
                          ("https://www.tiktok.com/@a/video/1", False),
                          ("https://instagram.com/p/A/", False),
                          ("https://example.com/v.mp4", False)):
        first = _dl._plan(u, False)[0][0]
        check(f"{u[:38]:40} -> {'через прокси' if want_proxy else 'напрямую'}",
              bool(first) is want_proxy, f"первый шаг: {first}")
    check("у YouTube есть запасной прямой путь",
          any(s[0] is None for s in _dl._plan("https://youtu.be/x", False)))
    check("у остальных прокси остаётся запасным",
          any(s[0] for s in _dl._plan("https://example.com/v.mp4", False)))

    print("\n133л. Отказ прокси по трафику распознаётся и не перебирается зря")
    for t, want in (("Tunnel connection failed: 402 Payment Required", True),
                    ("X-Webshare-Reason: bandwidthlimit", True),
                    ("407 Proxy Authentication Required", True),
                    ("HTTP Error 403: Forbidden", False),
                    ("Video unavailable", False)):
        check(f"{t[:42]:44} -> {'лимит' if want else 'обычная'}",
              _dl._proxy_exhausted(t) is want)

    _dl.clear_proxy_block()
    check("пока не упирались — прокси используются", _dl.proxies_usable())
    _dl._proxy_blocked_until = time.time() + 600
    check("после отказа прокси временно не трогаем", not _dl.proxies_usable())
    check("и план сразу идёт напрямую",
          _dl._plan("https://youtu.be/x", False)[0][0] is None)
    st = _dl.proxy_status()
    check("статус показывает паузу", st["blocked_sec"] > 0 and st["count"] == 2, str(st))
    _dl.clear_proxy_block()
    check("новый список снимает паузу", _dl.proxies_usable())

    m402 = _dl._explain("Tunnel connection failed: 402 Payment Required")
    check("человеку сказано про трафик, а не про робота",
          "кончился трафик" in m402, m402[:120])
    check("сказано, что остальное работает", "TikTok" in m402, m402[:200])
    check("сказано, как починить", "proxies.txt" in m402, m402[:250])

    print("\n133м. Один звук вместо видео — это неудача, а не результат")
    check("звуковые расширения перечислены", ".m4a" in _dl.AUDIO_EXT and ".mp3" in _dl.AUDIO_EXT)
    check("mp4 звуковым не считается", ".mp4" not in _dl.AUDIO_EXT)
    merr = _dl._explain("что-то\nвидеодорожка не скачалась")
    check("объяснение про недокачанное видео",
          "звуков" in merr and "видео не пришло" in merr, merr[:140])

    print("\n133н. Новый список прокси принимается файлом")
    _dl.PROXIES_PATH = TMP / "proxies_live.txt"
    _dl._proxy_pool = None
    ev = BotEvent("", FileMsg("Webshare_10_proxies.txt",
                              "9.9.9.9:7001:user:pass\n8.8.8.8:7002:user:pass\n"),
                  sender_id=OWNER)
    await main.on_bot_command(ev)
    check("список принят", _dl.PROXIES_PATH.exists(), str(ev.replies)[:150])
    check("сказано, сколько адресов", any("адресов: 2" in r for r in ev.replies),
          str(ev.replies)[:200])
    check("подхвачен без перезапуска", len(_dl.proxy_pool()) == 2, str(len(_dl.proxy_pool())))
    check("права только владельцу",
          oct(os.stat(_dl.PROXIES_PATH).st_mode)[-3:] == "600")

    ev = BotEvent("", FileMsg("proxies.txt", "это не список прокси"), sender_id=OWNER)
    await main.on_bot_command(ev)
    check("мусор отбит", any("не нашёл ни одного" in r for r in ev.replies),
          str(ev.replies)[:150])
    ev = BotEvent("", FileMsg("proxies.txt", "1.2.3.4:1:u:p\n"), sender_id=999999)
    await main.on_bot_command(ev)
    check("чужому нельзя", not ev.replies, str(ev.replies))

    ev = CommandEvent("/proxy", sender_id=OWNER)
    await main.on_command(ev)
    check("/proxy показывает состояние",
          ev.replies and "2" in ev.replies[0], str(ev.replies)[:150])
    ev = CommandEvent("/proxy reload", sender_id=OWNER)
    await main.on_command(ev)
    check("/proxy reload перечитывает",
          ev.replies and "Перечитал" in ev.replies[0], str(ev.replies)[:120])
    _dl.PROXIES_PATH = TMP / "proxies.txt"
    _dl._proxy_pool = None

    print("\n133п. YouTube: начинаем с того пути, что сработал в прошлый раз")
    _dl._proxy_pool = None
    (TMP / "proxies.txt").write_text("1.1.1.1:6001:u:p\n2.2.2.2:6002:u:p\n", "utf-8")
    _dl.reload_proxies(); _dl.clear_proxy_block()

    _dl.note_direct_result(False)
    check("напрямую не пустили -> идём через прокси",
          _dl._plan("https://youtu.be/x", False)[0][0] is not None)
    _dl.note_direct_result(True)
    p_direct = _dl._plan("https://youtu.be/x", False)
    check("напрямую получилось -> начинаем напрямую и бережём трафик",
          p_direct[0][0] is None, str(p_direct[0]))
    check("прокси при этом остаётся запасным", any(s[0] for s in p_direct))
    check("для не-YouTube ничего не изменилось",
          _dl._plan("https://www.tiktok.com/@a/video/1", False)[0][0] is None)
    _dl.note_direct_result(False)

    print("\n133п2. Куки для YouTube идут НАПРЯМУЮ и ПЕРВЫМИ (был баг: не доходили)")
    # Раньше куки цеплялись к прокси и последним шагом; когда на прокси кончался
    # трафик, шаг с куками выкидывался фильтром, и валидные куки выдавались за
    # протухшие. Теперь куки-напрямую обходят «ты не робот» и идут раньше прокси.
    _dl._proxy_pool = None
    (TMP / "proxies.txt").write_text("1.1.1.1:6001:u:p\n2.2.2.2:6002:u:p\n", "utf-8")
    _dl.reload_proxies(); _dl.clear_proxy_block()

    def _idx(steps, pred):
        return next((i for i, s in enumerate(steps) if pred(s)), 999)

    _dl.note_direct_result(False)   # адрес помечен как робот
    plan_ck = _dl._plan("https://youtu.be/x", True)
    ck_at = _idx(plan_ck, lambda s: s[0] is None and s[1] is True)
    prx_at = _idx(plan_ck, lambda s: s[0] is not None)
    check("помеченный адрес: куки-напрямую — самый первый шаг",
          plan_ck[0] == (None, True), str(plan_ck[0]))
    check("куки-напрямую всегда раньше любого прокси",
          ck_at < prx_at, f"куки@{ck_at} прокси@{prx_at}")

    # Ключевое: шаг с куками стоит раньше прокси, поэтому отказ прокси по
    # трафику физически не может его отменить — он уже отработал.
    check("шаг с куками не может быть отменён отказом прокси",
          ck_at == 0 and all(s[0] is None for s in plan_ck[:ck_at + 1]),
          str(plan_ck[:2]))

    _dl.note_direct_result(True)    # адрес сейчас чистый
    plan_ok = _dl._plan("https://youtu.be/x", True)
    ck_ok = _idx(plan_ok, lambda s: s[0] is None and s[1] is True)
    prx_ok = _idx(plan_ok, lambda s: s[0] is not None)
    check("чистый адрес: сперва бесплатный прямой, потом куки, потом прокси",
          plan_ok[0] == (None, False) and ck_ok < prx_ok, str(plan_ok[:3]))
    _dl.note_direct_result(False)

    # Прокси в паузе (кончился трафик) — куки-напрямую всё равно есть и первые
    _dl._proxy_blocked_until = time.time() + 600
    plan_dead = _dl._plan("https://youtu.be/x", True)
    check("прокси мертвы, но куки-напрямую на месте и первые",
          plan_dead[0] == (None, True), str(plan_dead))
    check("без куков и с мёртвыми прокси — просто прямой заход",
          _dl._plan("https://youtu.be/x", False) == [(None, False)],
          str(_dl._plan("https://youtu.be/x", False)))
    _dl.clear_proxy_block()

    # _build_args уважает флаг: куки-напрямую -> --cookies есть; голый прямой -> нет
    (TMP / "cookies.txt").write_text(
        "# Netscape HTTP Cookie File\n.youtube.com\tTRUE\t/\tTRUE\t1\tSID\tx\n", "utf-8")
    a_ck = _dl._build_args(["https://youtu.be/x"], TMP, TMP / "m", False,
                           proxy=None, with_cookies=True)
    a_bare = _dl._build_args(["https://youtu.be/x"], TMP, TMP / "m", False,
                             proxy=None, with_cookies=False)
    check("шаг куки-напрямую реально несёт --cookies", "--cookies" in a_ck)
    check("голый прямой шаг куки НЕ несёт", "--cookies" not in a_bare)
    (TMP / "cookies.txt").unlink()

    print("\n133п3. Причина отказа не теряется и не приписывается чужой площадке")
    # Было: после 402 делался ещё прямой заход, его «ты не робот» затирал хвост,
    # и человек читал «этот ролик закрыт до входа, обычные качаются нормально» —
    # хотя не качалось вообще ничего и виноват был кончившийся трафик.
    m_q = _dl._explain("ERROR: Sign in to confirm you are not a bot",
                       used_proxy=True, hit_quota=True)
    check("402 не теряется за ошибкой следующей попытки",
          "кончился трафик" in m_q, m_q[:160])
    check("и не выдаётся за возрастное ограничение",
          "именно этот ролик" not in m_q, m_q[:160])

    # Экстрактор Instagram сыплет «rate-limit reached or login required» — это
    # попадало в ветку бот-чека, и про сломанный Instagram сообщали, что он
    # работает, а виноват YouTube.
    ig_err = "ERROR: Requested content is not available, rate-limit reached or login required"
    m_ig = _dl._explain(ig_err, url="https://www.instagram.com/p/A/")
    check("беду Instagram не объясняют через YouTube", "YouTube" not in m_ig, m_ig[:220])
    check("и не уверяют, что Instagram работает",
          "работают и без этого" not in m_ig, m_ig[:220])
    check("а советуют куки именно этой площадки",
          "этот же" in m_ig.lower() or "этой площадки" in m_ig.lower(), m_ig[:220])
    m_yt = _dl._explain(ig_err, url="https://youtu.be/x")
    check("для YouTube ветка осталась прежней",
          "адрес сервера" in m_yt or "ролик" in m_yt, m_yt[:200])

    print("\n133п4. Неудача не по вине адреса не сбрасывает бесплатный прямой путь")
    # «Видео удалено» и «файл больше лимита» к адресу отношения не имеют, а
    # сбрасывали флаг так же, как настоящая блокировка — и следующая скачка
    # зря лезла в платный прокси с чистого адреса.
    for err, keeps in (("Video unavailable", True),
                       ("File is larger than max-filesize", True),
                       ("Sign in to confirm you're not a bot", False),
                       ("HTTP Error 403: Forbidden", False)):
        check(f"{err[:34]:36} -> {'флаг цел' if keeps else 'флаг сброшен'}",
              _dl._worth_retry(err) is not keeps)

    print("\n133р. Расход платного трафика виден")
    main.db.set_meta("proxy_bytes", "0")
    async def proxy_fetch(url, audio_only=False, on_progress=None, job_id=None, height=None):
        return {"paths": [str(dlfile)], "title": "Через прокси", "job": jobdir,
                "extra": 0, "via_proxy": True, "error": None}
    async def direct_fetch(url, audio_only=False, on_progress=None, job_id=None, height=None):
        return {"paths": [str(dlfile)], "title": "Напрямую", "job": jobdir,
                "extra": 0, "via_proxy": False, "error": None}
    _dl.discard = lambda j: None
    _cfg.DOWNLOAD_CACHE_DAYS = 0          # чтобы кэш не подменил вторую скачку

    _dl.fetch = proxy_fetch
    ev = CommandEvent("/dl https://youtu.be/a", sender_id=OWNER)
    await main.on_command(ev)
    await wait_for(lambda: bool(ev.files))
    after_proxy = main.db.get_int("proxy_bytes", 0)
    check("скачанное через прокси посчитано", after_proxy > 0, str(after_proxy))

    _dl.fetch = direct_fetch
    ev = CommandEvent("/dl https://youtu.be/b", sender_id=OWNER)
    await main.on_command(ev)
    await wait_for(lambda: bool(ev.files))
    check("скачанное напрямую в счётчик НЕ идёт",
          main.db.get_int("proxy_bytes", 0) == after_proxy,
          f"{after_proxy} -> {main.db.get_int('proxy_bytes', 0)}")
    _cfg.DOWNLOAD_CACHE_DAYS = 90
    _dl.fetch = orig_fetch
    _dl.discard = orig_discard

    print("\n133с. Про кончившийся трафик узнаём сразу, а не от клиента")
    main.bot = None
    fake = main.client = FakeClient()
    main.db.db.execute("DELETE FROM meta WHERE key='proxy_warn_ts'"); main.db.db.commit()
    _dl._proxy_blocked_until = time.time() + 600
    await main.warn_if_proxy_dead()
    check("уведомление пришло", len(fake.sent) == 1, f"пришло {len(fake.sent)}")
    body = fake.sent[0]["text"] if fake.sent else ""
    check("сказано, что виноват трафик", "трафик" in body, body[:150])
    check("успокоили насчёт остального", "остальное работает" in body, body[:250])
    check("сказано, что делать", "список файлом" in body, body[:250])

    fake = main.client = FakeClient()
    await main.warn_if_proxy_dead()
    check("второй раз подряд не спамит", not fake.sent, f"пришло {len(fake.sent)}")

    fake = main.client = FakeClient()
    _dl.clear_proxy_block()
    main.db.db.execute("DELETE FROM meta WHERE key='proxy_warn_ts'"); main.db.db.commit()
    await main.warn_if_proxy_dead()
    check("когда прокси живы — молчим", not fake.sent, f"пришло {len(fake.sent)}")

    print("\n134. Провал скачивания попадает в журнал")
    import logging as _lg2
    grabbed = []
    class Grab2(_lg2.Handler):
        def emit(self, rec): grabbed.append(rec.getMessage())
    h2 = Grab2(); h2.setLevel(_lg2.WARNING); main.log.addHandler(h2)
    async def err_fetch(url, audio_only=False, on_progress=None, job_id=None, height=None):
        return {"error": "видео-серверы площадки отказали этому адресу (403)."}
    _dl.fetch = err_fetch
    ev = CommandEvent("/dl https://youtube.com/watch?v=zzz", sender_id=OWNER)
    await main.on_command(ev)
    await wait_for(lambda: any("Не скачалось" in m for m in grabbed))
    main.log.removeHandler(h2)
    check("неудача записана в лог", any("Не скачалось" in m for m in grabbed),
          str(grabbed)[:200])
    check("в логе видно причину", any("403" in m for m in grabbed), str(grabbed)[:200])
    _dl.fetch = orig_fetch

    print("\n135. Лимит места проверяется ДО скачивания, а не раз в шесть часов")
    # Раньше суммарный потолок применяла только чистка, и в промежутке между
    # чистками собеседник мог залить сколько угодно.
    md = config.MEDIA_DIR / "9001"; md.mkdir(parents=True, exist_ok=True)
    hog = md / "hog.bin"; hog.write_bytes(b"0" * (3 * 1024 * 1024))
    add(9101, chat_id=9001, text="занимает место", media_type="файл",
        media_path=str(hog), ago=10)
    old_total, old_ret = config.MEDIA_MAX_TOTAL_MB, config.MEDIA_RETENTION_DAYS
    config.MEDIA_MAX_TOTAL_MB = 2          # потолок меньше, чем уже занято
    config.MEDIA_RETENTION_DAYS = 9999     # чистка по сроку ничего не спасёт
    main.db._media_bytes = None
    # Файл, который не влезет даже в пустой кэш, отклоняем сразу
    deny = main._media_budget(50 * 1024 * 1024)
    check("файл больше всего лимита отклонён", bool(deny), repr(deny))
    check("в причине названо занятое место", deny and "занято" in deny, repr(deny))
    # А ради небольшого файла кэш вытесняет старое и пропускает скачивание
    main.db._media_bytes = None
    check("ради маленького файла место освобождается",
          main._media_budget(1024) is None, repr(main._media_budget(1024)))
    check("после этого занято не больше потолка",
          main.db.media_total_bytes() <= config.MEDIA_MAX_TOTAL_MB * 1024 * 1024,
          str(main.db.media_total_bytes()))
    check("вытесненный файл действительно удалён с диска", not hog.exists())

    print("\n135б. Свободное место на диске тоже держим")
    import shutil as _sh
    real_usage = _sh.disk_usage
    _sh.disk_usage = lambda p: type("U", (), {"free": 100 * 1024 * 1024})()
    deny_disk = main._media_budget(1024)
    _sh.disk_usage = real_usage
    check("при почти полном диске не качаем", bool(deny_disk), repr(deny_disk))
    check("в причине сказано про диск", deny_disk and "диске" in deny_disk, repr(deny_disk))
    config.MEDIA_MAX_TOTAL_MB, config.MEDIA_RETENTION_DAYS = old_total, old_ret
    main.db._media_bytes = None

    print("\n135в. Счётчик занятого места живёт без полного пересчёта")
    main.db._media_bytes = None
    base = main.db.media_total_bytes()
    main.db.note_media_bytes(1234)
    check("прибавка учтена", main.db.media_total_bytes(max_age_sec=300) == base + 1234)
    main.db.note_media_bytes(-10 ** 9)
    check("счётчик не уходит в минус", main.db.media_total_bytes(max_age_sec=300) == 0)
    main.db._media_bytes = None

    print("\n136. Длинное аудио отсекается до загрузки модели")
    # model.transcribe() декодирует файл целиком в память ещё до того, как
    # отдаст длительность — значит проверять длину надо своим ffprobe.
    import transcribe as _tr
    old_max = config.TRANSCRIBE_MAX_SEC
    config.TRANSCRIBE_MAX_SEC = 60
    loaded = []
    orig_ensure = _tr._ensure_model
    async def spy_ensure():
        loaded.append(1)
        return None
    _tr._ensure_model = spy_ensure
    orig_probe = _tr.probe_duration
    _tr.probe_duration = lambda p: 3600.0
    snd = config.MEDIA_DIR / "long.ogg"; snd.write_bytes(b"0" * 100)
    txt, err = await _tr.transcribe(str(snd))
    check("часовое аудио отклонено", txt is None and err and "длиннее потолка" in err, repr(err))
    check("модель для этого не грузилась", not loaded, str(loaded))

    print("\n136б. Если ffprobe молчит — судим по размеру")
    _tr.probe_duration = lambda p: None
    big_snd = config.MEDIA_DIR / "big.ogg"
    big_snd.write_bytes(b"0" * (config.TRANSCRIBE_MAX_SEC * 64 * 1024 + 10))
    txt2, err2 = await _tr.transcribe(str(big_snd))
    check("подозрительно крупный файл отклонён",
          txt2 is None and err2 and "слишком большой" in err2, repr(err2))
    check("модель по-прежнему не грузилась", not loaded, str(loaded))
    _tr.probe_duration, _tr._ensure_model = orig_probe, orig_ensure
    config.TRANSCRIBE_MAX_SEC = old_max

    print("\n137. Пароль от прокси не утекает в текст ошибки")
    leaked = _dl._explain(
        "ERROR: Unable to connect to proxy http://myuser:sup3rsecret@pr.example.com:8080",
        used_proxy=True, tried_cookies=False)
    check("логин и пароль замаскированы",
          "sup3rsecret" not in leaked and "myuser" not in leaked, leaked)

    print("\n137б. Список прокси из блокнота Windows не ломается меткой BOM")
    check("строка с BOM разобрана",
          _dl._parse_proxy_line("﻿1.2.3.4:8080:user:pass")
          == _dl._parse_proxy_line("1.2.3.4:8080:user:pass"),
          repr(_dl._parse_proxy_line("﻿1.2.3.4:8080:user:pass")))

    print("\n138. Отзыв сессии не проходит молча")
    import pathlib as _pl
    src = _pl.Path("main.py").read_text()
    tail_src = src[src.index("runner = asyncio.create_task"):]
    check("исключение runner разбирается", "runner.exception()" in tail_src)
    check("владельцу уходит предупреждение", "Сессия отозвана" in tail_src)
    check("сказано, как поднять обратно", "setup.py" in tail_src)

    print("\n138б. Обрыв связи возвращает ненулевой код — иначе systemd не поднимет")
    # Restart=on-failure не реагирует на нулевой выход. Раньше бот при обрыве
    # выходил с нулём и оставался лежать, обещая «перезапуск произойдёт сам».
    check("отозванную сессию отличаем от обрыва",
          main._auth_revoked(type("AuthKeyUnregisteredError", (Exception,), {})())
          and not main._auth_revoked(type("ConnectionError", (Exception,), {})()))
    check("при обрыве main возвращает 1", "return 1" in tail_src)
    check("код возврата доходит до systemd", "sys.exit(asyncio.run(main())" in src)
    check("юнит перезапускает по ненулевому коду",
          "Restart=on-failure" in _pl.Path("telegram-antidelete.service").read_text())

    print("\n139. Куки подбираются под площадку, а не одни на всё")
    # Был баг: куки YouTube подставлялись Instagram-у, тот отвечал «нужен вход»,
    # а бот советовал выгрузить куки заново — человек выгружал ютубовские.
    ytj = _dl.COOKIES_PATH
    old_txt = ytj.read_text() if ytj.exists() else None
    ytj.write_text("# Netscape HTTP Cookie File\n"
                   ".youtube.com\tTRUE\t/\tTRUE\t0\tSID\tx\n")
    check("ютубовские куки не годятся для Instagram",
          not _dl.jar_covers(ytj, "https://www.instagram.com/p/a/"))
    check("для YouTube годятся", _dl.jar_covers(ytj, "https://www.youtube.com/watch?v=a"))
    # Файл мы не отбираем (лишние куки безвредны), но и не притворяемся,
    # что он сюда годится — именно на этом строится текст ошибки.
    check("для Instagram куки не считаются подходящими",
          not _dl.cookies_fit("https://www.instagram.com/p/a/"))
    check("для YouTube считаются",
          _dl.cookies_fit("https://www.youtube.com/watch?v=a"))
    check("YouTube свои куки получает",
          _dl.cookies_file("https://www.youtube.com/watch?v=a") == str(ytj))

    print("\n139б. Короткие адреса ведут к тем же кукам")
    check("youtu.be = youtube", _dl.site_key("https://youtu.be/a") == "youtube")
    check("vm.tiktok = tiktok", _dl.site_key("https://vm.tiktok.com/x/") == "tiktok")

    print("\n139в. Свой файл площадки имеет приоритет")
    igj = _dl.COOKIES_PATH.parent / "cookies.instagram.txt"
    igj.write_text("# Netscape HTTP Cookie File\n"
                   ".instagram.com\tTRUE\t/\tTRUE\t0\tsessionid\tx\n")
    check("теперь Instagram берёт свои куки",
          _dl.cookies_file("https://www.instagram.com/p/a/") == str(igj))
    check("YouTube при этом не пострадал",
          _dl.cookies_file("https://www.youtube.com/watch?v=a") == str(ytj))

    print("\n139г. Подсказка называет верную площадку")
    m = _dl._explain("ERROR: login required", url="https://www.instagram.com/p/a/")
    check("в подсказке про Instagram", "instagram" in m.lower(), m[:160])
    igj.unlink(missing_ok=True)
    m2 = _dl._explain("ERROR: login required", url="https://www.instagram.com/p/a/")
    check("без куков не врёт про «протухли»", "протухл" not in m2.lower(), m2[:200])
    check("сказано, что чужие куки не подойдут", "не подойдут" in m2, m2[:200])
    if old_txt is not None:
        ytj.write_text(old_txt)
    else:
        ytj.unlink(missing_ok=True)

    await run_logram1()


# ================================================================ logram-1
# Пять правок по голосовому владельца от 03.11.2026. Каждая проверка падает
# на коде до правки и проходит после.


async def run_logram1():
    main.bot = None                    # алерты идут через подставной клиент
    import transcribe as _trx
    _orig_tr = _trx.transcribe
    async def _no_whisper(path, *a, **kw):   # модель не грузим: это сеть и сотни МБ
        return None, "в тестах не расшифровываем"
    _trx.transcribe = _no_whisper
    _paused = main.db.get_meta("paused", "0")
    main.db.set_meta("paused", "0")
    _digest = config.DIGEST_MODE
    config.DIGEST_MODE = "off"
    await l1_viewonce()
    await l1_purge_names()
    await l1_edit_diff()
    await l1_media_reliable()
    await l1_insta_audio()
    config.DIGEST_MODE = _digest
    main.db.set_meta("paused", _paused)
    _trx.transcribe = _orig_tr


async def l1_viewonce():
    print("\nL1-3. Огонёк: без лишнего текста")
    add(880001, chat_id=777, text="подпись к огоньку")
    row = main.db.get(880001, 777)
    body = alerts.build_viewonce_alert(row, "фото", True)
    check("заголовок и кто", "Сохранён огонёк" in body and "Иван Петров" in body, body)
    check("вид файла на месте", "фото" in body, body)
    check("подпись на месте", "подпись к огоньку" in body, body)
    check("нет «файл ниже — собеседник думает…»",
          "думает" not in body and "файл ниже" not in body, body)
    check("нет «просмотр один раз»", "просмотр один раз" not in body, body)
    body2 = alerts.build_viewonce_alert(row, "видео", False)
    check("нет «самоуничтожающееся»", "самоуничтожающееся" not in body2, body2)
    add(880002, chat_id=777, text=None)
    body3 = alerts.build_viewonce_alert(main.db.get(880002, 777), "голосовое", True)
    check("без подписи — без пустых строк в конце",
          not body3.endswith("\n") and "голосовое" in body3, repr(body3))


class PeopleClient(FakeClient):
    """Клиент с get_entity: отвечает настоящими telethon.tl.types.User,
    про неизвестных — ValueError, как Telethon без сущности в кэше сессии."""
    def __init__(self, users=None):
        super().__init__()
        self.users = users or {}
        self.lookups = []
    async def get_entity(self, peer):
        self.lookups.append(peer)
        if peer in self.users:
            return self.users[peer]
        raise ValueError(f"Could not find the input entity for PeerUser(user_id={peer})")


def l1_row(chat_id, msg_id, outgoing, sender_id=None, name=None, username=None,
           title=None, text="текст", ago=120):
    main.db.add_message(
        chat_id=chat_id, msg_id=msg_id, sender_id=sender_id, sender_name=name,
        sender_username=username, chat_title=title, text=text,
        is_outgoing=int(outgoing), sent_ts=int(time.time()) - ago)


async def l1_purge_names():
    from telethon.tl import types as tlt
    ME = 4242000
    main.purge_people_reset() if hasattr(main, "purge_people_reset") else None

    print("\nL1-1а. Снёс всю переписку (только твои), @username берётся из Telegram")
    mark_monitored(77801, title="Олег")
    cli = main.client = PeopleClient({77801: tlt.User(
        id=77801, first_name="Олег", last_name="Т", username="oleg_t", access_hash=1)})
    for i in range(6):
        l1_row(77801, 881000 + i, True, sender_id=ME, name="Я", title="Олег Т")
    await main.on_deleted(DeleteEvent([881000 + i for i in range(6)]))
    await settle()
    body = cli.sent[0]["text"] if cli.sent else ""
    check("сводка пришла", "чистку переписки" in body, body[:200])
    check("случай «удалены только твои»", "только твои" in body, body[-200:])
    check("собеседник назван по @username", "@oleg_t" in body, body[:300])
    check("и по имени", "Олег" in body, body[:300])

    print("\nL1-1б. Снёс всю переписку, @username нет — ссылка tg://user и id")
    mark_monitored(77802, title="Аня")
    cli = main.client = PeopleClient({})      # Telegram сущность не отдал
    for i in range(6):
        l1_row(77802, 882000 + i, True, sender_id=ME, name="Я", title="Аня К")
    await main.on_deleted(DeleteEvent([882000 + i for i in range(6)]))
    await settle()
    body = cli.sent[0]["text"] if cli.sent else ""
    check("имя — кликабельная ссылка", '<a href="tg://user?id=77802">Аня К</a>' in body,
          body[:300])
    check("рядом числовой id", "id 77802" in body, body[:300])

    print("\nL1-1в. Входящие есть, но без @username — тоже ссылка и id")
    mark_monitored(77803, title="Петя")
    cli = main.client = PeopleClient({})
    for i in range(6):
        out = i % 2 == 0
        l1_row(77803, 883000 + i, out, sender_id=ME if out else 77803,
               name="Я" if out else "Петя", title="Петя")
    await main.on_deleted(DeleteEvent([883000 + i for i in range(6)]))
    await settle()
    body = cli.sent[0]["text"] if cli.sent else ""
    check("ссылка на Петю", 'href="tg://user?id=77803"' in body, body[:300])
    check("id Пети", "id 77803" in body, body[:300])

    print("\nL1-1г. Имени нет нигде — спрашиваем Telegram один раз, иначе «id N»")
    mark_monitored(77804, title=None)
    cli = main.client = PeopleClient({})
    for i in range(6):
        l1_row(77804, 884000 + i, True, sender_id=ME, name="Я", title=None)
    await main.on_deleted(DeleteEvent([884000 + i for i in range(6)]))
    await settle()
    body = cli.sent[0]["text"] if cli.sent else ""
    check("Telegram спросили", 77804 in cli.lookups, str(cli.lookups))
    check("запасной вариант «id N» ссылкой",
          '<a href="tg://user?id=77804">id 77804</a>' in body, body[:300])
    n_before = cli.lookups.count(77804)
    for i in range(6):
        l1_row(77804, 884100 + i, True, sender_id=ME, name="Я", title=None)
    await main.on_deleted(DeleteEvent([884100 + i for i in range(6)]))
    await settle()
    check("второй раз Telegram не дёргаем", cli.lookups.count(77804) == n_before,
          str(cli.lookups))

    print("\nL1-1д. Группа: каждый автор с @username, ссылкой или id")
    G = -1005551110000
    mark_monitored(G, "supergroup", "Кентухи-2", 8)
    cli = main.client = PeopleClient({90003: tlt.User(
        id=90003, first_name="Серый", username="seryi", access_hash=1)})
    l1_row(G, 500, False, sender_id=90001, name="Вася", username="vasya", title="Кентухи-2")
    l1_row(G, 501, False, sender_id=90001, name="Вася", username="vasya", title="Кентухи-2")
    l1_row(G, 502, False, sender_id=90002, name="Маша", title="Кентухи-2")
    l1_row(G, 503, False, sender_id=90003, name=None, title="Кентухи-2")
    l1_row(G, 504, False, sender_id=90004, name=None, title="Кентухи-2")
    l1_row(G, 505, True, sender_id=ME, name="Я", title="Кентухи-2")
    await main.on_deleted(DeleteEvent([500, 501, 502, 503, 504, 505], chat_id=G))
    await settle()
    body = cli.sent[0]["text"] if cli.sent else ""
    authors = body.split("Авторы:", 1)[-1].split("\n", 1)[0]
    check("автор с @username", "@vasya" in authors and "Вася" in authors, authors)
    check("автор без @username — ссылка и id",
          '<a href="tg://user?id=90002">Маша</a>' in authors and "id 90002" in authors, authors)
    check("автор без имени — найден в Telegram", "@seryi" in authors, authors)
    check("совсем неизвестный — «id N»",
          '<a href="tg://user?id=90004">id 90004</a>' in authors, authors)
    check("твои посчитаны", "твоих: 1" in authors, authors)

    print("\nL1-1е. Собеседник удалил пару твоих сообщений (не чистка) — назван")
    mark_monitored(77805, title="Лена")
    cli = main.client = PeopleClient({})
    config.ALERT_OWN_DELETES = True
    l1_row(77805, 885000, True, sender_id=ME, name="Я", title="Лена")
    await main.on_deleted(DeleteEvent([885000]))
    await settle()
    body = cli.sent[0]["text"] if cli.sent else ""
    check("алерт о твоём сообщении", "твоё сообщение" in body, body[:200])
    check("собеседник со ссылкой и id",
          'href="tg://user?id=77805"' in body and "id 77805" in body, body[:300])

    print("\nL1-1з. Бот не знает человека и Telegram ругается на ссылку — алерт всё равно уходит")
    from telethon.errors import RPCError as _RPCError
    class LinkFailClient(PeopleClient):
        async def send_message(self, chat, text, **kw):
            if "tg://user?id=" in text:
                raise _RPCError(None, "USER_ID_INVALID", 400)
            return await super().send_message(chat, text, **kw)
    mark_monitored(77806, title="Гоша")
    cli = main.client = LinkFailClient({})
    for i in range(6):
        l1_row(77806, 886000 + i, True, sender_id=ME, name="Я", title="Гоша")
    await main.on_deleted(DeleteEvent([886000 + i for i in range(6)]))
    await settle()
    body = cli.sent[0]["text"] if cli.sent else ""
    check("сводка дошла", "чистку переписки" in body, str(cli.sent)[:200])
    check("без ссылки, но с именем и id", "Гоша · id 77806" in body, body[:300])

    print("\nL1-1ж. Сборщик без подсказок main тоже всегда называет человека")
    r = [main.db.get(882000 + i, 77802) for i in range(6)]
    body = alerts.build_purge_alert("Аня К", r, [], int(time.time()), 3)
    check("ссылка и id даже без резолва", 'tg://user?id=77802' in body and "id 77802" in body,
          body[:300])


def l1_tags_balanced(html_text: str) -> bool:
    """Каждая строка сама по себе с закрытыми тегами: так split_text может
    резать по переводу строки, не ломая разметку."""
    from html.parser import HTMLParser
    class P(HTMLParser):
        def __init__(self):
            super().__init__()
            self.stack, self.bad = [], False
        def handle_starttag(self, tag, attrs):
            self.stack.append(tag)
        def handle_endtag(self, tag):
            if not self.stack or self.stack.pop() != tag:
                self.bad = True
    for line in html_text.split("\n"):
        p = P(); p.feed(line); p.close()
        if p.bad or p.stack:
            return False
    return True


async def l1_edit_diff():
    import re as _re
    add(887000, chat_id=777, text="x")
    row = main.db.get(887000, 777)
    now = int(time.time())
    def parts(body):
        payload = body.split("—\n", 1)[1]
        old, new = payload.split("\n↓\n", 1)
        return old, new

    print("\nL1-5а. Одно слово в сообщении на 600 символов")
    words = [f"слово{i}" for i in range(90)]
    old_t = " ".join(words)
    new_words = list(words); new_words[45] = "ИЗМЕНЕНО"
    new_t = " ".join(new_words)
    check("сообщение длинное", len(old_t) >= 600, str(len(old_t)))
    body = alerts.build_edit_alert(row, old_t, new_t, now)
    old, new = parts(body)
    check("зачёркнуто только старое слово", "<s>слово45</s>" in old and old.count("<s>") == 1, old)
    check("новое слово выделено", "<b>ИЗМЕНЕНО</b>" in new and new.count("<b>") == 1, new)
    check("вокруг правки контекст", "слово44" in old and "слово46" in new, old)
    check("далёкий текст не повторяется", "слово5 " not in old and "слово85" not in new, old)
    check("пропуски помечены «…»", old.startswith("…") and new.rstrip().endswith("…"), old)
    check("алерт стал короче исходника", len(body) < len(old_t) + 200, str(len(body)))

    print("\nL1-5б. Две отдельные правки")
    o = "Завтра в семь встречаемся у метро, возьми билеты и зарядку"
    n = "Завтра в восемь встречаемся у метро, возьми билеты и наушники"
    old, new = parts(alerts.build_edit_alert(row, o, n, now))
    check("оба старых слова зачёркнуты", "<s>семь</s>" in old and "<s>зарядку</s>" in old, old)
    check("оба новых выделены", "<b>восемь</b>" in new and "<b>наушники</b>" in new, new)
    check("общий текст без разметки", "встречаемся у метро, возьми билеты и" in old
          and "встречаемся у метро, возьми билеты и" in new, old)
    check("неизменённое не зачёркнуто", "<s>Завтра" not in old, old)

    print("\nL1-5в. Дописали предложение в конец")
    o = "Буду через час."
    n = "Буду через час. Захвачу пиццу и колу."
    old, new = parts(alerts.build_edit_alert(row, o, n, now))
    check("старое не зачёркнуто вовсе", "<s>" not in old, old)
    check("дописанное выделено", "<b>Захвачу пиццу и колу.</b>" in new, new)

    print("\nL1-5г. Текст заменён целиком — как раньше")
    o = "Привет, как дела у тебя сегодня"
    n = "Скинь адрес, я уже еду"
    old, new = parts(alerts.build_edit_alert(row, o, n, now))
    check("старое зачёркнуто целиком", old == "<s>" + alerts.esc(o) + "</s>", old)
    check("новое целиком без выделения", new == alerts.esc(n), new)

    print("\nL1-5д. Пунктуация, регистр, эмодзи, кириллица")
    old, new = parts(alerts.build_edit_alert(row, "Ну ты даёшь.", "Ну ты даёшь!", now))
    check("только точка → !", "<s>.</s>" in old and "<b>!</b>" in new and "даёшь" in new
          and "<b>даёшь" not in new, old + " | " + new)
    old, new = parts(alerts.build_edit_alert(row, "Встреча в пятницу вечером",
                                             "встреча в пятницу вечером", now))
    check("регистр замечен", "<s>Встреча</s>" in old and "<b>встреча</b>" in new, old + " | " + new)
    fam = "👨\u200d👩\u200d👧"
    old, new = parts(alerts.build_edit_alert(row, "идём в кино 🎬 всей семьёй",
                                             f"идём в кино {fam} всей семьёй", now))
    check("эмодзи-семья не разорвана", f"<b>{fam}</b>" in new and "<s>🎬</s>" in old,
          old + " | " + new)
    old, new = parts(alerts.build_edit_alert(row, "ёжик в тумане", "ёжик в Тумане", now))
    check("кириллица с ё", "<s>тумане</s>" in old and "<b>Тумане</b>" in new, old + " | " + new)

    print("\nL1-5е. Экранирование и теги")
    o = "if a<b && c>d then <i>x</i> ok"
    n = "if a<b && c>e then <i>x</i> ok"
    body = alerts.build_edit_alert(row, o, n, now)
    check("пользовательские < > & экранированы", "<i>x</i>" not in body.split("—\n", 1)[1]
          and "&lt;i&gt;" in body and "&amp;&amp;" in body, body)
    check("теги сбалансированы", l1_tags_balanced(body), body)
    o = "первая строка\nвторая строка тут\nтретья"
    n = "первая строка\nсовсем другая вторая строка тут\nтретья"
    body = alerts.build_edit_alert(row, o, n, now)
    check("тег не тянется через перевод строки", l1_tags_balanced(body), body)

    print("\nL1-5ж. Лимит 4096: огромный текст и много правок")
    import random as _r
    rnd = _r.Random(5)
    big_o = " ".join(f"w{rnd.randint(0, 99)}" for _ in range(1500)) + " 😀" * 300
    big_n_t = big_o.split(" ")
    for k in range(0, len(big_n_t), 7):
        big_n_t[k] = "Ж&<" + big_n_t[k]
    big_n = " ".join(big_n_t)
    body = alerts.build_edit_alert(row, big_o, big_n, now)
    vis = _re.sub(r"<[^>]+>", "", body)
    import html as _html
    u16 = len(_html.unescape(vis).encode("utf-16-le")) // 2
    check("влезает в одно сообщение", len(list(alerts.split_text(body))) == 1, str(len(body)))
    check("видимый текст ≤ 4096 в UTF-16", u16 <= 4096, str(u16))
    check("теги целы после урезания", l1_tags_balanced(body), body[-300:])
    check("сущности не разорваны", not _re.search(r"&[a-z]*$", vis.split("\n")[0][:5])
          and "&amp" not in _re.sub(r"&amp;|&lt;|&gt;|&quot;|&#x27;", "", body), body[:200])
    body = alerts.build_edit_alert(row, "а " * 3000, "б " * 3000, now)
    check("полная замена длинного тоже влезает",
          len(list(alerts.split_text(body))) == 1 and l1_tags_balanced(body), str(len(body)))

    print("\nL1-5з. Токенизатор ничего не теряет")
    for t in ["", "Привет, мир!", fam + " 1️⃣ 🇺🇦 й", "a\n\nb\t c", "<>&\"'"]:
        check(f"склейка токенов == исходник ({t[:10]!r})",
              "".join(alerts._edit_tokens(t)) == t if hasattr(alerts, "_edit_tokens") else False)


def l1_tg_message(client, chat_id, mid, kind="photo", size=50000, ref=b"ref0"):
    """Настоящий telethon Message с настоящим медиа: msg.file.size и
    msg.download_media работают как в бою, только через подставной клиент."""
    from datetime import datetime, timezone
    from telethon.tl import types as tlt
    now = datetime.now(timezone.utc)
    if kind == "photo":
        media = tlt.MessageMediaPhoto(photo=tlt.Photo(
            id=mid, access_hash=1, file_reference=ref, date=now, dc_id=4,
            sizes=[tlt.PhotoStrippedSize(type="i", bytes=b"\x01\x28\x28"),
                   tlt.PhotoSize(type="y", w=1280, h=960, size=size)]))
    elif kind == "geo":
        media = tlt.MessageMediaGeo(geo=tlt.GeoPoint(long=14.4, lat=50.1, access_hash=1))
    else:
        if kind == "voice":
            attrs, mime = [tlt.DocumentAttributeAudio(duration=3, voice=True)], "audio/ogg"
        else:   # кружок
            attrs, mime = [tlt.DocumentAttributeVideo(duration=2, w=384, h=384,
                                                      round_message=True)], "video/mp4"
        media = tlt.MessageMediaDocument(
            document=tlt.Document(id=mid, access_hash=1, file_reference=ref, date=now,
                                  mime_type=mime, size=size, dc_id=4, attributes=attrs),
            voice=kind == "voice", round=kind == "round")
    peer = tlt.PeerUser(chat_id) if chat_id > 0 else tlt.PeerChannel(-chat_id - 1000000000000)
    m = tlt.Message(id=mid, peer_id=peer, date=now, message="", media=media)
    m._finish_init(client, {}, None)
    return m


class MediaClient(FakeClient):
    """Скачивание по сценарию на каждое сообщение.

    ok — файл с байтами; empty — пустой файл, возвращён путь; ref/net —
    сперва создаётся пустой файл, потом исключение (ровно так ведёт себя
    Telethon: open(file, 'wb') до первого байта); flood — FloodWait.
    """
    _self_id = 4242000            # как TelegramClient после входа
    parse_mode = None
    def __init__(self, plans, deleted=(), delay=0.0):
        super().__init__()
        from telethon._updates import EntityCache
        self._mb_entity_cache = EntityCache()
        self.plans = {k: list(v) for k, v in plans.items()}
        self.deleted = set(deleted)
        self.calls = []
        self.refetched = []
        self.delay = delay
        self.msgs = {}
    async def download_media(self, message, file=None, **kw):
        from telethon import errors as tle
        mid = message.id
        self.calls.append((mid, bytes(getattr(getattr(message.media, "photo", None)
                                              or getattr(message.media, "document", None),
                                              "file_reference", b"") or b"")))
        if self.delay:
            await asyncio.sleep(self.delay)
        if message.media is None or not (getattr(message.media, "photo", None)
                                         or getattr(message.media, "document", None)):
            return None
        ext = ".jpg" if getattr(message.media, "photo", None) else (
            ".oga" if getattr(message.media, "voice", False) else ".mp4")
        path = file if os.path.splitext(file)[1] else file + ext
        step = self.plans.get(mid, ["ok"]).pop(0) if self.plans.get(mid) else "ok"
        if step == "ok":
            Path(path).write_bytes(b"\xff\xd8" + b"x" * 500)
            return path
        Path(path).write_bytes(b"")
        if step == "empty":
            return path
        if step == "ref":
            raise tle.FileReferenceExpiredError(request=None)
        if step == "flood":
            raise tle.FloodWaitError(request=None, capture=0)
        raise ConnectionError("Connection reset by peer")
    async def get_messages(self, chat, ids=None, **kw):
        self.refetched.append((chat, ids))
        if ids in self.deleted:
            return None
        old = self.msgs.get(ids)
        kind = "photo" if old is None or getattr(old.media, "photo", None) else (
            "voice" if getattr(old.media, "voice", False) else "round")
        return l1_tg_message(self, chat, ids, kind, ref=b"fresh%d" % len(self.refetched))


class NewMsgEvent:
    """events.NewMessage для on_new_message: только то, что код читает."""
    def __init__(self, msg, chat_id, sender=None, chat=None, chat_fails=False,
                 sender_fails=False):
        self.message = msg
        self.chat_id = chat_id
        self.id = msg.id
        self.out = False
        self._sender, self._chat = sender, chat
        self._cf, self._sf = chat_fails, sender_fails
    async def get_chat(self):
        if self._cf:
            raise ConnectionError("get_chat отвалился")
        return self._chat
    async def get_sender(self):
        if self._sf:
            raise ConnectionError("get_sender отвалился")
        return self._sender


async def l1_media_reliable():
    from telethon.tl import types as tlt
    if hasattr(main, "MEDIA_RETRY_SLEEP"):
        main.MEDIA_RETRY_SLEEP = (0, 0, 0)
    C = 77900
    mark_monitored(C, title="Ника")
    ivan = tlt.User(id=C, first_name="Ника", username="nika", access_hash=1)

    async def arrive(cli, mid, kind="photo", **ev):
        m = l1_tg_message(cli, C, mid, kind)
        cli.msgs[mid] = m
        await main.on_new_message(NewMsgEvent(m, C, sender=ivan, chat=ivan, **ev))
        return main.db.get(mid, C)

    print("\nL1-4а. Фото: первая попытка дала 0 байт — повтор со свежей ссылкой")
    cli = main.client = MediaClient({890001: ["empty", "ok"]})
    r = await arrive(cli, 890001)
    path = r["media_path"] if r else None
    check("файл сохранён", bool(path) and os.path.getsize(path) > 0, f"{path} {r and r['media_note']}")
    check("было две попытки", len(cli.calls) == 2, str(cli.calls))
    check("сообщение перечитано за свежей ссылкой",
          cli.refetched and cli.calls[1][1].startswith(b"fresh"), str(cli.calls))
    check("тот же путь перезаписан", path == str(config.MEDIA_DIR / str(C) / "890001.jpg"), path)

    print("\nL1-4б. Голосовое: ссылка протухла дважды, с третьей попытки скачано")
    cli = main.client = MediaClient({890002: ["ref", "ref", "ok"]})
    r = await arrive(cli, 890002, "voice")
    check("голосовое сохранено", bool(r and r["media_path"]) and os.path.getsize(r["media_path"]) > 0,
          str(r and dict(r)))
    check("три попытки", len(cli.calls) == 3, str(cli.calls))

    print("\nL1-4в. Кружок: каждый раз пусто — честное «не сохранился» и почему")
    cli = main.client = MediaClient({890003: ["empty", "net", "empty"]})
    r = await arrive(cli, 890003, "round")
    note = (r["media_note"] if r else "") or ""
    check("пустой файл не засчитан успехом", r is not None and not r["media_path"], str(r and dict(r)))
    check("в причине — попытки и 0 байт", "3 попыт" in note and "0 байт" in note, note)
    await main.on_deleted(DeleteEvent([890003]))
    await settle()
    body = cli.sent[0]["text"] if cli.sent else ""
    check("алерт прямо говорит, что файл не сохранился", "Файл не сохранился" in body, body)
    check("и почему", "0 байт" in body, body)

    print("\nL1-4г. Сообщение удалили до повтора — не долбим зря, причина в алерте")
    cli = main.client = MediaClient({890004: ["ref", "ok"]}, deleted={890004})
    r = await arrive(cli, 890004)
    note = (r["media_note"] if r else "") or ""
    check("одна попытка, дальше не достать", len(cli.calls) == 1, str(cli.calls))
    check("причина названа", "протухла" in note and "удалено" in note, note)

    print("\nL1-4д. FloodWait и обрыв сети — повторяем, а не сдаёмся")
    cli = main.client = MediaClient({890005: ["flood", "net", "ok"]})
    r = await arrive(cli, 890005)
    check("фото сохранено после FloodWait и обрыва",
          bool(r and r["media_path"]), str(r and dict(r)))

    print("\nL1-4е. Удалили, пока файл ещё качался — алерт ждёт файл")
    cli = main.client = MediaClient({}, delay=0.8)
    m = l1_tg_message(cli, C, 890006, "photo")
    task = asyncio.create_task(main.on_new_message(NewMsgEvent(m, C, sender=ivan, chat=ivan)))
    await asyncio.sleep(0.2)
    await main.on_deleted(DeleteEvent([890006]))
    await task
    await wait_for(lambda: bool(cli.sent), 5)
    check("алерт ушёл вместе с файлом",
          cli.sent and cli.sent[0]["kind"] == "file", str(cli.sent)[:200])

    print("\nL1-4ж. get_sender/get_chat упали — сообщение и файл всё равно сохранены")
    cli = main.client = MediaClient({})
    r = await arrive(cli, 890007, "voice", chat_fails=True, sender_fails=True)
    check("строка в кэше есть", r is not None, "сообщение потеряно")
    check("голосовое скачано", bool(r and r["media_path"]), str(r and dict(r)))

    print("\nL1-4з. Лимиты прежние: крупное не качаем, геолокацию не мучаем повторами")
    cli = main.client = MediaClient({})
    m = l1_tg_message(cli, C, 890008, "photo", size=(config.MEDIA_MAX_FILE_MB + 1) * 1024 * 1024)
    await main.on_new_message(NewMsgEvent(m, C, sender=ivan, chat=ivan))
    r = main.db.get(890008, C)
    check("сверх MEDIA_MAX_FILE_MB не качали", not cli.calls and "лимита" in (r["media_note"] or ""),
          str(dict(r)))
    m = l1_tg_message(cli, C, 890009, "geo")
    await main.on_new_message(NewMsgEvent(m, C, sender=ivan, chat=ivan))
    r = main.db.get(890009, C)
    check("геолокация: одна попытка, без «не сохранился»",
          len(cli.calls) <= 1 and "не сохран" not in (r["media_note"] or ""), str(dict(r)))


FAKE_YTDLP = r"""#!__PY__
# Подделка yt-dlp для тестов: без сети. -J печатает info dict из сценария,
# скачивание выбирает формат по правилам yt-dlp (b — цельный: vcodec и acodec
# не "none"; bv*+ba — склейка, нужна отдельная звуковая дорожка; иначе — format_id)
# и кладёт заранее сделанный ffmpeg-ом файл со звуком или без.
import json, os, re, shutil, sys
sc = json.load(open(os.environ["FAKE_YTDLP_SCENARIO"]))
args = sys.argv[1:]
fmts = sc["info"].get("formats") or []
with open(sc["log"], "a") as lg:
    lg.write(json.dumps(args) + "\n")
if "-J" in args:
    print(json.dumps(sc["info"])); sys.exit(0)
def real(c): return c not in (None, "none")
def pick(alt):
    base = re.sub(r"\[[^\]]*\]", "", alt)
    if "+" in base:
        v, a = base.split("+", 1)
        vids = [f for f in fmts if f.get("vcodec") != "none"] if v.startswith("bv") \
            else [f for f in fmts if f["format_id"] == v]
        auds = [f for f in fmts if f.get("vcodec") == "none" and real(f.get("acodec"))] \
            if a == "ba" else [f for f in fmts if f["format_id"] == a]
        return "@merged" if vids and auds else None
    if base in ("b", "bv*", "bv"):
        if base == "b":
            c = [f for f in fmts if f.get("vcodec") != "none" and f.get("acodec") != "none"]
        else:
            c = [f for f in fmts if f.get("vcodec") != "none"]
        c.sort(key=lambda f: -(f.get("height") or 0))
        return c[0]["format_id"] if c else None
    return alt if any(f["format_id"] == alt for f in fmts) else None
spec = args[args.index("-f") + 1]
home = args[args.index("-P") + 1].split(":", 1)[1]
meta = args[args.index("--print-to-file") + 2]
chosen = None
for alt in spec.split("/"):
    chosen = pick(alt)
    if chosen:
        break
if not chosen:
    print("ERROR: [instagram] x: Requested format is not available"); sys.exit(1)
with open(sc["log"], "a") as lg:
    lg.write(json.dumps({"chosen": chosen}) + "\n")
src = sc["files"]["with" if sc["media"].get(chosen) == "a" else "without"]
shutil.copy(src, os.path.join(home, "001-Video_by_test.mp4"))
open(meta, "w").write(sc["info"].get("title") or "x")
"""


def l1_has_audio(path) -> bool:
    """Свой ffprobe, независимый от проверяемого кода."""
    import subprocess as _sp
    r = _sp.run(["ffprobe", "-v", "error", "-select_streams", "a", "-show_entries",
                 "stream=index", "-of", "csv=p=0", str(path)], capture_output=True, text=True)
    return r.returncode == 0 and bool(r.stdout.strip())


def l1_ig_progressive(fid, height, has_audio=None, codec="avc1.64001F"):
    """Формат из video_versions так, как его собирает InstagramIE
    (_extract_product_media): url, format_id, width, height + vcodec из
    video_codec и acodec 'none' только при has_audio=False."""
    f = {"url": f"https://scontent.cdninstagram.com/o1/v/t16/f2/m86/{fid}.mp4",
         "format_id": fid, "width": height * 9 // 16, "height": height,
         "vcodec": codec, "ext": "mp4", "protocol": "https"}
    if has_audio is False:
        f["acodec"] = "none"
    return f


def l1_ig_dash(rep, kind, height=None):
    """Формат из video_dash_manifest (_parse_mpd_formats, mpd_id='dash')."""
    f = {"format_id": f"dash-{rep}", "manifest_url": None, "ext": "mp4" if kind == "video" else "m4a",
         "width": height * 9 // 16 if height else None, "height": height,
         "tbr": 1500.0 if kind == "video" else 128.0,
         "asr": None if kind == "video" else 44100, "fps": 30 if kind == "video" else None,
         "language": None, "format_note": f"DASH {kind}", "filesize": None,
         "container": ("mp4" if kind == "video" else "m4a") + "_dash",
         "url": f"https://scontent.cdninstagram.com/o1/v/t2/f2/m78/{rep}.mp4",
         "manifest_stream_number": 0, "protocol": "https"}
    if kind == "video":
        f.update(vcodec="avc1.4d401f", acodec="none")
    else:
        f.update(vcodec="none", acodec="mp4a.40.5")
    return f


async def l1_insta_audio():
    import json as _json
    import shutil as _sh
    import subprocess as _sp
    import downloader as _dl
    if not (_sh.which("ffmpeg") and _sh.which("ffprobe")):
        check("ffmpeg/ffprobe есть для теста звука", False, "нет ffmpeg")
        return
    d = TMP / "ig"
    d.mkdir(exist_ok=True)
    with_a, without_a = d / "with.mp4", d / "without.mp4"
    base = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi",
            "-i", "color=c=black:s=64x64:d=1"]
    _sp.run(base + ["-f", "lavfi", "-i", "sine=f=440:d=1", "-shortest",
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", str(with_a)], check=True)
    _sp.run(base + ["-an", "-c:v", "libx264", "-pix_fmt", "yuv420p", str(without_a)], check=True)
    fake = d / "yt-dlp"
    fake.write_text(FAKE_YTDLP.replace("__PY__", sys.executable))
    fake.chmod(0o755)
    orig_bin, orig_cur = _dl.YTDLP, _dl.check_url_resolved
    _dl.YTDLP = str(fake)
    async def _no_dns(u):
        return _dl.check_url(u)
    _dl.check_url_resolved = _no_dns
    logf = d / "calls.log"

    def scenario(info, media):
        logf.write_text("")
        sc = d / "sc.json"
        sc.write_text(_json.dumps({"info": info, "media": media, "log": str(logf),
                                   "files": {"with": str(with_a), "without": str(without_a)}}))
        os.environ["FAKE_YTDLP_SCENARIO"] = str(sc)

    def calls():
        return [_json.loads(x) for x in logf.read_text().splitlines() if x.strip()]

    def ig_info(fmts, code):
        return {"id": code, "display_id": code, "title": "Video by test_user",
                "channel": "test_user", "uploader": "Test", "duration": 1.0,
                "extractor": "Instagram", "extractor_key": "Instagram",
                "webpage_url": f"https://www.instagram.com/reel/{code}/", "formats": fmts}

    try:
        print("\nL1-2а. Цельный формат Instagram без звука — перекачка склейкой видео+звук")
        info = ig_info([l1_ig_progressive("1080p", 1280),
                        l1_ig_dash("v720", "video", 1280), l1_ig_dash("a1", "audio")], "Caaa1")
        scenario(info, {"1080p": "s", "@merged": "a"})
        res = await _dl.fetch("https://www.instagram.com/reel/Caaa1/")
        p0 = res.get("paths", [None])[0]
        check("скачано", not res.get("error") and p0, str(res.get("error")))
        check("в отданном файле есть звук", p0 and l1_has_audio(p0), str(p0))
        check("перекачано склейкой bv*+ba", any("-f" in c and "+ba" in c[c.index("-f") + 1]
                                                 and not c[c.index("-f") + 1].startswith("b[")
                                                 for c in calls() if isinstance(c, list)),
              str(calls())[:400])
        check("пометки нет — звук на месте", res.get("audio_note") is None, str(res.get("audio_note")))
        _dl.discard(res.get("job"))

        print("\nL1-2б. Склейки нет — пробуем другие форматы из списка yt-dlp")
        info = ig_info([l1_ig_progressive("101", 1280), l1_ig_progressive("102", 960),
                        l1_ig_progressive("103", 640)], "Cbbb2")
        scenario(info, {"101": "s", "102": "a", "103": "s"})
        res = await _dl.fetch("https://www.instagram.com/reel/Cbbb2/")
        p0 = res.get("paths", [None])[0]
        check("звук нашёлся в другом формате", p0 and l1_has_audio(p0),
              str(res)[:300])
        tried = [c[c.index("-f") + 1] for c in calls() if isinstance(c, list) and "-f" in c]
        check("перебраны format_id из списка", "101" in tried and "102" in tried, str(tried))
        _dl.discard(res.get("job"))

        print("\nL1-2в. В оригинале звука нет — видео всё равно приходит, с пометкой")
        info = ig_info([l1_ig_progressive("1080p", 1280, has_audio=False),
                        l1_ig_dash("v720", "video", 1280)], "Cccc3")
        scenario(info, {"1080p": "s", "dash-v720": "s"})
        res = await _dl.fetch("https://www.instagram.com/reel/Cccc3/")
        check("видео отдано, а не ошибка", bool(res.get("paths")), str(res.get("error")))
        check("пометка «в оригинале без звука»", res.get("audio_note") == "в оригинале без звука",
              str(res.get("audio_note")))
        _dl.discard(res.get("job"))

        print("\nL1-2г. Подпись в чате говорит «в оригинале без звука»")
        scenario(info, {"1080p": "s", "dash-v720": "s"})
        ev = CommandEvent("x", sender_id=main._me_id)
        await main.do_download("https://www.instagram.com/reel/Cccc3/?l1=1", ev)
        cap = next((r for r in ev.replies if "Video by test_user" in r), "")
        check("файл отправлен", bool(ev.files), str(ev.replies)[:300])
        check("в подписи «в оригинале без звука»", "в оригинале без звука" in cap, cap)

        print("\nL1-2д. Со звуком с первого раза — ничего не перекачиваем")
        info = ig_info([l1_ig_progressive("1080p", 1280)], "Cddd4")
        scenario(info, {"1080p": "a"})
        res = await _dl.fetch("https://www.instagram.com/reel/Cddd4/")
        n = [c for c in calls() if isinstance(c, list)]
        check("одна загрузка, без -J и повторов", len(n) == 1 and "-J" not in n[0], str(n)[:300])
        check("без пометки", res.get("audio_note") is None)
        _dl.discard(res.get("job"))
    finally:
        _dl.YTDLP, _dl.check_url_resolved = orig_bin, orig_cur
        os.environ.pop("FAKE_YTDLP_SCENARIO", None)


asyncio.run(run())
print("\n" + "=" * 50)
if FAILED:
    print(f"ПРОВАЛЕНО: {len(FAILED)}")
    for f in FAILED:
        print("  -", f)
    sys.exit(1)
print("Все проверки прошли.")
