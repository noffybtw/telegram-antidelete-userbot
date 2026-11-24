#!/usr/bin/env bash
# Установка Telegram Anti-Delete на чистый сервер.
#
#   ./install.sh
#
# Скрипт идемпотентный: повторный запуск ничего не ломает и не перетирает
# уже введённые ключи. Всё, что нельзя получить автоматически (API-ключи,
# код подтверждения из Telegram), он спрашивает.

set -euo pipefail
cd "$(dirname "$0")"

DIR="$(pwd)"
RUN_USER="$(id -un)"
SERVICE="telegram-antidelete"
PY=./venv/bin/python3

say()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m    %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m!!  %s\033[0m\n' "$*" >&2; exit 1; }

sudo_if_needed() {
    if [ "$(id -u)" -eq 0 ]; then "$@"; else sudo "$@"; fi
}

# ---------------------------------------------------------------- система

say "Проверяю систему"

# Ставим пакеты только через apt. На rpm/pacman-дистрибутивах скрипт
# раньше доходил до первой установки и падал «apt-get: command not found»
# уже после того, как что-то создал. Лучше честно сказать это сразу.
command -v apt-get >/dev/null || die "Установщик рассчитан на Debian/Ubuntu (нужен apt-get).
    На другом дистрибутиве поставь вручную: python3.11+, python3-venv,
    ffmpeg, gnupg, git, Node.js 22+ — и запусти ./install.sh снова."

command -v python3 >/dev/null || die "Нет python3. Поставь: apt install python3"

# Минимум — 3.11: его требует av (нужен faster-whisper для расшифровки
# голосовых). Раньше здесь стояло 3.9, и на Ubuntu 22.04 установка падала
# на pip уже после создания venv — с непонятным «No matching distribution».
PYBIN=""
for c in python3.13 python3.12 python3.11 python3; do
    command -v "$c" >/dev/null || continue
    "$c" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null \
        && { PYBIN="$c"; break; }
done
if [ -z "$PYBIN" ]; then
    HAVE=$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || echo "?")
    die "Нужен Python 3.11 или новее, а стоит $HAVE.
    Ubuntu 24.04 / Debian 12 и новее подходят сразу.
    На Ubuntu 22.04: sudo add-apt-repository ppa:deadsnakes/ppa &&
                     sudo apt install python3.12 python3.12-venv
    На Debian 11 — обновить систему до 12."
fi
PYVER=$("$PYBIN" -c 'import sys; print("%d.%d" % sys.version_info[:2])')
echo "    Python $PYVER — годится"

# Проверять надо именно ensurepip, а не venv: на Debian/Ubuntu модуль venv
# лежит в стандартной библиотеке и импортируется всегда, а pip внутрь
# окружения кладёт отдельный пакет python3.X-venv. Со старой проверкой
# ветка не срабатывала никогда, и venv создавался без pip.
if ! "$PYBIN" -c 'import ensurepip' 2>/dev/null; then
    say "Ставлю ${PYBIN}-venv"
    sudo_if_needed apt-get update -qq
    sudo_if_needed apt-get install -y -qq "${PYBIN}-venv" || \
        sudo_if_needed apt-get install -y -qq python3-venv
fi

command -v gpg >/dev/null || {
    say "Ставлю gnupg (нужен для шифрованных бэкапов)"
    sudo_if_needed apt-get install -y -qq gnupg
}

# ffmpeg нужен и загрузчику по ссылке (склейка видео+звука), и расшифровке
# голосовых (декодирование ogg/opus). Без него обе функции просто не работают.
command -v ffmpeg >/dev/null || {
    say "Ставлю ffmpeg (скачивание по ссылке и расшифровка голосовых)"
    sudo_if_needed apt-get install -y -qq ffmpeg
}

# Node нужен YouTube: он отдаёт ссылки на видео только тому, кто решил его
# js-задачу. Без движка — «No video formats found» даже при живых куках.
# Версия важна: генератор токенов требует node >= 22, а в apt лежит 12 (Debian 11)
# или 18 (Ubuntu 22.04/24.04, Debian 12) — с ними YouTube молча не работает.
NODE_MAJOR=0
command -v node >/dev/null && NODE_MAJOR=$(node -v 2>/dev/null | sed 's/^v\([0-9]*\).*/\1/')
if [ "${NODE_MAJOR:-0}" -lt 22 ]; then
    if [ "$NODE_MAJOR" -eq 0 ]; then
        say "Ставлю Node.js 22 (без него YouTube не качается)"
    else
        say "Обновляю Node.js: стоит $NODE_MAJOR, генератору токенов нужен 22+"
    fi
    if curl -fsSL https://deb.nodesource.com/setup_22.x -o /tmp/nodesource_setup.sh 2>/dev/null \
       && sudo_if_needed bash /tmp/nodesource_setup.sh >/dev/null 2>&1; then
        sudo_if_needed apt-get install -y -qq nodejs
    fi
    rm -f /tmp/nodesource_setup.sh
    NODE_MAJOR=0
    command -v node >/dev/null && NODE_MAJOR=$(node -v 2>/dev/null | sed 's/^v\([0-9]*\).*/\1/')
    if [ "${NODE_MAJOR:-0}" -lt 22 ]; then
        warn "Node 22+ поставить не удалось (сейчас: ${NODE_MAJOR:-нет})."
        warn "Всё остальное будет работать, YouTube — нет. Поставить вручную:"
        warn "    curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash - && sudo apt install -y nodejs"
    else
        echo "    Node $(node -v)"
    fi
fi
command -v git >/dev/null || sudo_if_needed apt-get install -y -qq git

# ---------------------------------------------------------------- окружение

# Проверяем не «есть каталог», а «есть рабочий pip»: если прошлый запуск
# оборвался на середине, каталог остаётся без pip, и все следующие запуски
# падали на ./venv/bin/pip. Такой venv просто пересоздаём.
if [ ! -x venv/bin/pip ]; then
    [ -d venv ] && { warn "Окружение venv/ неполное — пересоздаю"; rm -rf venv; }
    say "Создаю виртуальное окружение"
    "$PYBIN" -m venv venv
    [ -x venv/bin/pip ] || die "venv создан без pip. Поставь ${PYBIN}-venv и запусти ./install.sh снова."
fi

say "Ставлю зависимости"
warn "Это займёт несколько минут: вместе с ботом ставится распознавание речи (~400 МБ)."
./venv/bin/pip install -q --upgrade pip
./venv/bin/pip install -q -r requirements.txt
echo "    $(./venv/bin/pip show telethon | sed -n 's/^Version: /Telethon /p')"

# ---------------------------------------------------------------- .env

if [ ! -f .env ]; then
    say "Создаю .env"
    cp .env.example .env
fi
chmod 600 .env

# Читает значение ключа из .env
getval() { sed -n "s/^$1=//p" .env | head -1; }

# Записывает значение, не пропуская его через командную строку
setval() {
    K="$1" V="$2" $PY - <<'PY'
import os, pathlib, re
p = pathlib.Path(".env"); s = p.read_text()
k, v = os.environ["K"], os.environ["V"]
if re.search(rf'^{k}=', s, re.M):
    s = re.sub(rf'^{k}=.*$', f'{k}={v}', s, count=1, flags=re.M)
else:
    s = s.rstrip("\n") + f"\n{k}={v}\n"
p.write_text(s); p.chmod(0o600)
PY
}

if [ -z "$(getval API_ID)" ] || [ -z "$(getval API_HASH)" ]; then
    say "Нужны API-ключи Telegram"
    cat <<'TXT'
    Это делается руками в браузере, автоматизировать нельзя:
      1. Открой https://my.telegram.org
      2. Войди по своему номеру телефона
      3. API development tools -> создай приложение (название любое)
      4. Скопируй оттуда api_id и api_hash
TXT
    read -rp "    api_id: " V_ID
    read -rsp "    api_hash (ввод скрыт): " V_HASH; echo
    [ -n "$V_ID" ] && [ -n "$V_HASH" ] || die "Без ключей дальше никак"
    [[ "$V_ID" =~ ^[0-9]+$ ]] || die "api_id должен быть числом, а не «$V_ID»"
    [[ "$V_HASH" =~ ^[0-9a-fA-F]{32}$ ]] || die "api_hash — это 32 знака 0-9a-f"
    setval API_ID "$V_ID"
    setval API_HASH "$V_HASH"
    unset V_ID V_HASH
fi

# Генератор PO-токенов для YouTube. Отдельный проект на Node: pip-плагин без
# него бесполезен, а YouTube без токена отвечает «подтвердите, что вы не робот»
# даже на живых куках. Собирается один раз, лежит в папке проекта.
POT_VERSION=1.3.2
if [ ! -f pot-provider/server/build/generate_once.js ] && command -v node >/dev/null; then
    say "Собираю генератор токенов YouTube (разово, пара минут)"
    rm -rf pot-provider
    if git clone -q --depth 1 --branch "$POT_VERSION" \
         https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git pot-provider 2>/dev/null; then
        ( cd pot-provider/server \
          && npm install --silent --no-audit --no-fund >/dev/null 2>&1 \
          && npx --yes tsc >/dev/null 2>&1 ) || true
    fi
    if [ -f pot-provider/server/build/generate_once.js ]; then
        echo "    Готово — YouTube будет качаться."
    else
        warn "Не собралось. Всё остальное работает, YouTube — нет."
        warn "Повторить можно потом: просто запусти ./install.sh ещё раз."
    fi
fi

# Модель распознавания подбираем под память сервера. small держит русский
# заметно лучше, но на слабой машине его просто убьёт OOM.
if [ -z "$(getval WHISPER_MODEL)" ]; then
    RAM_MB=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo 2>/dev/null || echo 4096)
    if [ "$RAM_MB" -lt 2048 ]; then
        setval WHISPER_MODEL base
        warn "Памяти $RAM_MB МБ — ставлю модель распознавания «base» (полегче)."
    else
        setval WHISPER_MODEL small
        echo "    Память $RAM_MB МБ — модель распознавания «small»."
    fi
    echo "    Сама модель (~465 МБ) скачается при первом голосовом."
fi

if [ -z "$(getval BACKUP_PASSPHRASE)" ]; then
    say "Генерирую пароль для шифрования бэкапов"
    setval BACKUP_PASSPHRASE "$($PY -c 'import secrets; print(secrets.token_urlsafe(32))')"
    warn "Пароль записан в .env. Скопируй его ОТДЕЛЬНО от сервера, иначе"
    warn "бэкапы будет нечем восстановить:"
    warn "    grep BACKUP_PASSPHRASE $DIR/.env"
fi

# ---------------------------------------------------------------- вход

# Наличие файла сессии ещё ничего не значит: Telethon создаёт его при первом
# же подключении, даже если вход не довели до конца. Проверяем авторизацию.
is_authorized() {
    $PY - <<'AUTHCHECK' 2>/dev/null
import asyncio, sys
import config
from telethon import TelegramClient

async def go():
    c = TelegramClient(config.SESSION_PATH, config.API_ID, config.API_HASH)
    ok = False
    try:
        await c.connect()
        ok = await c.is_user_authorized()
    except Exception:
        ok = False
    finally:
        try:
            await c.disconnect()
        except Exception:
            pass
    sys.exit(0 if ok else 1)

asyncio.run(go())
AUTHCHECK
}

if is_authorized; then
    echo "    Вход уже выполнен, пропускаю"
else
    say "Первый вход в Telegram"
    echo "    Сейчас попросит код из приложения (и пароль 2FA, если он стоит)."
    echo "    Потом предложит создать канал для логов — соглашайся."
    $PY setup.py || die "Вход не выполнен. Запусти ещё раз: $DIR/install.sh"
    is_authorized || die "Вход не завершён — сервис без него работать не сможет"
fi

chmod 600 antidelete.session 2>/dev/null || true

# ---------------------------------------------------------------- бот

if [ -z "$(getval BOT_TOKEN)" ]; then
    say "Бот для уведомлений (по желанию, но рекомендую)"
    cat <<'TXT'
    Юзербот шлёт алерты от твоего же имени, а о своих сообщениях Telegram
    не уведомляет — они приходят уже прочитанными. Бот это чинит и заодно
    даёт меню команд.
      1. Открой @BotFather -> /newbot -> задай имя
      2. Скопируй токен сюда
      3. Потом обязательно нажми Start у своего бота
    Можно пропустить (Enter) и добавить позже: ./set-token.sh
TXT
    read -rsp "    Токен (ввод скрыт, Enter — пропустить): " V_TOK
    echo
    if [ -n "$V_TOK" ]; then
        setval BOT_TOKEN "$V_TOK"
        unset V_TOK
        warn "Не забудь нажать Start у бота, иначе он не сможет тебе написать."
    fi
fi

# ---------------------------------------------------------------- индекс

say "Строю индекс поиска"
$PY -c "from cache import Cache; print(f'    {Cache().rebuild_fts()} сообщений в индексе')"

# ---------------------------------------------------------------- systemd

say "Ставлю сервис systemd"
sudo_if_needed tee /etc/systemd/system/$SERVICE.service >/dev/null <<UNIT
[Unit]
Description=Telegram Anti-Delete Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$DIR
ExecStart=$DIR/venv/bin/python3 main.py
Restart=on-failure
RestartSec=10
KillSignal=SIGTERM
TimeoutStopSec=30
StandardOutput=journal
StandardError=journal

# Домашний каталог ниже открыт только на чтение, а генератору токенов YouTube
# и yt-dlp нужен записываемый кэш — иначе YouTube молча не качается.
Environment=XDG_CACHE_HOME=$DIR/.cache

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=$DIR
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
LockPersonality=true
UMask=0077

[Install]
WantedBy=multi-user.target
UNIT

sudo_if_needed tee /etc/systemd/system/$SERVICE-backup.service >/dev/null <<UNIT
[Unit]
Description=Telegram Anti-Delete: шифрованный бэкап

[Service]
Type=oneshot
User=$RUN_USER
WorkingDirectory=$DIR
ExecStart=$DIR/backup.sh
UMask=0077
# Тот же sandbox, что и у основного юнита: бэкап раскладывает открытые
# .env и .session во временную папку — держим её приватной и урезаем доступ.
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=$DIR
ProtectKernelTunables=true
ProtectControlGroups=true
RestrictSUIDSGID=true
LockPersonality=true
UNIT

sudo_if_needed tee /etc/systemd/system/$SERVICE-backup.timer >/dev/null <<'UNIT'
[Unit]
Description=Ежедневный бэкап Telegram Anti-Delete

[Timer]
OnCalendar=daily
Persistent=true
RandomizedDelaySec=1h

[Install]
WantedBy=timers.target
UNIT

chmod +x backup.sh set-token.sh 2>/dev/null || true
sudo_if_needed systemctl daemon-reload
sudo_if_needed systemctl enable --now $SERVICE
sudo_if_needed systemctl enable --now $SERVICE-backup.timer

sleep 4
if systemctl is-active --quiet $SERVICE; then
    say "Готово"
    cat <<TXT
    Сервис работает и поднимется сам после перезагрузки.

    Логи:        journalctl -u $SERVICE -f
    Перезапуск:  systemctl restart $SERVICE
    Бэкап:       $DIR/backup.sh
    Сменить бота:$DIR/set-token.sh

    Дальше по желанию — залить историю переписки, чтобы восстанавливались
    и старые удалённые сообщения (займёт часы, сервис на это время стоит):
        systemctl stop $SERVICE
        $DIR/venv/bin/python3 backfill.py --scan   # сначала оценить объём
        $DIR/venv/bin/python3 backfill.py
        systemctl start $SERVICE
TXT
else
    die "Сервис не поднялся. Смотри: journalctl -u $SERVICE -n 40"
fi
