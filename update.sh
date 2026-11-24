#!/usr/bin/env bash
# Обновление до новой версии без потери своих данных.
#
#   ./update.sh ФАЙЛ.tar.gz     — обновиться из скачанного архива
#   ./update.sh ССЫЛКА          — скачать и обновиться
#   ./update.sh --rollback      — вернуть предыдущую версию
#
# Что НИКОГДА не трогается: .env, файлы сессии, база переписки, сохранённое
# медиа, куки, список прокси, бэкапы. Обновляется только код.
#
# Перед заменой снимается копия текущей версии — если новая не заработает,
# откат делается одной командой.

set -euo pipefail
cd "$(dirname "$0")"

DIR="$(pwd)"
SERVICE="telegram-antidelete"
BACKUP_DIR=".prev-version"

say()  { printf '\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m !  %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m!!  %s\033[0m\n' "$*" >&2; exit 1; }

sudo_if_needed() {
    if [ "$(id -u)" -eq 0 ]; then "$@"; else sudo "$@"; fi
}

# Ровно то, что считается «кодом». Всё остальное в папке — твоё, и не трогается.
# Список намеренно белый: при чёрном однажды забудешь новый файл с данными
# и сотрёшь его обновлением.
CODE=(
    main.py cache.py alerts.py config.py util.py
    downloader.py transcribe.py export.py
    setup.py backfill.py test_pipeline.py
    install.sh backup.sh set-token.sh update.sh
    requirements.txt .env.example README.md INSTALL.md
    LICENSE THIRD-PARTY.md
)

# Юнит считаем «своим», только если он запускает бота ИМЕННО из этой папки.
# Иначе обновление, запущенное из копии или не из того каталога, остановило бы
# чужой работающий экземпляр — и человек этого даже не заметил бы.
service_exists() {
    systemctl cat "$SERVICE.service" >/dev/null 2>&1 || return 1
    local wd
    wd="$(systemctl show -p WorkingDirectory --value "$SERVICE.service" 2>/dev/null || true)"
    wd="${wd#-}"          # systemd помечает необязательный путь минусом
    [ -n "$wd" ] && [ "$wd" = "$DIR" ]
}

start_service() {
    service_exists || return 0
    sudo_if_needed systemctl start "$SERVICE" || warn "Сервис не поднялся, смотри: journalctl -u $SERVICE -n 50"
}

# ---------------------------------------------------------------- откат

if [ "${1:-}" = "--rollback" ]; then
    [ -d "$BACKUP_DIR" ] || die "Откатываться не к чему: папки $BACKUP_DIR нет."
    say "Возвращаю предыдущую версию"
    service_exists && sudo_if_needed systemctl stop "$SERVICE" || true
    for f in "$BACKUP_DIR"/*; do
        [ -e "$f" ] || continue
        cp -p "$f" "./$(basename "$f")"
    done
    start_service
    say "Готово. Вернулась версия, которая работала до последнего обновления."
    exit 0
fi

SRC="${1:-}"
[ -n "$SRC" ] || die "Укажи файл или ссылку на архив:
    ./update.sh telegram-antidelete-2026.09.06.tar.gz
    ./update.sh https://.../telegram-antidelete-2026.09.06.tar.gz
    ./update.sh --rollback"

# ---------------------------------------------------------------- получить архив

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
chmod 700 "$TMP"

if [ -f "$SRC" ]; then
    ARCHIVE="$(cd "$(dirname "$SRC")" && pwd)/$(basename "$SRC")"
else
    case "$SRC" in
        http://*|https://*) ;;
        *) die "Файла $SRC нет, и на ссылку это не похоже." ;;
    esac
    say "Качаю архив"
    ARCHIVE="$TMP/update.tar.gz"
    curl -fsSL --max-time 300 -o "$ARCHIVE" -- "$SRC" \
        || die "Не скачалось. Проверь ссылку и что сервер доступен."
fi

say "Распаковываю"
mkdir -p "$TMP/new"
tar -xzf "$ARCHIVE" -C "$TMP/new" || die "Архив не читается — скачался не полностью?"

# В архиве один каталог с версией внутри
NEW="$(find "$TMP/new" -mindepth 1 -maxdepth 1 -type d | head -1)"
[ -n "$NEW" ] && [ -f "$NEW/main.py" ] || die "Это не архив с ботом: внутри нет main.py."

# Версия из имени каталога — просто чтобы человек видел, куда идёт
NEW_VER="$(basename "$NEW" | sed 's/^telegram-antidelete-//')"

# Архив не должен нести чужой .env или сессию: и как защита от подмены
# настроек, и как проверка, что скачано именно то, что собирал продавец.
for bad in .env antidelete.session antidelete-bot.session antidelete.db; do
    [ -e "$NEW/$bad" ] && die "В архиве лежит $bad — так быть не должно, обновление отменено."
done

say "Обновляю до версии $NEW_VER"

# ---------------------------------------------------------------- копия текущей

rm -rf "$BACKUP_DIR"
mkdir -p "$BACKUP_DIR" && chmod 700 "$BACKUP_DIR"
for f in "${CODE[@]}"; do
    [ -e "$f" ] && cp -p "$f" "$BACKUP_DIR/"
done

# ---------------------------------------------------------------- замена

service_exists && { say "Останавливаю бота"; sudo_if_needed systemctl stop "$SERVICE"; } || true

for f in "${CODE[@]}"; do
    [ -e "$NEW/$f" ] || continue
    cp -p "$NEW/$f" "./$f"
done
chmod 755 install.sh update.sh 2>/dev/null || true
chmod 700 backup.sh set-token.sh 2>/dev/null || true

# Зависимости могли смениться — доставляем молча, ошибку не глотаем
if [ -x venv/bin/pip ]; then
    say "Проверяю зависимости"
    ./venv/bin/pip install -q -r requirements.txt || {
        warn "Зависимости не доставились. Откатываюсь."
        exec "$DIR/update.sh" --rollback
    }
else
    warn "Окружения venv нет — запусти ./install.sh"
fi

# ---------------------------------------------------------------- проверка

say "Прогоняю проверки"
if [ -x venv/bin/python3 ] && [ -f test_pipeline.py ]; then
    if ! ./venv/bin/python3 test_pipeline.py >"$TMP/tests.log" 2>&1; then
        warn "Проверки не прошли — новая версия не встала. Откатываюсь."
        tail -20 "$TMP/tests.log" >&2
        exec "$DIR/update.sh" --rollback
    fi
    echo "    $(grep -c '✅' "$TMP/tests.log" || echo 0) проверок пройдено"
fi

start_service

if service_exists; then
    sleep 5
    if [ "$(systemctl is-active "$SERVICE" || true)" != "active" ]; then
        warn "Бот не поднялся на новой версии. Откатываюсь."
        journalctl -u "$SERVICE" -n 20 --no-pager >&2 || true
        exec "$DIR/update.sh" --rollback
    fi
fi

say "Обновлено до $NEW_VER"
echo
echo "Прошлая версия лежит в $BACKUP_DIR — если что-то не так:"
echo "    ./update.sh --rollback"
