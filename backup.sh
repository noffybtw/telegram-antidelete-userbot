#!/usr/bin/env bash
# Шифрованный бэкап базы, сессии и .env.
#
# Зачем шифрование: в базе вся переписка открытым текстом, а .session — полный
# доступ к аккаунту без пароля и 2FA. Незашифрованный бэкап, уехавший в облако
# или на другой диск, — это утечка всего сразу.
#
# Пароль берётся из BACKUP_PASSPHRASE в .env и хранить его надо ОТДЕЛЬНО от
# самих бэкапов. Пароль передаётся gpg через файловый дескриптор, а не
# аргументом — иначе он был бы виден в списке процессов любому в системе.
#
#   ./backup.sh                — сделать бэкап
#   ./backup.sh restore ФАЙЛ   — расшифровать в ./restored/

set -euo pipefail
cd "$(dirname "$0")"

DIR="${BACKUP_DIR:-./backups}"
KEEP="${BACKUP_KEEP:-7}"

# Порядок важен для восстановления. Когда сервер потерян, у тебя на руках
# только .gpg-файл и отдельно сохранённый пароль — а .env лежит ВНУТРИ этого
# самого архива. Поэтому пароль можно передать переменной окружения или ввести
# руками, и только в последнюю очередь он берётся из .env.
PASS="${BACKUP_PASSPHRASE:-}"
if [ -z "$PASS" ]; then
    PASS="$(grep -E '^BACKUP_PASSPHRASE=' .env 2>/dev/null | cut -d= -f2- || true)"
fi
if [ -z "$PASS" ] && [ "${1:-}" = "restore" ] && [ -t 0 ]; then
    printf 'Пароль от бэкапа (тот, что сохранён отдельно от сервера): ' >&2
    read -r -s PASS
    printf '\n' >&2
fi
if [ -z "$PASS" ]; then
    echo "Пароль не задан. Укажи его так:" >&2
    echo "    BACKUP_PASSPHRASE='...' $0 ${*:-}" >&2
    echo "либо положи BACKUP_PASSPHRASE в .env." >&2
    exit 1
fi

# Свой каталог для gpg внутри проекта. Под systemd домашний каталог открыт
# только на чтение, а gpg даже для --symmetric создаёт там pubring.kbx,
# lock-файлы, сокет агента и random_seed. На чистом сервере (где ~/.gnupg ещё
# не существует) он из-за этого просто падает с кодом 2: архив либо не
# создаётся вовсе, либо создаётся, но set -o pipefail обрывает скрипт до
# ротации — и старые бэкапы копятся, пока не кончится диск.
export GNUPGHOME="$PWD/.gnupg"
mkdir -p "$GNUPGHOME" && chmod 700 "$GNUPGHOME"

gpg_run() {
    gpg --batch --yes --quiet --pinentry-mode loopback --passphrase-fd 3 "$@" 3<<<"$PASS"
}

if [ "${1:-}" = "restore" ]; then
    src="${2:?укажи файл бэкапа}"
    mkdir -p restored && chmod 700 restored
    gpg_run -d "$src" | tar -xz -C restored
    chmod -R go-rwx restored
    echo "Распаковано в ./restored — там ключи от аккаунта, удали после использования."
    exit 0
fi

mkdir -p "$DIR" && chmod 700 "$DIR"
out="$DIR/antidelete-$(date +%Y%m%d-%H%M%S).tar.gz.gpg"

tmp="$(mktemp -d)"
chmod 700 "$tmp"
trap 'rm -rf "$tmp"' EXIT

# Снимок базы через SQLite backup API: копировать файл под нагрузкой нельзя,
# WAL окажется рассинхронизирован с основным файлом и бэкап будет битым.
./venv/bin/python - "$tmp/antidelete.db" <<'PY'
import sqlite3, sys
src = sqlite3.connect("antidelete.db")
dst = sqlite3.connect(sys.argv[1])
with dst:
    src.backup(dst)
src.close(); dst.close()
PY

cp antidelete.session "$tmp/" 2>/dev/null || true
cp .env "$tmp/" 2>/dev/null || true

# Пишем во временное имя и переименовываем только после успеха: иначе
# оборвавшийся бэкап остался бы лежать как готовый и однажды вытеснил бы
# ротацией последний целый.
tar -czf - -C "$tmp" . | gpg_run --symmetric --cipher-algo AES256 -o "$out.part"
mv "$out.part" "$out"
chmod 600 "$out"
echo "Готово: $out ($(du -h "$out" | cut -f1))"

ls -1t "$DIR"/antidelete-*.tar.gz.gpg 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -f
echo "Храним последние $KEEP бэкапов."
