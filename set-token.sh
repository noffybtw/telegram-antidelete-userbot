#!/usr/bin/env bash
# Смена токена бота без светки в истории команд и в списке процессов.
#
#   ./set-token.sh
#
# Токен вводится скрыто (не отображается при наборе), пишется прямо в .env
# средствами Python — не через sed в командной строке, где он был бы виден
# в `ps` любому пользователю системы.

set -euo pipefail
cd "$(dirname "$0")"

read -rsp "Новый токен от @BotFather: " TOKEN
echo

if [[ ! "$TOKEN" =~ ^[0-9]+:[A-Za-z0-9_-]+$ ]]; then
    echo "Не похоже на токен. Формат: 1234567890:AAA..." >&2
    exit 1
fi

TOKEN="$TOKEN" ./venv/bin/python - <<'PY'
import os, pathlib, re
p = pathlib.Path(".env")
s = p.read_text()
tok = os.environ["TOKEN"]
if re.search(r'^BOT_TOKEN=', s, re.M):
    s = re.sub(r'^BOT_TOKEN=.*$', f'BOT_TOKEN={tok}', s, count=1, flags=re.M)
else:
    s = s.rstrip("\n") + f"\nBOT_TOKEN={tok}\n"
p.write_text(s)
p.chmod(0o600)
print("Токен записан в .env")
PY

unset TOKEN

# Порядок важен: сначала гасим сервис, потом трогаем файл сессии. Иначе
# работающий клиент пишет в удалённую из-под него базу и падает с
# «no such table: entities» — systemd поднимет, но зачем этот шум.
systemctl stop telegram-antidelete 2>/dev/null || true
rm -f antidelete-bot.session          # к новому токену старая сессия не подходит
systemctl start telegram-antidelete 2>/dev/null || echo "Запусти сервис вручную."
sleep 4
systemctl is-active --quiet telegram-antidelete \
    && echo "Сервис перезапущен, бот подключён." \
    || echo "Сервис не поднялся — смотри: journalctl -u telegram-antidelete -n 30"
