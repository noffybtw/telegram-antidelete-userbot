# Telegram anti-delete userbot

A userbot that runs on a personal Telegram account. When someone deletes a message "for everyone" in a private chat or a small group, it sends the deleted text and media to a private log chat. It also shows edits (old and new text, only the changed words struck through), keeps view-once photos, videos and voice notes, transcribes voice notes on the server, downloads videos by link and exports a whole chat to one HTML file.

User-facing texts, code comments and the comments in `.env.example` are in Russian.

## Stack

Python 3 with asyncio, [Telethon](https://docs.telethon.dev) (MTProto client), SQLite, [faster-whisper](https://github.com/SYSTRAN/faster-whisper) for local transcription, [yt-dlp](https://github.com/yt-dlp/yt-dlp) for downloads, an optional Bot API bot for notifications, a systemd unit and a bash installer.

## How it works

Telegram's delete event carries only a list of message ids: no text, no author, no chat. Nothing can be restored from Telegram afterwards, so every incoming message is written to SQLite first and its media is downloaded at once (the file reference stops working after the deletion).

```
incoming message -> row in SQLite -> file on disk
delete event     -> lookup in the cache -> alert to the log chat -> local file removed
```

- The event does not say which chat it came from. In private chats message ids come from one sequence per account, so the id alone finds the chat.
- A file that failed to download is never presented as saved: an empty or cut file is retried up to three times, each time with a fresh file reference; if it still fails, the alert says so and why. A deletion that arrives while the file is still downloading waits for it up to two minutes.
- The disk does not grow: a file stays local only until its alert is sent, then it lives in the log chat. Messages that are not deleted keep their media for `MEDIA_RETENTION_DAYS` (7) under a `MEDIA_MAX_TOTAL_MB` cap.
- Five or more deletions from one person within 30 seconds become one summary alert instead of five.
- Which chats are watched is decided once per chat and rechecked daily: private chats and groups up to `GROUP_MAX_MEMBERS` (30) yes; channels, bigger groups, bots and the log chat itself no. `/watch` and `/unwatch` override the decision.
- FloodWait is respected: the bot waits as long as Telegram says.

What it cannot do, because Telegram sends no event for it: catch "delete for me", record calls.

## Files

| | |
|---|---|
| `main.py` | event handlers, commands, background tasks |
| `cache.py` | SQLite: messages, edits, subscriptions, chat decisions |
| `alerts.py` | alert formatting and sending, FloodWait handling |
| `downloader.py` | downloads by link through yt-dlp |
| `transcribe.py` | voice-note transcription with a local Whisper model |
| `export.py` | chat export to HTML |
| `backfill.py` | one-off load of chat history into the cache |
| `config.py` | reads `.env` |
| `util.py` | shared helpers |
| `setup.py` | first run: login, log channel creation |
| `install.sh`, `update.sh`, `set-token.sh`, `backup.sh` | install on a clean server, update, change the bot token, encrypted backup |
| `test_pipeline.py` | offline test run of the whole pipeline |

## Run

On a Debian or Ubuntu server:

```
./install.sh
```

It checks Python, installs the dependencies into `venv/`, creates `.env`, asks for the Telegram login (code and 2FA password), builds the search index and starts the `telegram-antidelete` systemd service. Running it again does not overwrite keys that are already set.

By hand:

```
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp .env.example .env           # API_ID and API_HASH from my.telegram.org, PHONE
venv/bin/python setup.py       # login, creates the log channel and writes LOG_CHAT_ID
venv/bin/python main.py
```

`BOT_TOKEN` is optional: with a bot the alerts arrive as unread notifications, without it the userbot posts them itself. `./set-token.sh` reads the token without echo and writes it into `.env`, so it never shows up in the shell history or in `ps`.

Commands go to the bot, the log chat or Saved Messages, and only the account owner can use them:

| | |
|---|---|
| `/menu` | switches: what to send, pause |
| `/find <text>` | search the whole history, deleted messages included |
| `/export <chat>` | the whole chat as one HTML file |
| `/stats [chat]` | what is in the cache, what was caught |
| `/sub <chat>`, `/subs` | per-person subscriptions: voice transcripts, profile changes, online status |
| `/dl <link>` | download a video (`mp3` at the end for audio only) |
| `/voice [chat] [N]` | voice notes with transcripts; for the sender they stay unplayed |
| `/read <chat>` | the chat text without marking it as read |
| `/story <chat>` | stories of a person without appearing in the viewers list |
| `/watch`, `/unwatch`, `/auto <chat>` | force watching on or off, back to the automatic decision |
| `/pause`, `/resume` | stop alerts, the cache keeps filling |

## Security

- The `*.session` file is full access to the account without password or 2FA. It is in `.gitignore`, never committed or sent anywhere; `.env` is `chmod 600`.
- The process sets `umask 0077`, so the database, media and session are readable only by the owner.
- The systemd unit runs with `ProtectSystem=strict`, `ProtectHome=read-only`, `NoNewPrivileges` and `PrivateTmp`; it can write only to the project folder.
- yt-dlp runs as a separate process without a shell, with `--` before the link and `--ignore-config`; links to internal network addresses are refused.
- Transcription runs only on the server: the audio is not sent to any cloud service.

## Tests

```
venv/bin/python test_pipeline.py
```

631 checks against a fake Telegram client, with no network: single and bulk deletions, the account's own deletions, messages missing from the cache, media and failed sends, FloodWait, edits, pause, disk cleanup, small and large groups, full-text search, manual overrides, per-person subscriptions, voice transcription, refused internal-network links, no argument injection into yt-dlp, HTML escaping in the export and file name safety. The script prints every check with ✅ or ❌, lists the failed ones at the end and exits with code 1 if any check failed.
