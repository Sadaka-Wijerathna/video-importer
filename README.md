# BuddyStore Video Importer

Python microservice for bulk-importing Telegram videos using **Hydrogram** + **TgCrypto** + **FastAPI**.

Replaces the old `mtcute` (GramJS) implementation to eliminate OOM crashes on Render free tier.

## Architecture

```
Frontend (Vercel)
    └── BuddyStore Backend (Node.js / Render)
              └── video-importer (this service / Render)
                        └── Telegram MTProto API
```

## Stack

- **[Hydrogram](https://github.com/hydrogram/hydrogram)** — Pyrogram fork for MTProto
- **[TgCrypto](https://github.com/pyrogram/tgcrypto)** — C-extension crypto acceleration
- **FastAPI** — async HTTP API
- **Uvicorn** — ASGI server

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Keep-alive ping |
| POST | `/send-code` | Step 1 of session generation |
| POST | `/generate-session` | Step 2 — verify OTP, return session string |
| POST | `/generate-session-2fa` | 2FA variant |
| POST | `/start-job` | Start import loop |
| POST | `/stop-job` | Stop active import |
| GET | `/job-status/{admin_id}` | Live job status |

## Environment Variables

```env
TELEGRAM_API_ID=...
TELEGRAM_API_HASH=...
IMPORTER_API_SECRET=...   # shared secret with BuddyStore backend
```

## Local Development

```bash
python -m venv venv
source venv/bin/activate  # or .\venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env      # fill in your values
uvicorn main:app --reload --port 8001
```

## Deployment (Render)

1. Connect this repo to a new Render **Web Service**
2. Set env vars in Render dashboard (see `render.yaml`)
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
