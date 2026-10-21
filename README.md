---
title: BuddyStore Video Importer
emoji: 📹
colorFrom: blue
colorTo: purple
sdk: docker
pinned: false
short_description: Telegram video importer microservice for BuddyStore
startup_duration_timeout: 5m
---

# BuddyStore Video Importer

Python microservice for bulk-importing Telegram videos using **Hydrogram** + **TgCrypto** + **FastAPI**.

## Architecture

```
Frontend (Vercel)
    └── BuddyStore Backend (Node.js / Render)
              └── video-importer (this service / HuggingFace Spaces)
                        └── Telegram MTProto API
```

## Stack

- **[Hydrogram](https://github.com/hydrogram/hydrogram)** — Pyrogram fork for MTProto
- **[TgCrypto](https://github.com/pyrogram/tgcrypto)** — C-extension crypto acceleration
- **FastAPI** — async HTTP API
- **Uvicorn** — ASGI server (port 7860 on HF Spaces)

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

## Environment Variables (set as Secrets in HF Spaces settings)

```env
TELEGRAM_API_ID=...
TELEGRAM_API_HASH=...
IMPORTER_API_SECRET=...        # shared secret with BuddyStore backend
BUDDYSTORE_BACKEND_URL=...     # e.g. https://buddystore-backend.onrender.com
```

## Local Development

```bash
python -m venv venv
source venv/bin/activate  # or .\venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env      # fill in your values
uvicorn main:app --reload --port 8001
```

## Deployment (Hugging Face Spaces)

1. Create a new Space at [huggingface.co/new-space](https://huggingface.co/new-space) — pick **Docker** SDK
2. Add the HF Space as a git remote and push
3. Set **Secrets** (not env vars) in Space Settings → Repository secrets:
   - `TELEGRAM_API_ID`
   - `TELEGRAM_API_HASH`
   - `IMPORTER_API_SECRET`
   - `BUDDYSTORE_BACKEND_URL`
4. Space auto-builds from `Dockerfile` — app runs on port 7860

## Deployment (Render — legacy)

1. Connect repo to Render Web Service, set env vars from `render.yaml`
2. Build: `pip install -r requirements.txt`
3. Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`
