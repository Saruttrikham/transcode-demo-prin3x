# Transcoding service

HTTP service: downloads a source video from S3-compatible storage (MinIO), transcodes multiple MP4 renditions + SMIL manifest + thumbnails, uploads results, then **POSTs JSON to your `callbackUrl`**.

## Quick start (local Node)

```bash
npm install
npm run env:init          # creates .env from .env.example if missing
# edit .env (MinIO keys, CALLBACK_*, etc.)
npm run dev               # build + node dist/main.js
```

- Health: `GET http://localhost:8080/health`
- Start job: `POST http://localhost:8080/transcode` (JSON body below)

## Quick start (Docker, like Cloud Run)

Requires **Docker** + **Docker Compose**. MinIO (or API) on your machine is reached via `host.docker.internal` (see `compose.env`).

```bash
npm install
npm run env:init          # need .env for secrets (not in the image)
npm run docker:up         # http://localhost:8080
```

Or without Compose:

```bash
npm run docker:build
npm run docker:run
```

**Callbacks from the container:** `http://localhost:…` in `callbackUrl` would hit the container itself. When the service runs **inside a container** (detected via `/.dockerenv`), it automatically rewrites `localhost` / `127.0.0.1` to `host.docker.internal` before calling your API. Local `npm run dev` on the host does **not** rewrite, so `localhost` stays correct there.

**If MinIO is not on the host** (e.g. remote server): remove or edit `MINIO_ENDPOINT` in `compose.env` so it does not override your `.env`.

## Environment variables

| Variable | Description |
|----------|-------------|
| `PORT` | HTTP port (default `8080`; Cloud Run sets this) |
| `MINIO_ENDPOINT` | S3-compatible host (no `https://`) |
| `MINIO_PORT` | Port (e.g. `9000`) |
| `MINIO_USE_SSL` | `true` / `false` |
| `MINIO_ACCESS_KEY` | Access key |
| `MINIO_SECRET_KEY` | Secret key |
| `TRANSCODING_MAX_SOURCE_FILE_SIZE_BYTES` | Max input size (default 2 GiB) |
| `TRANSCODE_TIMEOUT_MS` | FFmpeg timeout (default 300000) |
| `CALLBACK_API_KEY` | Sent as `X-API-KEY` on outbound callback |
| `CALLBACK_API_VERSION` | Sent as `X-API-Version` on outbound callback |
| `TRANSCODE_INGRESS_TOKEN` | If set, `POST /transcode` must send `X-Callback-Token` with the same value |

`.env` is loaded automatically for local runs (`src/env.ts`). It is **not** copied into Docker images (see `.dockerignore`); Compose / Cloud Run inject env at runtime.

## HTTP API

### `POST /transcode`

Optional header if `TRANSCODE_INGRESS_TOKEN` is set: `X-Callback-Token: <token>`.

Body (minimal — full transcode, no subtitles):

```json
{
  "jobId": "uuid",
  "sourceMediaId": "uuid",
  "sourceObjectKey": "path/to/source.mp4",
  "sourceBucket": "my-bucket",
  "callbackUrl": "https://api.example.com/webhooks/transcode",
  "callbackToken": "secret-for-your-webhook"
}
```

Example with **Thai and/or English** subtitles (`subtitles` is optional; each track requires `language`: `"th"` or `"en"` and `objectKey`; languages must be unique within the array):

```json
{
  "jobId": "uuid",
  "sourceMediaId": "uuid",
  "sourceObjectKey": "path/to/source.mp4",
  "sourceBucket": "my-bucket",
  "callbackUrl": "https://api.example.com/webhooks/transcode",
  "callbackToken": "secret-for-your-webhook",
  "subtitles": [
    { "objectKey": "subs/en.vtt", "language": "en" },
    { "objectKey": "subs/th.vtt", "language": "th" }
  ]
}
```

**Subtitle-only update** (no FFmpeg): omit `sourceObjectKey` or send an empty/whitespace-only value, and provide a non-empty `subtitles` array **or** legacy `subtitleObjectKey` + `subtitleLanguage`. The service checks that shared renditions already exist under `transcoded/{sourceMediaId}/video/*.mp4` and fails with a clear error if not.

| Field | Description |
|-------|-------------|
| `sourceObjectKey` | Source video in `sourceBucket`. Required for a **full transcode** (non-empty string). Omit (or empty) only for **subtitle-only** jobs when subtitle input is present. |
| `subtitles` | Optional array of `{ "objectKey", "language": "th"\|"en", "bucket"?, "format"? }`. Per-track `bucket` defaults to `subtitleBucket` if set, else `sourceBucket`. If `objectKey` has **no file extension**, output uses **`.vtt`** unless `format` is **`"srt"`** (then **`.srt`**). |
| `subtitleObjectKey` | Legacy single subtitle key (use `subtitleBucket` or `sourceBucket`). |
| `subtitleLanguage` | **Required** with `subtitleObjectKey` when using the legacy single-track shape (`th` or `en`). |
| `subtitleFormat` | Optional **`"vtt"`** \| **`"srt"`** when legacy `subtitleObjectKey` has no extension (same rule as per-track `format`). |
| `subtitleBucket` | Default bucket for subtitle object keys when a track omits `bucket`. |
| `subtitleOnly` | **Ignored** if present; subtitle-only mode is **derived** from missing usable `sourceObjectKey` + subtitle fields. |

### Output layout (same bucket as `sourceBucket`)

- **Video renditions (shared):** `transcoded/{sourceMediaId}/video/1080p.mp4`, `720p.mp4`, `480p.mp4`
- **Thumbnails:** `transcoded/{sourceMediaId}/thumbnails/thumb_0.jpeg`, …
- **No subtitles:** manifest `transcoded/{sourceMediaId}/manifest.smil` (references `video/*.mp4`)
- **With subtitle(s):** same root manifest path `transcoded/{sourceMediaId}/manifest.smil`, plus tracks under `transcoded/{sourceMediaId}/subtitles/{language}.<ext>`. The SMIL references `video/*.mp4` and `../subtitles/*` (one `..` so URLs resolve when the playback path looks like `…/manifest.smil/playlist.m3u8`, e.g. nginx-vod). Playback URL stays `…/{sourceMediaId}/manifest.smil/…` — no `variants/` folder.

Object consoles usually sort names **lexicographically**, so you typically see: `manifest.smil`, then `subtitles/`, `thumbnails/`, `video/` — not “thumbnails before subtitles”. To force subtitles last in the list you’d need a naming convention such as `z_subtitles/` (not used by default here).

On success, the callback’s `manifestObjectKey` is always `transcoded/{sourceMediaId}/manifest.smil` when a manifest is written (with or without subtitles).

Response **202**: `{ "jobId": "...", "accepted": true }`. Work continues in the background.

### Callback (outbound)

`POST` to **`callbackUrl`** exactly (no path suffix). Header: `X-Callback-Token: callbackToken`. Body includes `status`: `"COMPLETED"` or `"FAILED"` plus job fields (see `TranscodeCallbackBody*` in `src/transcode.ts`).

## Cloud Run checklist

- Set the same env vars in the service (or Secret Manager).
- Enable **CPU always allocated** if you rely on work after the HTTP 202 response.
- Enough **memory** (e.g. 4–8 GiB) and **timeout** for long transcodes.
- Service account / network access to your object storage endpoint.

### What goes into the production image vs the repo

The **Dockerfile** only copies **`package.json`**, **`package-lock.json`**, **`tsconfig.json`**, and **`src/`**. Dependencies are installed with `npm ci` inside the build stage; **`node_modules`** and **`dist`** from your laptop are never copied in (see [`.dockerignore`](.dockerignore)).

**Committed for deploy / CI** (typical): `Dockerfile`, `src/`, `package.json`, `package-lock.json`, `tsconfig.json`, `.env.example`, `README.md`.

**Local / Compose only** (not in the image): `docker-compose.yml`, `compose.env`, `scripts/init-env.cjs`, your private `.env`.

Build and deploy image from this directory:

```bash
docker build -t transcoding-service .
# push to Artifact Registry, then deploy that image
```

## Scripts (npm)

| Script | Purpose |
|--------|---------|
| `npm run env:init` | Create `.env` from `.env.example` if missing |
| `npm run build` | TypeScript → `dist/` |
| `npm run start` | `node dist/main.js` (after build) |
| `npm run dev` | build + start |
| `npm run docker:build` | Build image `transcoding-service` |
| `npm run docker:run` | Run container with `.env` + `compose.env` |
| `npm run docker:up` | `docker compose up --build` |
| `npm run docker:down` | `docker compose down` |
| `npm run docker:logs` | Follow service logs |
