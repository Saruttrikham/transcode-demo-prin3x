# Build stage
FROM node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY src ./src
RUN npm run build

# Production: Debian slim + apt ffmpeg (includes lavfi; Alpine apk ffmpeg often omits it).
FROM node:22-bookworm-slim AS production

RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg \
  && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1001 appuser \
  && useradd --uid 1001 --gid appuser --shell /usr/sbin/nologin --no-create-home appuser

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY --from=builder --chown=appuser:appuser /app/dist ./dist

USER appuser

ENV PORT=8080

EXPOSE 8080

CMD ["node", "dist/main.js"]
