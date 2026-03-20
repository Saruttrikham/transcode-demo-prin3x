# Build stage
FROM node:22-slim AS builder

WORKDIR /app

COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY src ./src
RUN npm run build && npm prune --omit=dev

# Production stage
FROM node:22-slim AS production

RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1001 appuser && \
    useradd -u 1001 -g appuser -m -s /bin/false appuser

WORKDIR /app

COPY --from=builder --chown=appuser:appuser /app/dist ./dist
COPY --from=builder --chown=appuser:appuser /app/node_modules ./node_modules
COPY --from=builder --chown=appuser:appuser /app/package.json ./

USER appuser

# Cloud Run sets PORT; keep 8080 as default for local/docker.
ENV PORT=8080

EXPOSE 8080

CMD ["node", "dist/main.js"]
