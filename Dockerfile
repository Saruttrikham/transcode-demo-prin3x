# Build stage
FROM node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY src ./src
RUN npm run build && npm prune --omit=dev

# Production stage
FROM node:22-alpine AS production

RUN apk add --no-cache ffmpeg

RUN addgroup -g 1001 appuser && \
    adduser -u 1001 -G appuser -s /bin/false -D appuser

WORKDIR /app

COPY --from=builder --chown=appuser:appuser /app/dist ./dist
COPY --from=builder --chown=appuser:appuser /app/node_modules ./node_modules
COPY --from=builder --chown=appuser:appuser /app/package.json ./

USER appuser

ENV PORT=8080

EXPOSE 8080

CMD ["node", "dist/main.js"]
