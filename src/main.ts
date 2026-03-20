/**
 * Transcoding service (Cloud Run).
 * POST /transcode: optional X-Callback-Token header when TRANSCODE_INGRESS_TOKEN is set; returns 202; background job calls callback on completion/failure.
 */
import './env';
import { createServer, type IncomingMessage, type ServerResponse } from 'http';
import { runTranscode, type TranscodeRequest } from './transcode';

const PORT = parseInt(process.env.PORT ?? '8080', 10);
const TERMINATION_GRACE_MS = parseInt(
  process.env.TERMINATION_GRACE_MS ?? '30000',
  10,
);
/** When set, POST /transcode must send matching X-Callback-Token header. */
const TRANSCODE_INGRESS_TOKEN =
  process.env.TRANSCODE_INGRESS_TOKEN?.trim() ?? '';

if (process.env.K_SERVICE) {
  console.warn(
    '[Transcoding] Cloud Run (K_SERVICE set): enable "CPU is always allocated" for this revision — background work runs after HTTP 202. See README.',
  );
}

let draining = false;
let jobInFlight: Promise<void> | null = null;
let currentJobAbort: AbortController | null = null;

const MAX_BODY_BYTES = 1024 * 1024;

function parseBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        req.destroy();
        reject(new Error('Request body too large'));
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      try {
        const body = Buffer.concat(chunks).toString('utf-8');
        resolve(body ? JSON.parse(body) : {});
      } catch (e) {
        reject(e instanceof Error ? e : new Error(String(e)));
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res: ServerResponse, status: number, data: unknown): void {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function getHeader(req: IncomingMessage, name: string): string | undefined {
  const key = name.toLowerCase();
  const v = req.headers[key];
  if (v === undefined) return undefined;
  return Array.isArray(v) ? v[0] : v;
}

function validateTranscodeRequest(body: unknown): body is TranscodeRequest {
  if (!body || typeof body !== 'object') return false;
  const b = body as Record<string, unknown>;
  return (
    typeof b.jobId === 'string' &&
    typeof b.sourceMediaId === 'string' &&
    typeof b.sourceObjectKey === 'string' &&
    typeof b.sourceBucket === 'string' &&
    typeof b.callbackUrl === 'string' &&
    typeof b.callbackToken === 'string'
  );
}

function runJob(body: TranscodeRequest): Promise<void> {
  const ac = new AbortController();
  currentJobAbort = ac;
  return runTranscode(body, { signal: ac.signal }).finally(() => {
    if (currentJobAbort === ac) {
      currentJobAbort = null;
    }
  });
}

async function handleRequest(
  req: IncomingMessage,
  res: ServerResponse,
): Promise<void> {
  if (req.url === '/health' && req.method === 'GET') {
    if (draining) {
      sendJson(res, 503, { status: 'draining' });
      return;
    }
    sendJson(res, 200, { status: 'ok' });
    return;
  }

  if (req.url === '/transcode' && req.method === 'POST') {
    try {
      if (draining) {
        sendJson(res, 503, {
          error: 'Service is draining; retry on another instance',
        });
        return;
      }
      if (TRANSCODE_INGRESS_TOKEN) {
        const headerToken = getHeader(req, 'x-callback-token');
        if (headerToken !== TRANSCODE_INGRESS_TOKEN) {
          sendJson(res, 401, {
            error: 'Missing or invalid X-Callback-Token header',
          });
          return;
        }
      }
      const body = await parseBody(req);
      if (!validateTranscodeRequest(body)) {
        sendJson(res, 400, {
          error:
            'Invalid request body: jobId, sourceMediaId, sourceObjectKey, sourceBucket, callbackUrl, callbackToken required',
        });
        return;
      }
      if (jobInFlight) {
        sendJson(res, 503, {
          error:
            'Transcode already in progress on this instance; retry or scale out',
        });
        return;
      }
      sendJson(res, 202, { jobId: body.jobId, accepted: true });
      jobInFlight = runJob(body).catch((err) => {
        console.error(`[Transcode] jobId=${body.jobId} background error:`, err);
      });
      void jobInFlight.finally(() => {
        jobInFlight = null;
      });
    } catch (err) {
      console.error('[Transcode] Parse error:', err);
      sendJson(res, 400, { error: 'Invalid JSON body' });
    }
    return;
  }

  res.writeHead(404);
  res.end();
}

const server = createServer((req, res) => {
  void handleRequest(req, res);
});

server.listen(PORT, () => {
  console.log(`Transcoding service listening on port ${PORT}`);
});

function shutdown(): void {
  draining = true;
  currentJobAbort?.abort();
  server.close(() => {
    void (async () => {
      if (jobInFlight) {
        await Promise.race([
          jobInFlight,
          new Promise<void>((resolve) =>
            setTimeout(resolve, TERMINATION_GRACE_MS),
          ),
        ]);
      }
      process.exit(0);
    })();
  });
}

process.on('SIGTERM', () => {
  console.warn('[Transcoding] SIGTERM received; draining and closing server');
  shutdown();
});

process.on('SIGINT', () => {
  shutdown();
});
