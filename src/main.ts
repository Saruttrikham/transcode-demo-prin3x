/**
 * Transcoding service (Cloud Run).
 * POST /transcode: optional X-Callback-Token header when TRANSCODE_INGRESS_TOKEN is set; returns 202; background job calls callback on completion/failure.
 * POST /transcode-test: FFmpeg lavfi synthetic test (no object storage); optional callback; returns 200 with inline result.
 */
import './env';
import { timingSafeEqual } from 'crypto';
import { createServer, type IncomingMessage, type ServerResponse } from 'http';
import {
  isValidSubtitleFormatInput,
  postJsonCallback,
  runSyntheticFfmpegTest,
  runTranscode,
  SUBTITLE_LANGUAGES,
  type SubtitleLanguage,
  type SubtitleTrackInput,
  type SyntheticFfmpegTestRequest,
  type TranscodeRequest,
} from './transcode';

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

function safeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  return timingSafeEqual(Buffer.from(a), Buffer.from(b));
}

function isSubtitleLanguage(s: string): s is SubtitleLanguage {
  return (SUBTITLE_LANGUAGES as readonly string[]).includes(s);
}

function isValidSubtitleTrackEntry(x: unknown): x is SubtitleTrackInput {
  if (!x || typeof x !== 'object') return false;
  const o = x as Record<string, unknown>;
  if (typeof o.objectKey !== 'string' || !String(o.objectKey).trim()) {
    return false;
  }
  if (typeof o.language !== 'string' || !isSubtitleLanguage(o.language)) {
    return false;
  }
  if (o.bucket !== undefined && typeof o.bucket !== 'string') return false;
  if (!isValidSubtitleFormatInput(o.format)) return false;
  return true;
}

function validateTranscodeRequest(body: unknown): body is TranscodeRequest {
  if (!body || typeof body !== 'object') return false;
  const b = body as Record<string, unknown>;

  const base =
    typeof b.jobId === 'string' &&
    typeof b.sourceMediaId === 'string' &&
    typeof b.sourceBucket === 'string' &&
    typeof b.callbackUrl === 'string' &&
    typeof b.callbackToken === 'string';

  if (!base) return false;


  if (b.sourceObjectKey !== undefined && typeof b.sourceObjectKey !== 'string') {
    return false;
  }

  if (b.subtitleBucket !== undefined && typeof b.subtitleBucket !== 'string') {
    return false;
  }

  if (b.subtitles !== undefined) {
    if (!Array.isArray(b.subtitles)) return false;
    if (!b.subtitles.every(isValidSubtitleTrackEntry)) return false;
    const langs = b.subtitles.map(
      (t: SubtitleTrackInput) => t.language,
    );
    if (new Set(langs).size !== langs.length) return false;
  }

  const hasUsableSource =
    typeof b.sourceObjectKey === 'string' &&
    String(b.sourceObjectKey).trim() !== '';

  const hasSubtitleArray =
    Array.isArray(b.subtitles) && b.subtitles.length > 0;

  if (!hasUsableSource && !hasSubtitleArray) {
    return false;
  }

  return true;
}

function validationErrorHint(): string {
  return (
    'Invalid request body: jobId, sourceMediaId, sourceBucket, callbackUrl, callbackToken required. ' +
    'Provide sourceObjectKey (non-empty) for a full transcode, or omit it and send a non-empty subtitles array ' +
    '(each entry: objectKey + language \"th\"|\"en\", optional bucket, optional format). ' +
    'subtitleOnly is optional and ignored. Subtitle languages must be unique per job.'
  );
}

function syntheticTestErrorHint(): string {
  return (
    'Invalid request body for /transcode-test: jobId (non-empty string) required. ' +
    'Optional: durationSec (1–120), width/height (1–4096), fps (1–60), audio (boolean). ' +
    'Optional callback: send both callbackUrl and callbackToken, or omit both.'
  );
}

function validateSyntheticTestBody(
  body: unknown,
): body is SyntheticFfmpegTestRequest {
  if (!body || typeof body !== 'object') return false;
  const b = body as Record<string, unknown>;
  if (typeof b.jobId !== 'string' || !b.jobId.trim()) return false;

  if (
    b.durationSec !== undefined &&
    (typeof b.durationSec !== 'number' || !Number.isFinite(b.durationSec))
  ) {
    return false;
  }
  if (b.width !== undefined && (typeof b.width !== 'number' || !Number.isFinite(b.width))) {
    return false;
  }
  if (b.height !== undefined && (typeof b.height !== 'number' || !Number.isFinite(b.height))) {
    return false;
  }
  if (b.fps !== undefined && (typeof b.fps !== 'number' || !Number.isFinite(b.fps))) {
    return false;
  }
  if (b.audio !== undefined && typeof b.audio !== 'boolean') return false;

  const hasUrl =
    b.callbackUrl !== undefined &&
    b.callbackUrl !== null &&
    String(b.callbackUrl).trim() !== '';
  const hasToken =
    b.callbackToken !== undefined &&
    b.callbackToken !== null &&
    String(b.callbackToken).trim() !== '';

  if (hasUrl !== hasToken) return false;
  if (hasUrl) {
    if (typeof b.callbackUrl !== 'string' || !b.callbackUrl.trim()) return false;
    if (typeof b.callbackToken !== 'string' || !b.callbackToken.trim()) return false;
  }

  return true;
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
        if (!headerToken || !safeEqual(headerToken, TRANSCODE_INGRESS_TOKEN)) {
          sendJson(res, 401, {
            error: 'Missing or invalid X-Callback-Token header',
          });
          return;
        }
      }
      const body = await parseBody(req);
      if (!validateTranscodeRequest(body)) {
        sendJson(res, 400, {
          error: validationErrorHint(),
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

  if (
    (req.url === '/transcode-test' || req.url?.startsWith('/transcode-test?')) &&
    req.method === 'POST'
  ) {
    try {
      if (draining) {
        sendJson(res, 503, {
          error: 'Service is draining; retry on another instance',
        });
        return;
      }
      if (TRANSCODE_INGRESS_TOKEN) {
        const headerToken = getHeader(req, 'x-callback-token');
        if (!headerToken || !safeEqual(headerToken, TRANSCODE_INGRESS_TOKEN)) {
          sendJson(res, 401, {
            error: 'Missing or invalid X-Callback-Token header',
          });
          return;
        }
      }
      const body = await parseBody(req);
      if (!validateSyntheticTestBody(body)) {
        sendJson(res, 400, { error: syntheticTestErrorHint() });
        return;
      }
      const result = await runSyntheticFfmpegTest(body);
      if (body.callbackUrl && body.callbackToken) {
        try {
          await postJsonCallback(body.callbackUrl.trim(), body.callbackToken.trim(), {
            kind: 'SYNTHETIC_FFMPEG_TEST',
            ...result,
          });
        } catch (cbErr) {
          console.error('[Transcode] /transcode-test callback error:', cbErr);
          sendJson(res, 200, {
            ...result,
            callbackError:
              cbErr instanceof Error ? cbErr.message : String(cbErr),
          });
          return;
        }
      }
      sendJson(res, 200, result);
    } catch (err) {
      console.error('[Transcode] /transcode-test error:', err);
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
