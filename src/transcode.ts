/**
 * Transcoding logic for Cloud Run.
 * Single-pass multi-rendition FFmpeg, timeout guards, file-based uploads, structured logging.
 */
import ffmpeg from 'fluent-ffmpeg';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as Minio from 'minio';

const RENDITION_PRESETS = [
  { height: 1080, bitrate: '5000k', name: '1080p' },
  { height: 720, bitrate: '2800k', name: '720p' },
  { height: 480, bitrate: '1400k', name: '480p' },
];
const THUMBNAIL_COUNT = 3;
const THUMBNAIL_FORMAT = 'jpeg';
const DEFAULT_MAX_SOURCE_BYTES = 2 * 1024 * 1024 * 1024;
const _parsedMaxSource = parseInt(
  process.env.TRANSCODING_MAX_SOURCE_FILE_SIZE_BYTES ??
    String(DEFAULT_MAX_SOURCE_BYTES),
  10,
);
const MAX_SOURCE_SIZE_BYTES =
  Number.isFinite(_parsedMaxSource) && _parsedMaxSource > 0
    ? _parsedMaxSource
    : DEFAULT_MAX_SOURCE_BYTES;
const TRANSCODE_TIMEOUT_MS = parseInt(
  process.env.TRANSCODE_TIMEOUT_MS ?? '300000',
  10,
);
const CALLBACK_API_KEY = process.env.CALLBACK_API_KEY ?? '';
const CALLBACK_API_VERSION = process.env.CALLBACK_API_VERSION ?? '';

export const INSTANCE_SHUTTING_DOWN_MESSAGE = 'Instance shutting down';

export interface RunTranscodeOptions {
  signal?: AbortSignal;
}

function log(
  phase: string,
  jobId: string,
  sourceMediaId: string,
  message?: string,
): void {
  const msg = message ? ` ${message}` : '';
  console.log(
    `[Transcode] jobId=${jobId} sourceMediaId=${sourceMediaId} phase=${phase}${msg}`,
  );
}

export interface TranscodeRequest {
  jobId: string;
  sourceMediaId: string;
  sourceObjectKey: string;
  sourceBucket: string;
  /** Full URL to POST JSON when the job finishes (same URL for success and failure; body `status` is `COMPLETED` or `FAILED`). */
  callbackUrl: string;
  callbackToken: string;
}

/** POST body sent to `callbackUrl` on success. */
export interface TranscodeCallbackBodyCompleted {
  status: 'COMPLETED';
  jobId: string;
  manifestObjectKey: string;
  outputBucket: string;
  thumbnailObjectKeys: string[];
}

/** POST body sent to `callbackUrl` on failure. */
export interface TranscodeCallbackBodyFailed {
  status: 'FAILED';
  jobId: string;
  errorMessage: string;
}

export type TranscodeCallbackBody =
  | TranscodeCallbackBodyCompleted
  | TranscodeCallbackBodyFailed;

function createMinioClient(): Minio.Client {
  const endPoint = process.env.MINIO_ENDPOINT ?? 'localhost';
  const port = parseInt(process.env.MINIO_PORT ?? '9000', 10);
  const useSSL = process.env.MINIO_USE_SSL === 'true';
  const accessKey = process.env.MINIO_ACCESS_KEY ?? 'minioadmin';
  const secretKey = process.env.MINIO_SECRET_KEY ?? 'minioadmin';
  return new Minio.Client({
    endPoint,
    port,
    useSSL,
    accessKey,
    secretKey,
  });
}

async function downloadSource(
  client: Minio.Client,
  objectKey: string,
  bucket: string,
  destPath: string,
): Promise<void> {
  await client.fGetObject(bucket, objectKey, destPath);
}

async function uploadFile(
  client: Minio.Client,
  bucket: string,
  objectKey: string,
  filePath: string,
  contentType: string,
): Promise<void> {
  await client.fPutObject(bucket, objectKey, filePath, {
    'Content-Type': contentType,
  });
}

async function uploadBuffer(
  client: Minio.Client,
  bucket: string,
  objectKey: string,
  buffer: Buffer,
  contentType: string,
): Promise<void> {
  await client.putObject(bucket, objectKey, buffer, buffer.length, {
    'Content-Type': contentType,
  });
}

function transcodeAllRenditionsSinglePass(
  inputPath: string,
  workDir: string,
  presets: ReadonlyArray<{ height: number; bitrate: string; name: string }>,
  onCommandReady: (kill: () => void) => void,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const splitLabels = presets.map((p) => `[v${p.name}]`);
    const filterComplex = [
      `[0:v]split=${presets.length}${splitLabels.join('')}`,
      ...presets.map((p) => `[v${p.name}]scale=-2:${p.height}[vout${p.name}]`),
    ].join('; ');

    let cmd = ffmpeg(inputPath)
      .complexFilter(filterComplex)
      .on('progress', (progress: { percent?: number }) => {
        if (
          progress.percent != null &&
          Number.isFinite(progress.percent) &&
          progress.percent >= 0
        ) {
          process.stdout.write(
            `\r[Transcode] FFmpeg progress: ${progress.percent.toFixed(1)}%`,
          );
        }
      });

    for (const preset of presets) {
      cmd = cmd
        .outputOptions([
          '-map', `[vout${preset.name}]`,
          '-map', '0:a?',
          '-c:v', 'libx264',
          '-b:v', preset.bitrate,
          '-c:a', 'aac',
          '-b:a', '128k',
          '-movflags', '+faststart',
        ])
        .output(path.join(workDir, `${preset.name}.mp4`));
    }

    cmd
      .on('error', reject)
      .on('end', () => {
        process.stdout.write('\n');
        resolve();
      });

    onCommandReady(() => {
      try {
        (cmd as { kill?: (s?: string) => void }).kill?.('SIGKILL');
      } catch {
        // ignore
      }
    });
    cmd.run();
  });
}

function runWithTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  errorMessage: string,
  onTimeout?: () => void,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      onTimeout?.();
      reject(new Error(`${errorMessage} (timeout after ${timeoutMs}ms)`));
    }, timeoutMs);
    promise
      .then((res) => {
        clearTimeout(timer);
        resolve(res);
      })
      .catch((err) => {
        clearTimeout(timer);
        reject(err instanceof Error ? err : new Error(String(err)));
      });
  });
}

function generateSmilManifest(
  presets: Array<{ height: number; bitrate: string; name: string }>,
): string {
  const lines = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<smil title="">',
    '  <body>',
    '    <switch>',
  ];
  for (const preset of presets) {
    const bandwidth = parseInt(preset.bitrate.replace('k', ''), 10) * 1000;
    lines.push(
      `      <video height="${preset.height}" src="${preset.name}.mp4" systemBitrate="${bandwidth}" />`,
    );
  }
  lines.push('    </switch>');
  lines.push('  </body>');
  lines.push('</smil>');
  return lines.join('\n');
}

function getVideoDuration(filePath: string): Promise<number> {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(
      filePath,
      (err: Error | null, metadata: { format?: { duration?: number } }) => {
        if (err)
          return reject(err instanceof Error ? err : new Error(String(err)));
        resolve(metadata?.format?.duration ?? 0);
      },
    );
  });
}

function extractFrame(
  inputPath: string,
  outputPath: string,
  timestampSeconds: number,
): Promise<void> {
  return new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .seekInput(timestampSeconds)
      .frames(1)
      .output(outputPath)
      .on('error', reject)
      .on('end', () => resolve())
      .run();
  });
}

async function generateThumbnails(
  sourcePath: string,
  workDir: string,
  count: number,
  format: string,
  signal?: AbortSignal,
): Promise<string[]> {
  assertNotAborted(signal);
  const duration = await getVideoDuration(sourcePath);
  const actualCount = Math.min(
    THUMBNAIL_COUNT,
    count,
    Math.max(1, Math.floor(duration)),
  );
  const paths: string[] = [];
  for (let i = 0; i < actualCount; i++) {
    assertNotAborted(signal);
    const t =
      actualCount <= 1 ? duration / 2 : (i / (actualCount - 1)) * (duration - 1);
    const outPath = path.join(workDir, `thumb_${i}.${format}`);
    await extractFrame(sourcePath, outPath, t);
    paths.push(outPath);
  }
  return paths;
}

async function callCallback(
  callbackUrl: string,
  token: string,
  body: TranscodeCallbackBody,
): Promise<void> {
  const url = callbackUrl.trim().replace(/\/$/, '');
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'X-Callback-Token': token,
  };
  if (CALLBACK_API_KEY) headers['X-API-KEY'] = CALLBACK_API_KEY;
  if (CALLBACK_API_VERSION) headers['X-API-Version'] = CALLBACK_API_VERSION;
  const res = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`Callback failed: ${res.status} ${await res.text()}`);
  }
}

async function callCallbackWithRetry(
  callbackUrl: string,
  token: string,
  body: TranscodeCallbackBody,
): Promise<void> {
  let lastErr: unknown;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      await callCallback(callbackUrl, token, body);
      return;
    } catch (e) {
      lastErr = e;
      const waitMs = 400 * (attempt + 1);
      console.warn(
        `[Transcode] Callback (${body.status}) attempt ${attempt + 1}/3 failed; retrying in ${waitMs}ms`,
      );
      await new Promise((r) => setTimeout(r, waitMs));
    }
  }
  console.error(
    `[Transcode] Callback (${body.status}) failed after retries:`,
    lastErr,
  );
}

function assertNotAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) {
    throw new Error(INSTANCE_SHUTTING_DOWN_MESSAGE);
  }
}

export async function runTranscode(
  req: TranscodeRequest,
  options: RunTranscodeOptions = {},
): Promise<void> {
  const { jobId, sourceMediaId } = req;
  const { signal } = options;
  const client = createMinioClient();
  const workDir = path.join(
    os.tmpdir(),
    `transcode-${jobId}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
  );
  fs.mkdirSync(workDir, { recursive: true });

  let killFfmpeg: (() => void) | null = null;
  const onAbort = () => {
    try {
      killFfmpeg?.();
    } catch {
      // ignore
    }
  };
  signal?.addEventListener('abort', onAbort, { once: true });

  try {
    assertNotAborted(signal);
    log('download', jobId, sourceMediaId, 'starting');
    const sourcePath = path.join(workDir, 'source.mp4');
    await downloadSource(
      client,
      req.sourceObjectKey,
      req.sourceBucket,
      sourcePath,
    );

    assertNotAborted(signal);
    const stat = fs.statSync(sourcePath);
    if (stat.size > MAX_SOURCE_SIZE_BYTES) {
      throw new Error(
        `Source file size ${stat.size} exceeds max ${MAX_SOURCE_SIZE_BYTES}`,
      );
    }
    log('download', jobId, sourceMediaId, `completed size=${stat.size}`);

    const outputPrefix = `transcoded/${sourceMediaId}`;
    const outputBucket = req.sourceBucket;

    log('transcode', jobId, sourceMediaId, 'multi-rendition');
    const promise = transcodeAllRenditionsSinglePass(
      sourcePath,
      workDir,
      RENDITION_PRESETS,
      (kill) => {
        killFfmpeg = kill;
      },
    );
    await runWithTimeout(
      promise,
      TRANSCODE_TIMEOUT_MS,
      'Transcoding timed out',
      () => killFfmpeg?.(),
    );
    assertNotAborted(signal);
    log('transcode', jobId, sourceMediaId, 'completed');

    log('upload-renditions', jobId, sourceMediaId, 'starting');
    assertNotAborted(signal);
    await Promise.all(
      RENDITION_PRESETS.map((preset) => {
        const localPath = path.join(workDir, `${preset.name}.mp4`);
        const objectKey = `${outputPrefix}/${preset.name}.mp4`;
        return uploadFile(client, outputBucket, objectKey, localPath, 'video/mp4');
      }),
    );
    log('upload-renditions', jobId, sourceMediaId, 'completed');

    log('upload-manifest', jobId, sourceMediaId, 'starting');
    const smilContent = generateSmilManifest(RENDITION_PRESETS);
    const manifestObjectKey = `${outputPrefix}/manifest.smil`;
    await uploadBuffer(
      client,
      outputBucket,
      manifestObjectKey,
      Buffer.from(smilContent, 'utf-8'),
      'application/smil+xml',
    );
    log('upload-manifest', jobId, sourceMediaId, 'completed');

    log('thumbnails', jobId, sourceMediaId, 'generating');
    const thumbnailPaths = await generateThumbnails(
      sourcePath,
      workDir,
      THUMBNAIL_COUNT,
      THUMBNAIL_FORMAT,
      signal,
    );
    assertNotAborted(signal);
    const thumbnailObjectKeys = thumbnailPaths.map(
      (_, i) => `${outputPrefix}/thumbnails/thumb_${i}.${THUMBNAIL_FORMAT}`,
    );
    await Promise.all(
      thumbnailPaths.map((thumbPath, i) =>
        uploadFile(client, outputBucket, thumbnailObjectKeys[i], thumbPath, `image/${THUMBNAIL_FORMAT}`),
      ),
    );
    log(
      'thumbnails',
      jobId,
      sourceMediaId,
      `uploaded ${thumbnailObjectKeys.length}`,
    );

    log('callback', jobId, sourceMediaId, 'sending completed');
    await callCallbackWithRetry(req.callbackUrl, req.callbackToken, {
      status: 'COMPLETED',
      jobId: req.jobId,
      manifestObjectKey,
      outputBucket,
      thumbnailObjectKeys,
    } satisfies TranscodeCallbackBodyCompleted);
    log('callback', jobId, sourceMediaId, 'completed');
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    log('callback', jobId, sourceMediaId, `sending failed: ${errorMessage}`);
    await callCallbackWithRetry(req.callbackUrl, req.callbackToken, {
      status: 'FAILED',
      jobId: req.jobId,
      errorMessage,
    } satisfies TranscodeCallbackBodyFailed);
  } finally {
    try {
      fs.rmSync(workDir, { recursive: true, force: true });
      log('cleanup', jobId, sourceMediaId, 'work dir removed');
    } catch {
      // ignore cleanup errors
    }
  }
}
