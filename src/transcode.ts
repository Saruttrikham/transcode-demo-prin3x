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
] as const;
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
const TRANSCODE_OUTPUT_BUCKET = process.env.TRANSCODE_OUTPUT_BUCKET?.trim() ?? '';

export const SUBTITLE_LANGUAGES = ['th', 'en'] as const;
export type SubtitleLanguage = (typeof SUBTITLE_LANGUAGES)[number];

export const INSTANCE_SHUTTING_DOWN_MESSAGE = 'Instance shutting down';

export interface RunTranscodeOptions {
  signal?: AbortSignal;
}

export interface SubtitleTrackInput {
  objectKey: string;
  language: SubtitleLanguage;
  bucket?: string;
  format?: string;
}

export interface TranscodeRequest {
  jobId: string;
  sourceMediaId: string;
  sourceObjectKey?: string;
  sourceBucket: string;
  callbackUrl: string;
  callbackToken: string;
  subtitleBucket?: string;
  subtitles?: SubtitleTrackInput[];
}

/** Accepts `vtt`, `srt`, `.vtt`, `.srt` (case-insensitive). Undefined / empty omits a hint (basename extension wins). */
export function parseSubtitleFormatOptional(
  v: unknown,
): '.vtt' | '.srt' | undefined {
  if (v === undefined || v === null) return undefined;
  if (typeof v !== 'string') return undefined;
  const s = v.trim().toLowerCase();
  if (s === '') return undefined;
  if (s === 'srt' || s === '.srt') return '.srt';
  if (s === 'vtt' || s === '.vtt') return '.vtt';
  return undefined;
}

export function isValidSubtitleFormatInput(v: unknown): boolean {
  if (v === undefined || v === null) return true;
  if (typeof v === 'string' && v.trim() === '') return true;
  return parseSubtitleFormatOptional(v) !== undefined;
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

/** Synthetic lavfi test (no object storage). Optional callback fields. */
export interface SyntheticFfmpegTestRequest {
  jobId: string;
  durationSec?: number;
  width?: number;
  height?: number;
  fps?: number;
  /** Default true: add sine audio. Set false for video-only. */
  audio?: boolean;
  callbackUrl?: string;
  callbackToken?: string;
}

export interface SyntheticFfmpegTestResult {
  ok: boolean;
  jobId: string;
  outputBytes: number;
  durationMs: number;
  error?: string;
}

interface NormalizedSubtitleTrack {
  objectKey: string;
  bucket: string;
  language: SubtitleLanguage;
  format?: '.vtt' | '.srt';
}

/** Extension for output object + SMIL; from `objectKey` basename, else `format`, else `.vtt`. */
function subtitleOutputExtension(
  objectKey: string,
  formatHint?: '.vtt' | '.srt',
): string {
  const fromKey = path.extname(objectKey);
  if (fromKey !== '') return fromKey;
  return formatHint === '.srt' ? '.srt' : '.vtt';
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

function escapeXmlAttr(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function generateSmilManifest(
  presets: ReadonlyArray<{ height: number; bitrate: string; name: string }>,
  opts: {
    videoSrcPrefix: string;
    subtitleTracks?: ReadonlyArray<{ relativeSrc: string; language: string }>;
  },
): string {
  const prefix = opts.videoSrcPrefix.replace(/\/+$/, '');
  const lines = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<smil title="">',
    '  <body>',
    '    <switch>',
  ];
  for (const preset of presets) {
    const bandwidth = parseInt(preset.bitrate.replace('k', ''), 10) * 1000;
    const src =
      prefix === '' ? `${preset.name}.mp4` : `${prefix}/${preset.name}.mp4`;
    lines.push(
      `      <video height="${preset.height}" src="${escapeXmlAttr(src)}" systemBitrate="${bandwidth}" />`,
    );
  }
  lines.push('    </switch>');
  if (opts.subtitleTracks?.length) {
    for (const t of opts.subtitleTracks) {
      lines.push(
        `    <textstream src="${escapeXmlAttr(t.relativeSrc)}" systemLanguage="${escapeXmlAttr(t.language)}" />`,
      );
    }
  }
  lines.push('  </body>');
  lines.push('</smil>');
  return lines.join('\n');
}

function normalizeSubtitleTracks(req: TranscodeRequest): NormalizedSubtitleTrack[] {
  if (req.subtitles && req.subtitles.length > 0) {
    const defaultSubBucket = req.subtitleBucket?.trim() || req.sourceBucket.trim();
    return req.subtitles.map((t) => ({
      objectKey: t.objectKey.trim(),
      bucket: (t.bucket ?? defaultSubBucket).trim(),
      language: t.language,
      format: parseSubtitleFormatOptional(t.format),
    }));
  }
  return [];
}

function resolveOutputBucket(sourceBucket: string): string {
  return TRANSCODE_OUTPUT_BUCKET || sourceBucket;
}

function sharedVideoObjectKeys(sourceMediaId: string): string[] {
  return RENDITION_PRESETS.map(
    (p) => `transcoded/${sourceMediaId}/video/${p.name}.mp4`,
  );
}

async function statSharedRenditions(
  client: Minio.Client,
  bucket: string,
  sourceMediaId: string,
): Promise<{ missing: string[] }> {
  const missing: string[] = [];
  for (const key of sharedVideoObjectKeys(sourceMediaId)) {
    try {
      await client.statObject(bucket, key);
    } catch {
      missing.push(key);
    }
  }
  return { missing };
}

async function listThumbnailObjectKeys(
  client: Minio.Client,
  bucket: string,
  sourceMediaId: string,
): Promise<string[]> {
  const prefix = `transcoded/${sourceMediaId}/thumbnails/`;
  const keys: string[] = [];
  return new Promise((resolve, reject) => {
    const stream = client.listObjectsV2(bucket, prefix, true);
    stream.on('data', (item: Minio.BucketItem) => {
      if ('name' in item && item.name) keys.push(item.name);
    });
    stream.on('error', reject);
    stream.on('end', () => resolve(keys.sort()));
  });
}

async function copySubtitleToOutput(
  client: Minio.Client,
  workDir: string,
  track: NormalizedSubtitleTrack,
  destBucket: string,
  destObjectKey: string,
): Promise<void> {
  const localName = `sub-${track.language}-${path.basename(track.objectKey)}`;
  const localPath = path.join(workDir, localName);
  await downloadSource(client, track.objectKey, track.bucket, localPath);
  const extLower = path.extname(destObjectKey).toLowerCase() || '.vtt';
  // Vidstack’s parser keys off MIME for SRT in some setups; Nimble accepts SRT for VOD HLS but WebVTT is safest in m3u8.
  const contentType =
    extLower === '.vtt'
      ? 'text/vtt; charset=utf-8'
      : extLower === '.srt'
        ? 'text/srt; charset=utf-8'
        : 'application/octet-stream';
  await uploadFile(client, destBucket, destObjectKey, localPath, contentType);
}

/** Writes `transcoded/{id}/subtitles/{lang}{ext}` and root `transcoded/{id}/manifest.smil` (same path as no-subtitle jobs). */
async function uploadSubtitlesAndRootManifest(
  client: Minio.Client,
  workDir: string,
  outputBucket: string,
  sourceMediaId: string,
  tracks: NormalizedSubtitleTrack[],
): Promise<{ manifestObjectKey: string }> {
  const outputPrefix = `transcoded/${sourceMediaId}`;
  const subtitleSmilTracks: { relativeSrc: string; language: string }[] = [];

  for (const track of tracks) {
    const ext = subtitleOutputExtension(track.objectKey, track.format);
    const destKey = `${outputPrefix}/subtitles/${track.language}${ext}`;
    await copySubtitleToOutput(client, workDir, track, outputBucket, destKey);
    // From playlist base …/manifest.smil/ one ".." reaches the asset root (nginx-vod).
    subtitleSmilTracks.push({
      relativeSrc: `../subtitles/${track.language}${ext}`,
      language: track.language,
    });
  }

  const smilContent = generateSmilManifest(RENDITION_PRESETS, {
    videoSrcPrefix: 'video',
    subtitleTracks: subtitleSmilTracks,
  });
  const manifestObjectKey = `${outputPrefix}/manifest.smil`;
  await uploadBuffer(
    client,
    outputBucket,
    manifestObjectKey,
    Buffer.from(smilContent, 'utf-8'),
    'application/smil+xml',
  );
  return { manifestObjectKey };
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
          '-map',
          `[vout${preset.name}]`,
          '-map',
          '0:a?',
          '-c:v',
          'libx264',
          '-b:v',
          preset.bitrate,
          '-c:a',
          'aac',
          '-b:a',
          '128k',
          '-movflags',
          '+faststart',
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

const SYNTHETIC_DEFAULT_DURATION_SEC = 5;
const SYNTHETIC_MAX_DURATION_SEC = 120;
const SYNTHETIC_DEFAULT_WIDTH = 1280;
const SYNTHETIC_DEFAULT_HEIGHT = 720;
const SYNTHETIC_DEFAULT_FPS = 30;

/**
 * Runs FFmpeg on generated lavfi sources (test pattern + optional sine audio).
 * No MinIO/object storage. Temp output is deleted after measuring size.
 */
export async function runSyntheticFfmpegTest(
  req: SyntheticFfmpegTestRequest,
): Promise<SyntheticFfmpegTestResult> {
  const jobId = req.jobId.trim();
  const sourceMediaId = 'synthetic';
  const durationSec = Math.min(
    SYNTHETIC_MAX_DURATION_SEC,
    Math.max(
      1,
      req.durationSec != null && Number.isFinite(req.durationSec)
        ? Math.floor(Number(req.durationSec))
        : SYNTHETIC_DEFAULT_DURATION_SEC,
    ),
  );
  const width =
    req.width != null &&
    Number.isFinite(req.width) &&
    req.width > 0 &&
    req.width <= 4096
      ? Math.floor(Number(req.width))
      : SYNTHETIC_DEFAULT_WIDTH;
  const height =
    req.height != null &&
    Number.isFinite(req.height) &&
    req.height > 0 &&
    req.height <= 4096
      ? Math.floor(Number(req.height))
      : SYNTHETIC_DEFAULT_HEIGHT;
  const fps =
    req.fps != null && Number.isFinite(req.fps) && req.fps > 0 && req.fps <= 60
      ? Math.floor(Number(req.fps))
      : SYNTHETIC_DEFAULT_FPS;
  const withAudio = req.audio !== false;

  const workDir = path.join(
    os.tmpdir(),
    `ffmpeg-test-${jobId}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
  );
  const outPath = path.join(workDir, 'synthetic-out.mp4');
  fs.mkdirSync(workDir, { recursive: true });

  const started = Date.now();
  let killFfmpeg: (() => void) | null = null;

  try {
    log(
      'ffmpeg-test',
      jobId,
      sourceMediaId,
      `lavfi ${width}x${height}@${fps} ${durationSec}s audio=${withAudio}`,
    );

    const encodePromise = new Promise<void>((resolve, reject) => {
      let cmd = ffmpeg()
        .input(`testsrc=size=${width}x${height}:rate=${fps}`)
        .inputOptions(['-f', 'lavfi']);
      if (withAudio) {
        cmd = cmd
          .input('sine=frequency=1000:sample_rate=48000')
          .inputOptions(['-f', 'lavfi']);
      }
      const videoAudioOpts = withAudio
        ? ['-c:a', 'aac', '-b:a', '128k']
        : ['-an'];
      cmd = cmd
        .outputOptions([
          '-t',
          String(durationSec),
          '-c:v',
          'libx264',
          '-preset',
          'veryfast',
          '-pix_fmt',
          'yuv420p',
          ...videoAudioOpts,
          '-movflags',
          '+faststart',
        ])
        .output(outPath)
        .on('error', reject)
        .on('end', () => resolve());

      killFfmpeg = () => {
        try {
          (cmd as { kill?: (s?: string) => void }).kill?.('SIGKILL');
        } catch {
          // ignore
        }
      };
      cmd.run();
    });

    await runWithTimeout(
      encodePromise,
      TRANSCODE_TIMEOUT_MS,
      'Synthetic FFmpeg test timed out',
      () => killFfmpeg?.(),
    );

    const stat = fs.statSync(outPath);
    const durationMs = Date.now() - started;
    log(
      'ffmpeg-test',
      jobId,
      sourceMediaId,
      `ok bytes=${stat.size} wallMs=${durationMs}`,
    );

    return {
      ok: true,
      jobId,
      outputBytes: stat.size,
      durationMs,
    };
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    log('ffmpeg-test', jobId, sourceMediaId, `failed: ${errorMessage}`);
    return {
      ok: false,
      jobId,
      outputBytes: 0,
      durationMs: Date.now() - started,
      error: errorMessage,
    };
  } finally {
    try {
      fs.rmSync(workDir, { recursive: true, force: true });
    } catch {
      // ignore cleanup errors
    }
  }
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

function rewriteCallbackUrlIfDockerLocalhost(callbackUrl: string): string {
  const trimmed = callbackUrl.trim();
  let u: URL;
  try {
    u = new URL(trimmed);
  } catch {
    return trimmed.replace(/\/$/, '');
  }
  if (fs.existsSync('/.dockerenv')) {
    const h = u.hostname.toLowerCase();
    if (
      h === 'localhost' ||
      h === '127.0.0.1' ||
      h === '::1' ||
      h === '[::1]'
    ) {
      u.hostname = 'host.docker.internal';
    }
  }
  return u.toString().replace(/\/$/, '');
}

/** Outbound POST to callbackUrl with X-Callback-Token (and optional API headers). */
export async function postJsonCallback(
  callbackUrl: string,
  callbackToken: string,
  body: unknown,
): Promise<void> {
  const url = rewriteCallbackUrlIfDockerLocalhost(callbackUrl);
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'X-Callback-Token': callbackToken,
  };
  if (CALLBACK_API_KEY) headers['X-API-KEY'] = CALLBACK_API_KEY;
  if (CALLBACK_API_VERSION) headers['X-API-Version'] = CALLBACK_API_VERSION;
  const res = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) {
    throw new Error(`Callback failed: ${res.status} ${await res.text()}`);
  }
}

async function callCallback(
  callbackUrl: string,
  token: string,
  body: TranscodeCallbackBody,
): Promise<void> {
  await postJsonCallback(callbackUrl, token, body);
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

async function runSubtitleOnlyJob(
  req: TranscodeRequest,
  tracks: NormalizedSubtitleTrack[],
  options: RunTranscodeOptions,
): Promise<void> {
  const { jobId, sourceMediaId, sourceBucket } = req;
  const outputBucket = resolveOutputBucket(sourceBucket);
  const { signal } = options;
  const client = createMinioClient();
  const workDir = path.join(
    os.tmpdir(),
    `transcode-${jobId}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
  );
  fs.mkdirSync(workDir, { recursive: true });

  try {
    assertNotAborted(signal);
    log('subtitle-only-precheck', jobId, sourceMediaId, 'shared renditions');
    const { missing } = await statSharedRenditions(
      client,
      outputBucket,
      sourceMediaId,
    );
    if (missing.length > 0) {
      throw new Error(
        `Shared video renditions missing under transcoded/${sourceMediaId}/video/ (${missing.map((k) => path.basename(k)).join(', ')}). ` +
          'Complete a full transcode first, or if you meant to transcode from a new source, send sourceObjectKey.',
      );
    }

    assertNotAborted(signal);
    log('subtitle-upload', jobId, sourceMediaId, `${tracks.length} track(s)`);
    const { manifestObjectKey } = await uploadSubtitlesAndRootManifest(
      client,
      workDir,
      outputBucket,
      sourceMediaId,
      tracks,
    );

    const thumbnailObjectKeys = await listThumbnailObjectKeys(
      client,
      outputBucket,
      sourceMediaId,
    );

    log('callback', jobId, sourceMediaId, 'sending completed (subtitle-only)');
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

export async function runTranscode(
  req: TranscodeRequest,
  options: RunTranscodeOptions = {},
): Promise<void> {
  const tracks = normalizeSubtitleTracks(req);
  const hasUsableSource =
    typeof req.sourceObjectKey === 'string' &&
    req.sourceObjectKey.trim() !== '';

  if (!hasUsableSource) {
    if (tracks.length === 0) {
      await callCallbackWithRetry(req.callbackUrl, req.callbackToken, {
        status: 'FAILED',
        jobId: req.jobId,
        errorMessage:
          'Invalid job: provide sourceObjectKey for transcode, or a non-empty subtitles array for subtitle-only update.',
      } satisfies TranscodeCallbackBodyFailed);
      return;
    }
    return runSubtitleOnlyJob(req, tracks, options);
  }

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
    const sourceObjectKey = req.sourceObjectKey?.trim() ?? '';
    await downloadSource(client, sourceObjectKey, req.sourceBucket, sourcePath);

    assertNotAborted(signal);
    const stat = fs.statSync(sourcePath);
    if (stat.size > MAX_SOURCE_SIZE_BYTES) {
      throw new Error(
        `Source file size ${stat.size} exceeds max ${MAX_SOURCE_SIZE_BYTES}`,
      );
    }
    log('download', jobId, sourceMediaId, `completed size=${stat.size}`);

    const outputPrefix = `transcoded/${sourceMediaId}`;
    const outputBucket = resolveOutputBucket(req.sourceBucket);

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
        const objectKey = `${outputPrefix}/video/${preset.name}.mp4`;
        return uploadFile(client, outputBucket, objectKey, localPath, 'video/mp4');
      }),
    );
    log('upload-renditions', jobId, sourceMediaId, 'completed');

    let manifestObjectKey: string;
    if (tracks.length > 0) {
      log('subtitle-upload', jobId, sourceMediaId, `${tracks.length} track(s)`);
      const v = await uploadSubtitlesAndRootManifest(
        client,
        workDir,
        outputBucket,
        sourceMediaId,
        tracks,
      );
      manifestObjectKey = v.manifestObjectKey;
    } else {
      log('upload-manifest', jobId, sourceMediaId, 'starting');
      const smilContent = generateSmilManifest(RENDITION_PRESETS, {
        videoSrcPrefix: 'video',
      });
      manifestObjectKey = `${outputPrefix}/manifest.smil`;
      await uploadBuffer(
        client,
        outputBucket,
        manifestObjectKey,
        Buffer.from(smilContent, 'utf-8'),
        'application/smil+xml',
      );
      log('upload-manifest', jobId, sourceMediaId, 'completed');
    }

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
      (_, i) =>
        `${outputPrefix}/thumbnails/thumb_${i}.${THUMBNAIL_FORMAT}`,
    );
    await Promise.all(
      thumbnailPaths.map((thumbPath, i) =>
        uploadFile(
          client,
          outputBucket,
          thumbnailObjectKeys[i],
          thumbPath,
          `image/${THUMBNAIL_FORMAT}`,
        ),
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
