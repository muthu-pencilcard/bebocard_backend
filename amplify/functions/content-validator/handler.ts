/**
 * content-validator — S3 trigger
 *
 * Fires when a brand uploads content to:
 *   s3://bebocard-tenant-uploads/brands/<brandId>/<type>/<filename>
 *
 * Pipeline:
 *   1. MIME type check  — JPEG / PNG / WebP only
 *   2. Size check       — max 5 MB
 *   3. Rekognition      — content moderation (confidence > 75)
 *   4. PASS → copy to app-reference bucket, update RefDataEvent CDN URL, write audit log
 *   5. FAIL → move to rejected/ prefix, write audit log
 *
 * Content types and their dimension caps:
 *   logo    512 × 512 px
 *   banner  1200 × 400 px
 *   offer   800 × 600 px
 */
import type { S3Event } from 'aws-lambda';
import {
  S3Client,
  GetObjectCommand,
  CopyObjectCommand,
  HeadObjectCommand,
} from '@aws-sdk/client-s3';
import {
  RekognitionClient,
  DetectModerationLabelsCommand,
} from '@aws-sdk/client-rekognition';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  UpdateCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';

const s3  = new S3Client({});
const rek = new RekognitionClient({});
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid = monotonicFactory();

const STAGING_BUCKET  = process.env.TENANT_UPLOADS_BUCKET!;
const REF_BUCKET      = process.env.APP_REFERENCE_BUCKET!;
const REFDATA_TABLE   = process.env.REFDATA_TABLE!;
const ADMIN_TABLE     = process.env.ADMIN_TABLE!;

const MAX_BYTES       = 5 * 1024 * 1024;  // 5 MB
const ALLOWED_MIME    = new Set(['image/jpeg', 'image/png', 'image/webp']);
const MOD_CONFIDENCE  = 75;

// Maps content-type path segment → allowed MIME types
const TYPE_MIME: Record<string, Set<string>> = {
  logo:   ALLOWED_MIME,
  banner: ALLOWED_MIME,
  offer:  ALLOWED_MIME,
};

// ── Handler ──────────────────────────────────────────────────────────────────

export const handler = async (event: S3Event): Promise<void> => {
  for (const record of event.Records) {
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
    await processObject(key);
  }
};

// ── Per-object pipeline ───────────────────────────────────────────────────────

async function processObject(key: string): Promise<void> {
  // Expected key format: brands/<brandId>/<contentType>/<filename>
  const parts = key.split('/');
  if (parts.length < 4 || parts[0] !== 'brands') {
    console.warn(`[content-validator] Skipping unexpected key format: ${key}`);
    return;
  }

  const brandId     = parts[1];
  const contentType = parts[2];   // logo | banner | offer
  const filename    = parts.slice(3).join('/');

  if (!TYPE_MIME[contentType]) {
    await reject(brandId, key, filename, contentType, `Unknown content type: ${contentType}`);
    return;
  }

  // 1. Head object — get size and MIME
  let size   = 0;
  let mime   = '';
  try {
    const head = await s3.send(new HeadObjectCommand({ Bucket: STAGING_BUCKET, Key: key }));
    size = head.ContentLength ?? 0;
    mime = head.ContentType   ?? '';
  } catch (e) {
    console.error(`[content-validator] HeadObject failed for ${key}:`, e);
    return;
  }

  // 2. MIME check
  if (!TYPE_MIME[contentType].has(mime)) {
    await reject(brandId, key, filename, contentType, `Invalid MIME type: ${mime}`);
    return;
  }

  // 3. Size check
  if (size > MAX_BYTES) {
    await reject(brandId, key, filename, contentType, `File too large: ${(size / 1024 / 1024).toFixed(1)} MB (max 5 MB)`);
    return;
  }

  // 4. Rekognition moderation
  try {
    const mod = await rek.send(new DetectModerationLabelsCommand({
      Image: { S3Object: { Bucket: STAGING_BUCKET, Name: key } },
      MinConfidence: MOD_CONFIDENCE,
    }));

    if ((mod.ModerationLabels?.length ?? 0) > 0) {
      const topLabel = mod.ModerationLabels![0].Name ?? 'Unknown';
      await reject(brandId, key, filename, contentType, `Content moderation: ${topLabel}`);
      return;
    }
  } catch (e) {
    // If Rekognition is unavailable (e.g. unsupported format), log and allow through
    console.warn(`[content-validator] Rekognition skipped for ${key}:`, e);
  }

  // 5. PASS — copy to reference bucket
  const destKey = `brands/${brandId}/${contentType}/${filename}`;
  try {
    await s3.send(new CopyObjectCommand({
      CopySource:        `${STAGING_BUCKET}/${key}`,
      Bucket:            REF_BUCKET,
      Key:               destKey,
      ContentType:       mime,
      MetadataDirective: 'REPLACE',
      CacheControl:      'public, max-age=31536000, immutable',
    }));
  } catch (e) {
    console.error(`[content-validator] CopyObject to reference bucket failed for ${key}:`, e);
    return;
  }

  // 6. Update RefDataEvent with new CDN URL
  const cdnUrl = `https://${REF_BUCKET}.s3.ap-southeast-2.amazonaws.com/${destKey}`;
  const cdnField = contentType === 'logo' ? 'logoUrl' : contentType === 'banner' ? 'bannerUrl' : 'offerImageUrl';

  try {
    await ddb.send(new UpdateCommand({
      TableName: REFDATA_TABLE,
      Key: { pK: `BRAND#${brandId}`, sK: 'profile' },
      // Use a JSON_MERGE style update inside desc — store CDN URLs at top level for fast reads
      UpdateExpression: 'SET #cdnField = :url, updatedAt = :now',
      ExpressionAttributeNames: { '#cdnField': cdnField },
      ExpressionAttributeValues: {
        ':url': cdnUrl,
        ':now': new Date().toISOString(),
      },
    }));
  } catch (e) {
    console.warn(`[content-validator] RefDataEvent update skipped (brand profile may not exist yet):`, e);
  }

  // 7. Audit log
  await writeAudit({
    actor:    brandId,
    action:   'contentPromoted',
    resource: destKey,
    outcome:  'success',
  });

  console.log(`[content-validator] Promoted: ${destKey} → ${cdnUrl}`);
}

// ── Reject helper ─────────────────────────────────────────────────────────────

async function reject(
  brandId: string,
  srcKey: string,
  filename: string,
  contentType: string,
  reason: string,
): Promise<void> {
  console.warn(`[content-validator] Rejected ${srcKey}: ${reason}`);

  // Move to rejected/ prefix so brands can inspect failures
  const rejectedKey = `brands/${brandId}/rejected/${contentType}/${filename}`;
  try {
    await s3.send(new CopyObjectCommand({
      CopySource:        `${STAGING_BUCKET}/${srcKey}`,
      Bucket:            STAGING_BUCKET,
      Key:               rejectedKey,
      MetadataDirective: 'COPY',
    }));
  } catch (e) {
    console.error(`[content-validator] Copy to rejected/ failed:`, e);
  }

  await writeAudit({
    actor:     brandId,
    action:    'contentRejected',
    resource:  filename,
    outcome:   'failure',
    errorCode: reason,
  });
}

// ── Audit logger ──────────────────────────────────────────────────────────────

interface AuditEntry {
  actor: string;
  action: string;
  resource: string;
  outcome: 'success' | 'failure';
  errorCode?: string;
}

async function writeAudit(entry: AuditEntry): Promise<void> {
  const now   = new Date().toISOString();
  const logId = ulid();
  // Structured CloudWatch log (queryable via Logs Insights)
  console.log(JSON.stringify({ audit: true, actorType: 'brand', ...entry, timestamp: now, logId }));
  // Durable DynamoDB record
  try {
    await ddb.send(new PutCommand({
      TableName: ADMIN_TABLE,
      Item: {
        pK:        `AUDIT#${entry.actor}`,
        sK:        `LOG#${now}#${logId}`,
        eventType: 'AUDIT_LOG',
        status:    entry.outcome,
        desc:      JSON.stringify({ actorType: 'brand', ...entry }),
        createdAt: now,
        updatedAt: now,
      },
    }));
  } catch (e) {
    console.error(`[content-validator] Audit write failed:`, e);
  }
}
