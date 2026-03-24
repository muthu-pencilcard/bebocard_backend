import { createHash, randomBytes } from 'crypto';
import { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';

const ulid = monotonicFactory();

export type ApiKeyScope = 'scan' | 'receipt' | 'offers' | 'newsletters' | 'analytics' | 'stores';

export interface ApiKeyRecord {
  brandId: string;
  keyId: string;
  keyHash: string;
  scopes: ApiKeyScope[];
  rateLimit: number;        // requests per hour
  status: 'ACTIVE' | 'REVOKED' | 'GRACE';
  createdAt: string;
  createdBy: string;        // business portal userId
  revokedAt?: string;
  graceUntil?: string;      // ISO8601 — old key accepted until this time during rotation
}

export interface ValidatedKey {
  brandId: string;
  keyId: string;
  rateLimit: number;
  scopes: ApiKeyScope[];
}

const REFDATA_TABLE = process.env.REFDATA_TABLE!;

// ── Key format ────────────────────────────────────────────────────────────────
// Raw key format: bebo_<keyId>.<secret32bytes>
// keyId is a ULID (26 chars); secret is 32 random bytes as hex (64 chars)
// Full key: "bebo_01J..." (92 chars)

export function generateApiKey(): { rawKey: string; keyId: string; keyHash: string } {
  const keyId = ulid();
  const secret = randomBytes(32).toString('hex');
  const rawKey = `bebo_${keyId}.${secret}`;
  const keyHash = createHash('sha256').update(rawKey).digest('hex');
  return { rawKey, keyId, keyHash };
}

// ── Validation ─────────────────────────────────────────────────────────────────

export async function validateApiKey(
  ddb: DynamoDBDocumentClient,
  rawKey: string,
  requiredScope: ApiKeyScope,
): Promise<ValidatedKey | null> {
  if (!rawKey?.startsWith('bebo_')) return null;

  const withoutPrefix = rawKey.slice(5);          // strip 'bebo_'
  const dotIndex = withoutPrefix.indexOf('.');
  if (dotIndex === -1) return null;

  const keyId = withoutPrefix.slice(0, dotIndex);
  const keyHash = createHash('sha256').update(rawKey).digest('hex');

  // brandId is embedded in the keyId ULID prefix stored in the sk
  const res = await ddb.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    IndexName: 'byKeyId',               // GSI: keyId → BRAND record
    KeyConditionExpression: 'keyId = :kid',
    ExpressionAttributeValues: { ':kid': keyId },
    Limit: 1,
  }));

  const item = res.Items?.[0] as (ApiKeyRecord & { pK: string; sK: string }) | undefined;
  if (!item) return null;

  const now = new Date().toISOString();

  // Reject if revoked and grace period has expired
  if (item.status === 'REVOKED') return null;
  if (item.status === 'GRACE' && item.graceUntil && item.graceUntil < now) return null;

  // Constant-time hash comparison
  const expectedHash = createHash('sha256').update(rawKey).digest('hex');
  if (!timingSafeEqual(item.keyHash, expectedHash)) return null;

  if (!item.scopes.includes(requiredScope)) return null;

  return {
    brandId: item.brandId,
    keyId: item.keyId,
    rateLimit: item.rateLimit,
    scopes: item.scopes,
  };
}

// ── CRUD ───────────────────────────────────────────────────────────────────────

export async function createApiKey(
  ddb: DynamoDBDocumentClient,
  brandId: string,
  scopes: ApiKeyScope[],
  createdBy: string,
  rateLimit = 1000,
): Promise<{ rawKey: string; keyId: string }> {
  const { rawKey, keyId, keyHash } = generateApiKey();
  const now = new Date().toISOString();

  await ddb.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK: `BRAND#${brandId}`,
      sK: `APIKEY#${keyId}`,
      eventType: 'API_KEY',
      status: 'ACTIVE',
      primaryCat: 'api_key',
      keyId,
      brandId,
      desc: JSON.stringify({
        keyHash,
        scopes,
        rateLimit,
        createdBy,
      } satisfies Omit<ApiKeyRecord, 'brandId' | 'keyId' | 'keyHash' | 'status' | 'createdAt'>),
      createdAt: now,
      updatedAt: now,
    },
    // Never overwrite — each call creates a distinct key
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  // Raw key is returned ONCE and never stored — caller must display and save it
  return { rawKey, keyId };
}

/**
 * Rotates a brand's API key.
 * The old key enters a 24-hour grace period so integrations have time to update.
 * After grace period the old key becomes unresolvable.
 */
export async function rotateApiKey(
  ddb: DynamoDBDocumentClient,
  brandId: string,
  oldKeyId: string,
  createdBy: string,
): Promise<{ rawKey: string; newKeyId: string }> {
  const now = new Date();
  const graceUntil = new Date(now.getTime() + 24 * 60 * 60 * 1000).toISOString();

  // Put old key into grace period
  await ddb.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `APIKEY#${oldKeyId}` },
    UpdateExpression: 'SET #s = :grace, graceUntil = :gu, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: {
      ':grace': 'GRACE',
      ':gu': graceUntil,
      ':now': now.toISOString(),
    },
  }));

  // Fetch old key scopes to carry forward
  const old = await ddb.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `APIKEY#${oldKeyId}` },
  }));
  const oldDesc = JSON.parse(old.Item?.desc ?? '{}');

  // Create the new key
  const { rawKey, keyId: newKeyId } = await createApiKey(
    ddb, brandId, oldDesc.scopes ?? ['scan', 'receipt'], createdBy, oldDesc.rateLimit,
  );

  return { rawKey, newKeyId };
}

/**
 * Immediately revokes a key with no grace period.
 * Use for suspected compromise.
 */
export async function revokeApiKey(
  ddb: DynamoDBDocumentClient,
  brandId: string,
  keyId: string,
): Promise<void> {
  await ddb.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `APIKEY#${keyId}` },
    UpdateExpression: 'SET #s = :revoked, revokedAt = :now, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: {
      ':revoked': 'REVOKED',
      ':now': new Date().toISOString(),
    },
  }));
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Timing-safe string comparison to prevent timing attacks on key hashes. */
function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  return bufA.every((byte, i) => byte === bufB[i]);
}

/** Extract raw key from Authorization or x-api-key header. */
export function extractApiKey(headers: Record<string, string | undefined>): string | null {
  const xApiKey = headers['x-api-key'] ?? headers['X-Api-Key'];
  if (xApiKey) return xApiKey;
  const auth = headers['authorization'] ?? headers['Authorization'];
  if (auth?.startsWith('Bearer bebo_')) return auth.slice(7);
  return null;
}
