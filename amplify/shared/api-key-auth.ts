import { createHash, randomBytes, timingSafeEqual as nodeTimingSafeEqual } from 'crypto';
import { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';

const ADMIN_TABLE = process.env.ADMIN_TABLE;

const ulid = monotonicFactory();
const KEY_ID_INDEX = process.env.KEY_ID_GSI_NAME ?? 'refDataEventsByKeyId';

export type ApiKeyScope = 'scan' | 'receipt' | 'offers' | 'newsletters' | 'catalogues' | 'analytics' | 'stores' | 'payment' | 'consent' | 'recurring' | 'gift_card' | 'enrollment' | 'smb';

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

type ApiKeyItem = Partial<ApiKeyRecord> & {
  pK?: string;
  sK?: string;
  desc?: string;
};

const REFDATA_TABLE = process.env.REFDATA_TABLE!;

// ── Rate limiting ─────────────────────────────────────────────────────────

/**
 * Atomically increments the per-key hourly request counter in AdminDataEvent.
 * Returns { allowed: true } when under the limit, { allowed: false, usage, limit } when exceeded.
 *
 * Counter items use TTL = 2 hours so they auto-expire without manual cleanup.
 * Key pattern: RATE#<keyId> / <YYYY-MM-DDTHH> (UTC hour bucket)
 */
export async function checkRateLimit(
  ddb: DynamoDBDocumentClient,
  keyId: string,
  rateLimit: number,
): Promise<{ allowed: boolean; usage: number; limit: number }> {
  const adminTable = ADMIN_TABLE;
  if (!adminTable || rateLimit <= 0) return { allowed: true, usage: 0, limit: rateLimit };

  const now = new Date();
  const hourBucket = now.toISOString().slice(0, 13); // e.g. "2026-04-04T15"
  const ttl = Math.floor(now.getTime() / 1000) + 7200; // 2-hour TTL

  try {
    const res = await ddb.send(new UpdateCommand({
      TableName: adminTable,
      Key: { pK: `RATE#${keyId}`, sK: hourBucket },
      UpdateExpression: 'ADD usageCount :one SET #ttl = if_not_exists(#ttl, :ttl)',
      ExpressionAttributeNames: { '#ttl': 'ttl' },
      ExpressionAttributeValues: { ':one': 1, ':ttl': ttl },
      ReturnValues: 'UPDATED_NEW',
    }));

    const usage = Number(res.Attributes?.usageCount ?? 1);
    return { allowed: usage <= rateLimit, usage, limit: rateLimit };
  } catch (err) {
    // Rate limit counter failure should not block the request — log and allow.
    console.error('[api-key-auth] rate limit counter error:', err);
    return { allowed: true, usage: 0, limit: rateLimit };
  }
}

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
    IndexName: KEY_ID_INDEX,            // GSI: keyId → BRAND / TENANT API key record
    KeyConditionExpression: 'keyId = :kid',
    ExpressionAttributeValues: { ':kid': keyId },
    Limit: 1,
  }));

  const item = res.Items?.[0] as (ApiKeyRecord & { pK: string; sK: string }) | undefined;
  const normalized = item ? normalizeApiKeyItem(item) : null;

  // Always perform the hash comparison regardless of whether the item exists.
  // This ensures all code paths take equal time, preventing keyspace enumeration
  // via response-time measurement.
  const DUMMY_HASH = 'a'.repeat(64);
  const storedHash = normalized?.keyHash ?? DUMMY_HASH;
  const hashMatch = timingSafeEqual(keyHash, storedHash);

  if (!item || !normalized) return null;

  const now = new Date().toISOString();

  // Reject if revoked and grace period has expired
  if (normalized.status === 'REVOKED') return null;
  if (normalized.status === 'GRACE' && normalized.graceUntil && normalized.graceUntil < now) return null;

  if (!hashMatch) return null;

  if (!normalized.scopes.includes(requiredScope)) return null;

  // Enforce per-key hourly rate limit via atomic DynamoDB counter
  const rl = await checkRateLimit(ddb, normalized.keyId, normalized.rateLimit);
  if (!rl.allowed) {
    console.warn('[api-key-auth] rate limit exceeded', { keyId: normalized.keyId, usage: rl.usage, limit: rl.limit });
    return null; // caller should return 429
  }

  return {
    brandId: normalized.brandId,
    keyId: normalized.keyId,
    rateLimit: normalized.rateLimit,
    scopes: normalized.scopes,
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
      keyHash,
      scopes,
      rateLimit,
      createdBy,
      desc: JSON.stringify({
        keyHash,
        scopes,
        rateLimit,
        createdBy,
      }),
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

// ── Tenant key CRUD ───────────────────────────────────────────────────────────
//
// Tenant keys live under TENANT#<tenantId> / APIKEY#<keyId> in RefDataEvent.
// Same key format as brand keys (bebo_<keyId>.<secret>) and same byKeyId GSI
// lookup — the TENANT# pK prefix is what distinguishes them from brand keys.
//
// At tenant onboarding:
// 1. Call createTenantApiKey() → get rawKey back (shown once, never stored)
// 2. Create an AWS API Gateway API key with value = rawKey, associate with
//    the appropriate usage plan (starter / growth / enterprise).
//    This enforces per-tenant throttling at the gateway before Lambda is hit.

export async function createTenantApiKey(
  ddb: DynamoDBDocumentClient,
  tenantId: string,
  brandIds: string[],
  allowedScopes: string[],           // TenantScope values: 'segments' | 'receipts_aggregate' | 'subscriber_count'
  createdBy: string,
  opts: { rateLimit?: number; minCohortThreshold?: number } = {},
): Promise<{ rawKey: string; keyId: string }> {
  const { rawKey, keyId, keyHash } = generateApiKey();
  const now = new Date().toISOString();

  await ddb.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK:          `TENANT#${tenantId}`,
      sK:          `APIKEY#${keyId}`,
      eventType:   'TENANT_API_KEY',
      status:      'ACTIVE',
      primaryCat:  'tenant_api_key',
      keyId,
      keyHash,
      brandIds,
      allowedScopes,
      rateLimit: opts.rateLimit ?? 1000,
      minCohortThreshold: opts.minCohortThreshold ?? 50,
      createdBy,
      desc: JSON.stringify({
        keyHash,
        brandIds,
        allowedScopes,
        minCohortThreshold: opts.minCohortThreshold ?? 50,
        rateLimit:          opts.rateLimit          ?? 1000,
        createdBy,
      }),
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  // rawKey returned ONCE — caller must display and save it
  return { rawKey, keyId };
}

/**
 * Immediately revokes a tenant API key. No grace period.
 * Rotate by calling createTenantApiKey() first, then revoke the old key.
 */
export async function revokeTenantApiKey(
  ddb: DynamoDBDocumentClient,
  tenantId: string,
  keyId: string,
): Promise<void> {
  await ddb.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: `APIKEY#${keyId}` },
    UpdateExpression: 'SET #s = :revoked, revokedAt = :now, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: {
      ':revoked': 'REVOKED',
      ':now':     new Date().toISOString(),
    },
  }));
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Timing-safe string comparison to prevent timing attacks on key hashes. */
function timingSafeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return nodeTimingSafeEqual(bufA, bufB);
}

/** Extract raw key from Authorization or x-api-key header. */
export function extractApiKey(headers: Record<string, string | undefined>): string | null {
  const xApiKey = headers['x-api-key'] ?? headers['X-Api-Key'];
  if (xApiKey) return xApiKey;
  const auth = headers['authorization'] ?? headers['Authorization'];
  if (auth?.startsWith('Bearer bebo_')) return auth.slice(7);
  return null;
}

function normalizeApiKeyItem(item: ApiKeyItem): ApiKeyRecord | null {
  const parsedDesc = safeJsonParse(item.desc);
  const keyHash = asString(item.keyHash) ?? asString(parsedDesc.keyHash);
  const scopes = asScopeArray(item.scopes) ?? asScopeArray(parsedDesc.scopes);
  const rateLimit = asNumber(item.rateLimit) ?? asNumber(parsedDesc.rateLimit) ?? 1000;
  const createdBy = asString(item.createdBy) ?? asString(parsedDesc.createdBy) ?? 'unknown';
  const brandId = asString(item.brandId) ?? deriveBrandId(item.pK);
  const keyId = asString(item.keyId) ?? deriveKeyId(item.sK);

  if (!brandId || !keyId || !keyHash || !scopes) {
    return null;
  }

  return {
    brandId,
    keyId,
    keyHash,
    scopes,
    rateLimit,
    status: (asString(item.status) as ApiKeyRecord['status'] | null) ?? 'ACTIVE',
    createdAt: asString(item.createdAt) ?? new Date(0).toISOString(),
    createdBy,
    revokedAt: asString(item.revokedAt) ?? undefined,
    graceUntil: asString(item.graceUntil) ?? undefined,
  };
}

function safeJsonParse(value: unknown): Record<string, unknown> {
  if (typeof value !== 'string' || !value) return {};
  try {
    return JSON.parse(value) as Record<string, unknown>;
  } catch {
    return {};
  }
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function asScopeArray(value: unknown): ApiKeyScope[] | null {
  if (!Array.isArray(value)) return null;
  const scopes = value.filter((item): item is ApiKeyScope => typeof item === 'string');
  return scopes.length > 0 ? scopes : null;
}

function deriveBrandId(pK: unknown): string | null {
  return typeof pK === 'string' && pK.startsWith('BRAND#') ? pK.slice('BRAND#'.length) : null;
}

function deriveKeyId(sK: unknown): string | null {
  return typeof sK === 'string' && sK.startsWith('APIKEY#') ? sK.slice('APIKEY#'.length) : null;
}
