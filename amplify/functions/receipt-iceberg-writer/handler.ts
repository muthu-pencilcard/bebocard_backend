import type { DynamoDBStreamHandler } from 'aws-lambda';
import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  QueryExecutionState,
} from '@aws-sdk/client-athena';
import { createHmac } from 'crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const athena = new AthenaClient({});

const ANALYTICS_BUCKET      = process.env.ANALYTICS_BUCKET!;
const ATHENA_WORKGROUP      = process.env.ATHENA_WORKGROUP!;
const GLUE_DATABASE         = process.env.GLUE_DATABASE!;
const REFDATA_TABLE         = process.env.REFDATA_TABLE!;
const USER_HASH_SALT_PATH    = process.env.USER_HASH_SALT_PATH;
const GLOBAL_ANALYTICS_SALT = process.env.GLOBAL_ANALYTICS_SALT!; // BeboCard-global cross-tenant salt

const ssm = new SSMClient({});
let cachedSalt: string | undefined;

async function getSalt(): Promise<string> {
  if (cachedSalt) return cachedSalt;
  if (!USER_HASH_SALT_PATH) return process.env.USER_HASH_SALT || '';

  try {
    const res = await ssm.send(new GetParameterCommand({
      Name: USER_HASH_SALT_PATH,
      WithDecryption: true
    }));
    cachedSalt = res.Parameter?.Value;
    return cachedSalt || '';
  } catch (err) {
    console.error('[receipt-iceberg-writer] Failed to fetch salt from SSM', { path: USER_HASH_SALT_PATH, err });
    return process.env.USER_HASH_SALT || '';
  }
}

// Cache for tenant metadata
const tenantCache: Record<string, { salt: string; tenantId: string; tier: string; analyticsBucket?: string }> = {};
const brandTenantCache: Record<string, string> = {};

// Exported only for test cache teardown — not called in production
export const _testResetCaches = () => {
  Object.keys(tenantCache).forEach(k => { delete tenantCache[k]; });
  Object.keys(brandTenantCache).forEach(k => { delete brandTenantCache[k]; });
};

// ── Types ─────────────────────────────────────────────────────────────────────

interface ReceiptRow {
  visitorHash:       string | null;  // HMAC(permULID, GLOBAL_ANALYTICS_SALT) — cross-tenant, never egressed
  visitorHashTenant: string | null;  // HMAC(permULID, tenantMeta.salt) — per-tenant, safe to export
  isBebocard:        boolean;
  brandId:           string;
  tenantId:          string;
  purchaseDate:      string;   // YYYY-MM-DD
  amount:            number;
  currency:          string;
  category:          string;
  merchant:          string;
  ingestedAt:        string;   // ISO 8601 UTC
}

// ── Handler ───────────────────────────────────────────────────────────────────
//
// Triggered by DynamoDB Streams on UserDataEvent (same stream as segment-processor).
// On every RECEIPT# INSERT, checks whether the receipt record contains a
// `secondaryULID` field in its `desc` JSON — present only when the receipt was
// submitted via the brand POS scan path (/receipt endpoint in scan-handler).
//
// Manually entered receipts (source != 'brand_push') do NOT have secondaryULID
// and are skipped — their data is not visible to tenant analytics.
//
// Writes are batched per Lambda invocation and inserted into the Iceberg table
// via a single Athena INSERT statement, keeping Iceberg manifest overhead low.
//
// Privacy: permULID is hashed with HMAC-SHA256 before storage.
// The analytics table never contains permULID, secondaryULID, or any direct PII.

export const handler: DynamoDBStreamHandler = async (event) => {
  const rows: ReceiptRow[] = [];

  for (const record of event.Records) {
    if (record.eventName !== 'INSERT') continue;

    const newImage = record.dynamodb?.NewImage;
    if (!newImage) continue;

    const pk = newImage['pK']?.S ?? '';
    const sk = newImage['sK']?.S ?? '';
    const isAnonymous = pk.startsWith('ANON#');

    if (!isAnonymous && (!pk.startsWith('USER#') || !sk.startsWith('RECEIPT#'))) continue;
    if (isAnonymous && sk !== 'receipt') continue;

    const descStr = newImage['desc']?.S ?? '{}';
    let desc: Record<string, unknown>;
    try { desc = JSON.parse(descStr); } catch { continue; }

    // Only receipts that arrived via brand POS scan path carry secondaryULID OR are in anonymousMode.
    if (!desc['secondaryULID'] && !desc['isAnonymous']) continue;

    const brandId  = (newImage['subCategory']?.S ?? String(desc['brandId'] ?? '')) as string;
    if (!brandId) continue;

    // Resolve Tenant Context
    const tenantId = await getTenantIdForBrand(brandId);
    if (!tenantId) continue;

    const tenantMeta = await getTenantMetadata(tenantId);
    if (!tenantMeta || tenantMeta.tier === 'ENGAGEMENT') continue;

    // Dual visitor hash (P1-1, P1-9):
    //   visitorHash       = HMAC(permULID, GLOBAL_ANALYTICS_SALT) — stable across all tenants, never egressed
    //   visitorHashTenant = HMAC(permULID, tenantMeta.salt)       — per-tenant pseudonym, safe to export
    let visitorHash: string | null = null;
    let visitorHashTenant: string | null = null;
    if (!isAnonymous) {
      const permULID = pk.replace('USER#', '');
      visitorHash       = createHmac('sha256', GLOBAL_ANALYTICS_SALT).update(permULID).digest('hex');
      visitorHashTenant = createHmac('sha256', tenantMeta.salt).update(permULID).digest('hex');
    }

    const rawDate    = sanitizeDate(String(desc['purchaseDate'] ?? '')) ?? new Date().toISOString().substring(0, 10);
    const amount     = sanitizeAmount(desc['amount']);
    const currency   = sanitizeCurrency(String(desc['currency'] ?? 'AUD'));
    const category   = sanitizeIdentifier(String(desc['category'] ?? 'other'));
    const merchant   = sanitizeText(String(desc['merchant'] ?? ''));
    const ingestedAt = new Date().toISOString().replace('T', ' ').replace('Z', '');

    if (amount === null) continue;

    rows.push({ visitorHash, visitorHashTenant, isBebocard: !isAnonymous, brandId, tenantId, purchaseDate: rawDate, amount, currency, category, merchant, ingestedAt });
  }

  if (rows.length === 0) return;

  // Group rows by tenant — they write to different tables
  const rowsByTenant: Record<string, ReceiptRow[]> = {};
  for (const row of rows) {
    if (!rowsByTenant[row.tenantId]) rowsByTenant[row.tenantId] = [];
    rowsByTenant[row.tenantId].push(row);
  }

  for (const [tenantId, tenantRows] of Object.entries(rowsByTenant)) {
    const tableName = `receipts_${tenantId.toLowerCase().replace(/[^a-z0-9_]/g, '_')}`;
    const tenantMeta = await getTenantMetadata(tenantId);
    
    const valuesList = tenantRows.map(r =>
      `(${r.visitorHash ? `'${r.visitorHash}'` : 'NULL'}, ` +
      `${r.visitorHashTenant ? `'${r.visitorHashTenant}'` : 'NULL'}, ` +
      `${r.isBebocard ? 'true' : 'false'}, ` +
      `'${escapeSql(r.brandId)}', DATE '${r.purchaseDate}', ` +
      `${r.amount}, '${escapeSql(r.currency)}', '${escapeSql(r.category)}', '${escapeSql(r.merchant)}', TIMESTAMP '${r.ingestedAt}')`,
    ).join(',\n      ');

    const sql = `INSERT INTO \`${GLUE_DATABASE}\`.\`${tableName}\`
    (visitor_hash, visitor_hash_tenant, is_bebocard, brand_id, purchase_date, amount, currency, category, merchant, ingested_at)
  VALUES
    ${valuesList}`;

    const queryId = await startQuery(sql, tenantMeta?.analyticsBucket);
    if (!queryId) {
      console.error(`[receipt-iceberg-writer] Failed to start Athena query for tenant ${tenantId}`);
      continue;
    }
    await waitForQuery(queryId);
  }
};

// ── Helpers ───────────────────────────────────────────────────────────────────

async function startQuery(sql: string, customBucket?: string): Promise<string | undefined> {
  const outputLocation = customBucket 
    ? `s3://${customBucket}/athena-results/` 
    : `s3://${ANALYTICS_BUCKET}/athena-results/`;

  const res = await athena.send(new StartQueryExecutionCommand({
    QueryString:         sql,
    WorkGroup:           ATHENA_WORKGROUP,
    ResultConfiguration: { OutputLocation: outputLocation },
  }));
  return res.QueryExecutionId;
}

async function waitForQuery(
  queryId: string,
  maxWaitMs = 45_000,
): Promise<void> {
  const deadline = Date.now() + maxWaitMs;
  let delayMs    = 500;

  while (Date.now() < deadline) {
    const res = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));
    const state = res.QueryExecution?.Status?.State;

    if (state === QueryExecutionState.SUCCEEDED) return;

    if (
      state === QueryExecutionState.FAILED  ||
      state === QueryExecutionState.CANCELLED
    ) {
      const reason = res.QueryExecution?.Status?.StateChangeReason ?? 'unknown';
      throw new Error(`Athena query ${queryId} ${state}: ${reason}`);
    }

    await sleep(delayMs);
    delayMs = Math.min(delayMs * 2, 4_000);
  }

  throw new Error(`Athena query ${queryId} did not complete within ${maxWaitMs}ms`);
}

async function getTenantIdForBrand(brandId: string): Promise<string | null> {
  if (brandTenantCache[brandId]) return brandTenantCache[brandId];
  
  const res = await ddb.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'profile' }
  }));
  
  const tenantId = res.Item?.tenantId as string | undefined;
  if (tenantId) brandTenantCache[brandId] = tenantId;
  return tenantId ?? null;
}

async function getTenantMetadata(tenantId: string) {
  if (tenantCache[tenantId]) return tenantCache[tenantId];

  const res = await ddb.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'profile' }
  }));

  if (!res.Item) return null;
  const desc = JSON.parse(res.Item.desc || '{}');
  
  // Resolve analytics bucket for Enterprise-tier residency (P1-3)
  let analyticsBucket: string | undefined;
  if (res.Item.tier === 'ENTERPRISE') {
    if (desc.analyticsConfig?.customBucketArn) {
      analyticsBucket = desc.analyticsConfig.customBucketArn.split(':').pop();
    } else if (desc.analyticsConfig?.bucketType === 'DEDICATED') {
      analyticsBucket = `bebocard-enterprise-${tenantId.toLowerCase()}`;
    }
  }

  const meta = {
    tenantId,
    salt: desc.salt as string || await getSalt(),
    tier: res.Item.tier as string || 'ENGAGEMENT',
    analyticsBucket
  };
  tenantCache[tenantId] = meta;
  return meta;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ── SQL sanitizers ────────────────────────────────────────────────────────────
// Athena INSERT does not support parameterized execution, so each value type
// gets a dedicated sanitizer that enforces format before interpolation.

/** Escapes single quotes in strings intended for SQL single-quoted literals. */
function escapeSql(value: string): string {
  return value.replace(/'/g, "''").replace(/\\/g, '\\\\');
}

/** Validates YYYY-MM-DD format; returns null if invalid. */
function sanitizeDate(value: string): string | null {
  const trimmed = value.substring(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(trimmed) ? trimmed : null;
}

/** Coerces to a finite number; returns null if not numeric. */
function sanitizeAmount(value: unknown): number | null {
  const n = typeof value === 'number' ? value : parseFloat(String(value ?? ''));
  return Number.isFinite(n) && n >= 0 ? Math.round(n * 100) / 100 : null;
}

/** Validates ISO 4217 currency code (3 uppercase letters). Defaults to AUD. */
function sanitizeCurrency(value: string): string {
  return /^[A-Z]{3}$/.test(value) ? value : 'AUD';
}

/**
 * Validates an identifier-style string (category, brandId, etc.).
 * Allows alphanumerics, underscores, hyphens, spaces. Max 64 chars.
 * Falls back to 'other' if invalid.
 */
function sanitizeIdentifier(value: string): string {
  const trimmed = value.trim().substring(0, 64);
  return /^[\w\s-]+$/.test(trimmed) ? trimmed : 'other';
}

/**
 * Sanitizes free-text fields (merchant name).
 * Strips control characters, limits to 200 chars.
 * SQL escaping is applied separately via escapeSql().
 */
function sanitizeText(value: string): string {
  return value
    .replace(/[\x00-\x1F\x7F]/g, '')   // strip control chars
    .trim()
    .substring(0, 200);
}
