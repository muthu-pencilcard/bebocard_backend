import type { DynamoDBStreamHandler } from 'aws-lambda';
import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  QueryExecutionState,
} from '@aws-sdk/client-athena';
import { createHmac } from 'crypto';

const athena = new AthenaClient({});

const ANALYTICS_BUCKET = process.env.ANALYTICS_BUCKET!;
const ATHENA_WORKGROUP = process.env.ATHENA_WORKGROUP!;
const GLUE_DATABASE    = process.env.GLUE_DATABASE!;
const USER_HASH_SALT   = process.env.USER_HASH_SALT!;

// ── Types ─────────────────────────────────────────────────────────────────────

interface ReceiptRow {
  userHash:     string;
  brandId:      string;
  purchaseDate: string;   // YYYY-MM-DD
  amount:       number;
  currency:     string;
  category:     string;
  merchant:     string;
  ingestedAt:   string;   // ISO 8601 UTC
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
    if (!pk.startsWith('USER#') || !sk.startsWith('RECEIPT#')) continue;

    const descStr = newImage['desc']?.S ?? '{}';
    let desc: Record<string, unknown>;
    try { desc = JSON.parse(descStr); } catch { continue; }

    // Only receipts that arrived via brand POS scan path carry secondaryULID.
    // This is the consent-free analytics gate: brand submitted the data to us.
    if (!desc['secondaryULID']) continue;

    const permULID = pk.replace('USER#', '');
    const brandId  = (newImage['subCategory']?.S ?? String(desc['brandId'] ?? '')) as string;
    if (!brandId) continue;

    // Pseudonymous user identifier — permULID never stored in analytics table
    const userHash = createHmac('sha256', USER_HASH_SALT).update(permULID).digest('hex');

    const rawDate    = sanitizeDate(String(desc['purchaseDate'] ?? '')) ?? new Date().toISOString().substring(0, 10);
    const amount     = sanitizeAmount(desc['amount']);
    const currency   = sanitizeCurrency(String(desc['currency'] ?? 'AUD'));
    const category   = sanitizeIdentifier(String(desc['category'] ?? 'other'));
    const merchant   = sanitizeText(String(desc['merchant'] ?? ''));
    const ingestedAt = new Date().toISOString().replace('T', ' ').replace('Z', '');

    if (amount === null) continue;   // reject non-numeric amounts

    rows.push({ userHash, brandId, purchaseDate: rawDate, amount, currency, category, merchant, ingestedAt });
  }

  if (rows.length === 0) return;

  // Build single INSERT for the entire batch — one Athena round-trip per Lambda invocation.
  // All string values are escaped via dedicated sanitizers; numeric/date values are
  // type-enforced so no string injection path exists for those columns.
  const valuesList = rows.map(r =>
    `('${r.userHash}', '${escapeSql(r.brandId)}', DATE '${r.purchaseDate}', ` +
    `${r.amount}, '${escapeSql(r.currency)}', '${escapeSql(r.category)}', '${escapeSql(r.merchant)}', TIMESTAMP '${r.ingestedAt}')`,
  ).join(',\n    ');

  const sql = `INSERT INTO \`${GLUE_DATABASE}\`.\`receipts\`
  (user_hash, brand_id, purchase_date, amount, currency, category, merchant, ingested_at)
VALUES
  ${valuesList}`;

  const queryId = await startQuery(sql);
  if (!queryId) {
    console.error('[receipt-iceberg-writer] Failed to start Athena query');
    throw new Error('Athena query did not start');
  }

  await waitForQuery(queryId);
};

// ── Helpers ───────────────────────────────────────────────────────────────────

async function startQuery(sql: string): Promise<string | undefined> {
  const res = await athena.send(new StartQueryExecutionCommand({
    QueryString:         sql,
    WorkGroup:           ATHENA_WORKGROUP,
    ResultConfiguration: { OutputLocation: `s3://${ANALYTICS_BUCKET}/athena-results/` },
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
