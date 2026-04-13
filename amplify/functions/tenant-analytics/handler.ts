import type { APIGatewayProxyHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  QueryExecutionState,
} from '@aws-sdk/client-athena';
import { createHash, timingSafeEqual as cryptoTimingSafeEqual } from 'crypto';
import { extractApiKey } from '../../shared/api-key-auth';
import { withAuditLog } from '../../shared/audit-logger';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const REPORT_TABLE = process.env.REPORT_TABLE!;
const MIN_COHORT = parseInt(process.env.MIN_COHORT_THRESHOLD ?? '50', 10);
const KEY_ID_INDEX = process.env.KEY_ID_GSI_NAME ?? 'refDataEventsByKeyId';

const athena = new AthenaClient({});
const ANALYTICS_BUCKET = process.env.ANALYTICS_BUCKET!;
const ATHENA_WORKGROUP = process.env.ATHENA_WORKGROUP!;
const GLUE_DATABASE    = process.env.GLUE_DATABASE!;

// ── Types ─────────────────────────────────────────────────────────────────────

interface TenantRecord {
  tenantId: string;
  tenantName: string;
  brandIds: string[];          // allowlisted brandIds for this tenant
  allowedScopes: TenantScope[];
  minCohortThreshold: number;
  rateLimit: number;
  status: 'ACTIVE' | 'REVOKED';
}

type TenantScope = 'segments' | 'receipts_aggregate' | 'subscriber_count' | 'intelligence';

interface ValidatedTenant {
  tenantId: string;
  brandIds: string[];
  allowedScopes: TenantScope[];
  minCohortThreshold: number;
}

// Stored in UserDataEvent sK: SEGMENT#<brandId>
interface SegmentDesc {
  spendBucket: '<100' | '100-200' | '200-500' | '500+';
  visitFrequency: 'new' | 'occasional' | 'frequent' | 'lapsed';
  totalSpend: number;
  visitCount: number;
  lastVisit: string;
  persona: string[];
  computedAt: string;
  // Written by segment-processor from SUBSCRIPTION#<brandId> at segment compute time
  // so analytics can enforce the consent gate without a per-user join.
  subscribed: boolean;
}

interface SegmentsResponse {
  brandId: string;
  period: string;
  subscriberCount: number;
  spendDistribution: Record<string, number>;
  visitFrequency: Record<string, number>;
}

// ── Handler ───────────────────────────────────────────────────────────────────

const _handler: APIGatewayProxyHandler = async (event) => {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  };

  const path = event.path ?? '';

  // ── API Versioning Redirect (P2-7 & P3-12 Deprecation) ──
  if (!path.startsWith('/v1/')) {
    const v1Path = `/v1${path.startsWith('/') ? '' : '/'}${path}`;
    console.warn(`[tenant-analytics] Legacy unversioned request to ${path}. Redirecting to ${v1Path}`);
    return {
      statusCode: 308, // Permanent Redirect
      headers: { 
        ...headers, 
        'Location': v1Path,
        'Deprecation': 'true',
        'Sunset': 'Wed, 01 Jul 2026 00:00:00 GMT',
        'Link': `<${v1Path}>; rel="replacement"`,
        'X-BeboCard-Notice': 'This endpoint is deprecated. Transition to /v1/ routes immediately.'
      },
      body: JSON.stringify({ 
        error: 'Deprecated Endpoint', 
        message: 'Please update your integration to use /v1 prefix. Support for unversioned routes will be removed in v2.0.',
        suggestedPath: v1Path 
      })
    };
  }

  try {
    const rawKey = extractApiKey(event.headers as Record<string, string | undefined>);
    if (!rawKey) {
      return { statusCode: 401, headers, body: JSON.stringify({ error: 'Missing API key' }) };
    }

    const tenant = await validateTenantApiKey(rawKey);
    if (!tenant) {
      return { statusCode: 401, headers, body: JSON.stringify({ error: 'Invalid API key' }) };
    }

    if (path.endsWith('/analytics/segments')) {
      return handleSegments(event, headers, tenant);
    }

    if (path.endsWith('/analytics/subscriber-count')) {
      return handleSubscriberCount(event, headers, tenant);
    }

    if (path.endsWith('/analytics/intelligence')) {
      return handleIntelligence(event, headers, tenant);
    }
    
    if (path.endsWith('/analytics/trends')) {
      return handleTrends(event, headers, tenant);
    }

    return { statusCode: 404, headers, body: JSON.stringify({ error: 'Unknown route' }) };
  } catch (err) {
    console.error('[tenant-analytics]', err);
    return { statusCode: 500, headers, body: JSON.stringify({ error: 'Internal error' }) };
  }
};

export const handler = withAuditLog(dynamo, _handler);

// ── GET /analytics/segments ───────────────────────────────────────────────────
//
// Returns aggregate segment distributions for one of the tenant's brandIds.
// Only users with subscribed=true on their SEGMENT#<brandId> record are counted
// (i.e. users who have SUBSCRIPTION#<brandId> — consent gate).
// Response is suppressed entirely if cohortSize < minCohortThreshold.
//
// Query params:
//   brandId  — required, must be in tenant.brandIds
//   period   — optional, e.g. "2026-03". Currently echoed back only; the handler
//              does not time-slice segment records yet.

async function handleSegments(
  event: Parameters<APIGatewayProxyHandler>[0],
  headers: Record<string, string>,
  tenant: ValidatedTenant,
): Promise<{ statusCode: number; headers: Record<string, string>; body: string }> {
  if (!tenant.allowedScopes.includes('segments')) {
    console.warn('[tenant-analytics] scope not permitted', { tenantId: tenant.tenantId, requestedScope: 'segments' });
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  const brandId = event.queryStringParameters?.['brandId'];
  const period = event.queryStringParameters?.['period'] ?? currentPeriod();

  if (!brandId) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'brandId is required' }) };
  }

  if (!tenant.brandIds.includes(brandId)) {
    console.warn('[tenant-analytics] brandId not in tenant allowlist', { tenantId: tenant.tenantId, brandId });
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  // Query the UserDataEvent GSI with sK as the partition key.
  // backend.ts provisions this as 'sK-pK-index' and projects desc + status.
  // Query pattern: sK = 'SEGMENT#<brandId>' for all users with a segment record.

  const segmentSK = `SEGMENT#${brandId}`;
  const items: Array<{ desc: string }> = [];
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      IndexName: 'sK-pK-index',
      KeyConditionExpression: 'sK = :sk',
      ExpressionAttributeValues: { ':sk': segmentSK },
      ExclusiveStartKey: lastKey,
      Limit: 1000,
    }));
    for (const item of res.Items ?? []) {
      items.push(item as { desc: string });
    }
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  // Aggregate — only subscribed users (consent gate)
  const spendCounts: Record<string, number> = { '<100': 0, '100-200': 0, '200-500': 0, '500+': 0 };
  const visitCounts: Record<string, number> = { new: 0, occasional: 0, frequent: 0, lapsed: 0 };
  let subscriberCount = 0;

  for (const item of items) {
    let seg: Partial<SegmentDesc>;
    try { seg = JSON.parse(item.desc ?? '{}'); } catch { continue; }

    if (!seg.subscribed) continue;     // consent gate — skip non-subscribers

    subscriberCount++;
    if (seg.spendBucket) spendCounts[seg.spendBucket] = (spendCounts[seg.spendBucket] ?? 0) + 1;
    if (seg.visitFrequency) visitCounts[seg.visitFrequency] = (visitCounts[seg.visitFrequency] ?? 0) + 1;
  }

  // Suppress if below minimum cohort threshold — prevents statistical re-identification.
  // Return zeroed distributions with no suppression flag so tenants cannot infer user base size.
  const threshold = tenant.minCohortThreshold ?? MIN_COHORT;
  if (subscriberCount < threshold) {
    console.info('[tenant-analytics] cohort suppressed', { brandId, subscriberCount, threshold });
    const response: SegmentsResponse = {
      brandId,
      period,
      subscriberCount: 0,
      spendDistribution: { '<100': 0, '100-200': 0, '200-500': 0, '500+': 0 },
      visitFrequency: { new: 0, occasional: 0, frequent: 0, lapsed: 0 },
    };
    return { statusCode: 200, headers, body: JSON.stringify(response) };
  }

  // Convert counts to proportions
  const spendDistribution = normalise(spendCounts, subscriberCount);
  const visitFrequency = normalise(visitCounts, subscriberCount);

  const response: SegmentsResponse = {
    brandId,
    period,
    subscriberCount,
    spendDistribution,
    visitFrequency,
  };

  return { statusCode: 200, headers, body: JSON.stringify(response) };
}

// ── GET /analytics/subscriber-count ──────────────────────────────────────────
//
// Returns the raw count of active subscribers for one of the tenant's brandIds.
// Unlike /analytics/segments, no cohort suppression is applied — the tenant
// is entitled to know how many users have subscribed to their brand.
//
// Query params:
//   brandId  — required, must be in tenant.brandIds
//   period   — optional, echoed back only

async function handleSubscriberCount(
  event: Parameters<APIGatewayProxyHandler>[0],
  headers: Record<string, string>,
  tenant: ValidatedTenant,
): Promise<{ statusCode: number; headers: Record<string, string>; body: string }> {
  if (!tenant.allowedScopes.includes('subscriber_count')) {
    console.warn('[tenant-analytics] scope not permitted', { tenantId: tenant.tenantId, requestedScope: 'subscriber_count' });
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  const brandId = event.queryStringParameters?.['brandId'];
  const period = event.queryStringParameters?.['period'] ?? currentPeriod();

  if (!brandId) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'brandId is required' }) };
  }

  if (!tenant.brandIds.includes(brandId)) {
    console.warn('[tenant-analytics] brandId not in tenant allowlist', { tenantId: tenant.tenantId, brandId });
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  // Query the sK-pK-index for SUBSCRIPTION#<brandId> with status = ACTIVE.
  // Uses Select: 'COUNT' so DynamoDB returns only a count — no item data crosses the wire.
  const subscriptionSK = `SUBSCRIPTION#${brandId}`;
  let subscriberCount = 0;
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      IndexName: 'sK-pK-index',
      KeyConditionExpression: 'sK = :sk',
      FilterExpression: '#status = :active',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':sk': subscriptionSK, ':active': 'ACTIVE' },
      Select: 'COUNT',
      ExclusiveStartKey: lastKey,
    }));
    subscriberCount += res.Count ?? 0;
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({ brandId, period, subscriberCount }),
  };
}

// ── Tenant API key authentication ─────────────────────────────────────────────
//
// Tenant keys are stored in RefDataEvent under:
//   pK: TENANT#<tenantId>
//   sK: APIKEY#<keyId>
//
// The same byKeyId GSI used for brand keys resolves the keyId to the record.
// Tenant records carry brandIds[] instead of a single brandId.
//
// Tenant API keys follow the same bebo_<keyId>.<secret> format as brand API keys,
// but resolve to TENANT#<tenantId> records with multi-brand scope metadata.

async function validateTenantApiKey(rawKey: string): Promise<ValidatedTenant | null> {
  if (!rawKey?.startsWith('bebo_')) return null;

  const withoutPrefix = rawKey.slice(5);
  const dotIndex = withoutPrefix.indexOf('.');
  if (dotIndex === -1) return null;

  const keyId = withoutPrefix.slice(0, dotIndex);
  const keyHash = createHash('sha256').update(rawKey).digest('hex');

  // Resolve via byKeyId GSI — same index used by brand key validation
  const res = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    IndexName: KEY_ID_INDEX,
    KeyConditionExpression: 'keyId = :kid',
    ExpressionAttributeValues: { ':kid': keyId },
    Limit: 1,
  }));

  const item = res.Items?.[0] as
    | (TenantRecord & { pK: string; sK: string; status: string; desc?: string })
    | undefined;

  if (!item) return null;
  if (item.status !== 'ACTIVE') return null;
  if (!item.pK.startsWith('TENANT#')) return null;    // must be a tenant key, not a brand key

  const rawDesc = (item as unknown as { desc?: string }).desc ?? '{}';
  const desc: Partial<TenantRecord & { keyHash: string }> = JSON.parse(rawDesc);

  // keyHash is stored inside the desc JSON field (same pattern as brand keys in api-key-auth.ts)
  if (!desc.keyHash) return null;
  if (!timingSafeEqual(desc.keyHash, keyHash)) return null;

  return {
    tenantId: item.pK.replace('TENANT#', ''),
    brandIds: desc.brandIds ?? [],
    allowedScopes: desc.allowedScopes ?? [],
    minCohortThreshold: desc.minCohortThreshold ?? MIN_COHORT,
  };
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function normalise(counts: Record<string, number>, total: number): Record<string, number> {
  if (total === 0) return counts;
  return Object.fromEntries(
    Object.entries(counts).map(([k, v]) => [k, Math.round((v / total) * 100) / 100]),
  );
}

function currentPeriod(): string {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
}

// ── Bebo Intelligence: Data Lake Querying (P2-5) ──────────────────────────────
//
// Direct Athena queries against the Iceberg table for granular spend insights.
// Always enforced via 'brand_id' filter and pseudonymised 'user_hash'.

async function handleIntelligence(
  event: Parameters<APIGatewayProxyHandler>[0],
  headers: Record<string, string>,
  tenant: ValidatedTenant,
): Promise<{ statusCode: number; headers: Record<string, string>; body: string }> {
  if (!tenant.allowedScopes.includes('intelligence')) {
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  const brandId = event.queryStringParameters?.['brandId'];
  if (!brandId || !tenant.brandIds.includes(brandId)) {
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  const queryType = event.queryStringParameters?.['type'] ?? 'spend_mix';

  try {
    // 1. Optimization: Check for pre-computed report first (P2-17)
    const today = new Date().toISOString().split('T')[0];
    const reportRes = await dynamo.send(new GetCommand({
      TableName: REPORT_TABLE,
      Key: { pK: `REPORT#${brandId}`, sK: `DAILY#${today}` },
    }));

    if (reportRes.Item && queryType === 'spend_mix') {
      const stats = JSON.parse(reportRes.Item.desc ?? '{}');
      if (stats.categoryMix) {
        console.info(`[tenant-analytics] Serving spend_mix from pre-computed report for ${brandId}`);
        return { 
          statusCode: 200, 
          headers, 
          body: JSON.stringify({ 
            brandId, 
            type: queryType, 
            data: stats.categoryMix,
            source: 'report_snapshot' 
          }) 
        };
      }
    }

    let sql = '';
    
    if (queryType === 'spend_mix') {
      sql = `SELECT category, sum(amount) as total_amount, count(*) as tx_count, avg(amount) as atv 
             FROM "${GLUE_DATABASE}"."receipts" 
             WHERE brand_id = '${brandId}' 
             GROUP BY category 
             ORDER BY total_amount DESC`;
    } else {
      return { statusCode: 400, headers, body: JSON.stringify({ error: 'Unsupported intelligence type' }) };
    }

    const { QueryExecutionId } = await athena.send(new StartQueryExecutionCommand({
      QueryString: sql,
      WorkGroup: ATHENA_WORKGROUP,
      ResultConfiguration: { OutputLocation: `s3://${ANALYTICS_BUCKET}/athena-results/` },
    }));

    if (!QueryExecutionId) throw new Error('Query failed to start');

    // Polling logic (simplified for Lambda execution window — max 20s wait)
    const results = await pollAthenaResults(QueryExecutionId);
    
    return { statusCode: 200, headers, body: JSON.stringify({ brandId, type: queryType, data: results, source: 'athena_live' }) };
    
  } catch (err: any) {
    console.error('[tenant-analytics] Athena intelligence error:', err);
    return { statusCode: 500, headers, body: JSON.stringify({ error: 'Intelligence engine offline' }) };
  }
}

async function pollAthenaResults(queryId: string): Promise<any[]> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const res = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));
    const state = res.QueryExecution?.Status?.State;
    if (state === QueryExecutionState.SUCCEEDED) {
      const results = await athena.send(new GetQueryResultsCommand({ QueryExecutionId: queryId }));
      return parseAthenaResults(results);
    }
    if (state === QueryExecutionState.FAILED || state === QueryExecutionState.CANCELLED) {
      throw new Error(`Query ${state}: ${res.QueryExecution?.Status?.StateChangeReason}`);
    }
    await new Promise(r => setTimeout(r, 1000));
  }
  throw new Error('Query timed out');
}

function parseAthenaResults(results: any): any[] {
  const rows = results.ResultSet.Rows ?? [];
  if (rows.length < 2) return [];
  const headers = rows[0].Data.map((d: any) => d.VarCharValue);
  return rows.slice(1).map((row: any) => {
    const entry: any = {};
    row.Data.forEach((d: any, i: number) => {
      entry[headers[i]] = d.VarCharValue;
    });
    return entry;
  });
}

// ── GET /analytics/trends ─────────────────────────────────────────────────────
//
// Returns historical daily snapshots for a brand.
// Used for trend lines in the portal.
//
// Query params:
//   brandId  — required, must be in tenant.brandIds
//   period   — optional, '7d' | '30d' | '90d' (default '30d')

async function handleTrends(
  event: Parameters<APIGatewayProxyHandler>[0],
  headers: Record<string, string>,
  tenant: ValidatedTenant,
): Promise<{ statusCode: number; headers: Record<string, string>; body: string }> {
  const brandId = event.queryStringParameters?.['brandId'];
  if (!brandId || !tenant.brandIds.includes(brandId)) {
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  const period = event.queryStringParameters?.['period'] ?? '30d';
  const days = period === '7d' ? 7 : period === '90d' ? 90 : 30;
  const fromDate = new Date(Date.now() - days * 24 * 3600 * 1000).toISOString().split('T')[0];

  // Query ReportDataEvent for DAILY# snapshots
  const res = await dynamo.send(new QueryCommand({
    TableName: REPORT_TABLE,
    KeyConditionExpression: 'pK = :pk AND sK >= :sk',
    ExpressionAttributeValues: {
      ':pk': `REPORT#${brandId}`,
      ':sk': `DAILY#${fromDate}`,
    },
    Limit: 100,
  }));

  const snapshots = (res.Items ?? []).map(item => ({
    date: item.sK.replace('DAILY#', ''),
    metrics: JSON.parse(item.desc ?? '{}'),
  }));

  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({ brandId, period, snapshots }),
  };
}

function timingSafeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return cryptoTimingSafeEqual(bufA, bufB);
}
