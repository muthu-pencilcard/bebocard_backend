import type { APIGatewayProxyHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { createHash, timingSafeEqual as cryptoTimingSafeEqual } from 'crypto';
import { extractApiKey } from '../../shared/api-key-auth';
import { withAuditLog } from '../../shared/audit-logger';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const USER_TABLE    = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const MIN_COHORT    = parseInt(process.env.MIN_COHORT_THRESHOLD ?? '50', 10);

// ── Types ─────────────────────────────────────────────────────────────────────

interface TenantRecord {
  tenantId:            string;
  tenantName:          string;
  brandIds:            string[];          // allowlisted brandIds for this tenant
  allowedScopes:       TenantScope[];
  minCohortThreshold:  number;
  rateLimit:           number;
  status:              'ACTIVE' | 'REVOKED';
}

type TenantScope = 'segments' | 'receipts_aggregate' | 'subscriber_count';

interface ValidatedTenant {
  tenantId:           string;
  brandIds:           string[];
  allowedScopes:      TenantScope[];
  minCohortThreshold: number;
}

// Stored in UserDataEvent sK: SEGMENT#<brandId>
interface SegmentDesc {
  spendBucket:    '<100' | '100-200' | '200-500' | '500+';
  visitFrequency: 'new' | 'occasional' | 'frequent' | 'lapsed';
  totalSpend:     number;
  visitCount:     number;
  lastVisit:      string;
  persona:        string[];
  computedAt:     string;
  // Written by segment-processor from SUBSCRIPTION#<brandId> at segment compute time
  // so analytics can enforce the consent gate without a per-user join.
  subscribed:     boolean;
}

interface SegmentsResponse {
  brandId:           string;
  period:            string;
  cohortSize:        number;
  subscriberCount:   number;
  spendDistribution: Record<string, number>;
  visitFrequency:    Record<string, number>;
}

// ── Handler ───────────────────────────────────────────────────────────────────

const _handler: APIGatewayProxyHandler = async (event) => {
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  };

  try {
    const rawKey = extractApiKey(event.headers as Record<string, string | undefined>);
    if (!rawKey) {
      return { statusCode: 401, headers, body: JSON.stringify({ error: 'Missing API key' }) };
    }

    const tenant = await validateTenantApiKey(rawKey);
    if (!tenant) {
      return { statusCode: 401, headers, body: JSON.stringify({ error: 'Invalid API key' }) };
    }

    const path = event.path ?? '';

    if (path.endsWith('/analytics/segments')) {
      return handleSegments(event, headers, tenant);
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
): Promise<ReturnType<APIGatewayProxyHandler>> {
  if (!tenant.allowedScopes.includes('segments')) {
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Scope not permitted' }) };
  }

  const brandId = event.queryStringParameters?.['brandId'];
  const period  = event.queryStringParameters?.['period'] ?? currentPeriod();

  if (!brandId) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'brandId is required' }) };
  }

  if (!tenant.brandIds.includes(brandId)) {
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'brandId not permitted for this tenant' }) };
  }

  // Query the UserDataEvent GSI with sK as the partition key.
  // backend.ts provisions this as 'sK-pK-index' and projects desc + status.
  // Query pattern: sK = 'SEGMENT#<brandId>' for all users with a segment record.

  const segmentSK = `SEGMENT#${brandId}`;
  const items: Array<{ desc: string }> = [];
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName:                 USER_TABLE,
      IndexName:                 'sK-pK-index',
      KeyConditionExpression:    'sK = :sk',
      ExpressionAttributeValues: { ':sk': segmentSK },
      ExclusiveStartKey:         lastKey,
      Limit:                     1000,
    }));
    for (const item of res.Items ?? []) {
      items.push(item as { desc: string });
    }
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  // Aggregate — only subscribed users (consent gate)
  const spendCounts:   Record<string, number> = { '<100': 0, '100-200': 0, '200-500': 0, '500+': 0 };
  const visitCounts:   Record<string, number> = { new: 0, occasional: 0, frequent: 0, lapsed: 0 };
  let subscriberCount = 0;
  let totalInCohort   = 0;

  for (const item of items) {
    let seg: Partial<SegmentDesc>;
    try { seg = JSON.parse(item.desc ?? '{}'); } catch { continue; }

    totalInCohort++;
    if (!seg.subscribed) continue;     // consent gate — skip non-subscribers

    subscriberCount++;
    if (seg.spendBucket)    spendCounts[seg.spendBucket]    = (spendCounts[seg.spendBucket]    ?? 0) + 1;
    if (seg.visitFrequency) visitCounts[seg.visitFrequency] = (visitCounts[seg.visitFrequency] ?? 0) + 1;
  }

  // Suppress if below minimum cohort threshold — prevents statistical re-identification
  const threshold = tenant.minCohortThreshold ?? MIN_COHORT;
  if (subscriberCount < threshold) {
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        brandId,
        period,
        suppressed: true,
        reason:     `Cohort below minimum threshold of ${threshold}`,
      }),
    };
  }

  // Convert counts to proportions
  const spendDistribution = normalise(spendCounts, subscriberCount);
  const visitFrequency    = normalise(visitCounts,  subscriberCount);

  const response: SegmentsResponse = {
    brandId,
    period,
    cohortSize:       totalInCohort,
    subscriberCount,
    spendDistribution,
    visitFrequency,
  };

  return { statusCode: 200, headers, body: JSON.stringify(response) };
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
  const dotIndex      = withoutPrefix.indexOf('.');
  if (dotIndex === -1) return null;

  const keyId   = withoutPrefix.slice(0, dotIndex);
  const keyHash = createHash('sha256').update(rawKey).digest('hex');

  // Resolve via byKeyId GSI — same index used by brand key validation
  const res = await dynamo.send(new QueryCommand({
    TableName:                 REFDATA_TABLE,
    IndexName:                 'byKeyId',
    KeyConditionExpression:    'keyId = :kid',
    ExpressionAttributeValues: { ':kid': keyId },
    Limit:                     1,
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
    tenantId:           item.pK.replace('TENANT#', ''),
    brandIds:           desc.brandIds           ?? [],
    allowedScopes:      desc.allowedScopes       ?? [],
    minCohortThreshold: desc.minCohortThreshold  ?? MIN_COHORT,
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

function timingSafeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return cryptoTimingSafeEqual(bufA, bufB);
}
