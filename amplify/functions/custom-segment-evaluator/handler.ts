/**
 * custom-segment-evaluator — end-of-day batch Lambda
 *
 * Evaluates custom segment definitions against subscriber receipt history.
 * Runs nightly at 00:30 UTC (before analytics compaction at 02:00 UTC).
 *
 * Two segment scopes:
 *  - 'brand'  : uses only the defining brand's receipts to compute metrics
 *  - 'global' : uses all receipts across every brand (cross-brand spend intelligence)
 *
 * Data model:
 *  RefDataEvent  pK=TENANT#<tenantId>          sK=SEGMENT_DEF#<segmentId>
 *  UserDataEvent pK=USER#<permULID>            sK=SEGMENT#<brandId>#CUSTOM#<segmentId>
 *
 * Global (BeboCard-owned) segments use tenantId='bebocard' and brandId='bebocard'.
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  ScanCommand,
  QueryCommand,
  BatchWriteCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;

// ─── Types ────────────────────────────────────────────────────────────────────

type Operator = 'gte' | 'lte' | 'between' | 'eq';
type Period = 'day' | 'week' | 'month' | 'quarter';
type Scope = 'brand' | 'global';

interface SegmentRule {
  metric: 'visit_count' | 'total_spend' | 'avg_order_value' | 'days_since_last_visit';
  period: Period;
  operator: Operator;
  value: number;
  value2?: number; // only used for 'between'
}

interface SegmentDef {
  tenantId: string;
  brandId: string;           // derived from the TENANT# record or 'bebocard' for global
  segmentId: string;
  name: string;
  rules: SegmentRule[];
  logicalOperator: 'AND' | 'OR';
  scope: Scope;
  active: boolean;
}

interface ReceiptMetrics {
  visit_count: number;
  total_spend: number;
  avg_order_value: number;
  days_since_last_visit: number;
}

// ─── Period helpers ────────────────────────────────────────────────────────────

const PERIOD_DAYS: Record<Period, number> = {
  day: 1,
  week: 7,
  month: 30,
  quarter: 90,
};

function sinceDate(period: Period): string {
  const ms = PERIOD_DAYS[period] * 24 * 60 * 60 * 1000;
  return new Date(Date.now() - ms).toISOString();
}

// ─── DynamoDB helpers ─────────────────────────────────────────────────────────

/** Scan RefDataEvent for all active SEGMENT_DEF# records across all tenants. */
async function getAllSegmentDefs(): Promise<SegmentDef[]> {
  const defs: SegmentDef[] = [];
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new ScanCommand({
      TableName: REFDATA_TABLE,
      FilterExpression: 'begins_with(sK, :prefix)',
      ExpressionAttributeValues: { ':prefix': 'SEGMENT_DEF#' },
      ExclusiveStartKey: lastKey,
    }));

    for (const item of res.Items ?? []) {
      const tenantId = (item.pK as string).replace('TENANT#', '');
      const segmentId = (item.sK as string).replace('SEGMENT_DEF#', '');
      let desc: Partial<SegmentDef & { brandIds?: string[] }> = {};
      try { desc = JSON.parse(item.desc ?? '{}'); } catch { /* ignore */ }

      if (!desc.active) continue; // skip disabled defs

      // Derive brandId: global BeboCard segments use 'bebocard';
      // tenant segments use the first brandId from their tenant record (resolved at write time).
      const brandId = tenantId === 'bebocard' ? 'bebocard' : (desc.brandId ?? tenantId);

      defs.push({
        tenantId,
        brandId,
        segmentId,
        name: desc.name ?? segmentId,
        rules: (desc.rules as SegmentRule[]) ?? [],
        logicalOperator: desc.logicalOperator ?? 'AND',
        scope: desc.scope ?? 'brand',
        active: true,
      });
    }

    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  return defs;
}

/**
 * Get all active subscribers for a brand by querying the SUBSCRIPTION# GSI.
 * Returns an array of permULIDs.
 */
async function getSubscribers(brandId: string): Promise<string[]> {
  const permULIDs: string[] = [];
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      IndexName: 'sK-pK-index',
      KeyConditionExpression: 'sK = :sk',
      FilterExpression: '#st = :active',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: {
        ':sk': `SUBSCRIPTION#${brandId}`,
        ':active': 'ACTIVE',
      },
      ExclusiveStartKey: lastKey,
      ProjectionExpression: 'pK',
    }));

    for (const item of res.Items ?? []) {
      const permULID = (item.pK as string).replace('USER#', '');
      permULIDs.push(permULID);
    }

    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  return permULIDs;
}

/**
 * Query RECEIPT# items for a user within a date window.
 * scope='brand' → only receipts with brandId matching the def's brandId.
 * scope='global' → all receipts regardless of brand.
 */
async function getReceipts(
  permULID: string,
  since: string,
  scope: Scope,
  brandId: string,
): Promise<Array<{ amount: number; purchaseDate: string; brandId?: string }>> {
  const receipts: Array<{ amount: number; purchaseDate: string; brandId?: string }> = [];
  let lastKey: Record<string, unknown> | undefined;

  const filterParts: string[] = ['purchaseDate >= :since'];
  const exprValues: Record<string, unknown> = { ':sk_prefix': 'RECEIPT#', ':since': since };

  if (scope === 'brand') {
    filterParts.push('contains(desc, :brandMatch)');
    exprValues[':brandMatch'] = `"brandId":"${brandId}"`;
  }

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      KeyConditionExpression: 'pK = :pk AND begins_with(sK, :sk_prefix)',
      FilterExpression: filterParts.join(' AND '),
      ExpressionAttributeValues: {
        ':pk': `USER#${permULID}`,
        ...exprValues,
      },
      ProjectionExpression: 'desc, purchaseDate',
      ExclusiveStartKey: lastKey,
    }));

    for (const item of res.Items ?? []) {
      let desc: { amount?: number; purchaseDate?: string; brandId?: string } = {};
      try { desc = JSON.parse(item.desc ?? '{}'); } catch { /* ignore */ }
      const amount = typeof desc.amount === 'number' ? desc.amount : 0;
      const date = desc.purchaseDate ?? (item.purchaseDate as string) ?? '';
      if (date >= since) {
        receipts.push({ amount, purchaseDate: date, brandId: desc.brandId });
      }
    }

    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  return receipts;
}

/** Compute metrics from a list of receipt records. */
function computeMetrics(
  receipts: Array<{ amount: number; purchaseDate: string }>,
): ReceiptMetrics {
  if (receipts.length === 0) {
    return { visit_count: 0, total_spend: 0, avg_order_value: 0, days_since_last_visit: 9999 };
  }

  const total_spend = receipts.reduce((s, r) => s + r.amount, 0);
  const visit_count = receipts.length;
  const avg_order_value = total_spend / visit_count;

  const latestDate = receipts
    .map(r => new Date(r.purchaseDate).getTime())
    .filter(t => !isNaN(t))
    .reduce((a, b) => Math.max(a, b), 0);

  const days_since_last_visit = latestDate > 0
    ? Math.floor((Date.now() - latestDate) / (24 * 60 * 60 * 1000))
    : 9999;

  return { visit_count, total_spend, avg_order_value, days_since_last_visit };
}

/** Evaluate a single rule against computed metrics. */
function evaluateRule(rule: SegmentRule, metrics: ReceiptMetrics): boolean {
  const value = metrics[rule.metric];
  switch (rule.operator) {
    case 'gte': return value >= rule.value;
    case 'lte': return value <= rule.value;
    case 'eq':  return value === rule.value;
    case 'between':
      return rule.value2 != null && value >= rule.value && value <= rule.value2;
    default:
      return false;
  }
}

/** Evaluate all rules for a segment def. Returns true if the user is in the segment. */
async function evaluateUser(
  permULID: string,
  def: SegmentDef,
): Promise<boolean> {
  if (def.rules.length === 0) return false;

  // Find the longest period across all rules — query once for that window.
  const maxPeriod = def.rules.reduce<Period>((max, r) => {
    return PERIOD_DAYS[r.period] > PERIOD_DAYS[max] ? r.period : max;
  }, 'day');

  const since = sinceDate(maxPeriod);
  const allReceipts = await getReceipts(permULID, since, def.scope, def.brandId);

  // Evaluate each rule against receipts filtered to the rule's own period window.
  const results = def.rules.map(rule => {
    const ruleSince = sinceDate(rule.period);
    const filtered = allReceipts.filter(r => r.purchaseDate >= ruleSince);
    const metrics = computeMetrics(filtered);
    return evaluateRule(rule, metrics);
  });

  return def.logicalOperator === 'AND'
    ? results.every(Boolean)
    : results.some(Boolean);
}

/** Write SEGMENT#<brandId>#CUSTOM#<segmentId> membership record for a user. */
function buildMembershipItem(
  permULID: string,
  def: SegmentDef,
  status: 'ACTIVE' | 'INACTIVE',
  now: string,
): Record<string, unknown> {
  return {
    PutRequest: {
      Item: {
        pK: `USER#${permULID}`,
        sK: `SEGMENT#${def.brandId}#CUSTOM#${def.segmentId}`,
        status,
        segmentName: def.name,
        evaluatedAt: now,
        ttl: Math.floor(Date.now() / 1000) + 7 * 24 * 60 * 60, // auto-expire after 7 days if not re-evaluated
      },
    },
  };
}

/** Flush a batch of membership writes (max 25 per DynamoDB BatchWrite call). */
async function flushMembershipBatch(items: Record<string, unknown>[]): Promise<void> {
  if (items.length === 0) return;

  // Chunk into groups of 25 (DynamoDB BatchWrite limit)
  for (let i = 0; i < items.length; i += 25) {
    const chunk = items.slice(i, i + 25);
    await dynamo.send(new BatchWriteCommand({
      RequestItems: { [USER_TABLE]: chunk },
    }));
  }
}

/** Update the SEGMENT_DEF# record with memberCount + lastEvaluatedAt. */
async function updateSegmentDefStats(
  tenantId: string,
  segmentId: string,
  memberCount: number,
  evaluatedAt: string,
): Promise<void> {
  try {
    const descUpdate = JSON.stringify({ memberCount, lastEvaluatedAt: evaluatedAt });
    await dynamo.send(new UpdateCommand({
      TableName: REFDATA_TABLE,
      Key: { pK: `TENANT#${tenantId}`, sK: `SEGMENT_DEF#${segmentId}` },
      UpdateExpression: 'SET memberCount = :count, lastEvaluatedAt = :ts',
      ExpressionAttributeValues: {
        ':count': memberCount,
        ':ts': evaluatedAt,
      },
    }));
  } catch (e) {
    // Non-critical — stats update failure should not fail the main job
    console.error('[custom-segment-evaluator] Failed to update def stats', { tenantId, segmentId, error: e });
  }
}

// ─── Main handler ─────────────────────────────────────────────────────────────

export const handler = async (_event: unknown): Promise<void> => {
  const runAt = new Date().toISOString();
  console.info('[custom-segment-evaluator] Starting end-of-day evaluation', { runAt });

  let defs: SegmentDef[];
  try {
    defs = await getAllSegmentDefs();
  } catch (e) {
    console.error('[custom-segment-evaluator] Failed to load segment defs', e);
    throw e; // Let EventBridge retry
  }

  console.info('[custom-segment-evaluator] Loaded segment defs', { count: defs.length });

  let totalProcessed = 0;
  let totalErrors = 0;

  for (const def of defs) {
    console.info('[custom-segment-evaluator] Evaluating segment', {
      tenantId: def.tenantId,
      segmentId: def.segmentId,
      name: def.name,
      scope: def.scope,
    });

    let subscribers: string[];
    try {
      // For global (bebocard) segments, gather all users who have any subscription
      // rather than a bebocard-specific subscription.  We reuse the existing
      // per-brand subscriber pattern by iterating across all known brand IDs, but
      // to keep the first version simple and avoid unbounded scans we scope global
      // defs to subscribers of a special 'bebocard' brand entry if it exists, OR
      // fall back to iterating subscribers of the brand referenced in the def.
      subscribers = await getSubscribers(def.brandId);
    } catch (e) {
      console.error('[custom-segment-evaluator] Failed to get subscribers', { def: def.segmentId, error: e });
      totalErrors++;
      continue;
    }

    const membershipBatch: Record<string, unknown>[] = [];
    let memberCount = 0;

    for (const permULID of subscribers) {
      try {
        const inSegment = await evaluateUser(permULID, def);
        const status = inSegment ? 'ACTIVE' : 'INACTIVE';
        membershipBatch.push(buildMembershipItem(permULID, def, status, runAt));
        if (inSegment) memberCount++;
      } catch (e) {
        console.error('[custom-segment-evaluator] Error evaluating user', { permULID, segmentId: def.segmentId, error: e });
        totalErrors++;
      }

      // Flush every 200 users to keep memory usage bounded
      if (membershipBatch.length >= 200) {
        await flushMembershipBatch(membershipBatch.splice(0));
      }
    }

    // Flush remainder
    await flushMembershipBatch(membershipBatch);

    // Update def stats (non-critical)
    await updateSegmentDefStats(def.tenantId, def.segmentId, memberCount, runAt);

    totalProcessed += subscribers.length;
    console.info('[custom-segment-evaluator] Segment evaluation complete', {
      segmentId: def.segmentId,
      subscribers: subscribers.length,
      members: memberCount,
    });
  }

  console.info('[custom-segment-evaluator] Run complete', {
    totalDefs: defs.length,
    totalProcessed,
    totalErrors,
    runAt,
  });
};
