import type { DynamoDBStreamHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const USER_TABLE = process.env.USER_TABLE!;

// ── Handler ───────────────────────────────────────────────────────────────────
//
// Triggered by DynamoDB Streams on UserDataEvent.
// On every RECEIPT# write, recomputes SEGMENT#<brandId> for the affected user.
//
// The computed record sets `subscribed: boolean` by checking whether
// SUBSCRIPTION#<brandId> exists for the user at write time — so
// tenant-analytics queries can filter on this field without a per-user join.

export const handler: DynamoDBStreamHandler = async (event) => {
  for (const record of event.Records) {
    if (record.eventName !== 'INSERT' && record.eventName !== 'MODIFY') continue;

    const newImage = record.dynamodb?.NewImage;
    if (!newImage) continue;

    const pk = newImage['pK']?.S ?? '';
    const sk = newImage['sK']?.S ?? '';

    if (!pk.startsWith('USER#') || !sk.startsWith('RECEIPT#')) continue;

    const permULID  = pk.replace('USER#', '');
    const brandId   = newImage['subCategory']?.S;

    if (!brandId) continue;   // receipt not associated with a brand — skip

    try {
      await recomputeSegment(permULID, brandId);
    } catch (err) {
      console.error('[segment-processor] recompute failed', { permULID, brandId, err });
      // Do not rethrow — one failed record should not block the rest of the batch
    }
  }
};

// ── Segment recomputation ─────────────────────────────────────────────────────

async function recomputeSegment(permULID: string, brandId: string): Promise<void> {
  // 1. Query all receipts for this user + brand
  const receipts: Array<{ amount: number; purchaseDate: string }> = [];
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName:                 USER_TABLE,
      KeyConditionExpression:    'pK = :pk AND begins_with(sK, :prefix)',
      // Exclude ARCHIVED receipts (soft-deleted via card-manager/archiveRecord) so
      // removed receipts don't inflate totalSpend or visitCount.
      FilterExpression:          'subCategory = :brand AND #status = :active',
      ExpressionAttributeNames:  { '#status': 'status' },
      ExpressionAttributeValues: {
        ':pk':     `USER#${permULID}`,
        ':prefix': 'RECEIPT#',
        ':brand':  brandId,
        ':active': 'ACTIVE',
      },
      ExclusiveStartKey: lastKey,
    }));

    for (const item of res.Items ?? []) {
      const desc = parseDesc(item.desc);
      if (typeof desc.amount === 'number') {
        receipts.push({ amount: desc.amount, purchaseDate: desc.purchaseDate ?? '' });
      }
    }
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  if (receipts.length === 0) return;

  // 2. Check consent — does SUBSCRIPTION#<brandId> exist for this user?
  const subRes = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `SUBSCRIPTION#${brandId}` },
  }));
  const subscribed = !!subRes.Item;

  // 3. Compute stats
  const totalSpend = receipts.reduce((sum, r) => sum + r.amount, 0);
  const visitCount = receipts.length;
  const sortedDates = receipts.map(r => r.purchaseDate).filter(Boolean).sort();
  const lastVisit   = sortedDates[sortedDates.length - 1] ?? '';

  const lastVisitDaysAgo = lastVisit
    ? Math.floor((Date.now() - new Date(lastVisit).getTime()) / 86_400_000)
    : 9999;

  const desc = {
    spendBucket:    toSpendBucket(totalSpend),
    visitFrequency: toVisitFrequency(visitCount, lastVisitDaysAgo),
    totalSpend:     Math.round(totalSpend * 100) / 100,
    visitCount,
    lastVisit,
    persona:        [] as string[],   // Phase 3: ML classification
    computedAt:     new Date().toISOString(),
    subscribed,
  };

  // 4. Upsert SEGMENT#<brandId> record
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK:          `USER#${permULID}`,
      sK:          `SEGMENT#${brandId}`,
      eventType:   'SEGMENT',
      status:      'ACTIVE',
      primaryCat:  'segment',
      subCategory: brandId,
      desc:        JSON.stringify(desc),
      updatedAt:   new Date().toISOString(),
    },
  }));
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function parseDesc(raw: unknown): Record<string, unknown> {
  if (typeof raw === 'string') { try { return JSON.parse(raw); } catch { return {}; } }
  if (raw && typeof raw === 'object') return raw as Record<string, unknown>;
  return {};
}

function toSpendBucket(total: number): '<100' | '100-200' | '200-500' | '500+' {
  if (total < 100) return '<100';
  if (total < 200) return '100-200';
  if (total < 500) return '200-500';
  return '500+';
}

function toVisitFrequency(
  count: number,
  lastVisitDaysAgo: number,
): 'new' | 'occasional' | 'frequent' | 'lapsed' {
  if (lastVisitDaysAgo > 180) return 'lapsed';
  if (count < 3)              return 'new';
  if (count >= 12 && lastVisitDaysAgo <= 90) return 'frequent';
  return 'occasional';
}
