import type { DynamoDBStreamHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const USER_TABLE = process.env.USER_TABLE!;

// ── Handler ───────────────────────────────────────────────────────────────────
//
// Triggered by DynamoDB Streams on UserDataEvent.
//
// Two trigger paths:
//
//  1. RECEIPT# write   — full recompute of SEGMENT#<brandId> (spend totals, frequency, subscribed)
//
//  2. SUBSCRIPTION# change — patches just the `subscribed` field on the existing SEGMENT#<brandId>
//     record immediately, so the tenant-analytics consent gate stays accurate without waiting
//     for the next receipt. Uses a non-destructive UpdateExpression so all other segment fields
//     remain unchanged. If no segment record exists yet (user subscribed before any receipts),
//     the update is a no-op — a full recompute will set subscribed correctly on the first receipt.

export const handler: DynamoDBStreamHandler = async (event) => {
  for (const record of event.Records) {
    const eventName = record.eventName;
    if (eventName !== 'INSERT' && eventName !== 'MODIFY' && eventName !== 'REMOVE') continue;

    const image = record.dynamodb?.NewImage ?? record.dynamodb?.OldImage;
    if (!image) continue;

    const pk = image['pK']?.S ?? '';
    const sk = image['sK']?.S ?? '';

    if (!pk.startsWith('USER#')) continue;

    const permULID = pk.replace('USER#', '');

    // ── Path 1: RECEIPT# write — full segment recompute ───────────────────────
    if (sk.startsWith('RECEIPT#') && eventName !== 'REMOVE') {
      const brandId = image['subCategory']?.S;
      if (!brandId) continue;

      try {
        await recomputeSegment(permULID, brandId);
      } catch (err) {
        console.error('[segment-processor] recompute failed', { permULID, brandId, err });
        // Do not rethrow — one failed record must not block the rest of the batch
      }
      continue;
    }

    // ── Path 2: SUBSCRIPTION# change — patch subscribed flag ──────────────────
    if (sk.startsWith('SUBSCRIPTION#')) {
      // sK pattern: SUBSCRIPTION#<brandId>
      const brandId = sk.replace('SUBSCRIPTION#', '');
      if (!brandId) continue;

      // On REMOVE the subscription is gone — subscribed=false.
      // On INSERT/MODIFY check whether the record is ACTIVE.
      const newSubscribed = eventName !== 'REMOVE' &&
        (record.dynamodb?.NewImage?.['status']?.S === 'ACTIVE');

      try {
        await patchSegmentSubscribed(permULID, brandId, newSubscribed);
      } catch (err) {
        console.error('[segment-processor] subscription patch failed', { permULID, brandId, err });
      }
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
      TableName: USER_TABLE,
      KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
      // Exclude ARCHIVED receipts (soft-deleted via card-manager/archiveRecord) so
      // removed receipts don't inflate totalSpend or visitCount.
      FilterExpression: 'subCategory = :brand AND #status = :active',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: {
        ':pk': `USER#${permULID}`,
        ':prefix': 'RECEIPT#',
        ':brand': brandId,
        ':active': 'ACTIVE',
      },
      ExclusiveStartKey: lastKey,
    }));

    for (const item of res.Items ?? []) {
      const desc = parseDesc(item.desc);
      if (typeof desc.amount === 'number') {
        receipts.push({ amount: desc.amount as number, purchaseDate: (desc.purchaseDate as string) ?? '' });
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
  const lastVisit = sortedDates[sortedDates.length - 1] ?? '';

  const lastVisitDaysAgo = lastVisit
    ? Math.floor((Date.now() - new Date(lastVisit).getTime()) / 86_400_000)
    : 9999;

  const desc = {
    spendBucket: toSpendBucket(totalSpend),
    visitFrequency: toVisitFrequency(visitCount, lastVisitDaysAgo),
    totalSpend: Math.round(totalSpend * 100) / 100,
    visitCount,
    lastVisit,
    persona: [] as string[],   // Phase 3: ML classification
    computedAt: new Date().toISOString(),
    subscribed,
  };

  // 4. Upsert SEGMENT#<brandId> record
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: `SEGMENT#${brandId}`,
      eventType: 'SEGMENT',
      status: 'ACTIVE',
      primaryCat: 'segment',
      subCategory: brandId,
      desc: JSON.stringify(desc),
      updatedAt: new Date().toISOString(),
    },
  }));
}

// ── Subscription consent patch ────────────────────────────────────────────────
//
// Updates only the `subscribed` field inside the existing segment record's desc JSON.
// Uses a conditional UpdateExpression so the write is a no-op if SEGMENT# doesn't exist yet.

async function patchSegmentSubscribed(permULID: string, brandId: string, subscribed: boolean): Promise<void> {
  // Fetch the current segment record to merge the subscribed change into desc
  const existing = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `SEGMENT#${brandId}` },
  }));

  if (!existing.Item) {
    // No segment yet — no-op. recomputeSegment will set subscribed correctly on first receipt.
    console.info('[segment-processor] no segment to patch for subscription change', { permULID, brandId });
    return;
  }

  const currentDesc = parseDesc(existing.Item.desc);
  const updatedDesc = { ...currentDesc, subscribed, subscribedUpdatedAt: new Date().toISOString() };

  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `SEGMENT#${brandId}` },
    ConditionExpression: 'attribute_exists(sK)',
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
    ExpressionAttributeValues: {
      ':desc': JSON.stringify(updatedDesc),
      ':now': new Date().toISOString(),
    },
  }));

  console.info('[segment-processor] subscription patched on segment', { permULID, brandId, subscribed });
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
  if (count < 3) return 'new';
  if (count >= 12 && lastVisitDaysAgo <= 90) return 'frequent';
  return 'occasional';
}
