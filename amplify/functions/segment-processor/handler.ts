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
        const owner = image['owner']?.S ?? await getOwner(permULID);
        if (!owner) {
          console.warn('[segment-processor] Skipping recompute: owner not found', { permULID });
          continue;
        }
        await recomputeSegment(permULID, brandId, owner);
        await recomputeGlobalSegment(permULID, owner);
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

async function recomputeSegment(permULID: string, brandId: string, owner: string): Promise<void> {
  // 1. Query all receipts for this user + brand
  const receipts: Array<{ amount: number; purchaseDate: string }> = [];
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
      // Exclude ARCHIVED receipts (soft-deleted via card-manager/archiveRecord) so
      // removed receipts don't inflate totalSpend or visitCount.
      FilterExpression: 'subCategory = :brand AND (#status = :active OR #status = :claimed)',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: {
        ':pk': `USER#${permULID}`,
        ':prefix': 'RECEIPT#',
        ':brand': brandId,
        ':active': 'ACTIVE',
        ':claimed': 'CLAIMED',
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
      owner,
      desc: JSON.stringify(desc),
      updatedAt: new Date().toISOString(),
    },
  }));
}

// ── Global Segment recomputation ─────────────────────────────────────────────
// Aggregates across ALL brands to create a cross-brand persona

async function recomputeGlobalSegment(permULID: string, owner: string): Promise<void> {
  const receipts: Array<{ amount: number; purchaseDate: string; category?: string }> = [];
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
      FilterExpression: '#status = :active OR #status = :claimed',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: {
        ':pk': `USER#${permULID}`,
        ':prefix': 'RECEIPT#',
        ':active': 'ACTIVE',
        ':claimed': 'CLAIMED',
      },
      ExclusiveStartKey: lastKey,
    }));

    for (const item of res.Items ?? []) {
      const desc = parseDesc(item.desc);
      if (typeof desc.amount === 'number') {
        receipts.push({
          amount: desc.amount as number,
          purchaseDate: (desc.purchaseDate as string) ?? '',
          category: (desc.category as string) ?? 'other'
        });
      }
    }
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  if (receipts.length === 0) return;

  const totalSpend = receipts.reduce((sum, r) => sum + r.amount, 0);
  const visitCount = receipts.length;
  const lastVisit = receipts.map(r => r.purchaseDate).filter(Boolean).sort().pop() ?? '';
  const lastVisitDaysAgo = lastVisit
    ? Math.floor((Date.now() - new Date(lastVisit).getTime()) / 86_400_000)
    : 9999;

  // 1. Base Aggregations
  const catTotals: Record<string, number> = {};
  const brandTotals: Record<string, number> = {};
  for (const r of receipts) {
    const cat = r.category?.toLowerCase() ?? 'other';
    catTotals[cat] = (catTotals[cat] ?? 0) + r.amount;
  }
  
  for (const r of receipts as any) {
    const bId = r.brandId ?? 'unknown';
    brandTotals[bId] = (brandTotals[bId] ?? 0) + r.amount;
  }

  // 2. Nuanced Heuristics (P3-Advanced)
  const catIntensity: Record<string, number> = {};
  const catConfidence: Record<string, number> = {};

  for (const cat of Object.keys(catTotals)) {
    const total = catTotals[cat];
    const percentage = total / totalSpend;
    // Intensity = how much higher is this cat than a typical 10% baseline
    catIntensity[cat] = percentage / 0.1;
    catConfidence[cat] = Math.min(1.0, (total / 100) * percentage);
  }

  const scores: Array<{ id: string; score: number }> = [];

  // Persona: HIGH_VALUE (Magnitude signal)
  if (totalSpend > 1000) scores.push({ id: 'high_value', score: Math.min(1.0, totalSpend / 3000) });
  
  // Persona: POWER_USER (Frequency signal)
  if (visitCount > 30) scores.push({ id: 'power_user', score: Math.min(1.0, visitCount / 60) });

  // Persona: GROCERY_FOCUSED
  const groceryScore = (catTotals['groceries'] ?? 0) / totalSpend;
  if (groceryScore > 0.35) scores.push({ id: 'grocery_focused', score: Math.min(1.0, groceryScore * 2) });

  // Persona: TECH_ENTHUSIAST
  const techScore = (catTotals['electronics'] ?? 0 + (catTotals['tech'] ?? 0)) / totalSpend;
  if (techScore > 0.2) scores.push({ id: 'tech_enthusiast', score: Math.min(1.0, techScore * 3) });

  // Persona: DINING_ENTHUSIAST
  const diningScore = (catTotals['dining'] ?? 0) / totalSpend;
  if (diningScore > 0.25) scores.push({ id: 'dining_enthusiast', score: Math.min(1.0, diningScore * 2.5) });

  // Persona: TRAVELER
  const travelScore = (catTotals['travel'] ?? 0) / totalSpend;
  if (travelScore > 0.1) scores.push({ id: 'traveler', score: Math.min(1.0, travelScore * 2) });

  // Persona: BRAND_LOYALIST (Concentration signal)
  const maxBrandSpend = Math.max(...Object.values(brandTotals));
  const brandConcentration = maxBrandSpend / totalSpend;
  if (brandConcentration > 0.6 && visitCount > 5) {
    scores.push({ id: 'brand_loyalist', score: Math.min(1.0, brandConcentration) });
  }

  // Persona: DEAL_HUNTER
  const atv = totalSpend / visitCount; 
  if (atv < 25 && visitCount > 10) scores.push({ id: 'deal_hunter', score: 0.8 });

  // Sort by score (confidence)
  scores.sort((a, b) => b.score - a.score);
  const personas = scores.map(s => s.id);
  const personaMap = scores.reduce((m, s) => ({ ...m, [s.id]: Math.round(s.score * 100) }), {});

  const desc = {
    spendBucket: toSpendBucket(totalSpend),
    visitFrequency: toVisitFrequency(visitCount, lastVisitDaysAgo),
    totalSpend: Math.round(totalSpend * 100) / 100,
    visitCount,
    lastVisit,
    persona: personas.length > 0 ? personas : ['undetermined'],
    updatedAt: new Date().toISOString(),
  };

  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: 'SEGMENT#global',
      eventType: 'SEGMENT',
      status: 'ACTIVE',
      primaryCat: 'segment',
      subCategory: 'global',
      owner,
      persona: personas.length > 0 ? personas : ['undetermined'], // Top-level for GSI targeting
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

async function getOwner(permULID: string): Promise<string | null> {
  try {
    const res = await dynamo.send(new GetCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
    }));
    return res.Item?.owner as string ?? null;
  } catch (err) {
    console.error('[segment-processor] Failed to fetch owner', { permULID, err });
    return null;
  }
}
