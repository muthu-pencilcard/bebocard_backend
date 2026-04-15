import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const USER_TABLE = process.env.USER_TABLE!;

/**
 * aggregated-spending-processor handler
 * 
 * Generates personal "Spend vs. Save" reports.
 * Unlike brand analytics, this is USER-CENTRIC and PRIVATE.
 * It stays within the USER# namespace.
 * 
 * Records generated:
 * - INSIGHT#SPEND#GLOBAL
 * - INSIGHT#SAVINGS#POTENTIAL
 */
export const handler = async (event: any) => {
  console.info('[spending-processor] Starting aggregation');

  const subscribers = await getActiveUsers();
  
  for (const { permULID, owner } of subscribers) {
    try {
      await generateInsights(permULID, owner);
    } catch (err) {
      console.error(`[spending-processor] Failed for USER#${permULID}:`, err);
    }
  }

  console.info('[spending-processor] Complete');
};

async function getActiveUsers(): Promise<Array<{ permULID: string; owner: string }>> {
  const result: Array<{ permULID: string; owner: string }> = [];
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      IndexName: 'sK-pK-index',
      KeyConditionExpression: 'sK = :sk',
      ExpressionAttributeValues: { ':sk': 'IDENTITY' },
      ExclusiveStartKey: lastKey,
      Limit: 1000,
    }));

    for (const item of res.Items ?? []) {
      const pK = item.pK as string;
      const owner = item.owner as string;
      if (pK.startsWith('USER#') && owner) {
        result.push({ permULID: pK.replace('USER#', ''), owner });
      }
    }
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  return result;
}

async function generateInsights(permULID: string, owner: string) {
  // 1. Fetch all receipts for the last 90 days
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 90);

  const receiptRes = await dynamo.send(new QueryCommand({
    TableName: USER_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    FilterExpression: 'primaryCat = :cat AND createdAt >= :cutoff',
    ExpressionAttributeValues: {
      ':pk': `USER#${permULID}`,
      ':prefix': 'RECEIPT#',
      ':cat': 'receipt',
      ':cutoff': cutoff.toISOString(),
    },
  }));

  const receipts = receiptRes.Items ?? [];
  if (receipts.length === 0) return;

  // 2. Aggregate spending by category
  const categories: Record<string, { total: number; count: number }> = {};
  let totalSpend = 0;

  for (const item of receipts) {
    const desc = parseJSON(item.desc);
    const amount = Number(desc.amount ?? 0);
    const cat = desc.category ?? 'other';

    if (!categories[cat]) categories[cat] = { total: 0, count: 0 };
    categories[cat].total += amount;
    categories[cat].count += 1;
    totalSpend += amount;
  }

  // 3. Compute Category Ranking and Velocity
  const sortedCats = Object.entries(categories)
    .sort((a, b) => b[1].total - a[1].total)
    .map(([name, data]) => ({
      category: name,
      ...data,
      pct: totalSpend > 0 ? (data.total / totalSpend) : 0,
    }));

  // 4. Save INSIGHT#SPEND#GLOBAL
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: 'INSIGHT#SPEND#GLOBAL',
      eventType: 'SPENDING_INSIGHT',
      status: 'ACTIVE',
      owner,
      desc: JSON.stringify({
        period: 'LAST_90_DAYS',
        totalSpend,
        receiptCount: receipts.length,
        categoryBreakdown: sortedCats.slice(0, 5), // Top 5
        generatedAt: new Date().toISOString(),
      }),
      updatedAt: new Date().toISOString(),
    }
  }));

  console.info(`[spending-processor] Spend insights saved for USER#${permULID}`);

  // 5. Generate Reward Velocity (Next voucher forecasts)
  await generateRewardVelocity(permULID, owner, receipts);
}

async function generateRewardVelocity(permULID: string, owner: string, receipts: any[]) {
  // Brand thresholds: 2000 pts = $10 typically for Everyday Rewards / Flybuys
  const THRESHOLD = 2000;
  const VOUCHER_VALUE = 10;

  // Simple velocity: how much was earned in last 30 days
  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

  const brandPoints: Record<string, { balance: number; monthlyEarn: number }> = {};

  // Fetch current loyalty cards for initial balances
  const cardRes = await dynamo.send(new QueryCommand({
    TableName: USER_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    FilterExpression: 'primaryCat = :cat AND status = :active',
    ExpressionAttributeValues: {
      ':pk': `USER#${permULID}`,
      ':prefix': 'CARD#',
      ':cat': 'loyalty_card',
      ':active': 'ACTIVE',
    },
  }));

  for (const item of cardRes.Items ?? []) {
    const desc = parseJSON(item.desc);
    const brandId = desc.brandId;
    if (!brandId) continue;
    brandPoints[brandId] = { balance: Number(desc.pointsBalance ?? 0), monthlyEarn: 0 };
  }

  // Calculate earn rate from receipts
  for (const item of receipts) {
    if (new Date(item.createdAt) < thirtyDaysAgo) continue;
    const desc = parseJSON(item.desc);
    const brandId = desc.brandId;
    const points = Number(desc.pointsEarned ?? 0);
    if (brandId && brandPoints[brandId]) {
      brandPoints[brandId].monthlyEarn += points;
    }
  }

  // Generate FORECAST#REWARD#VELOCITY records
  for (const [brandId, data] of Object.entries(brandPoints)) {
    const remaining = THRESHOLD - (data.balance % THRESHOLD);
    if (remaining > THRESHOLD || remaining <= 0) continue; 

    const estDays = data.monthlyEarn > 0 ? (remaining / (data.monthlyEarn / 30)) : 999;

    await dynamo.send(new PutCommand({
      TableName: USER_TABLE,
      Item: {
        pK: `USER#${permULID}`,
        sK: `FORECAST#REWARD#${brandId}`,
        eventType: 'REWARD_VELOCITY',
        status: 'ACTIVE',
        owner,
        subCategory: brandId,
        desc: JSON.stringify({
          brandId,
          nextThreshold: THRESHOLD,
          pointsRemaining: remaining,
          voucherValue: VOUCHER_VALUE,
          estimatedDays: estDays > 90 ? null : Math.round(estDays),
          generatedAt: new Date().toISOString(),
        }),
        updatedAt: new Date().toISOString(),
      }
    }));
  }
}

function parseJSON(val: any) {
  if (typeof val === 'object') return val;
  try { return JSON.parse(val ?? '{}'); } catch { return {}; }
}
