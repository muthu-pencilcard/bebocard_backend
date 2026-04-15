import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
  GetCommand,
} from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;

/**
 * points-expiry-forecast handler
 *
 * Runs nightly to scan luxury points-bearing accounts and predict expiry dates.
 * Points expiration is the "Silent Churn" of loyalty; BeboCard alerts users
 * 30 days before they lose value.
 *
 * Strategy:
 * 1. Identify users with significant points balances.
 * 2. Fetch brand-specific expiry rules (e.g., Qantas = 18mo inactivity).
 * 3. Calculate "Velocity of Expiry" based on last activity date.
 * 4. Write FORECAST# records for user push notifications.
 */
export const handler = async (event: any) => {
  console.info('[points-expiry-forecast] Starting scan');

  // For this first version, we focus on manual receipts and linked identity records
  // that have provided a lastActivityDate or balance update.
  const subscribers = await getActiveUsers();
  console.info(`[points-expiry-forecast] Evaluating ${subscribers.length} users`);

  for (const { permULID, owner } of subscribers) {
    try {
      await evaluateUserPoints(permULID, owner);
    } catch (err) {
      console.error(`[points-expiry-forecast] Failed for USER#${permULID}:`, err);
    }
  }

  console.info('[points-expiry-forecast] Complete');
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

async function evaluateUserPoints(permULID: string, owner: string) {
  // Query all cards/accounts for this user
  const cardsRes = await dynamo.send(new QueryCommand({
    TableName: USER_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: {
      ':pk': `USER#${permULID}`,
      ':prefix': 'CARD#',
    },
  }));

  for (const card of cardsRes.Items ?? []) {
    const brandId = card.subCategory as string;
    if (!brandId) continue;

    const desc = parseJSON(card.desc);
    const balance = Number(desc.pointsBalance ?? 0);
    if (balance <= 0) continue;

    // Fetch expiry rules for this brand
    const rules = await getBrandExpiryRules(brandId);
    if (!rules) continue;

    const lastActivity = desc.lastPointsActivityDate ?? card.updatedAt ?? card.createdAt;
    if (!lastActivity) continue;

    const lastDate = new Date(lastActivity);
    const expiryDate = new Date(lastDate.getTime() + rules.expiryMonths * 30 * 24 * 60 * 60 * 1000);
    
    const daysRemaining = Math.floor((expiryDate.getTime() - Date.now()) / (24 * 60 * 60 * 1000));

    // If points expire in < 45 days, create/update a forecast record
    if (daysRemaining <= 45) {
      await dynamo.send(new PutCommand({
        TableName: USER_TABLE,
        Item: {
          pK: `USER#${permULID}`,
          sK: `FORECAST#EXPIRY#${brandId}`,
          eventType: 'POINTS_EXPIRY_FORECAST',
          status: 'ACTIVE',
          owner,
          subCategory: brandId,
          desc: JSON.stringify({
            points: balance,
            expiryDate: expiryDate.toISOString(),
            daysRemaining,
            confidence: 'high',
            rulesApplied: rules.ruleName,
            updatedAt: new Date().toISOString(),
          }),
          updatedAt: new Date().toISOString(),
        }
      }));
      console.info(`[points-expiry-forecast] Alert generated for USER#${permULID} / ${brandId}`);
    }
  }
}

async function getBrandExpiryRules(brandId: string) {
  // Mock brand rules for Phase 3 prototype
  // In production, these will live in RefDataEvent under BRAND#<id>#RULES
  const MOCK_RULES: Record<string, { expiryMonths: number; ruleName: string }> = {
    'qantas': { expiryMonths: 18, ruleName: '18_MONTH_INACTIVITY' },
    'velocity': { expiryMonths: 24, ruleName: '24_MONTH_INACTIVITY' },
    'flybuys': { expiryMonths: 12, ruleName: '12_MONTH_INACTIVITY' },
    'everyday-rewards': { expiryMonths: 12, ruleName: '12_MONTH_INACTIVITY' },
  };

  return MOCK_RULES[brandId.toLowerCase()];
}

function parseJSON(val: any) {
  if (typeof val === 'object') return val;
  try { return JSON.parse(val ?? '{}'); } catch { return {}; }
}
