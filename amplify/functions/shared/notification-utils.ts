import { DynamoDBDocumentClient, GetCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';

/**
 * Returns the UTC week identifier (e.g. 2026-W14)
 */
export function getNotificationWindowKey(date = new Date()): string {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((d.getTime() - yearStart.getTime()) / 86400000) + 1) / 7);
  return `${d.getUTCFullYear()}-W${weekNo}`;
}

/**
 * Checks if a user has reached the notification cap for a specific brand in the current week.
 * Intelligence/Enterprise brands only.
 */
export async function isNotificationCapReached(
  dynamo: DynamoDBDocumentClient,
  userTable: string,
  permULID: string,
  brandId: string,
  limit: number
): Promise<boolean> {
  const window = getNotificationWindowKey();
  const res = await dynamo.send(new GetCommand({
    TableName: userTable,
    Key: { pK: `USER#${permULID}`, sK: `NOTIFCAP#${brandId}#${window}` }
  }));
  
  const count = Number(res.Item?.count ?? 0);
  return count >= limit;
}

/**
 * Increments the weekly notification counter for a user/brand pair.
 * Includes an 8-day TTL for automatic cleanup.
 */
export async function incrementNotificationCounter(
  dynamo: DynamoDBDocumentClient,
  userTable: string,
  permULID: string,
  brandId: string
): Promise<void> {
  const window = getNotificationWindowKey();
  const ttl = Math.floor(Date.now() / 1000) + 8 * 24 * 3600; // 8 days
  
  await dynamo.send(new UpdateCommand({
    TableName: userTable,
    Key: { pK: `USER#${permULID}`, sK: `NOTIFCAP#${brandId}#${window}` },
    UpdateExpression: 'SET eventType = :et, #status = :active, ttl = :ttl ADD #count :inc',
    ExpressionAttributeNames: { '#count': 'count', '#status': 'status' },
    ExpressionAttributeValues: {
      ':et': 'NOTIF_CAP',
      ':active': 'ACTIVE',
      ':ttl': ttl,
      ':inc': 1
    }
  }));
}

/**
 * Performs relevance scoring based on user segments and campaign type.
 * Returns a priority label: 'PRIORITY' | 'NORMAL' | 'DEPRIORITIZE' | 'SKIP'
 */
export function getRelevancePriority(
  segment: { spendBucket?: string; visitFrequency?: string } | null,
  campaignType: 'untargeted' | 'acquisition' | 'loyalty_reward'
): 'PRIORITY' | 'NORMAL' | 'DEPRIORITIZE' | 'SKIP' {
  const frequency = segment?.visitFrequency ?? 'new';

  // Rules from P2-13:
  // 1. frequent + loyalty reward -> always deliver (PRIORITY)
  if (frequency === 'frequent' && campaignType === 'loyalty_reward') return 'PRIORITY';

  // 2. new + acquisition offer -> prioritise (PRIORITY)
  if (frequency === 'new' && campaignType === 'acquisition') return 'PRIORITY';

  // 3. lapsed + untargeted offer -> deprioritise (DEPRIORITIZE)
  if (frequency === 'lapsed' && campaignType === 'untargeted') return 'DEPRIORITIZE';

  // 4. Default
  return 'NORMAL';
}
