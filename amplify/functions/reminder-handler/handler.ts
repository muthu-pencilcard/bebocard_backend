/**
 * reminder-handler — Scheduled Lambda (EventBridge cron)
 *
 * Fires daily at 23:00 UTC (= 9:00 AEST / 10:00 AEDT).
 * Scans UserDataEvent for upcoming due dates and sends FCM reminders.
 * Writes sent-logs to AdminDataEvent to prevent duplicate pushes.
 *
 * EventBridge rule (wired in backend.ts):
 *   cron(0 23 * * ? *)
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  ScanCommand,
  QueryCommand,
  GetCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';
import { monotonicFactory } from 'ulid';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid = monotonicFactory();

const USER_TABLE = process.env.USER_TABLE!;
const ADMIN_TABLE = process.env.ADMIN_TABLE!;

function getFirebase() {
  if (getApps().length === 0) {
    const sa = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    if (!sa) throw new Error('FIREBASE_SERVICE_ACCOUNT_JSON not set');
    initializeApp({ credential: cert(JSON.parse(sa)) });
  }
  return getMessaging();
}

// ─── Reminder rule definitions ─────────────────────────────────────────────────

interface ReminderRule {
  type: string;
  prefKey: string;          // key in PREFERENCES.reminders
  skPrefix: string;         // sK prefix to find records
  descField: string;        // field inside desc JSON that holds the date
  daysAhead: number[];      // fire reminders N days before the date
  title: (days: number, record: Record<string, unknown>) => string;
  body: (days: number, record: Record<string, unknown>) => string;
  statusCheck?: (item: Record<string, unknown>) => boolean;
}

const REMINDER_RULES: ReminderRule[] = [
  {
    type: 'invoiceDue',
    prefKey: 'invoiceDue',
    skPrefix: 'INVOICE#',
    descField: 'dueDate',
    daysAhead: [3, 1],
    title: (d, r) => `Invoice due ${d === 1 ? 'tomorrow' : `in ${d} days`}`,
    body: (_, r) => `${r.supplier ?? 'Supplier'} — ${r.currency ?? 'AUD'} ${r.amount}`,
  },
  {
    type: 'warrantyExpiry',
    prefKey: 'warrantyExpiry',
    skPrefix: 'RECEIPT#',
    descField: 'warrantyExpiry',
    daysAhead: [30, 7],
    title: (d, r) => `Warranty expires ${d === 7 ? 'in 1 week' : 'in 30 days'}`,
    body: (_, r) => `${r.merchant ?? 'Your item'} — consider extending cover`,
  },
  {
    type: 'giftCardExpiry',
    prefKey: 'giftCardExpiry',
    skPrefix: 'GIFTCARD#',
    descField: 'expiryDate',
    daysAhead: [14, 3],
    title: (d, r) => `Gift card expires ${d === 3 ? 'in 3 days' : 'in 2 weeks'}`,
    body: (_, r) => `${r.brandName ?? 'Gift card'} — ${r.currency ?? 'AUD'} ${r.balance} remaining`,
  },
  {
    type: 'offerExpiry',
    prefKey: 'offerExpiry',
    skPrefix: 'OFFER#',
    descField: 'validTo',
    daysAhead: [2],
    title: (_d, r) => `Offer expiring soon`,
    body: (_d, r) => `${r.brandName ?? 'Brand'} — ${r.title ?? 'offer'} ends in 2 days`,
  },
  {
    type: 'pointsExpiry',
    prefKey: 'pointsExpiry',
    skPrefix: 'CARD#',
    descField: 'pointsExpiryDate',
    daysAhead: [60],
    title: (_d, _r) => `Points expiring in 60 days`,
    body: (_d, r) => `${r.brandName ?? 'Your loyalty points'} — redeem before they expire`,
  },
  {
    type: 'subscriptionRenewal',
    prefKey: 'subscriptionRenewal',
    skPrefix: 'RECURRING#',
    descField: 'nextBillingDate',
    daysAhead: [3, 1],
    title: (d, r) => `${r.brandName ?? 'Subscription'} renews ${d === 1 ? 'tomorrow' : `in ${d} days`}`,
    body: (_, r) => `${r.brandName ?? 'Your subscription'} will charge ${r.currency ?? 'AUD'} ${r.amount}`,
    statusCheck: (item) => item.status === 'ACTIVE',
  },
];

// ─── Main handler ──────────────────────────────────────────────────────────────

export const handler = async () => {
  console.log('[reminder-handler] Starting daily reminder scan');
  let total = 0;

  for (const rule of REMINDER_RULES) {
    for (const daysAhead of rule.daysAhead) {
      const count = await processRule(rule, daysAhead);
      total += count;
    }
  }

  console.log(`[reminder-handler] Sent ${total} reminders`);
  return { sent: total };
};

// ─── Per-rule processor ────────────────────────────────────────────────────────

async function processRule(rule: ReminderRule, daysAhead: number): Promise<number> {
  const targetDate = new Date();
  targetDate.setDate(targetDate.getDate() + daysAhead);
  // YYYY-MM-DD matches how we write to top-level expiryDate
  const targetDateStr = targetDate.toISOString().slice(0, 10);

  let count = 0;
  let lastKey: Record<string, unknown> | undefined;

  do {
    // 🔥 Optimized: Uses the userDataByExpiry GSI instead of table scan
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      IndexName: 'userDataByExpiry',
      KeyConditionExpression: 'expiryDate = :target',
      // Still filter by skPrefix and eventType/status if needed, 
      // but GSI makes the candidate set very small.
      FilterExpression: 'begins_with(sK, :prefix)',
      ExpressionAttributeValues: { 
        ':target': targetDateStr,
        ':prefix': rule.skPrefix 
      },
      ExclusiveStartKey: lastKey,
      Limit: 100,
    }));
    lastKey = res.LastEvaluatedKey as typeof lastKey;

    for (const item of res.Items ?? []) {
      if (rule.statusCheck && !rule.statusCheck(item)) continue;
      
      const desc = JSON.parse(item.desc ?? '{}') as Record<string, unknown>;
      // Extract permULID from pK: USER#<permULID>
      const permULID = (item.pK as string).replace('USER#', '');

      // Check user preferences
      if (!(await userWantsReminder(permULID, rule.prefKey))) continue;

      // Dedup check — have we already sent this reminder?
      const dedupKey = `REMINDER#${rule.type}#${daysAhead}d`;
      if (await alreadySent(permULID, item.sK as string, dedupKey)) continue;

      // Get device token
      const token = await getDeviceToken(permULID);
      if (!token) continue;

      try {
        await markSent(permULID, item.sK as string, dedupKey);
      } catch (e) {
        console.error(`[reminder-handler] markSent failed for ${permULID}, skipping FCM:`, e);
        continue;
      }

      try {
        await getFirebase().send({
          token,
          notification: {
            title: rule.title(daysAhead, desc),
            body: rule.body(daysAhead, desc),
          },
          data: {
            type: `REMINDER_${rule.type.toUpperCase()}`,
            recordSK: item.sK as string,
            daysAhead: String(daysAhead),
          },
        });
        count++;
      } catch (e) {
        console.error(`[reminder-handler] FCM failed for ${permULID}:`, e);
      }
    }
  } while (lastKey);

  return count;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function userWantsReminder(permULID: string, prefKey: string): Promise<boolean> {
  const res = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'PREFERENCES' },
  }));
  if (!res.Item) return true;  // Default: reminders on
  const prefs = JSON.parse(res.Item.desc ?? '{}');
  return prefs.reminders?.[prefKey] !== false;
}

async function alreadySent(permULID: string, recordSK: string, dedupKey: string): Promise<boolean> {
  const res = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: {
      pK: `REMINDER#${permULID}`,
      sK: `SENT#${recordSK}#${dedupKey}`,
    },
  }));
  return !!res.Item;
}

async function markSent(permULID: string, recordSK: string, dedupKey: string): Promise<void> {
  const now = new Date().toISOString();
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `REMINDER#${permULID}`,
      sK: `SENT#${recordSK}#${dedupKey}`,
      eventType: 'REMINDER_SENT',
      status: 'SENT',
      createdAt: now,
      updatedAt: now,
    },
  }));
}

async function getDeviceToken(permULID: string): Promise<string | null> {
  const res = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'DEVICE_TOKEN' },
  }));
  if (!res.Item) return null;
  return JSON.parse(res.Item.desc ?? '{}').token ?? null;
}
