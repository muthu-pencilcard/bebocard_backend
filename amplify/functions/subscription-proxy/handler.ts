/**
 * subscription-proxy — Subscription Revocation Proxy (Patent Claims 27–29)
 *
 * Allows brands to register recurring charges against a BeboCard user, and
 * gives users the ability to cancel those recurring charges through BeboCard.
 * BeboCard is the revocation proxy — it relays the cancellation to the brand's
 * webhook. The actual payment relationship is between the user and the brand.
 *
 * Routes:
 *   POST   /recurring/register          Brand registers a recurring charge
 *   DELETE /recurring/{subId}           Brand cancels a subscription on their side
 *   GET    /recurring/{subId}/status    Brand polls subscription status
 *
 * AppSync mutation (via card-manager):
 *   cancelRecurring(subId)              User cancels from within the app
 *
 * Subscription records stored in UserDataEvent:
 *   pK: USER#<permULID>
 *   sK: RECURRING#<brandId>#<subId>
 *   desc: { subId, brandId, brandName, productName, amount, currency,
 *           frequency, nextBillingDate, webhookUrl, status }
 *
 * Lifecycle: ACTIVE → CANCELLED_BY_USER | CANCELLED_BY_BRAND | CANCELLED_BY_TIMEOUT
 */

import type { APIGatewayProxyHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';
import { validateApiKey, extractApiKey } from '../../shared/api-key-auth';
import { withAuditLog } from '../../shared/audit-logger';
import https from 'https';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid = monotonicFactory();

const ADMIN_TABLE = process.env.ADMIN_TABLE!;
const USER_TABLE = process.env.USER_TABLE!;
const REF_TABLE = process.env.REF_TABLE!;

const VALID_FREQUENCIES = ['daily', 'weekly', 'fortnightly', 'monthly', 'quarterly', 'annually'] as const;

const CORS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
};

// ── Entry point ───────────────────────────────────────────────────────────────

export const handler = async (event: unknown) => {
  const apiHandler = withAuditLog(dynamo, _restHandler);
  return apiHandler(event as Parameters<APIGatewayProxyHandler>[0], {} as never, {} as never);
};

// ── REST handler ──────────────────────────────────────────────────────────────

const _restHandler: APIGatewayProxyHandler = async (event) => {
  const path = event.path ?? '';
  const method = event.httpMethod;

  try {
    if (method === 'POST' && path.endsWith('/recurring/register'))
      return handleRegister(event);

    const subMatch = path.match(/\/recurring\/([^/]+)$/);
    if (method === 'DELETE' && subMatch)
      return handleBrandCancel(event, subMatch[1]);

    const statusMatch = path.match(/\/recurring\/([^/]+)\/status$/);
    if (method === 'GET' && statusMatch)
      return handleStatus(event, statusMatch[1]);

    const amountChangeMatch = path.match(/\/recurring\/([^/]+)\/amount-change$/);
    if (method === 'POST' && amountChangeMatch)
      return handleAmountChange(event, amountChangeMatch[1]);

    return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Not found' }) };
  } catch (e) {
    console.error('[subscription-proxy]', e);
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Internal error' }) };
  }
};

// ── POST /recurring/register ──────────────────────────────────────────────────
// Called by brand backend when a user initiates a recurring payment.

interface RegisterBody {
  secondaryULID: string;
  productName: string;
  amount: number;
  currency: string;
  frequency: string;
  nextBillingDate: string;  // ISO 8601 date
  category?: string;
}

async function handleRegister(event: Parameters<APIGatewayProxyHandler>[0]) {
  const rawKey = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'recurring') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or missing API key' }) };

  const body: Partial<RegisterBody> = JSON.parse(event.body ?? '{}');
  const { secondaryULID, productName, amount, currency, frequency, nextBillingDate, category } = body;

  if (!secondaryULID || !productName || !amount || !currency || !frequency || !nextBillingDate) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing required fields: secondaryULID, productName, amount, currency, frequency, nextBillingDate' }) };
  }

  if (!VALID_FREQUENCIES.includes(frequency as typeof VALID_FREQUENCIES[number])) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: `Invalid frequency. Allowed: ${VALID_FREQUENCIES.join(', ')}` }) };
  }

  const permULID = await resolvePermULID(secondaryULID);
  if (!permULID) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'User not found' }) };

  // Fetch brand name + webhook from RefDataEvent
  const brandRef = await dynamo.send(new GetCommand({
    TableName: REF_TABLE,
    Key: { pK: `BRAND#${validKey.brandId}`, sK: 'PROFILE' },
  }));
  const brandDesc = JSON.parse(brandRef.Item?.desc ?? '{}');
  const brandName = brandDesc.brandName as string ?? validKey.brandId;
  const webhookUrl = brandDesc.recurringWebhookUrl as string | undefined;

  const subId = ulid();
  const now = new Date().toISOString();

  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: `RECURRING#${validKey.brandId}#${subId}`,
      eventType: 'RECURRING',
      status: 'ACTIVE',
      primaryCat: 'recurring',
      subCategory: validKey.brandId,
      desc: JSON.stringify({
        subId,
        brandId: validKey.brandId,
        brandName,
        productName,
        amount,
        currency,
        frequency,
        nextBillingDate,
        category: category ?? 'Other',
        webhookUrl,
        status: 'ACTIVE',
        registeredAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  // FCM push to notify user
  const token = await getDeviceToken(permULID);
  if (token) {
    try {
      const { initializeApp, getApps, cert } = await import('firebase-admin/app');
      const { getMessaging } = await import('firebase-admin/messaging');
      if (getApps().length === 0) {
        const sa = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
        if (sa) initializeApp({ credential: cert(JSON.parse(sa)) });
      }
      await getMessaging().send({
        token,
        notification: {
          title: `Recurring charge registered`,
          body: `${brandName} — ${currency} ${amount} ${frequency}`,
        },
        android: { priority: 'normal' },
        apns: { payload: { aps: { contentAvailable: true } } },
      });
    } catch (e) {
      console.error('[subscription-proxy] FCM push failed', e);
    }
  }

  return {
    statusCode: 201,
    headers: CORS,
    body: JSON.stringify({ subId, status: 'ACTIVE' }),
  };
}

// ── DELETE /recurring/{subId} — brand-initiated cancellation ──────────────────

async function handleBrandCancel(
  event: Parameters<APIGatewayProxyHandler>[0],
  subId: string,
) {
  const rawKey = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'recurring') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Unauthorized' }) };

  // Look up subscription in AdminDataEvent index (subId → permULID)
  const idxRes = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `RECURRING_IDX#${subId}` },
    Limit: 1,
  }));
  const idx = idxRes.Items?.[0];
  if (!idx) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Subscription not found' }) };

  const permULID = idx.sK as string;

  await cancelSubscription(permULID, validKey.brandId, subId, 'CANCELLED_BY_BRAND');

  return { statusCode: 200, headers: CORS, body: JSON.stringify({ subId, status: 'CANCELLED_BY_BRAND' }) };
}

async function handleAmountChange(
  event: Parameters<APIGatewayProxyHandler>[0],
  subId: string,
) {
  const rawKey = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'recurring') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Unauthorized' }) };

  const body = JSON.parse(event.body ?? '{}');
  const { newAmount, effectiveDate, reason } = body;
  if (newAmount === undefined || !effectiveDate) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing required fields: newAmount, effectiveDate' }) };
  }

  // Look up config idx
  const idxRes = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `RECURRING_IDX#${subId}` },
    Limit: 1,
  }));
  const idx = idxRes.Items?.[0];
  if (!idx) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Subscription not found' }) };

  const permULID = idx.sK as string;
  const userKey = { pK: `USER#${permULID}`, sK: `RECURRING#${validKey.brandId}#${subId}` };

  const itemResponse = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: userKey,
  }));
  if (!itemResponse.Item || itemResponse.Item.status !== 'ACTIVE') {
    return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Subscription not found or not active' }) };
  }

  const desc = JSON.parse(itemResponse.Item.desc ?? '{}');
  const oldAmount = desc.amount;
  const currency = desc.currency ?? 'AUD';
  const brandName = desc.brandName ?? validKey.brandId;

  if (oldAmount === newAmount) {
    return { statusCode: 200, headers: CORS, body: JSON.stringify({ subId, status: 'NO_CHANGE' }) };
  }

  const now = new Date().toISOString();

  // Audit record
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `BILLING_CHANGE#${subId}#${now}`,
      sK: permULID,
      eventType: 'BILLING_CHANGE',
      status: 'LOGGED',
      brandId: validKey.brandId,
      subId,
      oldAmount,
      newAmount,
      currency,
      effectiveDate,
      reason,
      createdAt: now,
      updatedAt: now,
    },
  }));

  // Update desc on USER_TABLE record
  desc.amount = newAmount;
  desc.priceChange = {
    oldAmount,
    newAmount,
    effectiveDate,
    reason,
    changedAt: now,
  };

  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: userKey,
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
    ExpressionAttributeValues: {
      ':desc': JSON.stringify(desc),
      ':now': now,
    },
  }));

  // FCM push
  const token = await getDeviceToken(permULID);
  if (token) {
    const changeWord = newAmount > oldAmount ? 'increased' : 'decreased';
    try {
      const { initializeApp, getApps, cert } = await import('firebase-admin/app');
      const { getMessaging } = await import('firebase-admin/messaging');
      if (getApps().length === 0) {
        const sa = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
        if (sa) initializeApp({ credential: cert(JSON.parse(sa)) });
      }
      await getMessaging().send({
        token,
        data: {
          type: 'SUBSCRIPTION_AMOUNT_CHANGE',
          subId,
          brandId: validKey.brandId,
        },
        notification: {
          title: `Subscription ${changeWord}`,
          body: `Your ${brandName} subscription ${changeWord} from $${oldAmount} to $${newAmount}`,
        },
        android: { priority: 'normal' },
        apns: { payload: { aps: { contentAvailable: true } } },
      });
    } catch (e) {
      console.error('[subscription-proxy] FCM push failed', e);
    }
  }

  return { statusCode: 200, headers: CORS, body: JSON.stringify({ subId, status: 'AMOUNT_CHANGED', oldAmount, newAmount }) };
}

// ── GET /recurring/{subId}/status ─────────────────────────────────────────────

async function handleStatus(
  event: Parameters<APIGatewayProxyHandler>[0],
  subId: string,
) {
  const rawKey = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'recurring') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Unauthorized' }) };

  const idxRes = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `RECURRING_IDX#${subId}` },
    Limit: 1,
  }));
  const idx = idxRes.Items?.[0];
  if (!idx) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Subscription not found' }) };

  const permULID = idx.sK as string;
  const item = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `RECURRING#${validKey.brandId}#${subId}` },
  }));
  if (!item.Item) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Subscription not found' }) };

  const desc = JSON.parse(item.Item.desc ?? '{}');
  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({ subId, status: item.Item.status, productName: desc.productName }),
  };
}

// ── Shared cancel helper ──────────────────────────────────────────────────────

export async function cancelSubscription(
  permULID: string,
  brandId: string,
  subId: string,
  reason: 'CANCELLED_BY_USER' | 'CANCELLED_BY_BRAND',
) {
  const key = { pK: `USER#${permULID}`, sK: `RECURRING#${brandId}#${subId}` };

  const res = await dynamo.send(new GetCommand({ TableName: USER_TABLE, Key: key }));
  if (!res.Item || res.Item.status !== 'ACTIVE') {
    throw new Error(`Subscription not active: ${res.Item?.status ?? 'not found'}`);
  }

  const desc = JSON.parse(res.Item.desc ?? '{}');
  const now = new Date().toISOString();

  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: key,
    UpdateExpression: 'SET #s = :s, desc = :desc, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: {
      ':s': reason,
      ':desc': JSON.stringify({ ...desc, status: reason, cancelledAt: now }),
      ':now': now,
      ':active': 'ACTIVE',
    },
    ConditionExpression: '#s = :active',
  }));

  // Notify brand webhook
  if (desc.webhookUrl) {
    await postWebhook(desc.webhookUrl, {
      subId,
      status: reason,
      cancelledAt: now,
    });
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function resolvePermULID(secondaryULID: string): Promise<string | null> {
  const res = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `SCAN#${secondaryULID}` },
    Limit: 1,
  }));
  return res.Items?.[0]?.sK ?? null;
}

async function getDeviceToken(permULID: string): Promise<string | null> {
  const res = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'DEVICE_TOKEN' },
  }));
  return (JSON.parse(res.Item?.desc ?? '{}') as { token?: string }).token ?? null;
}

function postWebhook(url: string, payload: unknown): Promise<void> {
  return new Promise((resolve) => {
    const body = JSON.stringify(payload);
    try {
      const req = https.request(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
      }, (res) => { res.resume(); res.on('end', resolve); });
      req.on('error', (e) => { console.error('[subscription-proxy] webhook error', e.message); resolve(); });
      req.setTimeout(5000, () => { req.destroy(); resolve(); });
      req.write(body);
      req.end();
    } catch (e) { console.error('[subscription-proxy] webhook error', e); resolve(); }
  });
}
