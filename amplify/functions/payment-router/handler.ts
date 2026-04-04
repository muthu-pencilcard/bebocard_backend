/**
 * payment-router — Federated Payment Routing (Patent Claims 19–22)
 *
 * Handles two event sources:
 *   1. API Gateway REST — brand-initiated checkout requests
 *   2. SQS — 90-second timeout checks for unacknowledged checkouts
 *
 * Routes:
 *   POST /checkout                   Brand initiates a payment request
 *   GET  /checkout/{orderId}/status  Brand polls for user response
 *
 * AppSync mutation (handled via card-manager):
 *   respondToCheckout(orderId, approved, paymentToken)
 *
 * Checkout lifecycle:
 *   PENDING → APPROVED | DECLINED | TIMEOUT
 *
 * Checkout records stored in AdminDataEvent:
 *   pK: CHECKOUT#<orderId>
 *   sK: <permULID>
 *   desc: { amount, currency, merchantName, brandId, brandWebhookUrl,
 *           status, paymentToken, expiresAt }
 */

import type { APIGatewayProxyHandler, SQSEvent } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';
import { monotonicFactory } from 'ulid';
import { validateApiKey, extractApiKey } from '../../shared/api-key-auth';
import { withAuditLog } from '../../shared/audit-logger';
import { getTenantStateForBrand, incrementTenantUsageCounter } from '../../shared/tenant-billing';
import https from 'https';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const sqs    = new SQSClient({});
const ulid   = monotonicFactory();

const ADMIN_TABLE        = process.env.ADMIN_TABLE!;
const USER_TABLE         = process.env.USER_TABLE!;
const REF_TABLE          = process.env.REF_TABLE!;
const TIMEOUT_QUEUE_URL  = process.env.TIMEOUT_QUEUE_URL!;
const CHECKOUT_TTL_SECS  = 90;

const CORS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
};

// ── Firebase ──────────────────────────────────────────────────────────────────

function getFirebase() {
  if (getApps().length === 0) {
    const sa = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    if (!sa) throw new Error('FIREBASE_SERVICE_ACCOUNT_JSON not set');
    initializeApp({ credential: cert(JSON.parse(sa)) });
  }
  return getMessaging();
}

// ── Entry point ───────────────────────────────────────────────────────────────
// Handles both API Gateway and SQS (timeout) events.

export const handler = async (event: unknown) => {
  const e = event as Record<string, unknown>;
  // SQS timeout events
  if (Array.isArray(e['Records']) && (e['Records'] as Record<string, unknown>[])[0]?.['eventSource'] === 'aws:sqs') {
    return handleTimeoutBatch(event as SQSEvent);
  }
  // API Gateway REST events — wrap with audit log
  const apiHandler = withAuditLog(dynamo, _restHandler);
  return apiHandler(event as Parameters<APIGatewayProxyHandler>[0], {} as never, {} as never);
};

// ── REST handler ──────────────────────────────────────────────────────────────

const _restHandler: APIGatewayProxyHandler = async (event) => {
  const path   = event.path ?? '';
  const method = event.httpMethod;

  try {
    if (method === 'POST' && path.endsWith('/checkout'))
      return handleCheckout(event);

    const statusMatch = path.match(/\/checkout\/([^/]+)\/status$/);
    if (method === 'GET' && statusMatch)
      return handleCheckoutStatus(statusMatch[1]);

    return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Not found' }) };
  } catch (e) {
    console.error('[payment-router]', e);
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Internal error' }) };
  }
};

// ── POST /checkout ─────────────────────────────────────────────────────────────
// Called by brand POS backend at checkout.
// Resolves secondaryULID → permULID, sends FCM push, enqueues 90s timeout.

interface CheckoutRequest {
  secondaryULID: string;
  amount: number;
  currency: string;
  merchantName: string;
  orderId: string;
}

async function handleCheckout(event: Parameters<APIGatewayProxyHandler>[0]) {
  const rawKey  = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'payment') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or missing API key' }) };

  const body: Partial<CheckoutRequest> = JSON.parse(event.body ?? '{}');
  const { secondaryULID, amount, currency, merchantName, orderId } = body;

  if (!secondaryULID || !amount || !currency || !merchantName || !orderId) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing required fields: secondaryULID, amount, currency, merchantName, orderId' }) };
  }

  const tenantState = await getTenantStateForBrand(dynamo, REF_TABLE, validKey.brandId);
  if (!tenantState.active) {
    return { statusCode: 403, headers: CORS, body: JSON.stringify({ error: 'Tenant billing is suspended' }) };
  }
  if (tenantState.tier !== 'intelligence') {
    return { statusCode: 403, headers: CORS, body: JSON.stringify({ error: 'Payments require the intelligence tier' }) };
  }

  // Resolve secondaryULID → permULID
  const permULID = await resolvePermULID(secondaryULID);
  if (!permULID) {
    return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'User not found' }) };
  }

  // Check for duplicate orderId
  const existing = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `CHECKOUT#${orderId}`, sK: permULID },
  }));
  if (existing.Item) {
    const existingDesc = JSON.parse(existing.Item.desc ?? '{}');
    return { statusCode: 409, headers: CORS, body: JSON.stringify({ orderId, status: existingDesc.status }) };
  }

  // Fetch brand webhook URL from RefDataEvent brand profile
  const brandRef = await dynamo.send(new GetCommand({
    TableName: REF_TABLE,
    Key: { pK: `BRAND#${validKey.brandId}`, sK: 'profile' },
  }));
  const brandDesc = JSON.parse(brandRef.Item?.desc ?? '{}');
  const brandWebhookUrl = brandDesc.paymentWebhookUrl as string | undefined;

  // Store checkout record
  const now       = new Date();
  const expiresAt = new Date(now.getTime() + CHECKOUT_TTL_SECS * 1000).toISOString();
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `CHECKOUT#${orderId}`,
      sK: permULID,
      eventType: 'CHECKOUT',
      status: 'PENDING',
      desc: JSON.stringify({ amount, currency, merchantName, brandId: validKey.brandId, brandWebhookUrl, expiresAt }),
      createdAt: now.toISOString(),
      updatedAt: now.toISOString(),
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  await incrementTenantUsageCounter(dynamo, REF_TABLE, tenantState.tenantId, validKey.brandId, 'payments');

  // Get user device token
  const token = await getDeviceToken(permULID);
  if (token) {
    try {
      await getFirebase().send({
        token,
        data: {
          type: 'CHECKOUT',
          orderId,
          amount: String(amount),
          currency,
          merchantName,
          expiresAt,
        },
        notification: {
          title: `Payment request — ${merchantName}`,
          body: `${currency} ${amount} — approve to complete your purchase`,
        },
        android: { priority: 'high' },
        apns: { payload: { aps: { contentAvailable: true, sound: 'default' } } },
      });
    } catch (e) {
      console.error('[payment-router] FCM push failed', e);
      // Non-fatal — brand will still get a response (TIMEOUT) via SQS
    }
  }

  // Enqueue 90s timeout check
  if (TIMEOUT_QUEUE_URL) {
    await sqs.send(new SendMessageCommand({
      QueueUrl: TIMEOUT_QUEUE_URL,
      MessageBody: JSON.stringify({ orderId, permULID }),
      DelaySeconds: CHECKOUT_TTL_SECS,
    }));
  }

  return {
    statusCode: 202,
    headers: CORS,
    body: JSON.stringify({ orderId, status: 'PENDING', expiresAt }),
  };
}

// ── GET /checkout/{orderId}/status ─────────────────────────────────────────────

async function handleCheckoutStatus(orderId: string) {
  // Scan for the checkout record (pK known, sK is permULID — query by pK)
  const res = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `CHECKOUT#${orderId}` },
    Limit: 1,
  }));
  const item = res.Items?.[0];
  if (!item) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Checkout not found' }) };

  const desc = JSON.parse(item.desc ?? '{}');
  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({ orderId, status: item.status, expiresAt: desc.expiresAt }),
  };
}

// ── SQS timeout handler ────────────────────────────────────────────────────────
// Fires 90s after checkout creation. If still PENDING, marks TIMEOUT and notifies brand.

async function handleTimeoutBatch(event: SQSEvent) {
  for (const record of event.Records) {
    try {
      const { orderId, permULID } = JSON.parse(record.body) as { orderId: string; permULID: string };
      await processTimeout(orderId, permULID);
    } catch (e) {
      console.error('[payment-router] timeout processing error', e);
    }
  }
}

async function processTimeout(orderId: string, permULID: string) {
  const res = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `CHECKOUT#${orderId}`, sK: permULID },
  }));
  if (!res.Item || res.Item.status !== 'PENDING') return; // Already resolved

  const desc = JSON.parse(res.Item.desc ?? '{}');

  // Update to TIMEOUT
  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `CHECKOUT#${orderId}`, sK: permULID },
    UpdateExpression: 'SET #s = :s, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':s': 'TIMEOUT', ':now': new Date().toISOString(), ':pending': 'PENDING' },
    ConditionExpression: '#s = :pending',
  }));

  // Notify brand webhook
  if (desc.brandWebhookUrl) {
    await postWebhook(desc.brandWebhookUrl, { orderId, status: 'TIMEOUT' });
  }

  console.info(`[payment-router] Checkout ${orderId} timed out`);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function resolvePermULID(secondaryULID: string): Promise<string | null> {
  const res = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `SCAN#${secondaryULID}` },
    Limit: 1,
  }));
  const item = res.Items?.[0];
  return item ? (item.sK as string) : null;
}

async function getDeviceToken(permULID: string): Promise<string | null> {
  const res = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'DEVICE_TOKEN' },
  }));
  if (!res.Item) return null;
  return (JSON.parse(res.Item.desc ?? '{}') as { token?: string }).token ?? null;
}

function postWebhook(url: string, payload: unknown): Promise<void> {
  return new Promise((resolve) => {
    const body = JSON.stringify(payload);
    try {
      const req = https.request(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
      }, (res) => {
        res.resume();
        res.on('end', resolve);
      });
      req.on('error', (e) => {
        console.error('[payment-router] webhook delivery failed', url, e.message);
        resolve();
      });
      req.setTimeout(5000, () => { req.destroy(); resolve(); });
      req.write(body);
      req.end();
    } catch (e) {
      console.error('[payment-router] webhook error', e);
      resolve();
    }
  });
}
