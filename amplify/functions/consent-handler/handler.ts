/**
 * consent-handler — Consent-Gated Identity Release (Patent Claims 25–26)
 *
 * Handles two event sources:
 *   1. API Gateway REST — brand-initiated consent requests
 *   2. SQS — 60-second timeout for unanswered consent prompts
 *
 * Routes:
 *   POST /consent-request               Brand requests identity fields from a user
 *   GET  /consent-request/{requestId}/status   Brand polls for user response
 *
 * AppSync mutation (handled via card-manager):
 *   respondToConsent(requestId, approvedFields)
 *
 * Consent lifecycle:
 *   PENDING → APPROVED (partial or full) | DENIED | TIMEOUT
 *
 * Consent records stored in AdminDataEvent:
 *   pK: CONSENT#<requestId>
 *   sK: <permULID>
 *   desc: { requestedFields, approvedFields, purpose, brandId, brandWebhookUrl, expiresAt }
 *
 * UserDataEvent consent log (written on every outcome):
 *   pK: USER#<permULID>
 *   sK: CONSENT_LOG#<requestId>
 *   desc: { brandId, brandName, requestedFields, approvedFields, status, resolvedAt }
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
import https from 'https';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const sqs    = new SQSClient({});
const ulid   = monotonicFactory();

const ADMIN_TABLE              = process.env.ADMIN_TABLE!;
const USER_TABLE               = process.env.USER_TABLE!;
const REF_TABLE                = process.env.REF_TABLE!;
const CONSENT_TIMEOUT_QUEUE_URL = process.env.CONSENT_TIMEOUT_QUEUE_URL!;
const CONSENT_TTL_SECS         = 60;

// Fields a brand is permitted to request. Extend as Phase 6 expands.
const ALLOWED_FIELDS = ['email', 'phone', 'firstName', 'lastName', 'address', 'dateOfBirth'] as const;
type ConsentField = typeof ALLOWED_FIELDS[number];

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

export const handler = async (event: unknown) => {
  const e = event as Record<string, unknown>;
  if (Array.isArray(e['Records']) && (e['Records'] as Record<string, unknown>[])[0]?.['eventSource'] === 'aws:sqs') {
    return handleTimeoutBatch(event as SQSEvent);
  }
  const apiHandler = withAuditLog(dynamo, _restHandler);
  return apiHandler(event as Parameters<APIGatewayProxyHandler>[0], {} as never, {} as never);
};

// ── REST handler ──────────────────────────────────────────────────────────────

const _restHandler: APIGatewayProxyHandler = async (event) => {
  const path   = event.path ?? '';
  const method = event.httpMethod;

  try {
    if (method === 'POST' && path.endsWith('/consent-request'))
      return handleConsentRequest(event);

    const statusMatch = path.match(/\/consent-request\/([^/]+)\/status$/);
    if (method === 'GET' && statusMatch)
      return handleConsentStatus(statusMatch[1]);

    return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Not found' }) };
  } catch (e) {
    console.error('[consent-handler]', e);
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Internal error' }) };
  }
};

// ── POST /consent-request ─────────────────────────────────────────────────────
// Called by brand POS backend. Validates fields, sends FCM prompt, enqueues timeout.

interface ConsentRequestBody {
  secondaryULID: string;
  requestedFields: ConsentField[];
  purpose: string;
}

async function handleConsentRequest(event: Parameters<APIGatewayProxyHandler>[0]) {
  const rawKey  = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'consent') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or missing API key' }) };

  const body: Partial<ConsentRequestBody> = JSON.parse(event.body ?? '{}');
  const { secondaryULID, requestedFields, purpose } = body;

  if (!secondaryULID || !requestedFields?.length || !purpose) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing required fields: secondaryULID, requestedFields, purpose' }) };
  }

  // Validate requested fields against allow-list
  const invalid = requestedFields.filter(f => !ALLOWED_FIELDS.includes(f));
  if (invalid.length) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: `Invalid fields: ${invalid.join(', ')}. Allowed: ${ALLOWED_FIELDS.join(', ')}` }) };
  }

  const permULID = await resolvePermULID(secondaryULID);
  if (!permULID) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'User not found' }) };

  // Fetch brand name + webhook from RefDataEvent
  const brandRef = await dynamo.send(new GetCommand({
    TableName: REF_TABLE,
    Key: { pK: `BRAND#${validKey.brandId}`, sK: 'PROFILE' },
  }));
  const brandDesc = JSON.parse(brandRef.Item?.desc ?? '{}');
  const brandName        = brandDesc.brandName as string | undefined ?? validKey.brandId;
  const brandWebhookUrl  = brandDesc.consentWebhookUrl as string | undefined;

  const requestId = ulid();
  const now       = new Date();
  const expiresAt = new Date(now.getTime() + CONSENT_TTL_SECS * 1000).toISOString();

  // Store consent request record
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `CONSENT#${requestId}`,
      sK: permULID,
      eventType: 'CONSENT_REQUEST',
      status: 'PENDING',
      desc: JSON.stringify({ requestedFields, purpose, brandId: validKey.brandId, brandName, brandWebhookUrl, expiresAt }),
      createdAt: now.toISOString(),
      updatedAt: now.toISOString(),
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  // FCM push to user device
  const token = await getDeviceToken(permULID);
  if (token) {
    try {
      await getFirebase().send({
        token,
        data: {
          type: 'CONSENT_REQUEST',
          requestId,
          brandName,
          requestedFields: JSON.stringify(requestedFields),
          purpose,
          expiresAt,
        },
        notification: {
          title: `${brandName} is requesting your details`,
          body: `Tap to review what they're asking for`,
        },
        android: { priority: 'high' },
        apns: { payload: { aps: { contentAvailable: true, sound: 'default' } } },
      });
    } catch (e) {
      console.error('[consent-handler] FCM push failed', e);
    }
  }

  // Enqueue 60s timeout check
  if (CONSENT_TIMEOUT_QUEUE_URL) {
    await sqs.send(new SendMessageCommand({
      QueueUrl: CONSENT_TIMEOUT_QUEUE_URL,
      MessageBody: JSON.stringify({ requestId, permULID }),
      DelaySeconds: CONSENT_TTL_SECS,
    }));
  }

  return {
    statusCode: 202,
    headers: CORS,
    body: JSON.stringify({ requestId, status: 'PENDING', expiresAt }),
  };
}

// ── GET /consent-request/{requestId}/status ───────────────────────────────────

async function handleConsentStatus(requestId: string) {
  const res = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `CONSENT#${requestId}` },
    Limit: 1,
  }));
  const item = res.Items?.[0];
  if (!item) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Consent request not found' }) };

  const desc = JSON.parse(item.desc ?? '{}');
  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({
      requestId,
      status: item.status,
      approvedFields: desc.approvedFields ?? null,
      expiresAt: desc.expiresAt,
    }),
  };
}

// ── SQS timeout handler ────────────────────────────────────────────────────────

async function handleTimeoutBatch(event: SQSEvent) {
  for (const record of event.Records) {
    try {
      const { requestId, permULID } = JSON.parse(record.body) as { requestId: string; permULID: string };
      await processTimeout(requestId, permULID);
    } catch (e) {
      console.error('[consent-handler] timeout error', e);
    }
  }
}

async function processTimeout(requestId: string, permULID: string) {
  const res = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `CONSENT#${requestId}`, sK: permULID },
  }));
  if (!res.Item || res.Item.status !== 'PENDING') return;

  const desc = JSON.parse(res.Item.desc ?? '{}');

  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `CONSENT#${requestId}`, sK: permULID },
    UpdateExpression: 'SET #s = :s, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':s': 'TIMEOUT', ':now': new Date().toISOString(), ':pending': 'PENDING' },
    ConditionExpression: '#s = :pending',
  }));

  if (desc.brandWebhookUrl) {
    await postWebhook(desc.brandWebhookUrl, { requestId, status: 'TIMEOUT' });
  }

  console.info(`[consent-handler] Consent request ${requestId} timed out`);
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
      req.on('error', (e) => { console.error('[consent-handler] webhook error', e.message); resolve(); });
      req.setTimeout(5000, () => { req.destroy(); resolve(); });
      req.write(body);
      req.end();
    } catch (e) { console.error('[consent-handler] webhook error', e); resolve(); }
  });
}
