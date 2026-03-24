import type { APIGatewayProxyHandler, APIGatewayProxyEvent } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  PutCommand, GetCommand, UpdateCommand, QueryCommand, ScanCommand,
} from '@aws-sdk/lib-dynamodb';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';
import { monotonicFactory } from 'ulid';
import { withAuditLog } from '../../shared/audit-logger';
import {
  validateApiKey, createApiKey, rotateApiKey, revokeApiKey, extractApiKey,
  type ApiKeyScope,
} from '../../shared/api-key-auth';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid = monotonicFactory();

const USER_TABLE    = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const ADMIN_TABLE   = process.env.ADMIN_TABLE!;

const CORS_HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': 'https://business.bebocard.com.au',
  'Access-Control-Allow-Headers': 'Content-Type,X-Api-Key,Authorization',
};

function ok(body: unknown) {
  return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify(body) };
}
function err(status: number, message: string) {
  return { statusCode: status, headers: CORS_HEADERS, body: JSON.stringify({ error: message }) };
}

// ─── Auth guard ────────────────────────────────────────────────────────────────

async function authGuard(
  event: APIGatewayProxyEvent,
  scope: ApiKeyScope,
): Promise<{ brandId: string } | null> {
  const rawKey = extractApiKey(event.headers as Record<string, string>);
  if (!rawKey) return null;
  const validated = await validateApiKey(dynamo, rawKey, scope);
  if (!validated) return null;
  return { brandId: validated.brandId };
}

// ─── Router ────────────────────────────────────────────────────────────────────

const _handler: APIGatewayProxyHandler = async (event) => {
  const method = event.httpMethod;
  const path   = event.path ?? '';

  // ── Receipt status ACK ──────────────────────────────────────────────────────
  if (method === 'GET' && /\/receipt\/[^/]+\/status/.test(path)) {
    const auth = await authGuard(event, 'receipt');
    if (!auth) return err(401, 'Unauthorized');
    return handleReceiptStatus(event, auth.brandId);
  }

  // ── Offers ──────────────────────────────────────────────────────────────────
  if (path.includes('/offers')) {
    const auth = await authGuard(event, 'offers');
    if (!auth) return err(401, 'Unauthorized');
    if (method === 'GET')    return listOffers(event, auth.brandId);
    if (method === 'POST')   return createOffer(event, auth.brandId);
    if (method === 'PUT')    return updateOffer(event, auth.brandId);
    if (method === 'DELETE') return archiveOffer(event, auth.brandId);
  }

  // ── Newsletters ─────────────────────────────────────────────────────────────
  if (path.includes('/newsletters')) {
    const auth = await authGuard(event, 'newsletters');
    if (!auth) return err(401, 'Unauthorized');
    if (method === 'POST') return sendNewsletter(event, auth.brandId);
    if (method === 'GET')  return listNewsletters(event, auth.brandId);
  }

  // ── Analytics ───────────────────────────────────────────────────────────────
  if (path.includes('/analytics')) {
    const auth = await authGuard(event, 'analytics');
    if (!auth) return err(401, 'Unauthorized');
    return getAnalytics(event, auth.brandId);
  }

  // ── Stores ──────────────────────────────────────────────────────────────────
  if (path.includes('/stores')) {
    const auth = await authGuard(event, 'stores');
    if (!auth) return err(401, 'Unauthorized');
    if (method === 'POST')   return upsertStore(event, auth.brandId);
    if (method === 'GET')    return listStores(auth.brandId);
    if (method === 'DELETE') return archiveStore(event, auth.brandId);
  }

  // ── API key self-management (brand rotates own key) ─────────────────────────
  if (path.includes('/api-keys/rotate')) {
    const auth = await authGuard(event, 'scan'); // any scope allows rotation
    if (!auth) return err(401, 'Unauthorized');
    return handleRotateKey(event, auth.brandId);
  }

  return err(404, 'Unknown route');
};

export const handler = withAuditLog(dynamo, _handler);

// ─── Receipt ACK ───────────────────────────────────────────────────────────────

async function handleReceiptStatus(event: APIGatewayProxyEvent, brandId: string) {
  // Path: /receipt/<receiptSK>/status  (URL-encoded)
  const rawSK = event.pathParameters?.receiptSK ?? decodeURIComponent(
    event.path.replace(/.*\/receipt\//, '').replace(/\/status.*/, ''),
  );

  // Fetch the receipt and verify it belongs to this brand
  const res = await dynamo.send(new ScanCommand({
    TableName: USER_TABLE,
    FilterExpression: 'sK = :sk',
    ExpressionAttributeValues: { ':sk': rawSK },
    Limit: 1,
  }));

  const item = res.Items?.[0];
  if (!item) return err(404, 'Receipt not found');

  const desc = JSON.parse(item.desc ?? '{}');
  if (desc.brandId && desc.brandId !== brandId) return err(403, 'Forbidden');

  return ok({ saved: true, receiptSK: rawSK, savedAt: item.createdAt, fcmStatus: desc.fcmStatus ?? 'sent' });
}

// ─── Offers ────────────────────────────────────────────────────────────────────

interface OfferInput {
  title: string;
  description: string;
  imageUrl?: string;
  validFrom: string;
  validTo: string;
  targetStoreIds?: string[];
  minVisitCount?: number;
  category?: string;
}

async function createOffer(event: APIGatewayProxyEvent, brandId: string) {
  const body: OfferInput = JSON.parse(event.body ?? '{}');
  if (!body.title || !body.validFrom || !body.validTo) return err(400, 'Missing required fields');

  const offerId = ulid();
  const now = new Date().toISOString();

  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK: `BRAND#${brandId}`,
      sK: `OFFER#${offerId}`,
      eventType: 'OFFER',
      status: 'ACTIVE',
      primaryCat: 'offer',
      desc: JSON.stringify({ ...body, brandId, offerId }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  // Fan-out FCM push to all subscribers with offers: true
  await fanOutToSubscribers(brandId, 'offers', {
    title: `New offer from your loyalty brand`,
    body: body.title,
    data: { type: 'NEW_OFFER', offerId, brandId },
  });

  return ok({ offerId, status: 'ACTIVE' });
}

async function listOffers(event: APIGatewayProxyEvent, brandId: string) {
  const res = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: { ':pk': `BRAND#${brandId}`, ':prefix': 'OFFER#' },
  }));
  return ok({ offers: res.Items?.map(i => ({ ...JSON.parse(i.desc), status: i.status, sK: i.sK })) ?? [] });
}

async function updateOffer(event: APIGatewayProxyEvent, brandId: string) {
  const offerId = event.pathParameters?.offerId ?? event.path.split('/offers/')[1];
  const body: Partial<OfferInput> = JSON.parse(event.body ?? '{}');
  const now = new Date().toISOString();

  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `OFFER#${offerId}` },
  }));
  if (!existing.Item) return err(404, 'Offer not found');

  const merged = { ...JSON.parse(existing.Item.desc), ...body };
  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `OFFER#${offerId}` },
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
    ExpressionAttributeValues: { ':desc': JSON.stringify(merged), ':now': now },
  }));

  return ok({ offerId, updated: true });
}

async function archiveOffer(event: APIGatewayProxyEvent, brandId: string) {
  const offerId = event.pathParameters?.offerId ?? event.path.split('/offers/')[1];
  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `OFFER#${offerId}` },
    UpdateExpression: 'SET #s = :archived, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':archived': 'ARCHIVED', ':now': new Date().toISOString() },
  }));
  return ok({ offerId, status: 'ARCHIVED' });
}

// ─── Newsletters ───────────────────────────────────────────────────────────────

interface NewsletterInput {
  subject: string;
  bodyHtml: string;
  imageUrl?: string;
  ctaUrl?: string;
  ctaLabel?: string;
}

async function sendNewsletter(event: APIGatewayProxyEvent, brandId: string) {
  const body: NewsletterInput = JSON.parse(event.body ?? '{}');
  if (!body.subject || !body.bodyHtml) return err(400, 'Missing subject or bodyHtml');

  const newsletterId = ulid();
  const now = new Date().toISOString();

  // Store newsletter record in RefDataEvent
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK: `BRAND#${brandId}`,
      sK: `NEWSLETTER#${newsletterId}`,
      eventType: 'NEWSLETTER',
      status: 'SENT',
      primaryCat: 'newsletter',
      desc: JSON.stringify({ ...body, brandId, newsletterId }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  // Fan-out to all newsletter subscribers
  const recipientCount = await fanOutToSubscribers(brandId, 'newsletters', {
    title: body.subject,
    body: 'New message from your loyalty brand',
    data: {
      type: 'NEWSLETTER',
      newsletterId,
      brandId,
      deepLink: `bebocard://newsletter/${newsletterId}`,
    },
  }, async (permULID) => {
    // Write per-user newsletter record so it appears in inbox
    await dynamo.send(new PutCommand({
      TableName: USER_TABLE,
      Item: {
        pK: `USER#${permULID}`,
        sK: `NEWSLETTER#${brandId}#${newsletterId}`,
        eventType: 'NEWSLETTER',
        status: 'UNREAD',
        primaryCat: 'newsletter',
        desc: JSON.stringify({ ...body, brandId, newsletterId }),
        createdAt: now,
        updatedAt: now,
      },
    }));
  });

  return ok({ newsletterId, recipientCount, sentAt: now });
}

async function listNewsletters(event: APIGatewayProxyEvent, brandId: string) {
  const res = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: { ':pk': `BRAND#${brandId}`, ':prefix': 'NEWSLETTER#' },
    ScanIndexForward: false,
  }));
  return ok({ newsletters: res.Items?.map(i => ({ ...JSON.parse(i.desc), sK: i.sK, createdAt: i.createdAt })) ?? [] });
}

// ─── Analytics ─────────────────────────────────────────────────────────────────

async function getAnalytics(event: APIGatewayProxyEvent, brandId: string) {
  const params = event.queryStringParameters ?? {};
  const from = params.from ?? new Date(Date.now() - 30 * 86400_000).toISOString();
  const to   = params.to   ?? new Date().toISOString();

  // Scan audit logs for this brand's scan events
  const scanRes = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk AND sK BETWEEN :from AND :to',
    FilterExpression: 'eventType = :et',
    ExpressionAttributeValues: {
      ':pk': `AUDIT#${brandId}`,
      ':from': `LOG#${from}`,
      ':to': `LOG#${to}`,
      ':et': 'AUDIT_LOG',
    },
  }));

  const logs = (scanRes.Items ?? []).map(i => JSON.parse(i.desc));
  const scanCount    = logs.filter(l => l.action === 'POST /scan').length;
  const receiptCount = logs.filter(l => l.action === 'POST /receipt').length;
  const offerCount   = logs.filter(l => l.action === 'createOffer').length;

  // Subscription count
  const subRes = await dynamo.send(new ScanCommand({
    TableName: USER_TABLE,
    FilterExpression: 'sK = :sk',
    ExpressionAttributeValues: { ':sk': `SUBSCRIPTION#${brandId}` },
    Select: 'COUNT',
  }));

  return ok({
    brandId,
    period: { from, to },
    scanCount,
    receiptCount,
    offerCount,
    subscriberCount: subRes.Count ?? 0,
  });
}

// ─── Stores ────────────────────────────────────────────────────────────────────

async function upsertStore(event: APIGatewayProxyEvent, brandId: string) {
  const body = JSON.parse(event.body ?? '{}');
  const { storeId, storeName, lat, lng, radiusMetres = 150 } = body;
  if (!storeId || !storeName || lat == null || lng == null) return err(400, 'Missing required fields');

  const now = new Date().toISOString();
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK: 'STORES',
      sK: `STORE#${brandId}#${storeId}`,
      eventType: 'STORE',
      status: 'ACTIVE',
      primaryCat: 'store',
      desc: JSON.stringify({ brandId, storeId, storeName, lat, lng, radiusMetres }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  return ok({ storeId, upserted: true });
}

async function listStores(brandId: string) {
  const res = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: { ':pk': 'STORES', ':prefix': `STORE#${brandId}#` },
  }));
  return ok({ stores: res.Items?.map(i => JSON.parse(i.desc)) ?? [] });
}

async function archiveStore(event: APIGatewayProxyEvent, brandId: string) {
  const storeId = event.pathParameters?.storeId ?? event.path.split('/stores/')[1];
  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: 'STORES', sK: `STORE#${brandId}#${storeId}` },
    UpdateExpression: 'SET #s = :archived, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':archived': 'ARCHIVED', ':now': new Date().toISOString() },
  }));
  return ok({ storeId, status: 'ARCHIVED' });
}

// ─── API key self-rotation ─────────────────────────────────────────────────────

async function handleRotateKey(event: APIGatewayProxyEvent, brandId: string) {
  const { oldKeyId, createdBy = 'brand_self' } = JSON.parse(event.body ?? '{}');
  if (!oldKeyId) return err(400, 'Missing oldKeyId');

  const { rawKey, newKeyId } = await rotateApiKey(dynamo, brandId, oldKeyId, createdBy);

  return ok({
    newKeyId,
    rawKey,    // Returned ONCE — display and save; never stored in plaintext
    graceNote: 'Old key remains valid for 24 hours to allow integration updates.',
  });
}

// ─── FCM fan-out helper ────────────────────────────────────────────────────────

function getFirebase() {
  if (getApps().length === 0) {
    const sa = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    if (!sa) throw new Error('FIREBASE_SERVICE_ACCOUNT_JSON not set');
    initializeApp({ credential: cert(JSON.parse(sa)) });
  }
  return getMessaging();
}

/**
 * Scans all UserDataEvent SUBSCRIPTION#<brandId> records where the given
 * preference key is true, fetches each user's DEVICE_TOKEN, and fires FCM.
 *
 * @param perSubscriberFn  Optional async side-effect per subscriber (e.g. write inbox record)
 * @returns Count of successfully queued FCM messages
 */
async function fanOutToSubscribers(
  brandId: string,
  preferenceKey: 'offers' | 'newsletters' | 'reminders' | 'catalogues',
  notification: { title: string; body: string; data?: Record<string, string> },
  perSubscriberFn?: (permULID: string) => Promise<void>,
): Promise<number> {
  let recipientCount = 0;
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new ScanCommand({
      TableName: USER_TABLE,
      FilterExpression: 'sK = :sk',
      ExpressionAttributeValues: { ':sk': `SUBSCRIPTION#${brandId}` },
      ExclusiveStartKey: lastKey,
      Limit: 100,
    }));
    lastKey = res.LastEvaluatedKey as typeof lastKey;

    for (const item of res.Items ?? []) {
      const desc = JSON.parse(item.desc ?? '{}');
      if (!desc[preferenceKey]) continue;

      // Extract permULID from pK: USER#<permULID>
      const permULID = (item.pK as string).replace('USER#', '');

      // Get device token
      const tokenItem = await dynamo.send(new GetCommand({
        TableName: USER_TABLE,
        Key: { pK: `USER#${permULID}`, sK: 'DEVICE_TOKEN' },
      }));
      const token = tokenItem.Item?.desc ? JSON.parse(tokenItem.Item.desc).token : null;
      if (!token) continue;

      // Per-subscriber side effect (e.g. write newsletter inbox record)
      if (perSubscriberFn) {
        await perSubscriberFn(permULID).catch(console.error);
      }

      // Send FCM
      try {
        await getFirebase().send({
          token,
          notification: { title: notification.title, body: notification.body },
          data: notification.data,
        });
        recipientCount++;
      } catch (e) {
        console.error(`[fanOut] FCM failed for ${permULID}:`, e);
      }
    }
  } while (lastKey);

  return recipientCount;
}
