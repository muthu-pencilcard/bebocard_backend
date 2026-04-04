import type { APIGatewayProxyHandler } from 'aws-lambda';
import { createHash } from 'crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';
import { monotonicFactory } from 'ulid';
import { withAuditLog } from '../../shared/audit-logger';
import { validateApiKey, extractApiKey } from '../../shared/api-key-auth';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid = monotonicFactory();

const ADMIN_TABLE = process.env.ADMIN_TABLE!;
const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;

function getFirebaseAdmin() {
  if (getApps().length === 0) {
    const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    if (!serviceAccountJson) throw new Error('FIREBASE_SERVICE_ACCOUNT_JSON not set');
    initializeApp({ credential: cert(JSON.parse(serviceAccountJson)) });
  }
  return getMessaging();
}

function parseRecord(value: unknown): Record<string, unknown> {
  if (typeof value === 'object' && value !== null) return value as Record<string, unknown>;
  if (typeof value !== 'string' || value.length === 0) return {};
  try {
    return JSON.parse(value) as Record<string, unknown>;
  } catch {
    return {};
  }
}

const _handler: APIGatewayProxyHandler = async (event) => {
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  };

  const path = event.path ?? '';

  try {
    if (path.endsWith('/scan')) return handleLoyaltyCheck(event, headers);
    if (path.endsWith('/receipt')) return handleReceipt(event, headers);
    return { statusCode: 404, headers, body: JSON.stringify({ error: 'Unknown route' }) };
  } catch (err) {
    console.error('[scan-handler]', err);
    return { statusCode: 500, headers, body: JSON.stringify({ error: 'Internal error' }) };
  }
};

export const handler = withAuditLog(dynamo, _handler);

// ── POST /scan ────────────────────────────────────────────────────────────────
// Called by brand backend at any point during checkout.
// Returns whether the user has a loyalty card for this brand, and if so the card id.

interface ScanRequest {
  secondaryULID: string;
  storeBrandLoyaltyName: string; // brand id e.g. "woolworths"
}

async function handleLoyaltyCheck(
  event: Parameters<APIGatewayProxyHandler>[0],
  headers: Record<string, string>,
) {
  const rawKey = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'scan') : null;
  if (!validKey) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: 'Invalid or missing API key' }) };
  }

  const body: ScanRequest = JSON.parse(event.body ?? '{}');
  const { secondaryULID, storeBrandLoyaltyName } = body;

  if (!secondaryULID || !storeBrandLoyaltyName) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'Missing required fields' }) };
  }

  if (storeBrandLoyaltyName !== validKey.brandId) {
    console.warn('[scan-handler] brand mismatch for API key', { requested: storeBrandLoyaltyName, keyBrand: validKey.brandId });
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  // Query by pK only — sK is permULID (not a constant like 'INDEX')
  const scanQuery = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `SCAN#${secondaryULID}` },
    Limit: 1,
  }));

  const scanItem = scanQuery.Items?.[0];
  if (!scanItem || scanItem.status === 'REVOKED') {
    return { statusCode: 404, headers, body: JSON.stringify({ error: 'Not found' }) };
  }

  const indexDesc = JSON.parse(scanItem.desc ?? '{}');
  const cards: Array<{ brand: string; cardId: string; isDefault: boolean }> = indexDesc.cards ?? [];
  const brandCards = cards.filter(c => c.brand === validKey.brandId);

  if (brandCards.length === 0) {
    const permULID: string = scanItem.sK;
    void maybeSendCardSuggestion(permULID, validKey.brandId).catch((err) => {
      console.error('[scan-handler] CARD_SUGGESTION failed', err);
    });
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ hasLoyaltyCard: false }),
    };
  }

  // Return the default card; fall back to the first if none is flagged
  const card = brandCards.find(c => c.isDefault) ?? brandCards[0];
  const permULID: string = scanItem.sK;

  // Fetch subscription consent + segment labels in parallel.
  // Labels are only included in the response if SUBSCRIPTION#<brandId> is ACTIVE.
  const [subRes, segRes] = await Promise.all([
    dynamo.send(new GetCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: `SUBSCRIPTION#${validKey.brandId}` },
    })),
    dynamo.send(new GetCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: `SEGMENT#${validKey.brandId}` },
    })),
  ]);

  const subscribed = !!subRes.Item && subRes.Item.status === 'ACTIVE';
  const segDesc = subscribed && segRes.Item?.desc ? JSON.parse(segRes.Item.desc as string) : null;

  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({
      hasLoyaltyCard: true,
      loyaltyId: card.cardId,
      ...(segDesc ? { tier: segDesc.visitFrequency, spendBucket: segDesc.spendBucket } : {}),
    }),
  };
}

// ── POST /receipt ─────────────────────────────────────────────────────────────
// Called by brand backend after transaction completes.
// Saves the receipt to the user's data and pushes an FCM notification.

interface ReceiptRequest {
  secondaryULID: string;
  merchant: string;
  amount: number;
  purchaseDate: string;   // ISO 8601
  brandId?: string;
  loyaltyCardId?: string; // brand card number if loyalty was applied
  pointsEarned?: number;
  currency?: string;
  items?: unknown[];
  category?: string;
  notes?: string;
}

async function handleReceipt(
  event: Parameters<APIGatewayProxyHandler>[0],
  headers: Record<string, string>,
) {
  const rawKey = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'receipt') : null;
  if (!validKey) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: 'Invalid or missing API key' }) };
  }

  const body: ReceiptRequest = JSON.parse(event.body ?? '{}');
  const { secondaryULID, merchant, amount, purchaseDate } = body;

  if (!secondaryULID || !merchant || amount == null || !purchaseDate) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'Missing required fields' }) };
  }

  if (body.brandId && body.brandId !== validKey.brandId) {
    console.warn('[scan-handler] receipt brand mismatch for API key', { requested: body.brandId, keyBrand: validKey.brandId });
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  // Resolve secondaryULID → permULID (sK of the SCAN index record IS the permULID)
  const scanQuery = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `SCAN#${secondaryULID}` },
    Limit: 1,
  }));

  const scanItem = scanQuery.Items?.[0];
  if (!scanItem || scanItem.status === 'REVOKED') {
    return { statusCode: 404, headers, body: JSON.stringify({ error: 'Not found' }) };
  }

  const permULID: string = scanItem.sK;

  // Idempotency — detect duplicate receipt submissions from brand POS retries.
  // Key is a hash of the immutable receipt attributes. If already present, return
  // the original receiptSK so the brand gets a consistent response.
  const brandId = validKey.brandId;
  const idempotencyKey = createHash('sha256')
    .update(`${permULID}|${brandId}|${purchaseDate.substring(0, 10)}|${merchant}|${amount}`)
    .digest('hex');

  const existingReceipt = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `RECEIPT_IDEM#${idempotencyKey}` },
  }));
  if (existingReceipt.Item) {
    const existingSK = existingReceipt.Item.receiptSK as string;
    return { statusCode: 200, headers, body: JSON.stringify({ success: true, receiptSK: existingSK }) };
  }

  // Save receipt
  const receiptSK = `RECEIPT#${purchaseDate.substring(0, 10)}#${ulid()}`;

  // Write idempotency sentinel first. If the Lambda is retried after this write
  // but before the receipt write, the next invocation will find the sentinel and
  // return early. The receipt write is a best-effort follow-up.
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: `RECEIPT_IDEM#${idempotencyKey}`,
      eventType: 'RECEIPT_IDEM',
      status: 'ACTIVE',
      receiptSK,
      createdAt: new Date().toISOString(),
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  })).catch(() => { /* race condition: another invocation wrote it first — safe to ignore */ });

  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: receiptSK,
      eventType: 'RECEIPT',
      status: 'ACTIVE',
      primaryCat: 'receipt',
      subCategory: brandId,
      desc: JSON.stringify({
        merchant,
        amount,
        currency: body.currency ?? 'AUD',
        purchaseDate,
        brandId,
        loyaltyCardId:  body.loyaltyCardId  ?? null,
        pointsEarned:   body.pointsEarned   ?? null,
        items:          body.items          ?? [],
        category:       body.category       ?? 'other',
        notes:          body.notes          ?? null,
        source:         'brand_push',
        // Presence of secondaryULID marks this receipt as POS-path (brand submitted).
        // The receipt-iceberg-writer stream consumer uses this field to gate S3 writes.
        secondaryULID,
      }),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    },
  }));

  // Push notification to user
  const deviceToken = await getDeviceToken(`USER#${permULID}`);
  if (deviceToken) {
    const title = `Receipt from ${merchant}`;
    const notifBody = body.pointsEarned
      ? `$${amount} · ${body.pointsEarned} pts earned`
      : `$${amount}`;
    try {
      await getFirebaseAdmin().send({
        token: deviceToken,
        notification: { title, body: notifBody },
        data: {
          type: 'receipt',
          receiptSK,
          brandId,
          merchant,
          amount: String(amount),
        },
        apns: { payload: { aps: { alert: { title, body: notifBody }, sound: 'default' } } },
        android: { priority: 'high', notification: { channelId: 'bebo_receipts' } },
      });
    } catch (e) {
      // Push failure must not fail the receipt save
      console.error('[scan-handler] FCM send failed:', e);
    }
  }

  return { statusCode: 200, headers, body: JSON.stringify({ success: true, receiptSK }) };
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function getDeviceToken(pK: string): Promise<string | null> {
  const result = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK, sK: 'DEVICE_TOKEN' },
  }));
  if (!result.Item) return null;
  const desc = JSON.parse(result.Item.desc ?? '{}');
  return desc.token ?? null;
}

async function maybeSendCardSuggestion(permULID: string, brandId: string): Promise<void> {
  const dedupKey = `CARD_SUGGESTION#${brandId}`;
  const existing = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: dedupKey },
  }));
  if (existing.Item?.createdAt) {
    const createdAt = Date.parse(existing.Item.createdAt as string);
    if (!Number.isNaN(createdAt) && Date.now() - createdAt < 30 * 24 * 60 * 60 * 1000) {
      return;
    }
  }

  const [deviceToken, brandRes] = await Promise.all([
    getDeviceToken(`USER#${permULID}`),
    dynamo.send(new GetCommand({
      TableName: REFDATA_TABLE,
      Key: { pK: `BRAND#${brandId}`, sK: 'profile' },
    })),
  ]);
  if (!deviceToken) return;

  const brandDesc = parseRecord(brandRes.Item?.desc);
  const brandName = String(brandDesc.brandName ?? brandDesc.name ?? brandId);
  const brandColor = String(brandDesc.brandColor ?? brandDesc.color ?? '#6366F1');
  const supportsDirectEnrollment = !!brandDesc.supportsDirectEnrollment;
  const loyaltySignupUrl = typeof brandDesc.loyaltySignupUrl === 'string'
    ? brandDesc.loyaltySignupUrl
    : undefined;

  await getFirebaseAdmin().send({
    token: deviceToken,
    notification: {
      title: `Shop at ${brandName}?`,
      body: supportsDirectEnrollment
        ? 'Link your card or join in one tap.'
        : 'Link your card or sign up and add it to BeboCard.',
    },
    data: {
      type: 'CARD_SUGGESTION',
      brandId,
      brandName,
      brandColor,
      supportsDirectEnrollment: supportsDirectEnrollment ? 'true' : 'false',
      ...(loyaltySignupUrl ? { loyaltySignupUrl } : {}),
    },
    apns: { payload: { aps: { sound: 'default' } } },
    android: { priority: 'high', notification: { channelId: 'bebo_offers' } },
  });

  const now = new Date().toISOString();
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: dedupKey,
      eventType: 'CARD_SUGGESTION',
      status: 'ACTIVE',
      primaryCat: 'card_suggestion',
      subCategory: brandId,
      desc: JSON.stringify({
        brandId,
        brandName,
        supportsDirectEnrollment,
        loyaltySignupUrl: loyaltySignupUrl ?? null,
        suggestionSentAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
}
