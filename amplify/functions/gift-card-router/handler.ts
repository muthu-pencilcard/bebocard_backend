/**
 * gift-card-router — Federated Gift Card Delivery (Patent Claims 23–24)
 *
 * Brands deliver gift cards to BeboCard users via the scan channel.
 * The user's identity is never exposed — the brand creates an opaque delegation
 * token that only they can resolve at their POS.
 *
 * Routes:
 *   POST /gift-card/deliver              Brand delivers a gift card to a user
 *   GET  /gift-card/{deliveryId}/status  Brand checks delivery status
 *
 * Gift card records stored in UserDataEvent:
 *   pK: USER#<permULID>
 *   sK: GIFTCARD#<deliveryId>
 *   primaryCat: gift_card
 *   desc: { deliveryId, token, brandId, brandName, brandColor,
 *           giftCardValue, currency, expiryDate, deliveredAt, source: 'brand' }
 *
 * The token is opaque — the brand created it and resolves it at their POS.
 * BeboCard stores and displays it as a barcode; it has no semantic value to BeboCard.
 */

import type { APIGatewayProxyHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';
import { validateApiKey, extractApiKey } from '../../shared/api-key-auth';
import { withAuditLog } from '../../shared/audit-logger';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid   = monotonicFactory();

const ADMIN_TABLE = process.env.ADMIN_TABLE!;
const USER_TABLE  = process.env.USER_TABLE!;
const REF_TABLE   = process.env.REF_TABLE!;

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
  const path   = event.path ?? '';
  const method = event.httpMethod;

  try {
    if (method === 'POST' && path.endsWith('/gift-card/deliver'))
      return handleDeliver(event);

    const statusMatch = path.match(/\/gift-card\/([^/]+)\/status$/);
    if (method === 'GET' && statusMatch)
      return handleStatus(event, statusMatch[1]);

    return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Not found' }) };
  } catch (e) {
    console.error('[gift-card-router]', e);
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Internal error' }) };
  }
};

// ── POST /gift-card/deliver ───────────────────────────────────────────────────
// Brand delivers a gift card to a BeboCard user via secondaryULID.

interface DeliverBody {
  secondaryULID:  string;
  token:          string;   // opaque delegation token — brand creates and resolves this
  giftCardValue:  number;
  currency:       string;
  expiryDate?:    string;   // ISO 8601
  cardLabel?:     string;
}

async function handleDeliver(event: Parameters<APIGatewayProxyHandler>[0]) {
  const rawKey   = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'gift_card') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or missing API key' }) };

  const body: Partial<DeliverBody> = JSON.parse(event.body ?? '{}');
  const { secondaryULID, token, giftCardValue, currency } = body;

  if (!secondaryULID || !token || !giftCardValue || !currency) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing required fields: secondaryULID, token, giftCardValue, currency' }) };
  }

  const permULID = await resolvePermULID(secondaryULID);
  if (!permULID) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'User not found' }) };

  // Fetch brand profile for name + colour
  const brandRef = await dynamo.send(new GetCommand({
    TableName: REF_TABLE,
    Key: { pK: `BRAND#${validKey.brandId}`, sK: 'PROFILE' },
  }));
  const brandDesc  = JSON.parse(brandRef.Item?.desc ?? '{}');
  const brandName  = brandDesc.brandName  as string ?? validKey.brandId;
  const brandColor = brandDesc.brandColor as string ?? '#6366F1';

  const deliveryId  = ulid();
  const now         = new Date().toISOString();

  // Store gift card in UserDataEvent — same schema as manually-added gift cards
  // so the existing GiftCardsPage renders it without changes.
  // source: 'brand' distinguishes brand-delivered from user-entered cards.
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: `GIFTCARD#${deliveryId}`,
      eventType:  'GIFTCARD',
      status:     'ACTIVE',
      primaryCat: 'gift_card',
      subCategory: validKey.brandId,
      desc: JSON.stringify({
        deliveryId,
        token,
        brandId:       validKey.brandId,
        brandName,
        brandColor,
        cardNumber:    token,   // token displayed as barcode
        giftCardValue,
        balance:       giftCardValue,
        currency,
        expiryDate:    body.expiryDate ?? null,
        cardLabel:     body.cardLabel ?? `${brandName} Gift Card`,
        isCustom:      false,
        source:        'brand',
        deliveredAt:   now,
      }),
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  // FCM push to notify user
  const deviceToken = await getDeviceToken(permULID);
  if (deviceToken) {
    try {
      const { initializeApp, getApps, cert } = await import('firebase-admin/app');
      const { getMessaging } = await import('firebase-admin/messaging');
      if (getApps().length === 0) {
        const sa = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
        if (sa) initializeApp({ credential: cert(JSON.parse(sa)) });
      }
      await getMessaging().send({
        token: deviceToken,
        data: {
          type:       'GIFT_CARD_DELIVERY',
          deliveryId,
          brandName,
          giftCardValue: String(giftCardValue),
          currency,
        },
        notification: {
          title: `You've received a gift card!`,
          body:  `${brandName} — ${currency} ${giftCardValue} gift card added to your wallet`,
        },
        android: { priority: 'high' },
        apns:    { payload: { aps: { contentAvailable: true, sound: 'default' } } },
      });
    } catch (e) {
      console.error('[gift-card-router] FCM push failed', e);
    }
  }

  return {
    statusCode: 201,
    headers: CORS,
    body: JSON.stringify({ deliveryId, status: 'DELIVERED' }),
  };
}

// ── GET /gift-card/{deliveryId}/status ────────────────────────────────────────

async function handleStatus(
  event: Parameters<APIGatewayProxyHandler>[0],
  deliveryId: string,
) {
  const rawKey   = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'gift_card') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Unauthorized' }) };

  // Look up via delivery index in AdminDataEvent
  const idxRes = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `GIFTCARD_IDX#${deliveryId}` },
    Limit: 1,
  }));
  const idx = idxRes.Items?.[0];
  if (!idx) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Delivery not found' }) };

  const permULID = idx.sK as string;
  const item = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `GIFTCARD#${deliveryId}` },
  }));
  if (!item.Item) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Delivery not found' }) };

  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({ deliveryId, status: item.Item.status }),
  };
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
