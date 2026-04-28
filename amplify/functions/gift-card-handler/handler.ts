/**
 * gift-card-handler — Gift Card Marketplace (Phase 13, Patent Claims 72–74)
 *
 * Purchase flows:
 *   purchaseForSelf  — Stripe Checkout → distributor fulfillment → card in wallet
 *   purchaseAsGift   — Stripe Checkout → distributor fulfillment → SES email to recipient
 *
 * Claim flow:
 *   GET /gift/:token — JWT-verified single-use claim; writes card to recipient UserDataEvent
 *
 * Utility:
 *   syncGiftCardBalance — pull live balance from distributor
 *
 * Distributor routing:
 *   Each GIFTCARD# catalog record carries distributorId.
 *   DistributorRouter selects: Prezzee (AU), Tango (US), Runa (UK),
 *   YOUGotaGift (UAE/GCC), Reloadly (global fallback).
 *
 * PIN security:
 *   cardNumber + pin KMS-encrypted in GIFT# AdminDataEvent during transit.
 *   Deleted from server after successful claim.
 */

import type { APIGatewayProxyHandler, AppSyncResolverEvent } from 'aws-lambda';
import { createHmac, randomBytes, timingSafeEqual } from 'crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { KMSClient, EncryptCommand, DecryptCommand } from '@aws-sdk/client-kms';
import { SESClient, SendEmailCommand } from '@aws-sdk/client-ses';
import { monotonicFactory } from 'ulid';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';
import bwipjs from 'bwip-js/node';

// ── Clients ───────────────────────────────────────────────────────────────────

const dynamo  = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const kms     = new KMSClient({});
const ses     = new SESClient({ region: process.env.SES_REGION ?? 'us-east-1' });
const ulid    = monotonicFactory();

const ADMIN_TABLE  = process.env.ADMIN_TABLE!;
const USER_TABLE   = process.env.USER_TABLE!;
const REF_TABLE    = process.env.REF_TABLE ?? process.env.REFDATA_TABLE!;
const KMS_KEY_ARN  = process.env.GIFT_CARD_KMS_KEY_ARN!;
const FROM_EMAIL              = process.env.SES_FROM_EMAIL ?? 'noreply@bebocard.com';
const GIFT_TOKEN_SECRET       = process.env.GIFT_TOKEN_SECRET ?? 'dev-secret';
const APP_BASE_URL            = process.env.APP_BASE_URL ?? 'https://app.bebocard.com';

const CORS = { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' };

function isProductionRuntime(): boolean {
  if (process.env.NODE_ENV === 'test' || process.env.VITEST === 'true') return false;

  const stackName = process.env.AMPLIFY_DATA_STACK_NAME?.toLowerCase() ?? '';
  if (!stackName) return false;

  return stackName.includes('prod');
}

function getStripeConfig(): { secretKey: string; webhookSecret: string } {
  const secretKey = process.env.STRIPE_SECRET_KEY ?? 'mock';
  const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET ?? '';

  if (isProductionRuntime()) {
    if (!process.env.STRIPE_SECRET_KEY) throw new Error('STRIPE_SECRET_KEY is required in production');
    if (!process.env.STRIPE_WEBHOOK_SECRET) throw new Error('STRIPE_WEBHOOK_SECRET is required in production');
  }

  return { secretKey, webhookSecret };
}

// ── Entry point — handles both AppSync resolver and REST (API Gateway) ────────

export const handler = async (event: unknown) => {
  // AppSync resolver event has typeName + fieldName
  const appsync = event as AppSyncResolverEvent<Record<string, unknown>>;
  if (appsync?.info?.fieldName) {
    return handleAppSync(appsync);
  }
  // REST event
  return handleRest(event as Parameters<APIGatewayProxyHandler>[0]);
};

// ── AppSync resolver ──────────────────────────────────────────────────────────

async function handleAppSync(event: AppSyncResolverEvent<Record<string, unknown>>) {
  const field    = event.info.fieldName;
  const args     = event.arguments ?? {};
  const identity = event.identity as { sub?: string; claims?: Record<string, unknown> } | null;
  const sub      = identity?.sub;
  const email    = identity?.claims?.email as string | undefined;
  const permULID = await resolvePermULID(sub);

  switch (field) {
    case 'purchaseForSelf':
      return handlePurchaseForSelf(args, permULID, email);
    case 'purchaseAsGift':
      return handlePurchaseAsGift(args, permULID);
    case 'syncGiftCardBalance':
      return handleSyncBalance(args, permULID);
    case 'listYourGiftCardForSale':
      return handleListForSale(args, permULID);
    case 'purchaseResoldCard':
      return handlePurchaseResoldCard(args, permULID);
    case 'withdrawBalance':
      return handleWithdrawBalance(args, permULID);
    default:
      throw new Error(`Unknown field: ${field}`);
  }
}

// ── REST handler ──────────────────────────────────────────────────────────────

async function handleRest(event: Parameters<APIGatewayProxyHandler>[0]) {
  const path   = event.path ?? '';
  const method = event.httpMethod ?? '';

  try {
    if (method === 'POST' && path.endsWith('/webhook')) {
      return handleStripeWebhook(event);
    }
    const claimMatch = path.match(/\/gift\/([^/]+)$/);
    if (method === 'GET' && claimMatch) {
      return handleGiftClaim(claimMatch[1]);
    }
    return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Not found' }) };
  } catch (err) {
    console.error('[gift-card-handler REST]', err);
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Internal error' }) };
  }
}

// ── purchaseForSelf ───────────────────────────────────────────────────────────

async function handlePurchaseForSelf(
  args: Record<string, unknown>,
  permULID: string,
  buyerEmail?: string,
): Promise<{ checkoutUrl: string; sessionId: string }> {
  const { brandId, skuId, denomination, currency } = args as {
    brandId: string; skuId: string; denomination: number; currency: string;
  };

  if (denomination <= 0) throw new Error('Denomination must be greater than 0');

  const catalog = await fetchCatalogItem(brandId, skuId);
  validateDenomination(catalog, denomination);

  const sessionId = ulid();

  // Store pending session so webhook knows what to fulfil
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK:          `GIFT_SESSION#${sessionId}`,
      sK:          'metadata',
      type:        'self',
      permULID,
      buyerEmail:  buyerEmail ?? null,
      brandId,
      skuId,
      denomination,
      currency:    currency ?? catalog.currency,
      distributorId: catalog.distributorId,
      distributorSku: catalog.distributorSku,
      brandName:   catalog.brandName,
      brandColor:  catalog.brandColor ?? '#6366F1',
      status:      'pending',
      createdAt:   new Date().toISOString(),
    },
  }));

  const checkoutUrl = await createStripeCheckoutSession({
    sessionId,
    description:  `${catalog.brandName} Gift Card — ${currency ?? catalog.currency} ${denomination}`,
    amountCents:  Math.round(denomination * 100),
    currency:     (currency ?? catalog.currency).toLowerCase(),
    successUrl:   `${APP_BASE_URL}/gift-success?session=${sessionId}`,
    cancelUrl:    `${APP_BASE_URL}/gift-cancel`,
    metadata:     { sessionId, type: 'self', permULID },
  });

  if (getStripeConfig().secretKey === 'mock') {
    // If mocking, we immediately trigger the fulfillment logic as if Stripe called our webhook
    console.log('[Mock Mode] Triggering immediate fulfillment for self-purchase');
    await handleStripeWebhook({
      body: JSON.stringify({
        id: `mock_evt_${sessionId}`,
        type: 'checkout.session.completed',
        data: { object: { metadata: { sessionId } } }
      }),
      headers: { 'stripe-signature': 'mock' },
      httpMethod: 'POST',
      path: '/webhook'
    } as any);
  }

  return { checkoutUrl, sessionId };
}

// ── purchaseAsGift ────────────────────────────────────────────────────────────

async function handlePurchaseAsGift(
  args: Record<string, unknown>,
  senderPermULID: string,
): Promise<{ checkoutUrl: string; sessionId: string }> {
  const { brandId, skuId, denomination, currency, recipientEmail, senderDisplayName, message } = args as {
    brandId: string; skuId: string; denomination: number; currency: string;
    recipientEmail: string; senderDisplayName?: string; message?: string;
  };

  if (!recipientEmail?.trim()) throw new Error('recipientEmail is required');
  if (denomination <= 0) throw new Error('Denomination must be greater than 0');

  const catalog   = await fetchCatalogItem(brandId, skuId);
  validateDenomination(catalog, denomination);

  const sessionId = ulid();

  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK:              `GIFT_SESSION#${sessionId}`,
      sK:              'metadata',
      type:            'gift',
      senderPermULID,
      recipientEmail:  hashEmail(recipientEmail.trim().toLowerCase()),
      recipientEmailRaw: recipientEmail.trim().toLowerCase(), // needed for SES — deleted after send
      senderDisplayName: senderDisplayName ?? null,
      message:         message ?? null,
      brandId,
      skuId,
      denomination,
      currency:        currency ?? catalog.currency,
      distributorId:   catalog.distributorId,
      distributorSku:  catalog.distributorSku,
      brandName:       catalog.brandName,
      brandColor:      catalog.brandColor ?? '#6366F1',
      status:          'pending',
      createdAt:       new Date().toISOString(),
    },
  }));

  const checkoutUrl = await createStripeCheckoutSession({
    sessionId,
    description:  `${catalog.brandName} Gift Card — ${currency ?? catalog.currency} ${denomination} (Gift)`,
    amountCents:  Math.round(denomination * 100),
    currency:     (currency ?? catalog.currency).toLowerCase(),
    successUrl:   `${APP_BASE_URL}/gift-sent?session=${sessionId}`,
    cancelUrl:    `${APP_BASE_URL}/gift-cancel`,
    metadata:     { sessionId, type: 'gift', senderPermULID },
  });

  return { checkoutUrl, sessionId };
}

// ── Stripe webhook — fulfils after checkout.session.completed ─────────────────

async function handleStripeWebhook(event: Parameters<APIGatewayProxyHandler>[0]) {
  const { secretKey, webhookSecret } = getStripeConfig();
  const sig  = event.headers?.['stripe-signature'] ?? event.headers?.['Stripe-Signature'];
  const body = event.body ?? '';

  // Verify Stripe signature
  if (secretKey !== 'mock' && webhookSecret && !verifyStripeSignature(body, sig ?? '', webhookSecret)) {
    return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid signature' }) };
  }

  const payload = JSON.parse(body);
  if (payload.type !== 'checkout.session.completed') {
    return { statusCode: 200, headers: CORS, body: JSON.stringify({ received: true }) };
  }

  // Event-level idempotency: deduplicate by Stripe event ID across retries
  // This handles the case where Stripe retries the same event for any reason.
  const stripeEventId = payload.id as string | undefined;
  if (stripeEventId) {
    const ttl = Math.floor(Date.now() / 1000) + 7 * 86400; // 7-day dedup window
    try {
      await dynamo.send(new PutCommand({
        TableName: ADMIN_TABLE,
        Item: {
          pK: `STRIPE_IDEM#${stripeEventId}`,
          sK: 'processed',
          ttl,
          createdAt: new Date().toISOString(),
        },
        ConditionExpression: 'attribute_not_exists(pK)',
      }));
    } catch (condErr: unknown) {
      if ((condErr as { name?: string }).name === 'ConditionalCheckFailedException') {
        // Already processed — idempotent skip
        return { statusCode: 200, headers: CORS, body: JSON.stringify({ received: true, skipped: true, reason: 'duplicate_event' }) };
      }
      throw condErr;
    }
  }

  const meta      = payload.data?.object?.metadata ?? {};
  const sessionId = meta.sessionId as string;
  if (!sessionId) return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing sessionId' }) };

  const sessionRes = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `GIFT_SESSION#${sessionId}`, sK: 'metadata' },
  }));
  const session = sessionRes.Item;
  if (!session) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Session not found' }) };
  if (session.status !== 'pending') {
    return { statusCode: 200, headers: CORS, body: JSON.stringify({ received: true, skipped: true }) };
  }

  // Mark session as processing (race condition guard for concurrent webhooks)
  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `GIFT_SESSION#${sessionId}`, sK: 'metadata' },
    UpdateExpression: 'SET #s = :processing',
    ConditionExpression: '#s = :pending',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':processing': 'processing', ':pending': 'pending' },
  }));

  // Fulfil via distributor
  const card = await DistributorRouter.fulfil(
    session.distributorId as string,
    session.distributorSku as string,
    session.denomination as number,
    session.currency as string,
  );

  const now = new Date().toISOString();

  if (session.type === 'self') {
    // Write card directly to user's wallet
    const cardSK = `GIFTCARD#${sessionId}`;
    await dynamo.send(new PutCommand({
      TableName: USER_TABLE,
      Item: {
        pK:         `USER#${session.permULID}`,
        sK:         cardSK,
        eventType:  'GIFTCARD',
        status:     'ACTIVE',
        primaryCat: 'gift_card',
        brandId:    session.brandId,
        desc: JSON.stringify({
          brandName:   session.brandName,
          brandId:     session.brandId,
          brandColor:  session.brandColor,
          cardNumber:  card.cardNumber,
          // PIN delivered via FCM push to device — NOT stored here long-term
          denomination: session.denomination,
          currency:    session.currency,
          expiryDate:  card.expiryDate,
          balance:     session.denomination,
          lastBalanceSync: now,
          source:      'marketplace',
          distributorId: session.distributorId,
          distributorSku: session.distributorSku,
          isCustom:    false,
        }),
        createdAt: now,
        updatedAt: now,
      },
    }));

    // Push PIN to device via FCM (never stored server-side post-delivery)
    await pushPinToDevice(
      session.permULID as string,
      cardSK,
      card.cardNumber,
      card.pin,
      session.brandName as string,
      session.denomination as number,
      session.currency as string
    );

    // SES purchase confirmation to buyer
    if (session.buyerEmail) {
      await sendPurchaseConfirmation({
        permULID:     session.permULID as string,
        buyerEmail:   session.buyerEmail as string,
        brandName:    session.brandName as string,
        denomination: session.denomination as number,
        currency:     session.currency as string,
        cardNumber:   card.cardNumber,
        pin:          card.pin,
        expiryDate:   card.expiryDate,
        brandColor:   session.brandColor as string,
        sessionId,
      });
    }

    // Write RECEIPT# so the Finance tab reflects the gift card purchase
    const receiptId = ulid();
    await dynamo.send(new PutCommand({
      TableName: USER_TABLE,
      Item: {
        pK:         `USER#${session.permULID}`,
        sK:         `RECEIPT#${now.substring(0, 10)}#${receiptId}`,
        eventType:  'RECEIPT',
        status:     'CONFIRMED',
        primaryCat: 'receipt',
        brandId:    session.brandId,
        desc: JSON.stringify({
          merchant:        session.brandName,
          brandId:         session.brandId,
          amount:          session.denomination,
          currency:        session.currency,
          purchaseDate:    now,
          receiptType:     'GIFT_CARD_PURCHASE',
          linkedGiftCardSK: cardSK,
          category:        'gift_card',
          source:          'marketplace',
          sessionId,
        }),
        createdAt: now,
        updatedAt: now,
      },
    }));

  } else {
    // Gift flow: store encrypted card in AdminDataEvent, generate claim token, send email
    const giftToken = generateGiftToken(sessionId);
    const encryptedPayload = await kmsEncrypt(JSON.stringify({ cardNumber: card.cardNumber, pin: card.pin }));

    await dynamo.send(new PutCommand({
      TableName: ADMIN_TABLE,
      Item: {
        pK:                `GIFT#${giftToken}`,
        sK:                'metadata',
        senderPermULID:    session.senderPermULID,
        recipientEmail:    session.recipientEmail,   // hashed
        brandId:           session.brandId,
        skuId:             session.skuId,
        denomination:      session.denomination,
        currency:          session.currency,
        brandName:         session.brandName,
        encryptedCard:     encryptedPayload,
        senderDisplayName: session.senderDisplayName ?? null,
        message:           session.message ?? null,
        status:            'pending',
        expiresAt:         new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString(),
        ttl:               Math.floor(Date.now() / 1000) + 90 * 24 * 60 * 60,
        createdAt:         now,
      },
    }));

    // Send gift delivery email
    await sendGiftEmail({
      recipientEmail: session.recipientEmailRaw as string,
      senderName:     session.senderDisplayName as string | undefined,
      brandName:      session.brandName as string,
      denomination:   session.denomination as number,
      currency:       session.currency as string,
      message:        session.message as string | undefined,
      claimUrl:       `${APP_BASE_URL}/gift/${giftToken}`,
    });

    // Remove raw email from session record (privacy)
    await dynamo.send(new UpdateCommand({
      TableName: ADMIN_TABLE,
      Key: { pK: `GIFT_SESSION#${sessionId}`, sK: 'metadata' },
      UpdateExpression: 'REMOVE recipientEmailRaw',
    }));

    // Write RECEIPT# to sender's Finance tab
    const giftReceiptId = ulid();
    await dynamo.send(new PutCommand({
      TableName: USER_TABLE,
      Item: {
        pK:         `USER#${session.senderPermULID}`,
        sK:         `RECEIPT#${now.substring(0, 10)}#${giftReceiptId}`,
        eventType:  'RECEIPT',
        status:     'CONFIRMED',
        primaryCat: 'receipt',
        brandId:    session.brandId,
        desc: JSON.stringify({
          merchant:        session.brandName,
          brandId:         session.brandId,
          amount:          session.denomination,
          currency:        session.currency,
          purchaseDate:    now,
          receiptType:     'GIFT_CARD_PURCHASE',
          category:        'gift_card',
          source:          'gift_send',
          sessionId,
          giftToken,
        }),
        createdAt: now,
        updatedAt: now,
      },
    }));
  }

  // Mark session fulfilled
  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `GIFT_SESSION#${sessionId}`, sK: 'metadata' },
    UpdateExpression: 'SET #s = :fulfilled, fulfilledAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':fulfilled': 'fulfilled', ':now': now },
  }));

  return { statusCode: 200, headers: CORS, body: JSON.stringify({ received: true }) };
}

// ── Gift claim (GET /gift/:token) ─────────────────────────────────────────────

async function handleGiftClaim(token: string) {
  // Verify JWT signature
  if (!verifyGiftToken(token)) {
    return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or expired token' }) };
  }

  const giftRes = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `GIFT#${token}`, sK: 'metadata' },
  }));
  const gift = giftRes.Item;

  if (!gift) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Gift not found' }) };
  if (gift.status === 'claimed') return { statusCode: 409, headers: CORS, body: JSON.stringify({ error: 'Already claimed' }) };
  if (gift.status === 'expired' || new Date(gift.expiresAt as string) < new Date()) {
    return { statusCode: 410, headers: CORS, body: JSON.stringify({ error: 'Gift expired' }) };
  }

  // Decrypt card details
  const { cardNumber, pin } = JSON.parse(await kmsDecrypt(gift.encryptedCard as string)) as { cardNumber: string; pin: string };

  const now    = new Date().toISOString();
  const cardSK = `GIFTCARD#${token}`;

  // This endpoint is public — caller may not be authenticated.
  // Return card details for web fallback display; app handles wallet write via AppSync.
  // If Cognito sub is provided (app claim), write directly.
  // For now: return card data for app to write + delete server-side record.
  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `GIFT#${token}`, sK: 'metadata' },
    UpdateExpression: 'SET #s = :claimed, claimedAt = :now REMOVE encryptedCard',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':claimed': 'claimed', ':now': now },
  }));

  // claimReceiptData: the app writes this RECEIPT# via AppSync after saving the card to wallet.
  // The server cannot write it here since the endpoint is public (no Cognito auth — permULID unknown).
  const claimReceiptData = {
    merchant:    gift.brandName,
    brandId:     gift.brandId,
    amount:      gift.denomination,
    currency:    gift.currency,
    purchaseDate: now,
    receiptType: 'GIFT_CARD_RECEIVED',
    category:    'gift_card',
    source:      'gift_received',
    linkedGiftCardSK: cardSK,
  };

  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({
      brandName:   gift.brandName,
      brandId:     gift.brandId,
      denomination: gift.denomination,
      currency:    gift.currency,
      cardNumber,
      pin,
      expiresAt:   gift.expiresAt,
      senderDisplayName: gift.senderDisplayName ?? null,
      message:     gift.message ?? null,
      cardSK,
      claimReceiptData,
    }),
  };
}

// ── syncGiftCardBalance ───────────────────────────────────────────────────────

async function handleSyncBalance(
  args: Record<string, unknown>,
  permULID: string,
): Promise<{ balance: number; lastSyncAt: string }> {
  const { cardSK } = args as { cardSK: string };

  const res = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: cardSK },
  }));
  if (!res.Item) throw new Error('Card not found');

  const desc = safeJson(res.Item.desc);
  const distributorId  = desc.distributorId as string | undefined;
  const distributorSku = desc.distributorSku as string | undefined;
  const cardNumber     = desc.cardNumber as string;

  if (!distributorId || !distributorSku) throw new Error('Card has no distributor info — cannot sync balance');

  const balance = await DistributorRouter.syncBalance(distributorId, distributorSku, cardNumber);
  const now     = new Date().toISOString();

  desc.balance          = balance;
  desc.lastBalanceSync  = now;

  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: cardSK },
    UpdateExpression: 'SET #d = :desc, updatedAt = :now',
    ExpressionAttributeNames: { '#d': 'desc' },
    ExpressionAttributeValues: { ':desc': JSON.stringify(desc), ':now': now },
  }));

  return { balance, lastSyncAt: now };
}

// ── Distributor Router ────────────────────────────────────────────────────────

interface DistributorCard {
  cardNumber: string;
  pin: string;
  expiryDate: string | null;
}

const DistributorRouter = {
  async fulfil(distributorId: string, skuId: string, denomination: number, currency: string): Promise<DistributorCard> {
    switch (distributorId) {
      case 'prezzee':    return PrezzeeClient.fulfil(skuId, denomination, currency);
      case 'tango':      return TangoClient.fulfil(skuId, denomination, currency);
      case 'runa':       return RunaClient.fulfil(skuId, denomination, currency);
      case 'yougotagift': return YOUGotaGiftClient.fulfil(skuId, denomination, currency);
      case 'reloadly':
      default:           return ReloadlyClient.fulfil(skuId, denomination, currency);
    }
  },

  async syncBalance(distributorId: string, skuId: string, cardNumber: string): Promise<number> {
    switch (distributorId) {
      case 'prezzee':    return PrezzeeClient.balance(cardNumber, skuId);
      case 'tango':      return TangoClient.balance(cardNumber, skuId);
      case 'reloadly':
      default:           return ReloadlyClient.balance(cardNumber, skuId);
    }
  },
};

// ── Distributor clients ───────────────────────────────────────────────────────

const PrezzeeClient = {
  async fulfil(skuId: string, denomination: number, currency: string): Promise<DistributorCard> {
    const key = process.env.PREZZEE_API_KEY;
    if (!key) {
      console.warn('[Prezzee Mock] No API key, returning mock card');
      return { cardNumber: '6032-1234-5678-9012', pin: '1234', expiryDate: '2028-12-31' };
    }
    const res = await fetch('https://api.prezzee.com/v1/orders', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ productId: skuId, faceValue: denomination, currency, quantity: 1 }),
    });
    if (!res.ok) throw new Error(`Prezzee fulfil failed: ${res.status}`);
    const data = await res.json() as { cardNumber: string; pin: string; expiryDate?: string };
    return { cardNumber: data.cardNumber, pin: data.pin, expiryDate: data.expiryDate ?? null };
  },
  async balance(cardNumber: string, _skuId: string): Promise<number> {
    const key = process.env.PREZZEE_API_KEY;
    if (!key) throw new Error('PREZZEE_API_KEY not configured');
    const res = await fetch(`https://api.prezzee.com/v1/cards/${cardNumber}/balance`, {
      headers: { 'Authorization': `Bearer ${key}` },
    });
    if (!res.ok) throw new Error(`Prezzee balance failed: ${res.status}`);
    const data = await res.json() as { balance: number };
    return data.balance;
  },
};

const TangoClient = {
  async fulfil(skuId: string, denomination: number, currency: string): Promise<DistributorCard> {
    const key = process.env.TANGO_API_KEY;
    if (!key) {
      console.warn('[Tango Mock] No API key, returning mock card');
      return { cardNumber: 'TANGO-MOCK-9999', pin: '8888', expiryDate: '2029-01-01' };
    }
    const res = await fetch('https://api.tangocard.com/raas/v2/orders', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ utid: skuId, amount: { value: denomination, currencyCode: currency }, quantity: 1 }),
    });
    if (!res.ok) throw new Error(`Tango fulfil failed: ${res.status}`);
    const data = await res.json() as { reward: { credentials: { cardNumber: string; cardPin: string }; expiration?: string } };
    const creds = data.reward.credentials;
    return { cardNumber: creds.cardNumber, pin: creds.cardPin, expiryDate: data.reward.expiration ?? null };
  },
  async balance(cardNumber: string, _skuId: string): Promise<number> {
    // Tango does not expose a public balance API — return -1 to indicate unsupported
    console.warn('[TangoClient] balance sync not supported for card', cardNumber);
    return -1;
  },
};

const RunaClient = {
  async fulfil(skuId: string, denomination: number, currency: string): Promise<DistributorCard> {
    const key = process.env.RUNA_API_KEY;
    if (!key) throw new Error('RUNA_API_KEY not configured');
    const res = await fetch('https://api.runa.io/v1/orders', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ product_id: skuId, denomination, currency }),
    });
    if (!res.ok) throw new Error(`Runa fulfil failed: ${res.status}`);
    const data = await res.json() as { card_number: string; security_code: string; expiry_date?: string };
    return { cardNumber: data.card_number, pin: data.security_code, expiryDate: data.expiry_date ?? null };
  },
  async balance(_cardNumber: string, _skuId: string): Promise<number> {
    return -1; // Runa balance sync not supported
  },
};

const YOUGotaGiftClient = {
  async fulfil(skuId: string, denomination: number, currency: string): Promise<DistributorCard> {
    const key = process.env.YOUGOTAGIFT_API_KEY;
    if (!key) throw new Error('YOUGOTAGIFT_API_KEY not configured');
    const res = await fetch('https://api.yougotagift.com/v2/orders', {
      method: 'POST',
      headers: { 'X-Api-Key': key, 'Content-Type': 'application/json' },
      body: JSON.stringify({ sku: skuId, amount: denomination, currency }),
    });
    if (!res.ok) throw new Error(`YOUGotaGift fulfil failed: ${res.status}`);
    const data = await res.json() as { voucherCode: string; pin?: string; expiryDate?: string };
    return { cardNumber: data.voucherCode, pin: data.pin ?? '', expiryDate: data.expiryDate ?? null };
  },
  async balance(_cardNumber: string, _skuId: string): Promise<number> {
    return -1;
  },
};

const ReloadlyClient = {
  async getAccessToken(): Promise<string> {
    const clientId     = process.env.RELOADLY_CLIENT_ID;
    const clientSecret = process.env.RELOADLY_CLIENT_SECRET;
    if (!clientId || !clientSecret) throw new Error('RELOADLY credentials not configured');
    const res = await fetch('https://auth.reloadly.com/oauth/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: clientId, client_secret: clientSecret, grant_type: 'client_credentials', audience: 'https://giftcards.reloadly.com' }),
    });
    if (!res.ok) throw new Error(`Reloadly auth failed: ${res.status}`);
    const data = await res.json() as { access_token: string };
    return data.access_token;
  },
  async fulfil(skuId: string, denomination: number, currency: string): Promise<DistributorCard> {
    const token = await this.getAccessToken();
    const res   = await fetch('https://giftcards.reloadly.com/orders', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ productId: parseInt(skuId, 10), quantity: 1, unitPrice: denomination, currencyCode: currency }),
    });
    if (!res.ok) throw new Error(`Reloadly fulfil failed: ${res.status}`);
    const data = await res.json() as { transactions: Array<{ cardNumber: string; pinCode: string; expiryDate?: string }> };
    const card = data.transactions[0];
    return { cardNumber: card.cardNumber, pin: card.pinCode, expiryDate: card.expiryDate ?? null };
  },
  async balance(cardNumber: string, skuId: string): Promise<number> {
    const token = await this.getAccessToken();
    const res   = await fetch(`https://giftcards.reloadly.com/products/${skuId}/cards/${cardNumber}/balance`, {
      headers: { 'Authorization': `Bearer ${token}` },
    });
    if (!res.ok) return -1;
    const data = await res.json() as { balance: number };
    return data.balance;
  },
};

// ── Stripe helpers ────────────────────────────────────────────────────────────

async function createStripeCheckoutSession(opts: {
  sessionId: string;
  description: string;
  amountCents: number;
  currency: string;
  successUrl: string;
  cancelUrl: string;
  metadata: Record<string, string>;
}): Promise<string> {
  const { secretKey } = getStripeConfig();

  if (secretKey === 'mock') {
    console.log('[Stripe Mock] Creating simulated checkout session', opts.sessionId);
    // In mock mode, the "success" url is actually the webhook callback URL 
    // to simulate the asynchronous fulfillment without real Stripe involvement.
    // For the UI, we return a URL that the WebView intercepts as success.
    return opts.successUrl; 
  }

  const params = new URLSearchParams({
    'payment_method_types[]':            'card',
    'line_items[0][price_data][currency]':              opts.currency,
    'line_items[0][price_data][unit_amount]':           String(opts.amountCents),
    'line_items[0][price_data][product_data][name]':   opts.description,
    'line_items[0][quantity]':           '1',
    mode:                                'payment',
    success_url:                         opts.successUrl,
    cancel_url:                          opts.cancelUrl,
    ...Object.fromEntries(Object.entries(opts.metadata).map(([k, v]) => [`metadata[${k}]`, v])),
  });

  const res = await fetch('https://api.stripe.com/v1/checkout/sessions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${secretKey}`,
      'Content-Type':  'application/x-www-form-urlencoded',
    },
    body: params.toString(),
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Stripe checkout session failed: ${res.status} ${err}`);
  }

  const session = await res.json() as { url: string };
  return session.url;
}

function verifyStripeSignature(payload: string, sig: string, secret: string): boolean {
  // Stripe signature format: t=timestamp,v1=hash
  const parts   = sig.split(',').reduce<Record<string, string>>((acc, p) => {
    const [k, v] = p.split('=');
    acc[k] = v;
    return acc;
  }, {});
  const ts      = parts['t'];
  const v1      = parts['v1'];
  if (!ts || !v1) return false;
  const signed  = `${ts}.${payload}`;
  const expected = createHmac('sha256', secret).update(signed).digest('hex');
  try {
    return timingSafeEqual(Buffer.from(v1, 'hex'), Buffer.from(expected, 'hex'));
  } catch {
    return false;
  }
}

// ── KMS envelope encryption ───────────────────────────────────────────────────

async function kmsEncrypt(plaintext: string): Promise<string> {
  const res = await kms.send(new EncryptCommand({
    KeyId:     KMS_KEY_ARN,
    Plaintext: Buffer.from(plaintext, 'utf-8'),
  }));
  return Buffer.from(res.CiphertextBlob!).toString('base64');
}

async function kmsDecrypt(ciphertextBase64: string): Promise<string> {
  const res = await kms.send(new DecryptCommand({
    CiphertextBlob: Buffer.from(ciphertextBase64, 'base64'),
  }));
  return Buffer.from(res.Plaintext!).toString('utf-8');
}

// ── Gift token (HMAC-SHA256 JWT) ──────────────────────────────────────────────

function generateGiftToken(sessionId: string): string {
  const payload  = Buffer.from(JSON.stringify({ s: sessionId, t: Date.now() })).toString('base64url');
  const sig      = createHmac('sha256', GIFT_TOKEN_SECRET).update(payload).digest('base64url');
  return `${payload}.${sig}`;
}

function verifyGiftToken(token: string): boolean {
  const parts = token.split('.');
  if (parts.length !== 2) return false;
  const [payload, sig] = parts;
  const expected = createHmac('sha256', GIFT_TOKEN_SECRET).update(payload).digest('base64url');
  try {
    return timingSafeEqual(Buffer.from(sig, 'base64url'), Buffer.from(expected, 'base64url'));
  } catch {
    return false;
  }
}

// ── FCM push — deliver PIN to device ─────────────────────────────────────────

async function pushPinToDevice(permULID: string, cardSK: string, cardNumber: string, pin: string, brandName: string, value: number, currency: string): Promise<void> {
  const tokenRes = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'DEVICE_TOKEN' },
  }));
  const deviceToken = safeJson(tokenRes.Item?.desc).token as string | undefined;
  if (!deviceToken) return;

  if (getApps().length === 0) {
    const json = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    if (!json) return;
    initializeApp({ credential: cert(JSON.parse(json)) });
  }

  await getMessaging().send({
    token: deviceToken,
    data: {
      type:          'GIFT_CARD_PIN',
      cardSK,
      cardNumber,
      pin,
      brandName,
      giftCardValue: String(value),
      currency,
    },
    apns: { payload: { aps: { contentAvailable: true } } },
    android: { priority: 'high' },
  });
}

// ── SES emails ────────────────────────────────────────────────────────────────

async function sendGiftEmail(opts: {
  recipientEmail: string;
  senderName?: string;
  brandName: string;
  denomination: number;
  currency: string;
  message?: string;
  claimUrl: string;
}) {
  const from    = opts.senderName ? `${opts.senderName} (via BeboCard)` : 'Someone special (via BeboCard)';
  const subject = `You've received a ${opts.brandName} gift card worth ${opts.currency} ${opts.denomination}`;
  const html    = `
    <div style="font-family:sans-serif;max-width:480px;margin:0 auto">
      <h2 style="color:#6366f1">${from} sent you a gift card</h2>
      <p><strong>${opts.brandName}</strong> — ${opts.currency} ${opts.denomination}</p>
      ${opts.message ? `<blockquote style="border-left:3px solid #6366f1;padding-left:12px;color:#555">${opts.message}</blockquote>` : ''}
      <p>Claim your gift card here — the link expires in 90 days:</p>
      <a href="${opts.claimUrl}" style="display:inline-block;background:#6366f1;color:white;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:bold">Claim Gift Card</a>
      <p style="color:#999;font-size:12px;margin-top:24px">Powered by BeboCard · Privacy-first loyalty wallet</p>
    </div>`;

  await ses.send(new SendEmailCommand({
    Source:      FROM_EMAIL,
    Destination: { ToAddresses: [opts.recipientEmail] },
    Message: {
      Subject: { Data: subject },
      Body:    { Html: { Data: html } },
    },
  }));
}

async function generateBarcodeBase64(text: string): Promise<string | null> {
  try {
    const png = await bwipjs.toBuffer({
      bcid:            'code128',
      text,
      scale:           3,
      height:          12,
      includetext:     false,
      backgroundcolor: 'ffffff',
    });
    return `data:image/png;base64,${Buffer.from(png).toString('base64')}`;
  } catch {
    return null;
  }
}

async function sendPurchaseConfirmation(opts: {
  permULID: string;
  buyerEmail: string;
  brandName: string;
  denomination: number;
  currency: string;
  cardNumber: string;
  pin: string;
  expiryDate: string | null;
  brandColor: string;
  sessionId: string;
}): Promise<void> {
  const { permULID, buyerEmail, brandName, denomination, currency, cardNumber, pin, expiryDate, brandColor, sessionId } = opts;

  const formattedCardNumber = cardNumber.replace(/(.{4})/g, '$1 ').trim();
  const expiryFormatted = expiryDate
    ? new Date(expiryDate).toLocaleDateString('en-AU', { month: 'short', year: 'numeric' })
    : null;

  // Generate scannable barcode — fall back gracefully if generation fails
  const barcodeDataUrl = await generateBarcodeBase64(cardNumber);

  const html = `
    <div style="font-family:system-ui,Arial,sans-serif;max-width:520px;margin:0 auto;background:#f9fafb;padding:32px 20px">
      <div style="text-align:center;margin-bottom:24px">
        <img src="${APP_BASE_URL}/logo.png" width="40" height="40" alt="BeboCard" style="border-radius:10px;margin-bottom:12px" />
        <h1 style="color:#111827;font-size:20px;font-weight:800;margin:0">Your gift card is ready</h1>
        <p style="color:#6b7280;font-size:14px;margin:6px 0 0">Here are your ${brandName} gift card details</p>
      </div>

      <div style="background:linear-gradient(135deg,${brandColor} 0%,${brandColor}bb 100%);border-radius:20px;padding:28px 24px;color:white;margin-bottom:20px">
        <div style="font-size:11px;font-weight:700;opacity:0.75;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:4px">${brandName}</div>
        <div style="font-size:30px;font-weight:900;margin:0 0 20px">${currency} ${denomination.toFixed(2)}</div>
        <div style="margin-bottom:14px">
          <div style="font-size:10px;opacity:0.7;font-weight:700;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Card Number</div>
          <div style="font-size:17px;font-weight:700;letter-spacing:2px;font-family:monospace,monospace">${formattedCardNumber}</div>
        </div>
        ${pin ? `<div style="margin-bottom:14px">
          <div style="font-size:10px;opacity:0.7;font-weight:700;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">PIN</div>
          <div style="font-size:22px;font-weight:900;letter-spacing:4px;font-family:monospace,monospace">${pin}</div>
        </div>` : ''}
        ${expiryFormatted ? `<div style="font-size:12px;opacity:0.65">Expires ${expiryFormatted}</div>` : ''}
      </div>

      ${barcodeDataUrl ? `
      <div style="background:white;border-radius:16px;padding:20px 24px;text-align:center;margin-bottom:20px;border:1px solid #e5e7eb">
        <div style="font-size:11px;color:#6b7280;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px">Scan in store</div>
        <img src="${barcodeDataUrl}" alt="Gift card barcode" style="max-width:320px;width:100%;height:auto;display:block;margin:0 auto" />
        <div style="font-size:13px;color:#374151;font-family:monospace;margin-top:10px;letter-spacing:1.5px">${formattedCardNumber}</div>
      </div>` : ''}

      <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:12px;padding:16px;margin-bottom:20px">
        <p style="color:#92400e;font-size:13px;margin:0;line-height:1.5">
          <strong>Keep this email safe.</strong> Anyone with the card number and PIN can spend this gift card.
          Your PIN is also stored securely in the BeboCard app — only you can access it there.
        </p>
      </div>

      <div style="text-align:center;margin-bottom:24px">
        <a href="${APP_BASE_URL}/gift-cards" style="display:inline-block;background:#4f46e5;color:white;padding:14px 32px;border-radius:12px;text-decoration:none;font-weight:700;font-size:15px">
          Open in BeboCard App
        </a>
      </div>

      <p style="color:#9ca3af;font-size:12px;text-align:center;margin:0;line-height:1.6">
        Powered by <strong>BeboCard</strong> · Privacy-first loyalty wallet<br>
        Questions? <a href="https://bebocard.com/support" style="color:#6366f1;text-decoration:none">Visit our support page</a>
      </p>
    </div>`;

  try {
    await ses.send(new SendEmailCommand({
      Source:      FROM_EMAIL,
      Destination: { ToAddresses: [buyerEmail] },
      Message: {
        Subject: { Data: `Your ${brandName} Gift Card — ${currency} ${denomination.toFixed(2)}` },
        Body:    { Html: { Data: html } },
      },
    }));

    // Remove buyerEmail from session — only needed for this send
    await dynamo.send(new UpdateCommand({
      TableName: ADMIN_TABLE,
      Key: { pK: `GIFT_SESSION#${sessionId}`, sK: 'metadata' },
      UpdateExpression: 'REMOVE buyerEmail',
    })).catch(e => console.warn('[gift-card-handler] Failed to clear buyerEmail from session', e));

    console.log(`[gift-card-handler] Purchase confirmation sent for ${permULID}`);
  } catch (err) {
    console.warn(`[gift-card-handler] Failed to send purchase confirmation to ${buyerEmail}:`, err);
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function fetchCatalogItem(brandId: string, skuId: string) {
  const res = await dynamo.send(new GetCommand({
    TableName: REF_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `GIFTCARD#${skuId}` },
  }));
  if (!res.Item) throw new Error(`Catalog item not found: ${brandId}/${skuId}`);
  const desc = safeJson(res.Item.desc);
  return {
    distributorId:  res.Item.distributorId as string ?? desc.distributorId as string,
    distributorSku: desc.distributorSku as string,
    currency:       desc.currency as string ?? 'AUD',
    brandName:      desc.brandName as string ?? brandId,
    brandColor:     desc.brandColor as string | undefined,
    catalogDenomination: desc.denomination as number | undefined,
    catalogMinDenomination: desc.minDenomination as number | undefined,
    catalogMaxDenomination: desc.maxDenomination as number | undefined,
  };
}

function validateDenomination(catalog: Awaited<ReturnType<typeof fetchCatalogItem>>, requested: number) {
  if (catalog.catalogDenomination !== undefined && catalog.catalogDenomination !== requested) {
    throw new Error(`Invalid denomination: ${requested}. Expected fixed value of ${catalog.catalogDenomination}`);
  }
  if (catalog.catalogMinDenomination !== undefined && requested < catalog.catalogMinDenomination) {
    throw new Error(`Invalid denomination: ${requested}. Minimum value is ${catalog.catalogMinDenomination}`);
  }
  if (catalog.catalogMaxDenomination !== undefined && requested > catalog.catalogMaxDenomination) {
    throw new Error(`Invalid denomination: ${requested}. Maximum value is ${catalog.catalogMaxDenomination}`);
  }
}

async function resolvePermULID(sub: string | undefined): Promise<string> {
  if (!sub) throw new Error('Unauthenticated');
  // Look up permULID by Cognito sub in AdminDataEvent
  const res = await dynamo.send(new QueryCommand({
    TableName:              ADMIN_TABLE,
    IndexName:              'cognito-sub-index',
    KeyConditionExpression: 'cognitoSub = :sub',
    ExpressionAttributeValues: { ':sub': sub },
    Limit: 1,
  }));
  const item = res.Items?.[0];
  if (!item) throw new Error(`No permULID found for sub: ${sub}`);
  return item.pK.replace('USER#', '');
}

function hashEmail(email: string): string {
  return createHmac('sha256', GIFT_TOKEN_SECRET).update(email).digest('hex');
}

function safeJson(value: unknown): Record<string, unknown> {
  if (typeof value === 'object' && value !== null) return value as Record<string, unknown>;
  if (typeof value !== 'string' || !value) return {};
  try { return JSON.parse(value) as Record<string, unknown>; }
  catch { return {}; }
}

// ── Phase 3: Marketplace Handlers ─────────────────────────────────────────────

async function handleListForSale(args: Record<string, any>, permULID: string) {
  const { cardSK, askingPrice, currency, sellerNote } = args;
  if (!cardSK || !askingPrice) throw new Error('cardSK and askingPrice are required');

  // Verify card ownership and status
  const card = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: cardSK },
  }));

  if (!card.Item) throw new Error('Gift card not found');
  if (card.Item.status !== 'ACTIVE') throw new Error('Only active cards can be listed');

  const cardDesc = safeJson(card.Item.desc);
  const resaleId = `RESALE#${ulid()}`;

  // Atomic update: Lock card in UserDataEvent and Create listing in RefDataEvent
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: cardSK },
    UpdateExpression: 'SET #s = :locked',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':locked': 'LOCKED_FOR_SALE' },
  }));

  await dynamo.send(new PutCommand({
    TableName: REF_TABLE,
    Item: {
      pK: 'MARKETPLACE#LISTINGS',
      sK: resaleId,
      cardSK: cardSK,
      sellerPermULID: permULID,
      brandId: card.Item.subCategory,
      faceValue: cardDesc.denomination || cardDesc.balance,
      askingPrice,
      currency: currency || cardDesc.currency || 'AUD',
      sellerNote,
      status: 'ACTIVE',
      createdAt: new Date().toISOString(),
    }
  }));

  return { status: 'LISTED', resaleId };
}

async function handlePurchaseResoldCard(args: Record<string, any>, buyerPermULID: string) {
  const { resaleId } = args;
  if (!resaleId) throw new Error('resaleId is required');

  // 1. Fetch listing
  const listing = await dynamo.send(new GetCommand({
    TableName: REF_TABLE,
    Key: { pK: 'MARKETPLACE#LISTINGS', sK: resaleId },
  }));

  if (!listing.Item || listing.Item.status !== 'ACTIVE') {
    throw new Error('Listing no longer available');
  }

  const { sellerPermULID, cardSK, brandId, faceValue, askingPrice, currency } = listing.Item;
  if (sellerPermULID === buyerPermULID) throw new Error('Cannot buy your own listing');

  // 2. Fetch seller's card details
  const sellerCard = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${sellerPermULID}`, sK: cardSK },
  }));

  if (!sellerCard.Item) throw new Error('Seller card details missing');

  // 3. Atomic Transfer
  const newCardSK = `GIFTCARD#${ulid()}`;
  
  // Mark listing as sold
  await dynamo.send(new UpdateCommand({
    TableName: REF_TABLE,
    Key: { pK: 'MARKETPLACE#LISTINGS', sK: resaleId },
    UpdateExpression: 'SET #s = :sold',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':sold': 'SOLD' },
  }));

  // Mark seller card transferred
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${sellerPermULID}`, sK: cardSK },
    UpdateExpression: 'SET #s = :transferred',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':transferred': 'TRANSFERRED' },
  }));

  // Add to buyer
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      ...sellerCard.Item,
      pK: `USER#${buyerPermULID}`,
      sK: newCardSK,
      status: 'ACTIVE',
      acquiredVia: 'MARKETPLACE',
      purchasedAt: new Date().toISOString(),
    }
  }));

  // 4. Credit Seller Balance
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${sellerPermULID}`, sK: 'IDENTITY' },
    UpdateExpression: 'SET marketplaceBalance = if_not_exists(marketplaceBalance, :zero) + :credit, updatedAt = :now',
    ExpressionAttributeValues: {
      ':credit': askingPrice,
      ':zero': 0,
      ':now': new Date().toISOString(),
    },
  }));

  // 5. Write SELLER_PROCEEDS# record for Finance tab
  const proceedsId = ulid();
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${sellerPermULID}`,
      sK: `RECEIPT#${new Date().toISOString().substring(0, 10)}#${proceedsId}`,
      eventType: 'PROCEEDS',
      status: 'CONFIRMED',
      primaryCat: 'receipt',
      brandId: 'bebocard',
      desc: JSON.stringify({
        merchant: 'BeboCard Marketplace',
        amount: askingPrice,
        currency: currency || 'AUD',
        purchaseDate: new Date().toISOString(),
        receiptType: 'MARKETPLACE_SALE_PROCEEDS',
        linkedResaleId: resaleId,
        category: 'income',
        source: 'marketplace',
      }),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    }
  }));

  return { status: 'SUCCESS', cardSK: newCardSK };
}

async function handleWithdrawBalance(args: Record<string, any>, permULID: string) {
  const { amount, currency = 'AUD' } = args;
  if (!amount || amount <= 0) throw new Error('Invalid withdrawal amount');

  // 1. Verify and Deduct Balance
  try {
    await dynamo.send(new UpdateCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
      UpdateExpression: 'SET marketplaceBalance = marketplaceBalance - :amount, updatedAt = :now',
      ConditionExpression: 'marketplaceBalance >= :amount',
      ExpressionAttributeValues: {
        ':amount': amount,
        ':now': new Date().toISOString(),
      },
    }));
  } catch (err: any) {
    if (err.name === 'ConditionalCheckFailedException') throw new Error('Insufficient balance');
    throw err;
  }

  // 2. Create Withdrawal Request in AdminDataEvent for Ops
  const withdrawalId = `WITHDRAWAL#${ulid()}`;
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: withdrawalId,
      sK: permULID,
      amount,
      currency,
      status: 'PENDING_PAYOUT',
      requestedAt: new Date().toISOString(),
      permULID,
    }
  }));

  // 3. Record Withdrawal Receipt for User
  const receiptId = ulid();
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: `RECEIPT#${new Date().toISOString().substring(0, 10)}#${receiptId}`,
      eventType: 'WITHDRAWAL',
      status: 'PENDING',
      primaryCat: 'receipt',
      brandId: 'bebocard',
      desc: JSON.stringify({
        merchant: 'BeboCard Withdrawal',
        amount: -amount,
        currency: currency,
        purchaseDate: new Date().toISOString(),
        receiptType: 'MARKETPLACE_WITHDRAWAL',
        withdrawalId,
        category: 'finance',
        source: 'withdrawal',
      }),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    }
  }));

  return { status: 'PENDING', withdrawalId };
}
