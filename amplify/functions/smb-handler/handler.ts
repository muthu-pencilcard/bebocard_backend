/**
 * smb-handler — Phase 11 (SMB Loyalty-as-a-Service)
 *
 * Self-serve stamp card loyalty program for small brands.
 * Brands stamp user cards at checkout; users redeem rewards at goal threshold.
 *
 * Routes:
 *   POST /smb/stamp    — Brand stamps a user's card
 *   POST /smb/redeem   — Brand redeems a completed stamp card
 *   GET  /smb/card     — Brand checks a user's stamp card status
 *   GET  /smb/analytics — Brand retrieves SMB lite analytics
 *
 * SMB_CONFIG record in RefDataEvent:
 *   pK: BRAND#<brandId>
 *   sK: SMB_CONFIG
 *   desc: { goal, rewardDescription, tier, monthlyQuota, stampsThisMonth, quotaMonthKey }
 *
 * Stamp record in UserDataEvent:
 *   pK: USER#<permULID>
 *   sK: STAMP#<brandId>
 *   primaryCat: stamp_card
 *   desc: { brandId, brandName, brandColor, stamps, goal, status, rewardDescription, redemptions, lastStampAt }
 *
 * Redemption record in AdminDataEvent:
 *   pK: REDEEM#<brandId>
 *   sK: <ISO8601>#<permULID>
 */

import type { APIGatewayProxyHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  UpdateCommand,
  QueryCommand,
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

// Tier → monthly stamp quota
const TIER_QUOTAS: Record<string, number> = {
  starter:  500,
  growth:   2000,
  business: 999_999,
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
    if (method === 'POST' && path.endsWith('/smb/stamp'))    return handleStamp(event);
    if (method === 'POST' && path.endsWith('/smb/redeem'))   return handleRedeem(event);
    if (method === 'GET'  && path.endsWith('/smb/card'))     return handleGetCard(event);
    if (method === 'GET'  && path.endsWith('/smb/analytics')) return handleAnalytics(event);
    if (method === 'POST' && path.endsWith('/smb/offer'))    return handleUpsertOffer(event);
    if (method === 'GET'  && path.endsWith('/smb/offer'))    return handleGetOffer(event);

    return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Not found' }) };
  } catch (e) {
    console.error('[smb-handler]', e);
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Internal error' }) };
  }
};

// ── POST /smb/stamp ───────────────────────────────────────────────────────────

interface StampBody {
  secondaryULID: string;
  brandId?:      string;
}

async function handleStamp(event: Parameters<APIGatewayProxyHandler>[0]) {
  const rawKey   = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'smb') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or missing API key' }) };

  const body: Partial<StampBody> = JSON.parse(event.body ?? '{}');
  const { secondaryULID } = body;
  if (!secondaryULID) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing required field: secondaryULID' }) };
  }

  const brandId = body.brandId ?? validKey.brandId;

  const permULID = await resolvePermULID(secondaryULID);
  if (!permULID) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'User not found' }) };

  // Load SMB config
  const config = await getSmbConfig(brandId);
  if (!config) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'SMB config not found for brand' }) };

  // Validate monthly quota
  const quotaErr = validateStampQuota(config);
  if (quotaErr) return { statusCode: 429, headers: CORS, body: JSON.stringify({ error: quotaErr }) };

  // Fetch brand profile for display fields
  const brandRef = await dynamo.send(new GetCommand({
    TableName: REF_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'PROFILE' },
  }));
  const brandDesc  = safeJson(brandRef.Item?.desc);
  const brandName  = (brandDesc.brandName  as string | undefined) ?? brandId;
  const brandColor = (brandDesc.brandColor as string | undefined) ?? '#6366F1';

  const now  = new Date().toISOString();

  // Ensure stamp record exists (create on first stamp, no-op if already exists)
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK:         `USER#${permULID}`,
      sK:         `STAMP#${brandId}`,
      eventType:  'STAMP_CARD',
      status:     'ACTIVE',
      primaryCat: 'stamp_card',
      subCategory: brandId,
      desc: JSON.stringify({
        brandId,
        brandName,
        brandColor,
        stamps: 0,
        goal:   config.goal,
        status: 'ACTIVE',
        rewardDescription: config.rewardDescription,
        redemptions: 0,
        lastStampAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  })).catch(() => { /* already exists — that's fine */ });

  // Increment stamps using ADD expression (atomic increment)
  const updateRes = await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `STAMP#${brandId}` },
    UpdateExpression: 'ADD stamps :one SET updatedAt = :now, lastStampAt = :now',
    ExpressionAttributeValues: { ':one': 1, ':now': now },
    ReturnValues: 'ALL_NEW',
  }));

  const updatedDesc = safeJson(updateRes.Attributes?.desc);
  const newStamps   = (updatedDesc.stamps as number | undefined) ?? 0;
  const goal        = config.goal;
  const rewardDescription = config.rewardDescription;
  let   stampStatus = 'ACTIVE';

  if (newStamps >= goal) {
    // Mark card as REDEEMABLE
    stampStatus = 'REDEEMABLE';
    await dynamo.send(new UpdateCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: `STAMP#${brandId}` },
      UpdateExpression: 'SET #st = :redeemable, updatedAt = :now',
      ExpressionAttributeNames:  { '#st': 'status' },
      ExpressionAttributeValues: { ':redeemable': 'REDEEMABLE', ':now': now },
    }));

    // FCM push to user
    const deviceToken = await getDeviceToken(permULID);
    if (deviceToken) {
      await sendFcmPush(deviceToken, {
        type:      'STAMP_REWARD',
        brandId,
        brandName,
      }, {
        title: "You've earned a reward!",
        body:  `${brandName} — ${rewardDescription}`,
      });
    }
  }

  // Increment monthly stamp counter in SMB_CONFIG
  const currentMonth = now.slice(0, 7); // 'YYYY-MM'
  await incrementMonthlyStamp(brandId, currentMonth);

  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({
      stamps:            newStamps,
      goal,
      status:            stampStatus,
      rewardDescription,
    }),
  };
}

// ── POST /smb/redeem ──────────────────────────────────────────────────────────

interface RedeemBody {
  secondaryULID: string;
}

async function handleRedeem(event: Parameters<APIGatewayProxyHandler>[0]) {
  const rawKey   = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'smb') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or missing API key' }) };

  const body: Partial<RedeemBody> = JSON.parse(event.body ?? '{}');
  const { secondaryULID } = body;
  if (!secondaryULID) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing required field: secondaryULID' }) };
  }

  const brandId = validKey.brandId;
  const permULID = await resolvePermULID(secondaryULID);
  if (!permULID) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'User not found' }) };

  // Load stamp card
  const cardItem = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `STAMP#${brandId}` },
  }));
  if (!cardItem.Item) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Stamp card not found' }) };

  const cardStatus = cardItem.Item.status as string;
  if (cardStatus !== 'REDEEMABLE') {
    return { statusCode: 409, headers: CORS, body: JSON.stringify({ error: 'Stamp card is not redeemable yet' }) };
  }

  const now          = new Date().toISOString();
  const redemptionId = ulid();

  // Reset stamps + mark ACTIVE + increment redemptions
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `STAMP#${brandId}` },
    UpdateExpression: 'SET stamps = :zero, #st = :active, updatedAt = :now ADD redemptions :one',
    ExpressionAttributeNames:  { '#st': 'status' },
    ExpressionAttributeValues: { ':zero': 0, ':active': 'ACTIVE', ':now': now, ':one': 1 },
  }));

  // Write redemption record to AdminDataEvent
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK:        `REDEEM#${brandId}`,
      sK:        `${now}#${permULID}`,
      eventType: 'REDEMPTION',
      status:    'REDEEMED',
      desc: JSON.stringify({ redemptionId, brandId, permULID, redeemedAt: now }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({ success: true, redemptionId }),
  };
}

// ── GET /smb/card ─────────────────────────────────────────────────────────────

async function handleGetCard(event: Parameters<APIGatewayProxyHandler>[0]) {
  const rawKey   = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'smb') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or missing API key' }) };

  const secondaryULID = event.queryStringParameters?.secondaryULID;
  if (!secondaryULID) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing required query parameter: secondaryULID' }) };
  }

  const brandId  = validKey.brandId;
  const permULID = await resolvePermULID(secondaryULID);
  if (!permULID) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'User not found' }) };

  const cardItem = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `STAMP#${brandId}` },
  }));

  if (!cardItem.Item) {
    // No card yet — return default state
    const config = await getSmbConfig(brandId);
    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify({
        stamps: 0,
        goal:   config?.goal ?? 10,
        status: 'ACTIVE',
        rewardDescription: config?.rewardDescription ?? '',
        redemptions: 0,
      }),
    };
  }

  const desc = safeJson(cardItem.Item.desc);
  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({
      stamps:            (desc.stamps            as number)  ?? 0,
      goal:              (desc.goal              as number)  ?? 10,
      status:            cardItem.Item.status    as string   ?? 'ACTIVE',
      rewardDescription: (desc.rewardDescription as string)  ?? '',
      redemptions:       (desc.redemptions       as number)  ?? 0,
    }),
  };
}

// ── GET /smb/analytics ────────────────────────────────────────────────────────

async function handleAnalytics(event: Parameters<APIGatewayProxyHandler>[0]) {
  const rawKey   = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'smb') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or missing API key' }) };

  const brandId = validKey.brandId;
  const config  = await getSmbConfig(brandId);

  const analytics = await computeAnalytics(brandId, config);

  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify(analytics),
  };
}

// ── Exported helpers for unit tests ──────────────────────────────────────────

export interface SmbConfig {
  goal:               number;
  rewardDescription:  string;
  tier:               string;
  monthlyQuota:       number;
  stampsThisMonth:    number;
  quotaMonthKey:      string;
}

/**
 * validateStampQuota — returns an error message string if quota exceeded, else null.
 * Exported so it can be unit tested without DynamoDB.
 */
export function validateStampQuota(config: SmbConfig): string | null {
  const currentMonth = new Date().toISOString().slice(0, 7);
  // Reset counter if we've moved into a new month
  const effectiveStamps = config.quotaMonthKey === currentMonth ? config.stampsThisMonth : 0;
  if (effectiveStamps >= config.monthlyQuota) {
    return `Monthly stamp quota of ${config.monthlyQuota} exceeded for tier '${config.tier}'`;
  }
  return null;
}

/**
 * validateStampRecord — validates a stamp record desc shape.
 * Exported for unit testing.
 */
export function validateStampRecord(desc: Record<string, unknown>): boolean {
  return (
    typeof desc.brandId           === 'string' &&
    typeof desc.stamps            === 'number' &&
    typeof desc.goal              === 'number' &&
    typeof desc.status            === 'string' &&
    typeof desc.rewardDescription === 'string'
  );
}

/**
 * computeAnalytics — queries AdminDataEvent REDEEM# records and SMB_CONFIG.
 * Exported for unit testing.
 */
export async function computeAnalytics(
  brandId: string,
  config:  SmbConfig | null,
): Promise<{
  totalStamps:      number;
  totalRedemptions: number;
  activeCards:      number;
  stampsThisMonth:  number;
  quota:            number;
  tier:             string;
}> {
  // Count redemption records
  const redeemRes = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `REDEEM#${brandId}` },
    Select: 'COUNT',
  }));
  const totalRedemptions = redeemRes.Count ?? 0;

  const currentMonth    = new Date().toISOString().slice(0, 7);
  const stampsThisMonth = config?.quotaMonthKey === currentMonth
    ? (config?.stampsThisMonth ?? 0)
    : 0;
  const quota = config?.monthlyQuota ?? TIER_QUOTAS[config?.tier ?? 'starter'] ?? 500;
  const tier  = config?.tier ?? 'starter';

  // Total stamps = historical redemptions × goal + current progress
  // We don't scan all user records for a count — use quota counter as a proxy
  const totalStamps = stampsThisMonth + totalRedemptions * (config?.goal ?? 10);

  return {
    totalStamps,
    totalRedemptions,
    activeCards:     0,    // would require GSI scan — not implemented in lite tier
    stampsThisMonth,
    quota,
    tier,
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

async function getSmbConfig(brandId: string): Promise<SmbConfig | null> {
  const res = await dynamo.send(new GetCommand({
    TableName: REF_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'SMB_CONFIG' },
  }));
  if (!res.Item) return null;
  const d = safeJson(res.Item.desc);
  return {
    goal:              (d.goal              as number) ?? 10,
    rewardDescription: (d.rewardDescription as string) ?? '',
    tier:              (d.tier              as string) ?? 'starter',
    monthlyQuota:      (d.monthlyQuota      as number) ?? TIER_QUOTAS[(d.tier as string) ?? 'starter'] ?? 500,
    stampsThisMonth:   (d.stampsThisMonth   as number) ?? 0,
    quotaMonthKey:     (d.quotaMonthKey     as string) ?? '',
  };
}

async function incrementMonthlyStamp(brandId: string, currentMonth: string): Promise<void> {
  // If month key matches, ADD 1 to stampsThisMonth.
  // If month has changed, reset to 1 and update quotaMonthKey.
  const now = new Date().toISOString();
  try {
    // Attempt increment only if month key matches
    await dynamo.send(new UpdateCommand({
      TableName: REF_TABLE,
      Key: { pK: `BRAND#${brandId}`, sK: 'SMB_CONFIG' },
      UpdateExpression: 'ADD stampsThisMonth :one SET updatedAt = :now',
      ConditionExpression: 'quotaMonthKey = :month',
      ExpressionAttributeValues: { ':one': 1, ':now': now, ':month': currentMonth },
    }));
  } catch (e: unknown) {
    if ((e as { name?: string })?.name === 'ConditionalCheckFailedException') {
      // New month — reset counter
      await dynamo.send(new UpdateCommand({
        TableName: REF_TABLE,
        Key: { pK: `BRAND#${brandId}`, sK: 'SMB_CONFIG' },
        UpdateExpression: 'SET stampsThisMonth = :one, quotaMonthKey = :month, updatedAt = :now',
        ExpressionAttributeValues: { ':one': 1, ':month': currentMonth, ':now': now },
      }));
    }
  }
}

async function getDeviceToken(permULID: string): Promise<string | null> {
  const res = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'DEVICE_TOKEN' },
  }));
  return (safeJson(res.Item?.desc) as { token?: string }).token ?? null;
}

async function sendFcmPush(
  token: string,
  data:  Record<string, string>,
  notification: { title: string; body: string },
): Promise<void> {
  try {
    const { initializeApp, getApps, cert } = await import('firebase-admin/app');
    const { getMessaging }                  = await import('firebase-admin/messaging');
    if (getApps().length === 0) {
      const sa = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
      if (sa) initializeApp({ credential: cert(JSON.parse(sa)) });
    }
    await getMessaging().send({
      token,
      data,
      notification,
      android: { priority: 'high' },
      apns:    { payload: { aps: { contentAvailable: true, sound: 'default' } } },
    });
  } catch (e) {
    console.error('[smb-handler] FCM push failed', e);
  }
}

// ── POST /smb/offer ───────────────────────────────────────────────────────────
// Upsert the brand's one active offer template and fan-out FCM to all subscribers
// with offers:true preference. Replaces any previous offer (SMBs have one at a time).

interface OfferBody {
  title:       string;
  description: string;
  expiresAt?:  string; // ISO 8601 optional
}

async function handleUpsertOffer(event: Parameters<APIGatewayProxyHandler>[0]) {
  const rawKey   = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'smb') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or missing API key' }) };

  const body: Partial<OfferBody> = JSON.parse(event.body ?? '{}');
  if (!body.title?.trim())       return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'title is required' }) };
  if (!body.description?.trim()) return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'description is required' }) };
  if (body.expiresAt && isNaN(Date.parse(body.expiresAt))) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'expiresAt must be a valid ISO 8601 date' }) };
  }

  const brandId = validKey.brandId;
  const now     = new Date().toISOString();

  // Fetch brand display name for the notification
  const brandRef  = await dynamo.send(new GetCommand({ TableName: REF_TABLE, Key: { pK: `BRAND#${brandId}`, sK: 'PROFILE' } }));
  const brandDesc = safeJson(brandRef.Item?.desc);
  const brandName = (brandDesc.brandName as string | undefined) ?? brandId;

  // Upsert SMB_OFFER record in REFDATA
  await dynamo.send(new PutCommand({
    TableName: REF_TABLE,
    Item: {
      pK:         `BRAND#${brandId}`,
      sK:         'SMB_OFFER',
      eventType:  'SMB_OFFER',
      status:     'ACTIVE',
      primaryCat: 'smb_offer',
      brandId,
      desc: JSON.stringify({
        title:       body.title.trim(),
        description: body.description.trim(),
        expiresAt:   body.expiresAt ?? null,
        createdAt:   now,
        brandName,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  // Fan-out FCM to all subscribers with offers:true using sK-pK-index GSI
  const sent = await broadcastOfferToSubscribers(brandId, brandName, body.title.trim(), body.description.trim());

  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({ ok: true, sent }),
  };
}

// ── GET /smb/offer ────────────────────────────────────────────────────────────

async function handleGetOffer(event: Parameters<APIGatewayProxyHandler>[0]) {
  const rawKey   = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'smb') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or missing API key' }) };

  const brandId = validKey.brandId;
  const res = await dynamo.send(new GetCommand({ TableName: REF_TABLE, Key: { pK: `BRAND#${brandId}`, sK: 'SMB_OFFER' } }));

  if (!res.Item) return { statusCode: 200, headers: CORS, body: JSON.stringify(null) };

  const d = safeJson(res.Item.desc);
  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({
      title:       d.title       as string,
      description: d.description as string,
      expiresAt:   d.expiresAt   as string | null,
      createdAt:   d.createdAt   as string,
      brandName:   d.brandName   as string,
    }),
  };
}

// ── Fan-out: broadcast offer FCM to all brand subscribers ─────────────────────

async function broadcastOfferToSubscribers(
  brandId:     string,
  brandName:   string,
  title:       string,
  description: string,
): Promise<number> {
  let sent = 0;
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName:     USER_TABLE,
      IndexName:     'sK-pK-index',
      KeyConditionExpression:    'sK = :sk',
      ExpressionAttributeValues: { ':sk': `SUBSCRIPTION#${brandId}` },
      ExclusiveStartKey: lastKey as Record<string, unknown> | undefined,
      Limit: 100,
    }));

    for (const item of res.Items ?? []) {
      const desc = safeJson(item.desc);
      if (desc.offers === false) continue;
      if (isFutureIso(desc.offersSnoozeUntil)) continue;
      const permULID = (item.pK as string).replace('USER#', '');
      const prefsRes = await dynamo.send(new GetCommand({
        TableName: USER_TABLE,
        Key: { pK: `USER#${permULID}`, sK: 'PREFERENCES' },
      }));
      const prefs = safeJson(prefsRes.Item?.desc);
      if (isFutureIso(prefs.offersGlobalSnoozeUntil)) continue;
      const deviceToken = await getDeviceToken(permULID);
      if (!deviceToken) continue;
      await sendFcmPush(
        deviceToken,
        { type: 'NEW_OFFER', brandId, brandName },
        { title: `${brandName}: ${title}`, body: description },
      );
      sent++;
    }

    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  return sent;
}

function safeJson(value: unknown): Record<string, unknown> {
  if (typeof value === 'object' && value !== null) return value as Record<string, unknown>;
  if (typeof value !== 'string' || !value) return {};
  try { return JSON.parse(value) as Record<string, unknown>; }
  catch { return {}; }
}

function isFutureIso(value: unknown): boolean {
  if (typeof value !== 'string' || value.length === 0) return false;
  const ts = Date.parse(value);
  return !Number.isNaN(ts) && ts > Date.now();
}
