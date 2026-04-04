/**
 * enrollment-handler — Enrollment Marketplace (Patent Claims 65–72)
 *
 * Brands deliver enrollment offers to BeboCard users via the scan channel.
 * The user accepts or declines within the app. On acceptance, BeboCard generates
 * a pseudonymous email alias and delivers it to the brand's webhook — the brand
 * never learns the user's real email or phone number.
 *
 * Alias format: bebo_<sha256(permULID:brandId).slice(0,16)>@relay.bebocard.com
 * Deterministic — same user + brand always produces the same alias, preventing
 * duplicate enrollment. BeboCard holds the alias→real-email mapping privately.
 *
 * Routes:
 *   POST /enroll                        Brand sends enrollment offer to user
 *   GET  /enroll/{enrollmentId}/status  Brand checks enrollment status + alias
 *
 * AppSync mutations (via card-manager):
 *   respondToEnrollment(enrollmentId, accepted)   User accepts or declines
 *
 * Records:
 *   AdminDataEvent  pK: ENROLL#<enrollmentId>  sK: <permULID>
 *   UserDataEvent   pK: USER#<permULID>         sK: ENROLL#<brandId>#<enrollmentId>
 *
 * CPA tracking:
 *   AdminDataEvent  pK: CPA#<brandId>  sK: <enrollmentId>
 */

import { createHash } from 'crypto';
import https from 'https';
import type { APIGatewayProxyHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, QueryCommand,
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
    if (method === 'POST' && path.endsWith('/enroll'))
      return handleEnroll(event);

    const statusMatch = path.match(/\/enroll\/([^/]+)\/status$/);
    if (method === 'GET' && statusMatch)
      return handleStatus(event, statusMatch[1]);

    return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Not found' }) };
  } catch (e) {
    console.error('[enrollment-handler]', e);
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Internal error' }) };
  }
};

// ── POST /enroll ──────────────────────────────────────────────────────────────

interface EnrollBody {
  secondaryULID:      string;
  programName:        string;
  programDescription?: string;
  rewardDescription?:  string;
}

async function handleEnroll(event: Parameters<APIGatewayProxyHandler>[0]) {
  const rawKey   = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'enrollment') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid or missing API key' }) };

  const body: Partial<EnrollBody> = JSON.parse(event.body ?? '{}');
  const { secondaryULID, programName } = body;

  if (!secondaryULID || !programName) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing required fields: secondaryULID, programName' }) };
  }

  const permULID = await resolvePermULID(secondaryULID);
  if (!permULID) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'User not found' }) };

  const enrollmentId = ulid();
  const now = new Date().toISOString();

  // Check for duplicate: same user + brand already has a PENDING or ACCEPTED enrollment
  const existing = await findExistingEnrollment(permULID, validKey.brandId);
  if (existing === 'ACCEPTED') {
    return { statusCode: 409, headers: CORS, body: JSON.stringify({ error: 'User is already enrolled in this program' }) };
  }

  // Store enrollment record in AdminDataEvent (brand polls this for status)
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK:          `ENROLL#${enrollmentId}`,
      sK:          permULID,
      eventType:   'ENROLLMENT',
      status:      'PENDING',
      brandId:     validKey.brandId,
      programName,
      programDescription: body.programDescription ?? null,
      rewardDescription:  body.rewardDescription  ?? null,
      createdAt:   now,
      updatedAt:   now,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  // FCM push to notify user
  const deviceToken = await getDeviceToken(permULID);
  if (deviceToken) {
    const brandName = await getBrandName(validKey.brandId);
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
          type:               'ENROLLMENT_OFFER',
          enrollmentId,
          brandId:            validKey.brandId,
          brandName,
          programName,
          programDescription: body.programDescription ?? '',
          rewardDescription:  body.rewardDescription  ?? '',
        },
        notification: {
          title: `Join ${brandName} rewards`,
          body:  programName + (body.rewardDescription ? ` — ${body.rewardDescription}` : ''),
        },
        android: { priority: 'high' },
        apns:    { payload: { aps: { contentAvailable: true, sound: 'default' } } },
      });
    } catch (e) {
      console.error('[enrollment-handler] FCM push failed', e);
    }
  }

  return {
    statusCode: 201,
    headers: CORS,
    body: JSON.stringify({ enrollmentId, status: 'PENDING' }),
  };
}

// ── GET /enroll/{enrollmentId}/status ─────────────────────────────────────────

async function handleStatus(
  event: Parameters<APIGatewayProxyHandler>[0],
  enrollmentId: string,
) {
  const rawKey   = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'enrollment') : null;
  if (!validKey) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Unauthorized' }) };

  const res = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `ENROLL#${enrollmentId}` },
    Limit: 1,
  }));
  const item = res.Items?.[0];
  if (!item) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Enrollment not found' }) };

  // Brand can only query their own enrollments
  if (item.brandId !== validKey.brandId) {
    return { statusCode: 403, headers: CORS, body: JSON.stringify({ error: 'Forbidden' }) };
  }

  const payload: Record<string, unknown> = {
    enrollmentId,
    status:    item.status,
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
  };
  // Only expose alias once enrollment is accepted
  if (item.status === 'ACCEPTED' && item.alias) {
    payload.alias = item.alias;
  }

  return { statusCode: 200, headers: CORS, body: JSON.stringify(payload) };
}

// ── Exported helper called by card-manager for respondToEnrollment ─────────────

export async function respondToEnrollmentFn(
  ddb: DynamoDBDocumentClient,
  permULID: string,
  enrollmentId: string,
  accepted: boolean,
): Promise<{ ok: boolean; alias?: string }> {
  const res = await ddb.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `ENROLL#${enrollmentId}` },
    Limit: 1,
  }));
  const item = res.Items?.[0];
  if (!item) throw new Error('Enrollment not found');
  if (item.sK !== permULID) throw new Error('Enrollment does not belong to this user');
  if (item.status !== 'PENDING') throw new Error(`Enrollment already ${item.status}`);

  const now = new Date().toISOString();

  if (!accepted) {
    await ddb.send(new UpdateCommand({
      TableName: ADMIN_TABLE,
      Key: { pK: `ENROLL#${enrollmentId}`, sK: permULID },
      UpdateExpression: 'SET #s = :s, updatedAt = :now',
      ExpressionAttributeNames: { '#s': 'status' },
      ExpressionAttributeValues: { ':s': 'DECLINED', ':now': now },
    }));
    // Record in UserDataEvent for consent history
    await ddb.send(new PutCommand({
      TableName: USER_TABLE,
      Item: {
        pK:          `USER#${permULID}`,
        sK:          `ENROLL#${item.brandId}#${enrollmentId}`,
        eventType:   'ENROLLMENT',
        status:      'DECLINED',
        primaryCat:  'enrollment',
        subCategory: item.brandId,
        desc: JSON.stringify({ enrollmentId, brandId: item.brandId, programName: item.programName, respondedAt: now }),
        createdAt:   now,
        updatedAt:   now,
      },
    }));
    await callBrandWebhook(ddb, item.brandId as string, { enrollmentId, status: 'DECLINED', declinedAt: now });
    return { ok: true };
  }

  // Generate deterministic pseudonymous alias
  const alias = generateAlias(permULID, item.brandId as string);

  // Mark accepted + store alias in AdminDataEvent
  await ddb.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `ENROLL#${enrollmentId}`, sK: permULID },
    UpdateExpression: 'SET #s = :s, alias = :alias, acceptedAt = :now, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':s': 'ACCEPTED', ':alias': alias, ':now': now },
  }));

  // Record in UserDataEvent — shown in app's enrollment history
  await ddb.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK:          `USER#${permULID}`,
      sK:          `ENROLL#${item.brandId}#${enrollmentId}`,
      eventType:   'ENROLLMENT',
      status:      'ACCEPTED',
      primaryCat:  'enrollment',
      subCategory: item.brandId,
      desc: JSON.stringify({
        enrollmentId,
        brandId:     item.brandId,
        programName: item.programName,
        alias,
        respondedAt: now,
      }),
      createdAt:   now,
      updatedAt:   now,
    },
  }));

  // CPA tracking record
  await ddb.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK:          `CPA#${item.brandId}`,
      sK:          enrollmentId,
      eventType:   'CPA_ENROLLMENT',
      status:      'PENDING_VERIFICATION',
      enrollmentId,
      brandId:     item.brandId,
      permULID,
      alias,
      createdAt:   now,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  // Deliver alias + enrollment confirmation to brand webhook
  await callBrandWebhook(ddb, item.brandId as string, {
    enrollmentId,
    status:     'ACCEPTED',
    alias,
    programName: item.programName,
    acceptedAt:  now,
  });

  return { ok: true, alias };
}

// ── Alias generation ──────────────────────────────────────────────────────────

/** Deterministic pseudonymous email alias. Same user+brand always produces same alias. */
export function generateAlias(permULID: string, brandId: string): string {
  const hash = createHash('sha256').update(`${permULID}:${brandId}`).digest('hex').slice(0, 16);
  return `bebo_${hash}@relay.bebocard.com`;
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

async function getBrandName(brandId: string): Promise<string> {
  const res = await dynamo.send(new GetCommand({
    TableName: REF_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'PROFILE' },
  }));
  const desc = JSON.parse(res.Item?.desc ?? '{}');
  return (desc.brandName as string) ?? brandId;
}

async function findExistingEnrollment(permULID: string, brandId: string): Promise<string | null> {
  const res = await dynamo.send(new QueryCommand({
    TableName: USER_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    FilterExpression: '#s = :accepted',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: {
      ':pk':       `USER#${permULID}`,
      ':prefix':   `ENROLL#${brandId}#`,
      ':accepted': 'ACCEPTED',
    },
    Limit: 1,
  }));
  return (res.Items?.[0]?.status as string) ?? null;
}

async function callBrandWebhook(
  ddb: DynamoDBDocumentClient,
  brandId: string,
  payload: Record<string, unknown>,
): Promise<void> {
  const ref = await ddb.send(new GetCommand({
    TableName: REF_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'PROFILE' },
  }));
  const desc = JSON.parse(ref.Item?.desc ?? '{}');
  const webhookUrl = desc.enrollmentWebhookUrl as string | undefined;
  if (!webhookUrl) return;

  try {
    const url = new URL(webhookUrl);
    if (url.protocol !== 'https:') return;
    const body = JSON.stringify(payload);
    await new Promise<void>((resolve, reject) => {
      const req = https.request(
        { hostname: url.hostname, path: url.pathname + url.search, method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } },
        (res) => { res.resume(); res.on('end', resolve); },
      );
      req.on('error', reject);
      req.write(body);
      req.end();
    });
  } catch (e) {
    console.error('[enrollment-handler] webhook call failed', e);
  }
}
