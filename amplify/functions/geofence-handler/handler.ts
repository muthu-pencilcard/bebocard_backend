/**
 * geofence-handler Lambda
 *
 * Receives geofence entry events from the app and:
 *   1. Logs the store visit to DynamoDB (for frequency tracking)
 *   2. Applies personalisation rules to select the best offer:
 *        - Frequency bonus: user visited this brand > 2× this month
 *        - Broadcast offers: active tenant campaigns right now
 *        - Fallback: generic "your loyalty card is ready" message
 *   3. Fetches the user's FCM/APNs device token from DynamoDB
 *   4. Sends a push notification via Firebase Admin SDK (FCM covers iOS APNs too)
 *
 * Also handles:
 *   - registerDeviceToken  — store FCM token against permULID
 *   - unregisterDeviceToken — remove token on sign-out
 *   - getNearbyStores       — returns store locations within radiusKm
 */

import { DynamoDBClient, PutItemCommand, QueryCommand, GetItemCommand, AttributeValue } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';
import { withGraphQLHandler } from '../../shared/secure-handler-wrapper';

const ddb = new DynamoDBClient({});
const USER_TABLE = process.env.USER_TABLE!;
const REF_TABLE = process.env.REF_TABLE!;

// Initialise Firebase Admin once (Lambda warm starts reuse this)
function getFirebaseAdmin() {
  if (getApps().length === 0) {
    const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    if (!serviceAccountJson) throw new Error('FIREBASE_SERVICE_ACCOUNT_JSON env var not set');
    initializeApp({ credential: cert(JSON.parse(serviceAccountJson)) });
  }
  return getMessaging();
}

export const handler = withGraphQLHandler(async (event) => {
  const fieldName = event.info.fieldName;
  const args = event.arguments;

  // AppSync types event.arguments as Record<string,unknown>; cast at the dispatch boundary.
  // Each handler validates its own required fields at runtime.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const a = args as any;
  switch (fieldName) {
    case 'reportGeofenceEntry': return reportGeofenceEntry(a);
    case 'registerDeviceToken': return registerDeviceToken(a);
    case 'unregisterDeviceToken': return unregisterDeviceToken(a);
    case 'getNearbyStores': return getNearbyStores(a);
    default:
      throw new Error(`Unknown field: ${fieldName}`);
  }
});

// ─── reportGeofenceEntry ─────────────────────────────────────────────────────

async function reportGeofenceEntry(args: {
  secondaryULID: string;
  geofenceId: string;
  entryTime: string;
}) {
  const { secondaryULID, geofenceId, entryTime } = args;

  // Resolve secondaryULID → permULID via AdminDataEvent.
  // Device never sends permULID or explicit brand/store — server resolves from its own records.
  const adminResult = await ddb.send(new QueryCommand({
    TableName: process.env.ADMIN_TABLE!,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: marshall({ ':pk': `SCAN#${secondaryULID}` }),
    Limit: 1,
  }));
  const adminItem = adminResult.Items?.[0];
  if (!adminItem) {
    console.warn(`[geofence-handler] secondaryULID not found: ${secondaryULID}`);
    return 'ULID_NOT_FOUND';
  }
  const permULID = unmarshall(adminItem).sK as string; // sK = permULID per key design

  // Parse geofenceId (format: STORE#<brandId>#<storeId>) to get brand and store.
  const parts = geofenceId.split('#');
  const brandId = parts[1] ?? '';
  const storeId = parts[2] ?? '';

  const pK = `USER#${permULID}`;

  // 1. Write the visit event (used for frequency calculation)
  const visitSK = `VISIT#${brandId}#${entryTime}`;
  try {
    await ddb.send(new PutItemCommand({
      TableName: USER_TABLE,
      Item: marshall({
        pK,
        sK: visitSK,
        primaryCat: 'store_visit',
        status: 'ACTIVE',
        desc: JSON.stringify({ brandId, storeId, geofenceId, entryTime }),
        createdAt: entryTime,
      }),
      ConditionExpression: 'attribute_not_exists(sK)', // idempotent
    }));
  } catch {
    // ignore ConditionCheckFailedException (duplicate entry)
  }

  // 2. Count visits this month for this brand
  const monthPrefix = entryTime.substring(0, 7); // "YYYY-MM"
  const visitCount = await countVisitsThisMonth(pK, brandId, monthPrefix);

  // 3. Fetch active broadcast offer from the brand (if any)
  const broadcastOffer = await getActiveBroadcastOffer(brandId);

  // 4. Fetch user's device token
  const deviceToken = await getDeviceToken(pK);
  if (!deviceToken) return 'NO_TOKEN'; // user hasn't registered a token yet

  // 5. Build personalised notification
  const notification = buildNotification({ brandId, visitCount, broadcastOffer });

  // 6. Send push via FCM (covers iOS APNs natively)
  try {
    const messaging = getFirebaseAdmin();
    await messaging.send({
      token: deviceToken,
      notification: { title: notification.title, body: notification.body },
      data: {
        type: 'geofence_arrival',
        brandId,
        offerId: broadcastOffer?.offerId ?? '',
        voucherCode: broadcastOffer?.voucherCode ?? '',
        isPersonalised: visitCount > 2 ? 'true' : 'false',
      },
      apns: { payload: { aps: { alert: { title: notification.title, body: notification.body }, sound: 'default' } } },
      android: { priority: 'high', notification: { channelId: 'bebo_offers' } },
    });
    return 'SENT';
  } catch (e) {
    console.error('FCM send failed:', e);
    return 'FCM_ERROR';
  }
}

// ─── Personalisation rules ────────────────────────────────────────────────────

function buildNotification(params: {
  brandId: string;
  visitCount: number;
  broadcastOffer: BroadcastOffer | null;
}): { title: string; body: string } {
  const { visitCount, broadcastOffer } = params;

  // Frequency bonus — more than 2 visits this calendar month
  if (visitCount > 2 && broadcastOffer) {
    return {
      title: `${broadcastOffer.brandName} — Loyal customer offer 🎁`,
      body: `You're a regular! ${broadcastOffer.headline}`,
    };
  }

  if (broadcastOffer) {
    return {
      title: broadcastOffer.brandName,
      body: broadcastOffer.headline,
    };
  }

  // Generic fallback — no active offer configured by tenant
  return {
    title: 'Your loyalty card is ready',
    body: 'Tap to show your card at checkout.',
  };
}

// ─── registerDeviceToken ──────────────────────────────────────────────────────

async function registerDeviceToken(args: {
  token: string;
  platform: string;
  permULID?: string;
}) {
  const { token, platform, permULID } = args;
  if (!permULID) return 'QUEUED'; // user not signed in yet — token stored anonymously later

  await ddb.send(new PutItemCommand({
    TableName: USER_TABLE,
    Item: marshall({
      pK: `USER#${permULID}`,
      sK: 'DEVICE_TOKEN',
      primaryCat: 'device',
      status: 'ACTIVE',
      desc: JSON.stringify({ token, platform, updatedAt: new Date().toISOString() }),
      createdAt: new Date().toISOString(),
    }),
  }));
  return 'OK';
}

// ─── unregisterDeviceToken ────────────────────────────────────────────────────

async function unregisterDeviceToken(args: { token: string; permULID: string }) {
  const { permULID } = args;
  // Overwrite DEVICE_TOKEN record with status=INACTIVE so FCM pushes stop
  // after sign-out. Token field is cleared to prevent accidental re-use.
  await ddb.send(new PutItemCommand({
    TableName: USER_TABLE,
    Item: marshall({
      pK: `USER#${permULID}`,
      sK: 'DEVICE_TOKEN',
      primaryCat: 'device',
      status: 'INACTIVE',
      desc: JSON.stringify({ token: null, updatedAt: new Date().toISOString() }),
      createdAt: new Date().toISOString(),
    }),
  }));
  return 'OK';
}

// ─── getNearbyStores ──────────────────────────────────────────────────────────

async function getNearbyStores(args: {
  brandId: string;
  lat: number;
  lng: number;
  radiusKm: number;
  limit: number;
}) {
  const { brandId, lat, lng, radiusKm, limit } = args;

  // Query store locations from RefDataEvent: pK=STORES, sK begins_with STORE#<brandId>#
  const result = await ddb.send(new QueryCommand({
    TableName: REF_TABLE,
    KeyConditionExpression: 'pK = :pK AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: marshall({
      ':pK': 'STORES',
      ':prefix': `STORE#${brandId}#`,
    }),
  }));

  type StoreRow = { sK: string; desc: string;[k: string]: unknown };
  const stores = (result.Items ?? []).map((i: Record<string, AttributeValue>) => unmarshall(i) as StoreRow);

  // Filter by haversine distance
  const nearby = stores
    .filter((s: StoreRow) => {
      const desc = JSON.parse(s.desc ?? '{}');
      return haversineKm(lat, lng, desc.lat, desc.lng) <= radiusKm;
    })
    .sort((a: StoreRow, b: StoreRow) => {
      const da = JSON.parse(a.desc ?? '{}');
      const db_ = JSON.parse(b.desc ?? '{}');
      return haversineKm(lat, lng, da.lat, da.lng) - haversineKm(lat, lng, db_.lat, db_.lng);
    })
    .slice(0, limit);

  return nearby.map((s: StoreRow) => ({ sK: s.sK, desc: s.desc }));
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function countVisitsThisMonth(pK: string, brandId: string, monthPrefix: string): Promise<number> {
  const result = await ddb.send(new QueryCommand({
    TableName: USER_TABLE,
    KeyConditionExpression: 'pK = :pK AND begins_with(sK, :prefix)',
    FilterExpression: 'primaryCat = :cat',
    ExpressionAttributeValues: marshall({
      ':pK': pK,
      ':prefix': `VISIT#${brandId}#${monthPrefix}`,
      ':cat': 'store_visit',
    }),
    Select: 'COUNT',
  }));
  return result.Count ?? 0;
}

interface BroadcastOffer {
  offerId: string;
  brandId: string;
  brandName: string;
  headline: string;
  voucherCode?: string;
  expiresAt?: string;
}

async function getActiveBroadcastOffer(brandId: string): Promise<BroadcastOffer | null> {
  const result = await ddb.send(new QueryCommand({
    TableName: REF_TABLE,
    KeyConditionExpression: 'pK = :pK AND begins_with(sK, :prefix)',
    FilterExpression: '#status = :active',
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: marshall({
      ':pK': `BRAND#${brandId}`,
      ':prefix': 'OFFER#',
      ':active': 'ACTIVE',
    }),
    Limit: 1,
  }));

  const item = result.Items?.[0];
  if (!item) return null;
  const row = unmarshall(item);
  const desc = JSON.parse(row.desc ?? '{}');

  // Check offer hasn't expired
  if (desc.expiresAt && new Date(desc.expiresAt) < new Date()) return null;

  return {
    offerId: row.sK,
    brandId: desc.brandId ?? brandId,
    brandName: desc.brandName ?? brandId,
    headline: desc.headline ?? '',
    voucherCode: desc.voucherCode,
    expiresAt: desc.expiresAt,
  };
}

async function getDeviceToken(pK: string): Promise<string | null> {
  const result = await ddb.send(new GetItemCommand({
    TableName: USER_TABLE,
    Key: marshall({ pK, sK: 'DEVICE_TOKEN' }),
  }));
  if (!result.Item) return null;
  const row = unmarshall(result.Item);
  const desc = JSON.parse(row.desc ?? '{}');
  return desc.token ?? null;
}

function haversineKm(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function toRad(deg: number) { return deg * Math.PI / 180; }
