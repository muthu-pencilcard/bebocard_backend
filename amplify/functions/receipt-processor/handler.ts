import type { SQSHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, GetCommand } from '@aws-sdk/lib-dynamodb';
import { KMSClient, SignCommand } from '@aws-sdk/client-kms';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';
import { createHash } from 'crypto';
import { monotonicFactory } from 'ulid';
import { idempotentPut } from '../shared/idempotency';
import { enrichReceipt } from './receipt-enricher';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const kms = new KMSClient({});
const ulid = monotonicFactory();

const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const SIGNING_KEY_ID = process.env.RECEIPT_SIGNING_KEY_ID!;

async function signReceipt(data: string): Promise<string> {
  const res = await kms.send(new SignCommand({
    KeyId: SIGNING_KEY_ID,
    Message: Buffer.from(data),
    MessageType: 'RAW',
    SigningAlgorithm: 'RSASSA_PSS_SHA_256',
  }));
  return Buffer.from(res.Signature!).toString('base64');
}

function getFirebaseAdmin() {
  if (getApps().length === 0) {
    const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    if (!serviceAccountJson) throw new Error('FIREBASE_SERVICE_ACCOUNT_JSON not set');
    initializeApp({ credential: cert(JSON.parse(serviceAccountJson)) });
  }
  return getMessaging();
}

export const handler: SQSHandler = async (event) => {
  for (const record of event.Records) {
    try {
      const payload = JSON.parse(record.body);
      await processReceipt(payload);
    } catch (err) {
      console.error('[receipt-processor] Failed to process record:', record.messageId, err);
      // Throwing error here will cause SQS to retry based on visibility timeout/maxReceiveCount
      throw err;
    }
  }
};

interface ReceiptTask {
  permULID: string;
  brandId: string;
  merchant: string;
  amount: number;
  purchaseDate: string;
  currency?: string;
  loyaltyCardId?: string;
  pointsEarned?: number;
  items?: any[];
  category?: string;
  notes?: string;
  secondaryULID: string;
  isInvoice: boolean;
  owner?: string;
  idempotencyKey?: string; // Optional brand-supplied key
  supplierTaxId?: string;
  supplierTaxIdType?: string;
}

async function processReceipt(task: ReceiptTask) {
  const { permULID, brandId, merchant, amount, purchaseDate, isInvoice, idempotencyKey: brandIdempKey } = task;
  const isAnonymous = permULID.startsWith('ANON#');
  const targetTable = isAnonymous ? REFDATA_TABLE : USER_TABLE;
  
  // 1. Generate core idempotency key if brand didn't supply one
  const computedIdempKey = createHash('sha256')
    .update(`${permULID}|${brandId}|${purchaseDate.substring(0, 10)}|${merchant}|${amount}`)
    .digest('hex');
    
  const idempotencyKey = brandIdempKey || computedIdempKey;
  const itemTag = isInvoice ? 'INVOICE' : 'RECEIPT';
  const prefix = isInvoice ? 'invoice' : 'receipt';

  // 2. Check/Write Idempotency Sentinel
  const sentinelSK = `${itemTag}_IDEM#${idempotencyKey}`;
  const sentinel = {
    pK: isAnonymous ? permULID : `USER#${permULID}`,
    sK: sentinelSK,
    eventType: `${itemTag}_IDEM`,
    status: 'ACTIVE',
    owner: task.owner,
    createdAt: new Date().toISOString(),
  };

  const sentinelResult = await idempotentPut(dynamo, targetTable, sentinel);
  
  let receiptSK: string;
  if (!sentinelResult.success && sentinelResult.item) {
    receiptSK = sentinelResult.item.receiptSK as string;
    if (receiptSK) {
      console.log('[receipt-processor] Duplicate detected, skipping body write:', receiptSK);
      return; 
    }
  }

  // 3. Prepare Receipt Body & Enrichment
  const enrichment = enrichReceipt(task.items ?? []);
  receiptSK = isAnonymous ? 'receipt' : `${itemTag}#${purchaseDate.substring(0, 10)}#${ulid()}`;
  
  // Generate signature
  const canonicalData = `${permULID}|${merchant}|${amount}|${purchaseDate}|${brandId}`;
  const signature = await signReceipt(canonicalData).catch(err => {
    console.error('[receipt-processor] Signing failed:', err);
    return 'SIGNATURE_FAILED';
  });
  
  // Update sentinel with the mapping
  await dynamo.send(new PutCommand({
    TableName: targetTable,
    Item: {
      ...sentinel,
      receiptSK,
      updatedAt: new Date().toISOString(),
    }
  }));

  // 4. Write Receipt Record
  const now = new Date().toISOString();
  await dynamo.send(new PutCommand({
    TableName: targetTable,
    Item: {
      pK: isAnonymous ? permULID : `USER#${permULID}`,
      sK: receiptSK,
      eventType: itemTag,
      status: 'ACTIVE',
      primaryCat: prefix,
      subCategory: brandId,
      owner: task.owner,
      desc: JSON.stringify({
        merchant,
        amount,
        currency: task.currency ?? 'AUD',
        purchaseDate,
        brandId,
        loyaltyCardId: task.loyaltyCardId ?? null,
        pointsEarned: task.pointsEarned ?? null,
        items: task.items ?? [],
        category: task.category ?? 'other',
        notes: task.notes ?? null,
        source: 'brand_push',
        secondaryULID: task.secondaryULID ?? null,
        supplierTaxId: task.supplierTaxId ?? null,
        supplierTaxIdType: task.supplierTaxIdType ?? null,
        signature,
        signingAlgorithm: 'RSASSA_PSS_SHA_256',
        enrichment,
        isSandbox: (task as any).isSandbox ?? false,
        ...(isAnonymous ? { claimToken: (task as any).claimToken, isAnonymous: true } : {})
      }),
      createdAt: now,
      updatedAt: now,
      // 7-year TTL for real users (P3-3), 30-day TTL for anonymous (P1-9)
      exp: Math.floor(Date.now() / 1000) + (isAnonymous ? (60 * 60 * 24 * 30) : (60 * 60 * 24 * 365 * 7)),
    },
  }));

  // 5. Push Notification (Skip for anonymous)
  if (isAnonymous) {
    console.info(`[receipt-processor] Written anonymous receipt to ${targetTable} for ${permULID}`);
    return;
  }

  const deviceToken = await getDeviceToken(`USER#${permULID}`);
  if (deviceToken) {
    const label = isInvoice ? 'Invoice' : 'Receipt';
    const title = `${label} from ${merchant}`;
    let notifBody = task.pointsEarned
      ? `$${amount} · ${task.pointsEarned} pts earned`
      : `$${amount}`;
    
    if (enrichment.totalSavings > 0) {
      notifBody += ` · $${enrichment.totalSavings} saved!`;
    }
    try {
      await getFirebaseAdmin().send({
        token: deviceToken,
        notification: { title, body: notifBody },
        data: {
          type: prefix,
          receiptSK,
          brandId,
          merchant,
          amount: String(amount),
        },
        apns: { payload: { aps: { alert: { title, body: notifBody }, sound: 'default' } } },
        android: { priority: 'high', notification: { channelId: `bebo_${prefix}s` } },
      });
    } catch (e) {
      console.error('[receipt-processor] FCM send failed:', e);
    }
  }
}

async function getDeviceToken(pK: string): Promise<string | null> {
  const result = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK, sK: 'DEVICE_TOKEN' },
  }));
  if (!result.Item) return null;
  const desc = JSON.parse(result.Item.desc ?? '{}');
  return desc.token ?? null;
}
