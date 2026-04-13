import type { APIGatewayProxyHandler } from 'aws-lambda';
import { createHash } from 'crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';
import { monotonicFactory } from 'ulid';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { KMSClient, GetPublicKeyCommand } from '@aws-sdk/client-kms';
import { withAuditLog } from '../../shared/audit-logger';
import { validateApiKey, extractApiKey } from '../../shared/api-key-auth';
import { 
  getTenantStateForBrand, 
  checkTenantQuota, 
  incrementTenantUsageCounter 
} from '../../shared/tenant-billing';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid = monotonicFactory();
const sqs = new SQSClient({});
const kms = new KMSClient({});

const ADMIN_TABLE = process.env.ADMIN_TABLE!;
const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const RECEIPT_QUEUE_URL = process.env.RECEIPT_QUEUE_URL!;
const SIGNING_KEY_ID = process.env.RECEIPT_SIGNING_KEY_ID!;

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
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  };

  const path = event.path ?? '';

  // ── API Versioning Redirect (P2-7) ──
  if (!path.startsWith('/v1/')) {
    const v1Path = `/v1${path.startsWith('/') ? '' : '/'}${path}`;
    console.warn(`[scan-handler] Legacy unversioned request to ${path}. Redirecting to ${v1Path}`);
    return {
      statusCode: 308, // Permanent Redirect (preserves POST body)
      headers: { 
        ...headers, 
        'Location': v1Path,
        'Deprecation': 'true',
        'Sunset': 'Thu, 31 Dec 2026 23:59:59 GMT',
        'Link': '<https://docs.bebocard.com/v1/migration>; rel="deprecation"'
      },
      body: JSON.stringify({ 
        error: 'Deprecated Endpoint', 
        message: 'Please update your integration to use /v1 prefix. This endpoint will be sunset on 2026-12-31.',
        suggestedPath: v1Path 
      })
    };
  }

  try {
    if (path.endsWith('/scan')) return handleLoyaltyCheck(event, headers);
    if (path.endsWith('/receipt')) {
      if (event.httpMethod === 'GET') return handleGetReceipt(event, headers);
      return handleReceipt(event, headers, false);
    }
    if (path.endsWith('/health')) return handleHealthCheck(headers);
    if (path.endsWith('/security/receipt-public-key')) return handleGetPublicKey(headers);
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
  requestedFields?: string[];    // e.g. ["email_alias", "phone_alias", "first_name"]
  purpose?: string;             // e.g. "To send your digital receipt and offers"
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

  // ── Sandbox Mocking (P2-8) ──
  if (validKey.isSandbox) {
    if (secondaryULID === 'SANDBOX_USER_SUCCESS') {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          hasLoyaltyCard: true,
          loyaltyId: 'MOCK_CARD_001',
          tier: 'frequent',
          spendBucket: '100-200',
          attributes: body.requestedFields?.includes('email_alias') ? { email_alias: 'sandbox_test@bebocard.me' } : undefined,
        }),
      };
    }
    if (secondaryULID === 'SANDBOX_USER_NO_CARD') {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ hasLoyaltyCard: false }),
      };
    }
    if (secondaryULID === 'SANDBOX_USER_EXPIRED') {
      return { statusCode: 404, headers, body: JSON.stringify({ error: 'Identity expired or rotated' }) };
    }
    if (secondaryULID === 'SANDBOX_USER_REVOKED') {
      return { statusCode: 404, headers, body: JSON.stringify({ error: 'Identity revoked' }) };
    }
    if (secondaryULID === 'SANDBOX_USER_CONSENT') {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          hasLoyaltyCard: true,
          loyaltyId: 'MOCK_CARD_CONSENT',
          consentRequired: true,
          requestId: 'SANDBOX_REQ_789'
        }),
      };
    }
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

  // ── Billing Check (P1-8) ──
  const tenantState = await getTenantStateForBrand(dynamo, REFDATA_TABLE, validKey.brandId);
  if (!tenantState.active) {
    console.warn('[scan-handler] tenant suspended or grace period expired', { brandId: validKey.brandId, tenantId: tenantState.tenantId });
  }

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

  // ── Consent-Gated Identity Release (P1-6) ──
  let releasedAttributes: Record<string, string> | undefined;
  let consentRequired = false;
  let requestId: string | undefined;

  if (body.requestedFields && body.requestedFields.length > 0) {
    // Audit check: attributes are billable. Gate if over quota for 'base' tier.
    const quota = await checkTenantQuota(dynamo, REFDATA_TABLE, tenantState, 'consent');
    if (!quota.allowed) {
      return { statusCode: 403, headers, body: JSON.stringify({ error: 'Quota exceeded', message: quota.message }) };
    }

    const consentQuery = await dynamo.send(new QueryCommand({
      TableName: ADMIN_TABLE,
      IndexName: 'GSI1', 
      KeyConditionExpression: 'GSI1PK = :bpk AND GSI1SK = :bsk',
      FilterExpression: '#s = :approved',
      ExpressionAttributeNames: { '#s': 'status' },
      ExpressionAttributeValues: {
        ':bpk': `CONSENT#${validKey.brandId}`,
        ':bsk': permULID,
        ':approved': 'APPROVED'
      },
      Limit: 1,
    })).catch(() => ({ Items: [] }));

    const activeConsent = consentQuery.Items?.[0];

    if (activeConsent) {
      const approvedFields: string[] = JSON.parse(activeConsent.desc ?? '{}').approvedFields ?? [];
      const fieldsToRelease = body.requestedFields.filter(f => approvedFields.includes(f));
      releasedAttributes = await resolveAttributes(permULID, fieldsToRelease);

      // ── Single-Use Consumption (P1-6 AC: replay returns consentRequired) ──
      // Mark consent record as CONSUMED so the same token cannot be replayed.
      // ConditionExpression guards against a race where two POS terminals scan simultaneously.
      await dynamo.send(new UpdateCommand({
        TableName: ADMIN_TABLE,
        Key: { pK: activeConsent.pK as string, sK: activeConsent.sK as string },
        UpdateExpression: 'SET #s = :consumed, updatedAt = :now',
        ConditionExpression: '#s = :approved',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: {
          ':consumed': 'CONSUMED',
          ':approved': 'APPROVED',
          ':now': new Date().toISOString(),
        },
      })).catch(err => {
        // ConditionalCheckFailed means another request consumed it first — safe to ignore.
        if (err.name !== 'ConditionalCheckFailedException') throw err;
      });

      // ── Quota Logging (P1-6 AC 220) ──
      // Log each field release as a billable event for the tenant
      if (fieldsToRelease.length > 0) {
        const now = new Date().toISOString();
        await dynamo.send(new PutCommand({
          TableName: ADMIN_TABLE,
          Item: {
            pK: `TENANT_QUOTA#${validKey.tenantId}`,
            sK: `RELEASE#${now}#${ulid()}`,
            eventType: 'ATTRIBUTE_RELEASE',
            brandId: validKey.brandId,
            desc: JSON.stringify({
              fieldCount: fieldsToRelease.length,
              fields: fieldsToRelease,
              secondaryULID
            }),
            createdAt: now
          }
        }));
        console.info(`[scan-handler] Logged ${fieldsToRelease.length} billable attributes for tenant ${validKey.tenantId}`);
        // Increment usage counter (P1-8) — Skip for Sandbox
        if (!validKey.isSandbox) {
          await incrementTenantUsageCounter(dynamo, REFDATA_TABLE, tenantState.tenantId, validKey.brandId, 'consent');
        }
      }
    } else {
      // Create a NEW consent request and notify the user
      consentRequired = true;
      requestId = ulid();
      await initiateConsentRequest(permULID, requestId, validKey.brandId, body.requestedFields, body.purpose ?? 'To personalize your experience');
    }
  }

  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({
      hasLoyaltyCard: true,
      loyaltyId: card.cardId,
      ...(segDesc ? { tier: segDesc.visitFrequency, spendBucket: segDesc.spendBucket } : {}),
      ...(consentRequired ? { consentRequired: true, requestId } : {}),
      ...(releasedAttributes ? { attributes: releasedAttributes } : {}),
    }),
  };
}

async function initiateConsentRequest(permULID: string, requestId: string, brandId: string, fields: string[], purpose: string) {
  const now = new Date().toISOString();
  const expiresAt = new Date(Date.now() + 60 * 1000).toISOString(); // 60s window for scan path

  // 1. Fetch brand profile for the name
  const brandRes = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'profile' },
  }));
  const brandName = parseRecord(brandRes.Item?.desc).brandName ?? brandId;

  // 2. Store the request in ADMIN_TABLE for respondToConsent lookup
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `CONSENT#${requestId}`,
      sK: permULID,
      GSI1PK: `CONSENT#${brandId}`,
      GSI1SK: permULID,
      eventType: 'CONSENT_REQUEST',
      status: 'PENDING',
      desc: JSON.stringify({ requestedFields: fields, purpose, brandId, brandName, expiresAt }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  // 3. Push to user device
  const deviceToken = await getDeviceToken(`USER#${permULID}`);
  if (deviceToken) {
    await getFirebaseAdmin().send({
      token: deviceToken,
      data: {
        type: 'CONSENT_REQUEST',
        requestId: String(requestId),
        brandId: String(brandId),
        brandName: String(brandName),
        purpose: String(purpose),
        requestedFields: JSON.stringify(fields),
        expiresAt: String(expiresAt),
      },
      apns: { payload: { aps: { 'content-available': 1 } } },
      android: { priority: 'high' },
    });
  }
}

async function resolveAttributes(permULID: string, fields: string[]): Promise<Record<string, string>> {
  const result: Record<string, string> = {};
  const identityRes = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
  }));
  const identity = JSON.parse(identityRes.Item?.desc ?? '{}');

  for (const field of fields) {
    if (field === 'email_alias') {
      const hash = createHash('sha256').update(permULID + 'email').digest('hex').substring(0, 12);
      result[field] = `${hash}@bebocard.me`;
    } else if (field === 'phone_alias') {
       const hash = createHash('sha256').update(permULID + 'phone').digest('hex').substring(0, 8);
       result[field] = `+614${hash.replace(/[^0-9]/g, '0').substring(0, 7)}`;
    } else if (identity[field]) {
      result[field] = String(identity[field]);
    }
  }
  return result;
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
  idempotencyKey?: string;
  supplierTaxId?: string;
  supplierTaxIdType?: 'ABN' | 'VAT' | 'TRN' | 'GSTIN' | 'EIN' | 'OTHER';
}

async function handleReceipt(
  event: Parameters<APIGatewayProxyHandler>[0],
  headers: Record<string, string>,
  isInvoice: boolean,
) {
  const rawKey = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'receipt') : null;
  if (!validKey) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: 'Invalid or missing API key' }) };
  }

  const body: ReceiptRequest & { anonymousMode?: boolean } = JSON.parse(event.body ?? '{}');
  const { secondaryULID, merchant, amount, purchaseDate, anonymousMode } = body;

  // Validation: either secondaryULID OR anonymousMode must be present
  if (!anonymousMode && !secondaryULID) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'Missing secondaryULID or anonymousMode' }) };
  }
  if (!merchant || amount == null || !purchaseDate) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'Missing required fields' }) };
  }

  // Validate supplierTaxIdType if present
  if (body.supplierTaxIdType && !['ABN', 'VAT', 'TRN', 'GSTIN', 'EIN', 'OTHER'].includes(body.supplierTaxIdType)) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'Invalid supplierTaxIdType' }) };
  }

  if (body.brandId && body.brandId !== validKey.brandId) {
    console.warn('[scan-handler] receipt brand mismatch for API key', { requested: body.brandId, keyBrand: validKey.brandId });
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  let permULID: string;
  let claimToken: string | undefined;
  const receiptId = ulid();

  // ── Sandbox Identity (P2-8) ──
  const isSandboxUser = validKey.isSandbox && secondaryULID === 'SANDBOX_USER_123';
  
  if (anonymousMode) {
    // ── Anonymous Walk-In Receipt (P1-9) ──
    console.info(`[scan-handler] Processing anonymous receipt ${receiptId} for brand ${validKey.brandId}`);
    permULID = `ANON#${receiptId}`;
    // Simple 8-char claim token. In production, this would be a signed HMAC.
    claimToken = createHash('sha256').update(`${receiptId}${validKey.brandId}${Date.now()}`).digest('hex').substring(0, 8).toUpperCase();
  } else {
    // Handle idempotency check BEFORE enqueuing (to avoid SQS bloat)
    // Resolve secondaryULID → permULID
    const scanQuery = await dynamo.send(new QueryCommand({
      TableName: ADMIN_TABLE,
      KeyConditionExpression: 'pK = :pk',
      ExpressionAttributeValues: { ':pk': `SCAN#${secondaryULID}` },
      Limit: 1,
    }));

    let scanItem = scanQuery.Items?.[0];

    if (!scanItem) {
      // ── GHOST Identity flow ──
      console.info(`[scan-handler] Unknown secondaryULID ${secondaryULID}. Creating GHOST profile.`);
      permULID = `GHOST#${secondaryULID}`;
      
      await dynamo.send(new PutCommand({
        TableName: ADMIN_TABLE,
        Item: {
          pK: `SCAN#${secondaryULID}`,
          sK: permULID,
          status: 'GHOST',
          createdAt: new Date().toISOString(),
        }
      }));
    } else if (scanItem.status === 'REVOKED') {
      return { statusCode: 404, headers, body: JSON.stringify({ error: 'Identity revoked' }) };
    } else {
      permULID = scanItem.sK;
    }
  }

  if (isSandboxUser) {
    permULID = 'SANDBOX_IDENTITY_123';
  }
 
  // ── Billing Check (Receipt) ──
  // Note: Receipt ingestion is considered core and is generally not hard-blocked except for suspended/inactive tenants.
  const tenantState = await getTenantStateForBrand(dynamo, REFDATA_TABLE, validKey.brandId);
  if (!tenantState.active) {
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Tenant account suspended. Please contact BeboCard Billing.' }) };
  }
  
  // Enqueue task to SQS for async processing
  await sqs.send(new SendMessageCommand({
    QueueUrl: RECEIPT_QUEUE_URL,
    MessageBody: JSON.stringify({
      ...body,
      receiptId,
      permULID,
      claimToken,
      brandId: validKey.brandId,
      isInvoice,
      isSandbox: validKey.isSandbox,
    }),
  }));

  return { 
    statusCode: 202, 
    headers, 
    body: JSON.stringify({ 
      success: true, 
      message: 'Receipt received for processing',
      receiptId,
      ...(claimToken ? { 
        claimToken, 
        claimQRPayload: `bebocard://claim?receiptId=${receiptId}&token=${claimToken}&brand=${validKey.brandId}` 
      } : {})
    }) 
  };
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

async function handleHealthCheck(headers: Record<string, string>) {
  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({
      status: 'OPERATIONAL',
      timestamp: new Date().toISOString(),
      region: process.env.AWS_REGION,
      version: 'v1.0.0',
    }),
  };
}

async function handleGetReceipt(
  event: Parameters<APIGatewayProxyHandler>[0],
  headers: Record<string, string>,
) {
  const rawKey = extractApiKey(event.headers as Record<string, string | undefined>);
  const validKey = rawKey ? await validateApiKey(dynamo, rawKey, 'receipt') : null;
  if (!validKey) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: 'Invalid or missing API key' }) };
  }

  const receiptId = event.queryStringParameters?.receiptId;
  const permULID = event.queryStringParameters?.permULID;

  if (!receiptId || !permULID) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'Missing receiptId or permULID' }) };
  }

  // Brands can only query their own receipts
  const result = await dynamo.send(new GetCommand({
    TableName: permULID.startsWith('ANON#') ? REFDATA_TABLE : USER_TABLE,
    Key: { 
      pK: permULID.startsWith('ANON#') ? permULID : `USER#${permULID}`, 
      sK: receiptId.startsWith('RECEIPT#') ? receiptId : `RECEIPT#${receiptId}` 
    },
  }));

  if (!result.Item) {
    return { statusCode: 404, headers, body: JSON.stringify({ error: 'Receipt not found' }) };
  }

  const desc = JSON.parse(result.Item.desc ?? '{}');
  if (desc.brandId !== validKey.brandId) {
    return { statusCode: 403, headers, body: JSON.stringify({ error: 'Unauthorized: Brand mismatch' }) };
  }

  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({
      receiptId,
      brandId: desc.brandId,
      merchant: desc.merchant,
      amount: desc.amount,
      purchaseDate: desc.purchaseDate,
      signature: desc.signature,
      signingAlgorithm: desc.signingAlgorithm,
      publicKeyUrl: `https://api.bebocard.com/v1/security/receipt-public-key`
    }),
  };
}

async function handleGetPublicKey(headers: Record<string, string>) {
  const result = await kms.send(new GetPublicKeyCommand({ KeyId: SIGNING_KEY_ID }));
  if (!result.PublicKey) throw new Error('Failed to retrieve public key from KMS');

  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({
      publicKey: Buffer.from(result.PublicKey).toString('base64'),
      keyId: SIGNING_KEY_ID,
      algorithm: 'RSASSA_PSS_SHA_256',
      format: 'DER',
    }),
  };
}
