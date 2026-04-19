import type { APIGatewayProxyHandler, APIGatewayProxyEvent } from 'aws-lambda';
import { createHmac, timingSafeEqual } from 'crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  PutCommand, GetCommand, UpdateCommand, QueryCommand, ScanCommand, DeleteCommand, BatchGetCommand,
} from '@aws-sdk/lib-dynamodb';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';
import { monotonicFactory } from 'ulid';
import { withAuditLog, writeAuditLog } from '../../shared/audit-logger';
import {
  validateApiKey, rotateApiKey, extractApiKey,
  type ApiKeyScope,
} from '../../shared/api-key-auth';
import { sanitizeHtml, isHtmlClean } from '../shared/html-sanitizer';
import { CircuitBreakerFactory } from '../shared/circuit-breaker';
import {
  OfferInputSchema,
  NewsletterInputSchema,
  CatalogueInputSchema,
  StoreInputSchema,
  SubscriptionCatalogInputSchema,
} from '../../shared/validation-schemas';
import {
  ALL_USAGE_TYPES,
  checkTenantQuota as checkSharedTenantQuota,
  getTenantStateForBrand as getSharedTenantStateForBrand,
  getTenantUsageCounter as getSharedTenantUsageCounter,
  getUsageMonthKey,
  incrementTenantUsageCounter as incrementSharedTenantUsageCounter,
  type TenantTier,
  type UsageType,
} from '../../shared/tenant-billing';
import {
  isNotificationCapReached,
  incrementNotificationCounter,
  getRelevancePriority
} from '../shared/notification-utils';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid = monotonicFactory();

const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const ADMIN_TABLE = process.env.ADMIN_TABLE!;

const CORS_HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Headers': 'Content-Type,X-Api-Key,Authorization,X-Correlation-Id',
  'X-Content-Type-Options': 'nosniff',
  'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
};

// Allowed portal origin from env — no localhost in production
const PORTAL_ORIGIN = process.env.PORTAL_ORIGIN ?? 'https://business.bebocard.com.au';

function resolveOrigin(event: APIGatewayProxyEvent): string {
  const origin = event.headers.origin || event.headers.Origin || PORTAL_ORIGIN;
  return origin === PORTAL_ORIGIN ? origin : PORTAL_ORIGIN;
}

function ok(event: APIGatewayProxyEvent, body: unknown) {
  return {
    statusCode: 200,
    headers: { ...CORS_HEADERS, 'Access-Control-Allow-Origin': resolveOrigin(event) },
    body: JSON.stringify(body),
  };
}

function err(event: APIGatewayProxyEvent, status: number, message: string) {
  return {
    statusCode: status,
    headers: { ...CORS_HEADERS, 'Access-Control-Allow-Origin': resolveOrigin(event) },
    body: JSON.stringify({ error: message }),
  };
}

function isConditionalCheckFailed(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return error.name === 'ConditionalCheckFailedException';
}

// Admin API key — only for BeboCard ops endpoints (e.g. /admin/subscription-catalog).
// Read from process.env at call time so Lambda env updates and test overrides take effect.
function verifyAdminKey(event: APIGatewayProxyEvent): boolean {
  const adminKey = process.env.ADMIN_API_KEY ?? '';
  if (!adminKey) return false;
  const key = event.headers['x-admin-api-key'] ?? event.headers['X-Admin-Api-Key'] ?? '';
  const keyBuf = Buffer.from(key);
  const expBuf = Buffer.from(adminKey);
  if (keyBuf.length === 0 || keyBuf.length !== expBuf.length) return false;
  return timingSafeEqual(keyBuf, expBuf);
}

// ─── Internal call signing ────────────────────────────────────────────────────
// Portal invokes brand-api-handler directly via IAM. As defence-in-depth,
// the payload is signed with INTERNAL_SIGNING_SECRET so that even if the
// Lambda resource policy is accidentally widened, unsigned calls are rejected.
const INTERNAL_SIGNING_SECRET = process.env.INTERNAL_SIGNING_SECRET ?? '';
const INTERNAL_TIMESTAMP_WINDOW_MS = 5 * 60 * 1000; // 5-minute replay window

function verifyInternalSignature(brandId: string, timestamp: string, sig: string): boolean {
  if (!INTERNAL_SIGNING_SECRET) return false; // secret not configured — reject
  const now = Date.now();
  const ts = parseInt(timestamp, 10);
  if (isNaN(ts) || Math.abs(now - ts) > INTERNAL_TIMESTAMP_WINDOW_MS) return false;
  const expected = createHmac('sha256', INTERNAL_SIGNING_SECRET)
    .update(`${brandId}:${timestamp}`)
    .digest('hex');
  const sigBuf = Buffer.from(sig);
  const expBuf = Buffer.from(expected);
  if (sigBuf.length !== expBuf.length) return false;
  return timingSafeEqual(sigBuf, expBuf);
}

// ─── Auth guard ────────────────────────────────────────────────────────────────

async function authGuard(
  event: APIGatewayProxyEvent,
  scope: ApiKeyScope,
): Promise<{ brandId: string; authMethod: 'internal' | 'api_key' } | null> {
  // Direct Lambda invocation from the business portal — IAM auth is enforced at
  // the Lambda resource policy level. As defence-in-depth, the portal also signs
  // the internal payload with INTERNAL_SIGNING_SECRET (HMAC-SHA256).
  const eventAny = event as unknown as Record<string, string>;
  const internalBrandId = eventAny._internalBrandId;
  const internalTimestamp = eventAny._internalTimestamp ?? '';
  const internalSig = eventAny._internalSig ?? '';

  if (internalBrandId) {
    if (!verifyInternalSignature(internalBrandId, internalTimestamp, internalSig)) {
      console.warn('[brand-api-handler] internal call signature invalid', { brandId: internalBrandId, scope });
      return null; // reject unsigned/replayed internal calls
    }
    console.info('[brand-api-handler] internal call verified', { brandId: internalBrandId, scope });
    return { brandId: internalBrandId, authMethod: 'internal' };
  }

  const rawKey = extractApiKey(event.headers as Record<string, string>);
  if (!rawKey) return null;
  const validated = await validateApiKey(dynamo, rawKey, scope);
  if (!validated) return null;
  return { brandId: validated.brandId, authMethod: 'api_key' };
}

// ─── Router ────────────────────────────────────────────────────────────────────

const _handler: APIGatewayProxyHandler = async (event) => {
  const method = event.httpMethod;
  const path = event.path ?? '';

  // ── Receipt status ACK ──────────────────────────────────────────────────────
  if (method === 'GET' && /\/receipt\/[^/]+\/status/.test(path)) {
    const auth = await authGuard(event, 'receipt');
    if (!auth) return err(event, 401, 'Unauthorized');
    return handleReceiptStatus(event, auth.brandId);
  }

  // ── Offers ──────────────────────────────────────────────────────────────────
  if (path.includes('/offers')) {
    const auth = await authGuard(event, 'offers');
    if (!auth) return err(event, 401, 'Unauthorized');
    if (method === 'GET') return listOffers(event, auth.brandId);
    if (method === 'POST') return createOffer(event, auth.brandId);
    if (method === 'PUT') return updateOffer(event, auth.brandId);
    if (method === 'DELETE') return archiveOffer(event, auth.brandId);
  }

  // ── Newsletters ─────────────────────────────────────────────────────────────
  if (path.includes('/newsletters')) {
    const auth = await authGuard(event, 'newsletters');
    if (!auth) return err(event, 401, 'Unauthorized');
    if (method === 'POST') return sendNewsletter(event, auth.brandId);
    if (method === 'GET') return listNewsletters(event, auth.brandId);
  }

  // ── Catalogues ──────────────────────────────────────────────────────────────
  if (path.includes('/catalogues')) {
    const auth = await authGuard(event, 'catalogues');
    if (!auth) return err(event, 401, 'Unauthorized');
    if (method === 'POST') return createCatalogue(event, auth.brandId);
    if (method === 'GET') return listCatalogues(event, auth.brandId);
  }

  // ── Analytics ───────────────────────────────────────────────────────────────
  if (path.includes('/analytics')) {
    const auth = await authGuard(event, 'analytics');
    if (!auth) return err(event, 401, 'Unauthorized');
    return getAnalytics(event, auth.brandId);
  }

  if (path.includes('/usage')) {
    const auth = await authGuard(event, 'analytics');
    if (!auth) return err(event, 401, 'Unauthorized');
    return getUsage(event, auth.brandId);
  }

  // ── Stores ──────────────────────────────────────────────────────────────────
  if (path.includes('/stores')) {
    const auth = await authGuard(event, 'stores');
    if (!auth) return err(event, 401, 'Unauthorized');
    if (method === 'POST') return upsertStore(event, auth.brandId);
    if (method === 'GET') return listStores(event, auth.brandId);
    if (method === 'DELETE') return archiveStore(event, auth.brandId);
  }

  // ── Tenant invoice state sync ─────────────────────────────────────────────
  // Allows brands to push invoice lifecycle transitions in an idempotent,
  // out-of-order-safe way (e.g. paid, overdue, cancelled).
  if (path.includes('/invoice-status')) {
    const auth = await authGuard(event, 'recurring');
    if (!auth) return err(event, 401, 'Unauthorized');
    if (method === 'POST') return upsertInvoiceStatus(event, auth.brandId);
  }

  // ── Tenant invoice payment session ───────────────────────────────────────
  // Generates a payable URL for linked tenant invoices.
  if (path.includes('/invoice-payment-session')) {
    const auth = await authGuard(event, 'recurring');
    if (!auth) return err(event, 401, 'Unauthorized');
    if (method === 'POST') return createInvoicePaymentSession(event, auth.brandId);
  }

  // ── API key self-management (brand rotates own key) ─────────────────────────
  if (path.includes('/api-keys/rotate')) {
    const auth = await authGuard(event, 'scan'); // any scope allows rotation
    if (!auth) return err(event, 401, 'Unauthorized');
    return handleRotateKey(event, auth.brandId);
  }

  // ── Admin: subscription catalog listing + management ─────────────────────
  if (path.includes('/admin/subscription-catalog')) {
    if (!verifyAdminKey(event)) return err(event, 401, 'Unauthorized');
    if (method === 'GET') return adminListSubscriptionCatalog(event);
    if (method === 'PUT') return adminUpdateCatalogListing(event);
  }

  // ── Subscription catalog: tenant self-onboarding ─────────────────────────
  if (path.includes('/subscription-catalog')) {
    const auth = await authGuard(event, 'recurring');
    if (!auth) return err(event, 401, 'Unauthorized');
    if (method === 'POST') return createSubscriptionCatalogEntry(event, auth.brandId);
    if (method === 'PUT') return updateSubscriptionCatalogEntry(event, auth.brandId);
    if (method === 'GET') return getSubscriptionCatalogEntry(event, auth.brandId);
  }

  // ── Custom segments ──────────────────────────────────────────────────────
  if (path.includes('/segments')) {
    const auth = await authGuard(event, 'analytics');
    if (!auth) return err(event, 401, 'Unauthorized');
    if (method === 'POST') return createSegmentDef(event, auth.brandId);
    if (method === 'GET') return listSegmentDefs(event, auth.brandId);
    if (method === 'PUT') return updateSegmentDef(event, auth.brandId);
    if (method === 'DELETE') return deleteSegmentDef(event, auth.brandId);
  }

  // ── Card Config (loyalty template opt-in) ───────────────────────────────
  // Brand tenants use this to opt into an approved loyalty card template.
  // Requires admin approval before the card config becomes discoverable to users.
  if (path.includes('/card-config')) {
    // Admin routes — super_admin only.
    // Accepts: (a) ADMIN_API_KEY header, or (b) internal portal call with _internalBrandId='__admin__'
    if (path.includes('/admin/card-configs')) {
      const eventAny = event as unknown as Record<string, string>;
      const isInternalAdmin =
        eventAny._internalBrandId === '__admin__' &&
        verifyInternalSignature('__admin__', eventAny._internalTimestamp ?? '', eventAny._internalSig ?? '');
      if (!isInternalAdmin && !verifyAdminKey(event)) return err(event, 401, 'Unauthorized');
      if (method === 'GET') return adminListCardConfigs(event);
      const approveMatch = path.match(/\/admin\/card-configs\/([^/]+)\/([^/]+)\/(approve|reject|revoke)$/);
      if (approveMatch) {
        const [, brandId, templateId, action] = approveMatch;
        return adminActOnCardConfig(event, brandId, templateId, action as 'approve' | 'reject' | 'revoke');
      }
    }
    // Brand-scoped routes — internal call (portal, tenant_admin/brand_admin) or API key
    const auth = await authGuard(event, 'card_config');
    if (!auth) return err(event, 401, 'Unauthorized');
    if (method === 'POST') return createCardConfig(event, auth.brandId);
    if (method === 'GET') return listCardConfigs(event, auth.brandId);
    const deleteMatch = path.match(/\/card-config\/([^/]+)$/);
    if (method === 'DELETE' && deleteMatch) return deleteCardConfig(event, auth.brandId, deleteMatch[1]);
  }

  return err(event, 404, 'Unknown route');
};

export const handler = withAuditLog(dynamo, _handler);

// ─── Receipt ACK ───────────────────────────────────────────────────────────────

async function handleReceiptStatus(event: APIGatewayProxyEvent, brandId: string) {
  // Path: /receipt/<receiptSK>/status  (URL-encoded)
  const rawSK = event.pathParameters?.receiptSK ?? decodeURIComponent(
    event.path.replace(/.*\/receipt\//, '').replace(/\/status.*/, ''),
  );

  // Fetch the receipt via GSI (sK-pK-index) — avoids full table scan
  const res = await dynamo.send(new QueryCommand({
    TableName: USER_TABLE,
    IndexName: 'sK-pK-index',
    KeyConditionExpression: 'sK = :sk',
    ExpressionAttributeValues: { ':sk': rawSK },
    Limit: 1,
  }));

  const item = res.Items?.[0];
  if (!item) return err(event, 404, 'Receipt not found');

  const desc = JSON.parse(item.desc ?? '{}');
  if (desc.brandId && desc.brandId !== brandId) return err(event, 403, 'Forbidden');

  return ok(event, { saved: true, receiptSK: rawSK, savedAt: item.createdAt, fcmStatus: desc.fcmStatus ?? 'sent' });
}

function normalizeInvoiceTopLevelStatus(status: string): string {
  switch (status) {
    case 'paid':
      return 'PAID';
    case 'cancelled':
      return 'CANCELLED';
    case 'overdue':
      return 'OVERDUE';
    default:
      return 'ACTIVE';
  }
}

async function upsertInvoiceStatus(event: APIGatewayProxyEvent, brandId: string) {
  const body = JSON.parse(event.body ?? '{}') as {
    invoiceSK?: string;
    status?: string;
    paidDate?: string;
    eventTime?: string;
    idempotencyKey?: string;
    reason?: string;
    externalInvoiceId?: string;
  };

  const invoiceSK = body.invoiceSK;
  const incomingStatus = (body.status ?? '').toLowerCase();
  const idempotencyKey = body.idempotencyKey
    ?? event.headers['x-idempotency-key']
    ?? event.headers['X-Idempotency-Key'];

  if (!invoiceSK) return err(event, 400, 'Missing invoiceSK');
  if (!incomingStatus) return err(event, 400, 'Missing status');
  if (!['issued', 'unpaid', 'due_soon', 'overdue', 'paid', 'cancelled'].includes(incomingStatus)) {
    return err(event, 400, 'Invalid status');
  }

  if (idempotencyKey) {
    try {
      await dynamo.send(new PutCommand({
        TableName: ADMIN_TABLE,
        Item: {
          pK: `IDEMP#INVOICE_STATUS#${brandId}#${idempotencyKey}`,
          sK: 'EVENT',
          eventType: 'IDEMPOTENCY',
          status: 'PROCESSED',
          primaryCat: 'idempotency',
          createdAt: new Date().toISOString(),
        },
        ConditionExpression: 'attribute_not_exists(pK)',
      }));
    } catch (e) {
      if (isConditionalCheckFailed(e)) {
        return ok(event, {
          invoiceSK,
          status: incomingStatus,
          updated: false,
          idempotent: true,
          staleIgnored: false,
        });
      }
      throw e;
    }
  }

  const queryRes = await dynamo.send(new QueryCommand({
    TableName: USER_TABLE,
    IndexName: 'sK-pK-index',
    KeyConditionExpression: 'sK = :sk',
    ExpressionAttributeValues: { ':sk': invoiceSK },
    Limit: 1,
  }));

  const invoiceItem = queryRes.Items?.[0];
  if (!invoiceItem) return err(event, 404, 'Invoice not found');

  const desc = parseRecord(invoiceItem.desc);
  const itemBrandId = typeof desc.brandId === 'string' ? desc.brandId : null;
  if (itemBrandId && itemBrandId !== brandId) return err(event, 403, 'Forbidden');

  const stateEventAt = body.eventTime ?? new Date().toISOString();
  const previousEventAt = typeof desc.lastStateEventAt === 'string' ? desc.lastStateEventAt : null;
  if (previousEventAt) {
    const prevTs = Date.parse(previousEventAt);
    const incomingTs = Date.parse(stateEventAt);
    if (!Number.isNaN(prevTs) && !Number.isNaN(incomingTs) && incomingTs < prevTs) {
      return ok(event, {
        invoiceSK,
        status: incomingStatus,
        updated: false,
        idempotent: false,
        staleIgnored: true,
      });
    }
  }

  desc.status = incomingStatus;
  if (body.paidDate) desc.paidDate = body.paidDate;
  if (body.reason) desc.statusReason = body.reason;
  if (body.externalInvoiceId) desc.externalInvoiceId = body.externalInvoiceId;
  desc.lastStateEventAt = stateEventAt;
  desc.stateSource = 'tenant_push';

  const now = new Date().toISOString();
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: invoiceItem.pK, sK: invoiceSK },
    UpdateExpression: 'SET #status = :status, #desc = :desc, updatedAt = :now',
    ExpressionAttributeNames: {
      '#status': 'status',
      '#desc': 'desc',
    },
    ExpressionAttributeValues: {
      ':status': normalizeInvoiceTopLevelStatus(incomingStatus),
      ':desc': JSON.stringify(desc),
      ':now': now,
    },
  }));

  return ok(event, {
    invoiceSK,
    status: incomingStatus,
    updated: true,
    idempotent: false,
    staleIgnored: false,
  });
}

function applyInvoiceTemplate(template: string, invoice: {
  invoiceSK: string;
  invoiceNumber?: string | null;
  amount: number;
  currency: string;
}): string {
  return template
    .replaceAll('{invoiceSK}', encodeURIComponent(invoice.invoiceSK))
    .replaceAll('{invoiceNumber}', encodeURIComponent(invoice.invoiceNumber ?? ''))
    .replaceAll('{amount}', encodeURIComponent(String(invoice.amount)))
    .replaceAll('{currency}', encodeURIComponent(invoice.currency));
}

async function createStripeConnectInvoiceSession(input: {
  stripeSecretKey: string;
  connectedAccountId: string;
  successUrl: string;
  cancelUrl: string;
  brandId: string;
  invoiceSK: string;
  invoiceNumber?: string | null;
  supplier: string;
  amount: number;
  currency: string;
}): Promise<string> {
  const lineItemLabel = input.invoiceNumber
    ? `${input.supplier} invoice ${input.invoiceNumber}`
    : `${input.supplier} invoice`;

  const params = new URLSearchParams({
    mode: 'payment',
    success_url: input.successUrl,
    cancel_url: input.cancelUrl,
    'line_items[0][quantity]': '1',
    'line_items[0][price_data][currency]': input.currency.toLowerCase(),
    'line_items[0][price_data][unit_amount]': String(Math.round(input.amount * 100)),
    'line_items[0][price_data][product_data][name]': lineItemLabel,
    'metadata[invoiceSK]': input.invoiceSK,
    'metadata[brandId]': input.brandId,
  });

  const response = await fetch('https://api.stripe.com/v1/checkout/sessions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${input.stripeSecretKey}`,
      'Stripe-Account': input.connectedAccountId,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: params.toString(),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Stripe checkout session failed: ${response.status} ${text}`);
  }

  const payload = await response.json() as { url?: string };
  if (!payload.url) throw new Error('Stripe checkout session returned no url');
  return payload.url;
}

async function createInvoicePaymentSession(event: APIGatewayProxyEvent, brandId: string) {
  const body = JSON.parse(event.body ?? '{}') as { invoiceSK?: string };
  const invoiceSK = body.invoiceSK;
  if (!invoiceSK) return err(event, 400, 'Missing invoiceSK');

  const invoiceRes = await dynamo.send(new QueryCommand({
    TableName: USER_TABLE,
    IndexName: 'sK-pK-index',
    KeyConditionExpression: 'sK = :sk',
    ExpressionAttributeValues: { ':sk': invoiceSK },
    Limit: 1,
  }));

  const invoiceItem = invoiceRes.Items?.[0];
  if (!invoiceItem) return err(event, 404, 'Invoice not found');

  const invoiceDesc = parseRecord(invoiceItem.desc);
  const invoiceBrandId = typeof invoiceDesc.brandId === 'string' ? invoiceDesc.brandId : null;
  if (invoiceBrandId && invoiceBrandId !== brandId) return err(event, 403, 'Forbidden');

  const invoiceStatus = typeof invoiceDesc.status === 'string' ? invoiceDesc.status.toLowerCase() : 'unpaid';
  if (invoiceStatus !== 'unpaid') return err(event, 409, 'Invoice is not payable');

  const amount = typeof invoiceDesc.amount === 'number' ? invoiceDesc.amount : Number(invoiceDesc.amount ?? 0);
  const currency = typeof invoiceDesc.currency === 'string' ? invoiceDesc.currency : 'AUD';
  const supplier = typeof invoiceDesc.supplier === 'string' ? invoiceDesc.supplier : brandId;
  const invoiceNumber = typeof invoiceDesc.invoiceNumber === 'string' ? invoiceDesc.invoiceNumber : null;
  if (!Number.isFinite(amount) || amount <= 0) return err(event, 400, 'Invoice amount is invalid');

  if (typeof invoiceDesc.paymentUrl === 'string' && invoiceDesc.paymentUrl.startsWith('http')) {
    return ok(event, {
      invoiceSK,
      paymentMode: 'hosted_link',
      checkoutUrl: invoiceDesc.paymentUrl,
    });
  }

  const brandRes = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'profile' },
  }));
  const brandDesc = parseRecord(brandRes.Item?.desc);

  const paymentMode = typeof brandDesc.invoicePaymentMode === 'string'
    ? brandDesc.invoicePaymentMode
    : 'none';

  if (paymentMode === 'hosted_link') {
    const baseUrl = typeof brandDesc.invoicePaymentBaseUrl === 'string' ? brandDesc.invoicePaymentBaseUrl : '';
    if (!baseUrl.startsWith('http')) return err(event, 400, 'Brand hosted link payment is not configured');

    const checkoutUrl = applyInvoiceTemplate(baseUrl, { invoiceSK, invoiceNumber, amount, currency });
    return ok(event, {
      invoiceSK,
      paymentMode,
      checkoutUrl,
    });
  }

  if (paymentMode === 'stripe_connect') {
    const connectedAccountId = typeof brandDesc.invoiceStripeConnectedAccountId === 'string'
      ? brandDesc.invoiceStripeConnectedAccountId
      : '';
    const successUrl = typeof brandDesc.invoiceStripeSuccessUrl === 'string'
      ? brandDesc.invoiceStripeSuccessUrl
      : 'https://business.bebocard.com.au/payment/success';
    const cancelUrl = typeof brandDesc.invoiceStripeCancelUrl === 'string'
      ? brandDesc.invoiceStripeCancelUrl
      : 'https://business.bebocard.com.au/payment/cancel';
    const stripeSecretKey = process.env.STRIPE_SECRET_KEY ?? '';

    if (!stripeSecretKey) return err(event, 500, 'Stripe is not configured');
    if (!connectedAccountId.startsWith('acct_')) return err(event, 400, 'Brand Stripe Connect account is not configured');

    const checkoutUrl = await createStripeConnectInvoiceSession({
      stripeSecretKey,
      connectedAccountId,
      successUrl,
      cancelUrl,
      brandId,
      invoiceSK,
      invoiceNumber,
      supplier,
      amount,
      currency,
    });

    return ok(event, {
      invoiceSK,
      paymentMode,
      checkoutUrl,
    });
  }

  return err(event, 400, 'Brand invoice payment is not configured');
}

// ─── Offers ────────────────────────────────────────────────────────────────────


async function createOffer(event: APIGatewayProxyEvent, brandId: string) {
  const tenantState = await getTenantStateForBrand(brandId);
  if (!tenantState.active) return err(event, 403, 'Tenant billing is suspended');
  const quotaCheck = await checkTenantQuota(tenantState, 'offers');
  if (!quotaCheck.allowed) return err(event, 403, quotaCheck.message ?? 'Tenant quota exceeded');

  const parsed = OfferInputSchema.safeParse(JSON.parse(event.body ?? '{}'));
  if (!parsed.success) return err(event, 400, parsed.error.issues[0]?.message ?? 'Invalid input');
  const body = parsed.data;

  const offerId = ulid();
  const now = new Date().toISOString();
  const { brandName: offerBrandName, brandColor: offerBrandColor, brandRegion: offerBrandRegion } = await getBrandProfile(brandId);
  // Offer TTL: expire 30 days after validTo (or 2 years if no validTo)
  const validToDate = body.validTo ? new Date(body.validTo as string) : new Date(Date.now() + 2 * 365 * 86400_000);
  const offerTtl = Math.floor(validToDate.getTime() / 1000) + 30 * 86400;

  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK: `BRAND#${brandId}`,
      sK: `OFFER#${offerId}`,
      eventType: 'OFFER',
      status: body.status || 'ACTIVE',
      primaryCat: 'offer',
      desc: JSON.stringify({ ...body, brandId, offerId, brandName: offerBrandName, brandColor: offerBrandColor, brandRegion: offerBrandRegion }),
      ttl: offerTtl,
      createdAt: now,
      updatedAt: now,
    },
  }));

  const usage = await incrementTenantUsageCounter(tenantState.tenantId, brandId, 'offers');

  // Fan-out FCM push to all subscribers with offers: true
  fanOutToSubscribers(brandId, 'offers', {
    title: `New offer from your loyalty brand`,
    body: body.title as string,
    data: { type: 'NEW_OFFER', offerId, brandId },
  }, undefined, undefined, tenantState.notifCap, (body as any).campaignType ?? 'untargeted').then(async count => {
    const fanoutAt = new Date().toISOString();
    await Promise.all([
      // Store delivery count on the offer record so the portal can surface it
      dynamo.send(new UpdateCommand({
        TableName: REFDATA_TABLE,
        Key: { pK: `BRAND#${brandId}`, sK: `OFFER#${offerId}` },
        UpdateExpression: 'SET fanoutCount = :c, fanoutAt = :at',
        ExpressionAttributeValues: { ':c': count, ':at': fanoutAt },
      })),
      writeAuditLog(dynamo, {
        actor: brandId,
        actorType: 'brand',
        action: 'offer.delivered',
        resource: `OFFER#${offerId}`,
        outcome: 'success',
        metadata: { delivered: count },
      }),
    ]).catch(() => {});
  }).catch(e => console.error('[createOffer] fanOut error:', e));

  return ok(event, {
    offerId,
    status: body.status || 'ACTIVE',
    billing: buildBillingUsageSnapshot(tenantState, usage),
  });
}

async function listOffers(event: APIGatewayProxyEvent, brandId: string) {
  const params = event.queryStringParameters ?? {};
  const limit = Math.min(parseInt(params.limit ?? '50', 10), 100);
  const cursor = params.cursor ? JSON.parse(Buffer.from(params.cursor, 'base64url').toString()) : undefined;

  const res = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: { ':pk': `BRAND#${brandId}`, ':prefix': 'OFFER#' },
    ScanIndexForward: false,
    Limit: limit,
    ExclusiveStartKey: cursor,
  }));

  const nextCursor = res.LastEvaluatedKey
    ? Buffer.from(JSON.stringify(res.LastEvaluatedKey)).toString('base64url')
    : null;

  return ok(event, {
    offers: res.Items?.map(i => ({ ...JSON.parse(i.desc), status: i.status, sK: i.sK })) ?? [],
    nextCursor,
  });
}

async function updateOffer(event: APIGatewayProxyEvent, brandId: string) {
  const offerId = event.pathParameters?.offerId ?? event.path.split('/offers/')[1];
  const body = JSON.parse(event.body ?? '{}') as Record<string, unknown>;
  const now = new Date().toISOString();

  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `OFFER#${offerId}` },
  }));
  if (!existing.Item) return err(event, 404, 'Offer not found');

  const merged = { ...JSON.parse(existing.Item.desc), ...body };
  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `OFFER#${offerId}` },
    UpdateExpression: 'SET desc = :desc, #s = :status, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { 
      ':desc': JSON.stringify(merged), 
      ':status': body.status ?? existing.Item.status,
      ':now': now 
    },
  }));

  // Log explicit status change if provided
  if (body.status && body.status !== existing.Item.status) {
    writeAuditLog(dynamo, {
      actor: brandId,
      actorType: 'brand',
      action: 'offer.status_changed',
      resource: `OFFER#${offerId}`,
      outcome: 'success',
      metadata: { from: existing.Item.status, to: body.status }
    }).catch(e => console.error('[audit] log failed', e));
  }

  return ok(event, { offerId, updated: true, status: body.status ?? existing.Item.status });
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
  return ok(event, { offerId, status: 'ARCHIVED' });
}

// ─── Newsletters ───────────────────────────────────────────────────────────────


async function sendNewsletter(event: APIGatewayProxyEvent, brandId: string) {
  const tenantState = await getTenantStateForBrand(brandId);
  if (!tenantState.active) return err(event, 403, 'Tenant billing is suspended');
  const quotaCheck = await checkTenantQuota(tenantState, 'newsletters');
  if (!quotaCheck.allowed) return err(event, 403, quotaCheck.message ?? 'Tenant quota exceeded');

  const parsed = NewsletterInputSchema.safeParse(JSON.parse(event.body ?? '{}'));
  if (!parsed.success) return err(event, 400, parsed.error.issues[0]?.message ?? 'Invalid input');
  const body = parsed.data;

  // Sanitise HTML body — log if brand submitted suspicious content
  const rawHtml = body.bodyHtml as string;
  const safeHtml = sanitizeHtml(rawHtml);
  if (!isHtmlClean(rawHtml)) {
    console.warn('[sendNewsletter] HTML sanitised — brand submitted content with disallowed tags', { brandId });
  }
  const sanitisedBody = { ...body, bodyHtml: safeHtml };

  const newsletterId = ulid();
  const now = new Date().toISOString();
  const { brandName: nlBrandName, brandColor: nlBrandColor, brandRegion: nlBrandRegion } = await getBrandProfile(brandId);
  // Newsletter TTL: 7 years for compliance
  const newsletterTtl = Math.floor(Date.now() / 1000) + 7 * 365 * 24 * 3600;

  // Store newsletter record in RefDataEvent
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK: `BRAND#${brandId}`,
      sK: `NEWSLETTER#${newsletterId}`,
      eventType: 'NEWSLETTER',
      status: 'ACTIVE',
      primaryCat: 'newsletter',
      desc: JSON.stringify({ ...sanitisedBody, brandId, newsletterId, brandName: nlBrandName, brandColor: nlBrandColor, brandRegion: nlBrandRegion }),
      ttl: newsletterTtl,
      createdAt: now,
      updatedAt: now,
    },
  }));

  const usage = await incrementTenantUsageCounter(tenantState.tenantId, brandId, 'newsletters');

  // Fan-out to all newsletter subscribers
  fanOutToSubscribers(brandId, 'newsletters', {
    title: body.subject as string,
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
  }, undefined, tenantState.notifCap, 'untargeted').then(async count => {
    const fanoutAt = new Date().toISOString();
    await Promise.all([
      dynamo.send(new UpdateCommand({
        TableName: REFDATA_TABLE,
        Key: { pK: `BRAND#${brandId}`, sK: `NEWSLETTER#${newsletterId}` },
        UpdateExpression: 'SET fanoutCount = :c, fanoutAt = :at',
        ExpressionAttributeValues: { ':c': count, ':at': fanoutAt },
      })),
      writeAuditLog(dynamo, {
        actor: brandId,
        actorType: 'brand',
        action: 'newsletter.delivered',
        resource: `NEWSLETTER#${newsletterId}`,
        outcome: 'success',
        metadata: { delivered: count },
      }),
    ]).catch(() => {});
  }).catch(e => console.error('[sendNewsletter] fanOut error:', e));

  return ok(event, {
    newsletterId,
    sentAt: now,
    billing: buildBillingUsageSnapshot(tenantState, usage),
  });
}

async function listNewsletters(event: APIGatewayProxyEvent, brandId: string) {
  const params = event.queryStringParameters ?? {};
  const limit = Math.min(parseInt(params.limit ?? '50', 10), 100);
  const cursor = params.cursor ? JSON.parse(Buffer.from(params.cursor, 'base64url').toString()) : undefined;

  const res = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: { ':pk': `BRAND#${brandId}`, ':prefix': 'NEWSLETTER#' },
    ScanIndexForward: false,
    Limit: limit,
    ExclusiveStartKey: cursor,
  }));

  const nextCursor = res.LastEvaluatedKey
    ? Buffer.from(JSON.stringify(res.LastEvaluatedKey)).toString('base64url')
    : null;

  return ok(event, {
    newsletters: res.Items?.map(i => ({ ...JSON.parse(i.desc), sK: i.sK, createdAt: i.createdAt })) ?? [],
    nextCursor,
  });
}

// ─── Catalogues ───────────────────────────────────────────────────────────────

async function createCatalogue(event: APIGatewayProxyEvent, brandId: string) {
  const tenantState = await getTenantStateForBrand(brandId);
  if (!tenantState.active) return err(event, 403, 'Tenant billing is suspended');
  const quotaCheck = await checkTenantQuota(tenantState, 'catalogues');
  if (!quotaCheck.allowed) return err(event, 403, quotaCheck.message ?? 'Tenant quota exceeded');

  const parsed = CatalogueInputSchema.safeParse(JSON.parse(event.body ?? '{}'));
  if (!parsed.success) return err(event, 400, parsed.error.issues[0]?.message ?? 'Invalid input');
  const body = parsed.data;
  const validItems = ((body as Record<string, unknown>)['items'] as unknown[] ?? []).filter((item: unknown) => (item as Record<string, unknown>)?.['name']);
  if (validItems.length === 0) return err(event, 400, 'Catalogue must have at least one item');

  const catalogueId = ulid();
  const now = new Date().toISOString();
  const { brandName, brandColor, brandRegion } = await getBrandProfile(brandId);
  const payload = {
    ...body,
    imageUrl: body.headerImageUrl ?? body.imageUrl ?? null,
    headerImageUrl: body.headerImageUrl ?? body.imageUrl ?? null,
    brandId,
    brandName,
    brandColor,
    brandRegion,
    catalogueId,
    cataloguePdfKey: body.cataloguePdfKey ?? null,
    items: validItems,
    itemCount: validItems.length,
    targetSegments: normalizeTargetSegments(body.targetSegments as { spendBuckets?: string[]; visitFrequencies?: string[] } | undefined),
  };

  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK: `BRAND#${brandId}`,
      sK: `CATALOGUE#${catalogueId}`,
      eventType: 'CATALOGUE',
      status: 'ACTIVE',
      primaryCat: 'catalogue',
      desc: JSON.stringify(payload),
      createdAt: now,
      updatedAt: now,
    },
  }));

  const usage = await incrementTenantUsageCounter(tenantState.tenantId, brandId, 'catalogues');

  fanOutToSubscribers(
    brandId,
    'catalogues',
    {
      title: body.title as string,
      body: (body.description as string | undefined)?.trim() || 'New catalogue from your loyalty brand',
      data: {
        type: 'CATALOGUE',
        catalogueId,
        brandId,
        deepLink: `bebocard://catalogue/${catalogueId}`,
      },
    },
    async (permULID) => {
      await dynamo.send(new PutCommand({
        TableName: USER_TABLE,
        Item: {
          pK: `USER#${permULID}`,
          sK: `CATALOGUE#${brandId}#${catalogueId}`,
          eventType: 'CATALOGUE',
          status: 'UNREAD',
          primaryCat: 'catalogue',
          desc: JSON.stringify(payload),
          createdAt: now,
          updatedAt: now,
        },
      }));
    },
    async (permULID) => matchesTargetSegments(permULID, brandId, payload.targetSegments),
    tenantState.notifCap,
    'untargeted'
  ).then(async count => {
    const fanoutAt = new Date().toISOString();
    await Promise.all([
      dynamo.send(new UpdateCommand({
        TableName: REFDATA_TABLE,
        Key: { pK: `BRAND#${brandId}`, sK: `CATALOGUE#${catalogueId}` },
        UpdateExpression: 'SET fanoutCount = :c, fanoutAt = :at',
        ExpressionAttributeValues: { ':c': count, ':at': fanoutAt },
      })),
      writeAuditLog(dynamo, {
        actor: brandId,
        actorType: 'brand',
        action: 'catalogue.delivered',
        resource: `CATALOGUE#${catalogueId}`,
        outcome: 'success',
        metadata: { delivered: count },
      }),
    ]).catch(() => {});
  }).catch(e => console.error('[createCatalogue] fanOut error:', e));

  return ok(event, {
    catalogueId,
    status: deriveCampaignStatus(body.validFrom as string | undefined, body.validTo as string | undefined, 'ACTIVE'),
    billing: buildBillingUsageSnapshot(tenantState, usage),
  });
}

async function listCatalogues(event: APIGatewayProxyEvent, brandId: string) {
  const params = event.queryStringParameters ?? {};
  const limit = Math.min(parseInt(params.limit ?? '50', 10), 100);
  const cursor = params.cursor ? JSON.parse(Buffer.from(params.cursor, 'base64url').toString()) : undefined;

  const res = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: { ':pk': `BRAND#${brandId}`, ':prefix': 'CATALOGUE#' },
    ScanIndexForward: false,
    Limit: limit,
    ExclusiveStartKey: cursor,
  }));

  const catalogues = (res.Items ?? []).map((item) => {
    const desc = JSON.parse(item.desc ?? '{}');
    return {
      ...desc,
      catalogueId: desc.catalogueId ?? String(item.sK).replace('CATALOGUE#', ''),
      itemCount: Array.isArray(desc.items) ? desc.items.length : (desc.itemCount ?? 0),
      status: deriveCampaignStatus(desc.validFrom, desc.validTo, item.status as string | undefined),
      createdAt: item.createdAt,
      updatedAt: item.updatedAt,
    };
  });

  const nextCursor = res.LastEvaluatedKey
    ? Buffer.from(JSON.stringify(res.LastEvaluatedKey)).toString('base64url')
    : null;

  return ok(event, { catalogues, nextCursor });
}

// ─── Analytics ─────────────────────────────────────────────────────────────────

async function getAnalytics(event: APIGatewayProxyEvent, brandId: string) {
  const params = event.queryStringParameters ?? {};
  const from = params.from ?? new Date(Date.now() - 30 * 86400_000).toISOString();
  const to = params.to ?? new Date().toISOString();

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
  const scanCount = logs.filter(l => l.action === 'POST /scan').length;
  const receiptCount = logs.filter(l => l.action === 'POST /receipt').length;
  const offerCount = logs.filter(l => l.action === 'createOffer').length;

  // Subscription count via GSI
  const subRes = await dynamo.send(new QueryCommand({
    TableName: USER_TABLE,
    IndexName: 'sK-pK-index',
    KeyConditionExpression: 'sK = :sk',
    ExpressionAttributeValues: { ':sk': `SUBSCRIPTION#${brandId}` },
    Select: 'COUNT',
  }));

  return ok(event, {
    brandId,
    period: { from, to },
    scanCount,
    receiptCount,
    offerCount,
    subscriberCount: subRes.Count ?? 0,
  });
}

async function getUsage(event: APIGatewayProxyEvent, brandId: string) {
  const tenantState = await getTenantStateForBrand(brandId);
  if (!tenantState.tenantId) return err(event, 404, 'Tenant not found');

  const month = getUsageMonthKey();
  const usageEntries = await Promise.all(ALL_USAGE_TYPES.map(async (type) => {
    const usage = await getTenantUsageCounter(tenantState.tenantId!, type, month);
    return { type, ...usage };
  }));

  return ok(event, {
    tenantId: tenantState.tenantId,
    tier: tenantState.tier,
    active: tenantState.active,
    includedEventsPerMonth: tenantState.includedEventsPerMonth,
    month,
    usage: usageEntries,
  });
}

// ─── Stores ────────────────────────────────────────────────────────────────────

async function upsertStore(event: APIGatewayProxyEvent, brandId: string) {
  const rawBody = JSON.parse(event.body ?? '{}');
  const parsed = StoreInputSchema.safeParse(rawBody);
  if (!parsed.success) return err(event, 400, parsed.error.issues[0]?.message ?? 'Invalid input');
  const body = parsed.data;
  const { storeId } = rawBody as { storeId?: string };
  if (!storeId) return err(event, 400, 'Missing storeId');
  const { name: storeName, latitude: lat, longitude: lng, radiusKm } = body;
  const radiusMetres = Math.round(radiusKm * 1000);

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

  return ok(event, { storeId, upserted: true });
}

async function listStores(event: APIGatewayProxyEvent, brandId: string) {
  const res = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: { ':pk': 'STORES', ':prefix': `STORE#${brandId}#` },
  }));
  return ok(event, { stores: res.Items?.map(i => JSON.parse(i.desc)) ?? [] });
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
  return ok(event, { storeId, status: 'ARCHIVED' });
}

// ─── Subscription catalog — tenant self-onboarding ────────────────────────────

async function createSubscriptionCatalogEntry(event: APIGatewayProxyEvent, brandId: string) {
  const parsed = SubscriptionCatalogInputSchema.safeParse(JSON.parse(event.body ?? '{}'));
  if (!parsed.success) return err(event, 400, parsed.error.issues[0]?.message ?? 'Invalid input');
  const body = parsed.data;

  // providerId = brandId — brands register their own entry only
  const providerId = brandId;
  const now = new Date().toISOString();

  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `SUBSCRIPTION_CATALOG#${providerId}`, sK: 'profile' },
  }));
  if (existing.Item) return err(event, 409, 'Catalog entry already exists — use PUT to update');

  const desc = {
    providerId,
    tenantBrandId: brandId,
    providerName:  body.providerName,
    name:          body.providerName,
    category:      body.category,
    invoiceType:   body.invoiceType,
    plans:         body.plans.map(p => ({ ...p, planId: `${providerId}-${p.planName.toLowerCase().replace(/\s+/g, '-')}` })),
    websiteUrl:    body.websiteUrl ?? null,
    logoUrl:       body.logoUrl ?? null,
    cancelUrl:     body.cancelUrl ?? null,
    portalUrl:     body.portalUrl ?? null,
    affiliateUrl:  null,
    description:   body.description ?? null,
    region:        body.region,
    hasLinking:    body.hasLinking,
    isAffiliate:   false,       // admin grants affiliate status
    isTenantLinked: true,       // tenant self-registered
    listingStatus: 'UNLISTED',  // admin approves before surfacing in marketplace
    source:        'tenant',
    createdAt:     now,
    updatedAt:     now,
  };

  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK:         `SUBSCRIPTION_CATALOG#${providerId}`,
      sK:         'profile',
      eventType:  'SUBSCRIPTION_CATALOG',
      primaryCat: 'subscription_catalog',
      status:     'ACTIVE',
      source:     'tenant',  // top-level — allows catalog-subscription-sync to skip this entry
      desc:       JSON.stringify(desc),
      createdAt:  now,
      updatedAt:  now,
    },
  }));

  return {
    statusCode: 201,
    headers: { ...CORS_HEADERS, 'Access-Control-Allow-Origin': resolveOrigin(event) },
    body: JSON.stringify({ providerId, listingStatus: 'UNLISTED', status: 'ACTIVE' }),
  };
}

async function updateSubscriptionCatalogEntry(event: APIGatewayProxyEvent, brandId: string) {
  const providerId = brandId; // brands can only update their own entry

  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `SUBSCRIPTION_CATALOG#${providerId}`, sK: 'profile' },
  }));
  if (!existing.Item) return err(event, 404, 'Catalog entry not found — use POST to create');

  const existingDesc = JSON.parse(existing.Item.desc as string ?? '{}');
  // Reject if not tenant-owned
  if (existingDesc.tenantBrandId && existingDesc.tenantBrandId !== brandId) return err(event, 403, 'Forbidden');

  const parsed = SubscriptionCatalogInputSchema.partial().safeParse(JSON.parse(event.body ?? '{}'));
  if (!parsed.success) return err(event, 400, parsed.error.issues[0]?.message ?? 'Invalid input');

  const now = new Date().toISOString();
  const merged = {
    ...existingDesc,
    ...parsed.data,
    // Preserve admin-controlled fields
    isAffiliate:   existingDesc.isAffiliate,
    affiliateUrl:  existingDesc.affiliateUrl,
    listingStatus: existingDesc.listingStatus,
    isTenantLinked: true,
    updatedAt: now,
  };

  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `SUBSCRIPTION_CATALOG#${providerId}`, sK: 'profile' },
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
    ExpressionAttributeValues: { ':desc': JSON.stringify(merged), ':now': now },
  }));

  return ok(event, { providerId, updated: true });
}

async function getSubscriptionCatalogEntry(event: APIGatewayProxyEvent, brandId: string) {
  const res = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `SUBSCRIPTION_CATALOG#${brandId}`, sK: 'profile' },
  }));
  if (!res.Item) return err(event, 404, 'No catalog entry found — use POST to register');
  return ok(event, JSON.parse(res.Item.desc as string ?? '{}'));
}

// ─── Admin: subscription catalog management ────────────────────────────────────

async function adminListSubscriptionCatalog(event: APIGatewayProxyEvent) {
  const params = event.queryStringParameters ?? {};
  const filterStatus    = params.listingStatus;  // ACTIVE | INACTIVE | UNLISTED
  const filterAffiliate = params.isAffiliate;    // 'true' | 'false'
  const filterSource    = params.source;         // 'sync' | 'tenant'

  const items: Record<string, unknown>[] = [];
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new ScanCommand({
      TableName:        REFDATA_TABLE,
      FilterExpression: 'primaryCat = :cat',
      ExpressionAttributeValues: { ':cat': 'subscription_catalog' },
      ExclusiveStartKey: lastKey,
    }));
    items.push(...(res.Items ?? []));
    lastKey = res.LastEvaluatedKey as typeof lastKey;
  } while (lastKey);

  let catalog = items.map(i => {
    const d = JSON.parse(i.desc as string ?? '{}');
    return { ...d, _pK: i.pK, _status: i.status, _source: i.source };
  });

  if (filterStatus)    catalog = catalog.filter(c => c.listingStatus === filterStatus);
  if (filterAffiliate !== undefined) catalog = catalog.filter(c => String(c.isAffiliate) === filterAffiliate);
  if (filterSource)    catalog = catalog.filter(c => (c._source ?? c.source) === filterSource);

  // Strip internal DynamoDB key fields from response
  const cleaned = catalog.map(({ _pK: _pk, _status, _source, ...rest }) => ({ ...rest, pK: _pk, status: _status, source: _source ?? rest.source }));

  return ok(event, { catalog: cleaned, total: cleaned.length });
}

async function adminUpdateCatalogListing(event: APIGatewayProxyEvent) {
  const body = JSON.parse(event.body ?? '{}') as Record<string, unknown>;
  const providerId = (event.pathParameters?.providerId ?? body.providerId) as string | undefined;
  if (!providerId) return err(event, 400, 'Missing providerId');

  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `SUBSCRIPTION_CATALOG#${providerId}`, sK: 'profile' },
  }));
  if (!existing.Item) return err(event, 404, 'Catalog entry not found');

  const desc = JSON.parse(existing.Item.desc as string ?? '{}');
  const now = new Date().toISOString();

  // Admin-controllable fields only
  if (body.listingStatus !== undefined) desc.listingStatus = body.listingStatus;
  if (body.isAffiliate   !== undefined) desc.isAffiliate   = !!body.isAffiliate;
  if (body.affiliateUrl  !== undefined) desc.affiliateUrl  = body.affiliateUrl;
  if (body.hasLinking    !== undefined) desc.hasLinking    = !!body.hasLinking;
  if (body.isTenantLinked !== undefined) desc.isTenantLinked = !!body.isTenantLinked;
  desc.updatedAt = now;

  const itemStatus = desc.listingStatus === 'ACTIVE' ? 'ACTIVE'
    : desc.listingStatus === 'INACTIVE' ? 'INACTIVE'
    : 'UNLISTED';

  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `SUBSCRIPTION_CATALOG#${providerId}`, sK: 'profile' },
    UpdateExpression: 'SET desc = :desc, #s = :status, updatedAt = :now',
    ExpressionAttributeNames:  { '#s': 'status' },
    ExpressionAttributeValues: { ':desc': JSON.stringify(desc), ':status': itemStatus, ':now': now },
  }));

  return ok(event, { providerId, updated: true, listingStatus: desc.listingStatus, isAffiliate: desc.isAffiliate });
}

// ─── API key self-rotation ─────────────────────────────────────────────────────

async function handleRotateKey(event: APIGatewayProxyEvent, brandId: string) {
  const { oldKeyId, createdBy = 'brand_self' } = JSON.parse(event.body ?? '{}');
  if (!oldKeyId) return err(event, 400, 'Missing oldKeyId');

  const { rawKey, newKeyId } = await rotateApiKey(dynamo, brandId, oldKeyId, createdBy);

  return ok(event, {
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

function parseRecord(value: unknown): Record<string, unknown> {
  if (typeof value === 'object' && value !== null) return value as Record<string, unknown>;
  if (typeof value !== 'string' || value.length === 0) return {};
  try {
    return JSON.parse(value) as Record<string, unknown>;
  } catch {
    return {};
  }
}

async function getTenantStateForBrand(brandId: string): Promise<{ tenantId: string | null; tier: TenantTier; active: boolean; includedEventsPerMonth: number | null; notifCap: number }> {
  return getSharedTenantStateForBrand(dynamo, REFDATA_TABLE, brandId);
}

async function incrementTenantUsageCounter(
  tenantId: string | null,
  brandId: string,
  type: UsageType,
): Promise<{ month: string; usageCount: number; lastUpdatedAt: string; lastBrandId: string }> {
  return incrementSharedTenantUsageCounter(dynamo, REFDATA_TABLE, tenantId, brandId, type);
}

async function getTenantUsageCounter(tenantId: string, type: UsageType, month = getUsageMonthKey()) {
  return getSharedTenantUsageCounter(dynamo, REFDATA_TABLE, tenantId, type, month);
}

async function checkTenantQuota(
  tenantState: { tenantId: string | null; tier: TenantTier; includedEventsPerMonth: number | null },
  type: UsageType,
): Promise<{ allowed: boolean; message?: string }> {
  return checkSharedTenantQuota(dynamo, REFDATA_TABLE, tenantState, type);
}

function buildBillingUsageSnapshot(
  tenantState: { tier: TenantTier; includedEventsPerMonth: number | null },
  usage: { month: string; usageCount: number },
) {
  return {
    tier: tenantState.tier,
    month: usage.month,
    currentTypeUsage: usage.usageCount,
    includedEventsPerMonth: tenantState.includedEventsPerMonth,
  };
}

function isFutureIso(value: unknown): boolean {
  if (typeof value !== 'string' || value.length === 0) return false;
  const ts = Date.parse(value);
  return !Number.isNaN(ts) && ts > Date.now();
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
  shouldSendToUser?: (permULID: string) => Promise<boolean>,
  notifCap = 3,
  campaignType: 'untargeted' | 'acquisition' | 'loyalty_reward' = 'untargeted'
): Promise<number> {
  let recipientCount = 0;
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      IndexName: 'sK-pK-index',
      KeyConditionExpression: 'sK = :sk',
      ExpressionAttributeValues: { ':sk': `SUBSCRIPTION#${brandId}` },
      ExclusiveStartKey: lastKey,
      Limit: 100,
    }));
    lastKey = res.LastEvaluatedKey as typeof lastKey;

    const BATCH_SIZE = 10;
    const items = res.Items ?? [];

    for (let i = 0; i < items.length; i += BATCH_SIZE) {
      const chunk = items.slice(i, i + BATCH_SIZE);
      await Promise.all(chunk.map(async (item) => {
        // Status gate — skip INACTIVE/REVOKED subscriptions
        if (item.status !== 'ACTIVE') return;

        const desc = parseRecord(item.desc);
        const preferenceEnabled = item[preferenceKey] !== undefined
          ? !!item[preferenceKey]
          : desc[preferenceKey] !== undefined
            ? !!desc[preferenceKey]
            : true;
        if (!preferenceEnabled) return;
        if (preferenceKey === 'offers' && isFutureIso(desc.offersSnoozeUntil)) return;

        const permULID = (item.pK as string).replace('USER#', '');

        // 1. Notification Intelligence (P2-13)
        const [segmentRes, identityRes] = await Promise.all([
          dynamo.send(new GetCommand({
            TableName: USER_TABLE,
            Key: { pK: `USER#${permULID}`, sK: `SEGMENT#${brandId}` },
          })),
          dynamo.send(new GetCommand({
            TableName: USER_TABLE,
            Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
          }))
        ]);

        const segment = segmentRes.Item?.desc ? JSON.parse(segmentRes.Item.desc) : null;
        const identity = identityRes.Item?.desc ? JSON.parse(identityRes.Item.desc) : {};
        const priority = getRelevancePriority(segment, campaignType);

        if (priority === 'SKIP') return;

        // 2. Frequency Capping (P2-13)
        // Note: Acquisition offers waive the cap to ensure first-time engagement
        if (campaignType !== 'acquisition') {
          const capReached = await isNotificationCapReached(dynamo, USER_TABLE, permULID, brandId, notifCap);
          if (capReached) {
            // Deprioritized users are strictly capped. Normal/Priority users also capped but logged differently if needed.
            console.info(`[fanOut] CAP_REACHED for ${permULID} (brand: ${brandId}, cap: ${notifCap})`);
            return;
          }
        }

        // 3. Global Snooze (Quiet Hours) - P2-13
        // Stored in IDENTITY as HH:MM UTC
        if (identity.globalSnoozeStart && identity.globalSnoozeEnd) {
          const now = new Date();
          const currentTime = `${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}`;
          const { globalSnoozeStart, globalSnoozeEnd } = identity;

          const isSnoozed = globalSnoozeStart <= globalSnoozeEnd
            ? (currentTime >= globalSnoozeStart && currentTime <= globalSnoozeEnd)
            : (currentTime >= globalSnoozeStart || currentTime <= globalSnoozeEnd);

          if (isSnoozed) {
            console.info(`[fanOut] SNOOZE_ACTIVE for ${permULID} (${currentTime} is within ${globalSnoozeStart}-${globalSnoozeEnd})`);
            return;
          }
        }

        if (preferenceKey === 'offers') {
          const prefRes = await dynamo.send(new GetCommand({
            TableName: USER_TABLE,
            Key: { pK: `USER#${permULID}`, sK: 'PREFERENCES' },
          }));
          const prefs = parseRecord(prefRes.Item?.desc);
          if (isFutureIso(prefs.offersGlobalSnoozeUntil)) return;
        }

        if (shouldSendToUser && !(await shouldSendToUser(permULID))) return;

        const tokenItem = await dynamo.send(new GetCommand({
          TableName: USER_TABLE,
          Key: { pK: `USER#${permULID}`, sK: 'DEVICE_TOKEN' },
        }));
        const token = tokenItem.Item?.desc ? JSON.parse(tokenItem.Item.desc).token : null;
        if (!token) return;

        if (perSubscriberFn) await perSubscriberFn(permULID).catch(console.error);

        try {
          await getFirebase().send({
            token,
            notification: { title: notification.title, body: notification.body },
            data: { 
              ...notification.data,
              priority, // Pass priority to app for potential UI badges
            },
          });
          recipientCount++;
          
          // Increment weekly cap counter on successful send
          await incrementNotificationCounter(dynamo, USER_TABLE, permULID, brandId).catch(err => {
            console.warn(`[fanOut] counter increment failed for ${permULID}:`, err);
          });
        } catch (e) {
          console.error(`[fanOut] FCM failed for ${permULID}:`, e);
        }
      }));
    }
  } while (lastKey);

  return recipientCount;
}

async function getBrandProfile(brandId: string): Promise<{ brandName: string; brandColor: string; brandRegion: string | null }> {
  const res = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'profile' },
  }));
  const desc = JSON.parse(res.Item?.desc ?? '{}');
  return {
    brandName: desc.brandName ?? brandId,
    brandColor: desc.brandColor ?? '#4F46E5',
    brandRegion: typeof desc.region === 'string' ? desc.region : null,
  };
}

function normalizeTargetSegments(targetSegments?: {
  spendBuckets?: string[];
  visitFrequencies?: string[];
  customSegmentIds?: string[];
}) {
  const spendBuckets = (targetSegments?.spendBuckets ?? []).filter(Boolean);
  const visitFrequencies = (targetSegments?.visitFrequencies ?? []).filter(Boolean);
  const customSegmentIds = (targetSegments?.customSegmentIds ?? []).filter(Boolean);
  if (spendBuckets.length === 0 && visitFrequencies.length === 0 && customSegmentIds.length === 0) return undefined;
  return { spendBuckets, visitFrequencies, customSegmentIds };
}

function deriveCampaignStatus(validFrom?: string, validTo?: string, storedStatus?: string) {
  if (storedStatus === 'ARCHIVED') return 'ARCHIVED';
  const now = Date.now();
  const from = validFrom ? Date.parse(validFrom) : Number.NaN;
  const to = validTo ? Date.parse(validTo) : Number.NaN;
  if (!Number.isNaN(from) && now < from) return 'SCHEDULED';
  if (!Number.isNaN(to) && now > to) return 'EXPIRED';
  return 'ACTIVE';
}

async function matchesTargetSegments(
  permULID: string,
  brandId: string,
  targetSegments?: { spendBuckets?: string[]; visitFrequencies?: string[]; customSegmentIds?: string[] },
): Promise<boolean> {
  if (!targetSegments) return true;

  // ── Standard pre-computed segment (spendBucket / visitFrequency) ──────────
  const hasStandardFilters =
    (targetSegments.spendBuckets?.length ?? 0) > 0 ||
    (targetSegments.visitFrequencies?.length ?? 0) > 0;

  if (hasStandardFilters) {
    const segmentRes = await dynamo.send(new GetCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: `SEGMENT#${brandId}` },
    }));

    if (!segmentRes.Item?.desc) return true; // No segment record yet → inclusive default
    const desc = JSON.parse(segmentRes.Item.desc);
    const spendBucket = desc.spendBucket as string | undefined;
    const visitFrequency = desc.visitFrequency as string | undefined;
    const spendOk = !targetSegments.spendBuckets?.length || (spendBucket != null && targetSegments.spendBuckets.includes(spendBucket));
    const visitOk = !targetSegments.visitFrequencies?.length || (visitFrequency != null && targetSegments.visitFrequencies.includes(visitFrequency));
    if (!spendOk || !visitOk) return false;
  }

  // ── Custom segment membership check ──────────────────────────────────────
  const customIds = targetSegments.customSegmentIds ?? [];
  if (customIds.length === 0) return true;

  // BatchGet all custom segment membership records for this user
  const keys = [
    ...customIds.map(id => ({ pK: `USER#${permULID}`, sK: `SEGMENT#${brandId}#CUSTOM#${id}` })),
    // Also check global (bebocard-level) membership for each custom ID
    ...customIds.map(id => ({ pK: `USER#${permULID}`, sK: `SEGMENT#bebocard#CUSTOM#${id}` })),
  ];

  const batchRes = await dynamo.send(new BatchGetCommand({
    RequestItems: { [USER_TABLE]: { Keys: keys, ProjectionExpression: 'sK, #st', ExpressionAttributeNames: { '#st': 'status' } } },
  }));

  const memberItems = batchRes.Responses?.[USER_TABLE] ?? [];
  const activeSKs = new Set(memberItems.filter(i => i.status === 'ACTIVE').map(i => i.sK as string));

  // User must be ACTIVE in ALL specified custom segments (brand-level OR global counts)
  return customIds.every(id =>
    activeSKs.has(`SEGMENT#${brandId}#CUSTOM#${id}`) ||
    activeSKs.has(`SEGMENT#bebocard#CUSTOM#${id}`),
  );
}

// ─── Custom segment CRUD ──────────────────────────────────────────────────────

const SEGMENT_RULE_METRICS = ['visit_count', 'total_spend', 'avg_order_value', 'days_since_last_visit'] as const;
const SEGMENT_RULE_OPERATORS = ['gte', 'lte', 'between', 'eq'] as const;
const SEGMENT_RULE_PERIODS = ['day', 'week', 'month', 'quarter'] as const;

function validateSegmentRules(rules: unknown[]): string | null {
  if (!Array.isArray(rules) || rules.length === 0) return 'rules must be a non-empty array';
  for (const r of rules) {
    if (typeof r !== 'object' || r === null) return 'each rule must be an object';
    const rule = r as Record<string, unknown>;
    if (!SEGMENT_RULE_METRICS.includes(rule.metric as never)) return `invalid metric: ${rule.metric}`;
    if (!SEGMENT_RULE_OPERATORS.includes(rule.operator as never)) return `invalid operator: ${rule.operator}`;
    if (!SEGMENT_RULE_PERIODS.includes(rule.period as never)) return `invalid period: ${rule.period}`;
    if (typeof rule.value !== 'number') return 'rule.value must be a number';
    if (rule.operator === 'between' && typeof rule.value2 !== 'number') return 'rule.value2 required for between';
  }
  return null;
}

async function getTenantIdForBrand(brandId: string): Promise<string | null> {
  const res = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    IndexName: 'sK-pK-index',
    KeyConditionExpression: 'sK = :profile',
    FilterExpression: 'contains(#brandIds, :brandId)',
    ExpressionAttributeNames: { '#brandIds': 'brandIds' },
    ExpressionAttributeValues: { ':profile': 'profile', ':brandId': brandId },
    Limit: 1,
  }));
  if (!res.Items?.[0]) return null;
  return (res.Items[0].pK as string).replace('TENANT#', '');
}

async function createSegmentDef(event: APIGatewayProxyEvent, brandId: string) {
  const body = JSON.parse(event.body ?? '{}') as Record<string, unknown>;
  const { name, rules, logicalOperator = 'AND', scope = 'brand' } = body;

  if (!name || typeof name !== 'string') return err(event, 400, 'name is required');
  if (!Array.isArray(rules)) return err(event, 400, 'rules is required');

  const ruleError = validateSegmentRules(rules);
  if (ruleError) return err(event, 400, ruleError);
  if (!['AND', 'OR'].includes(logicalOperator as string)) return err(event, 400, 'logicalOperator must be AND or OR');
  if (!['brand', 'global'].includes(scope as string)) return err(event, 400, 'scope must be brand or global');

  const tenantId = await getTenantIdForBrand(brandId);
  if (!tenantId) return err(event, 403, 'Brand is not associated with a tenant');

  const segmentId = ulid();
  const now = new Date().toISOString();

  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK: `TENANT#${tenantId}`,
      sK: `SEGMENT_DEF#${segmentId}`,
      brandId,
      desc: JSON.stringify({
        name,
        rules,
        logicalOperator,
        scope,
        brandId,
        active: true,
        memberCount: 0,
        lastEvaluatedAt: null,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  return ok(event, { segmentId, tenantId, name, rules, logicalOperator, scope, active: true, createdAt: now });
}

async function listSegmentDefs(event: APIGatewayProxyEvent, brandId: string) {
  const tenantId = await getTenantIdForBrand(brandId);
  if (!tenantId) return err(event, 403, 'Brand is not associated with a tenant');

  const res = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: {
      ':pk': `TENANT#${tenantId}`,
      ':prefix': 'SEGMENT_DEF#',
    },
  }));

  const segments = (res.Items ?? []).map(item => {
    const segmentId = (item.sK as string).replace('SEGMENT_DEF#', '');
    let desc: Record<string, unknown> = {};
    try { desc = JSON.parse(item.desc ?? '{}'); } catch { /* ignore */ }
    return {
      segmentId,
      name: desc.name,
      rules: desc.rules,
      logicalOperator: desc.logicalOperator,
      scope: desc.scope,
      active: desc.active,
      memberCount: desc.memberCount ?? 0,
      lastEvaluatedAt: desc.lastEvaluatedAt ?? null,
      createdAt: item.createdAt,
      updatedAt: item.updatedAt,
    };
  });

  return ok(event, { segments });
}

async function updateSegmentDef(event: APIGatewayProxyEvent, brandId: string) {
  const segmentId = event.pathParameters?.segmentId ?? event.path.split('/segments/')[1]?.split('/')[0];
  if (!segmentId) return err(event, 400, 'segmentId is required');

  const body = JSON.parse(event.body ?? '{}') as Record<string, unknown>;

  const tenantId = await getTenantIdForBrand(brandId);
  if (!tenantId) return err(event, 403, 'Brand is not associated with a tenant');

  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: `SEGMENT_DEF#${segmentId}` },
  }));
  if (!existing.Item) return err(event, 404, 'Segment not found');

  let existingDesc: Record<string, unknown> = {};
  try { existingDesc = JSON.parse(existing.Item.desc ?? '{}'); } catch { /* ignore */ }

  const rules = body.rules !== undefined ? body.rules as unknown[] : existingDesc.rules;
  if (body.rules !== undefined) {
    const ruleError = validateSegmentRules(rules as unknown[]);
    if (ruleError) return err(event, 400, ruleError);
  }

  const updatedDesc = {
    ...existingDesc,
    ...(body.name !== undefined && { name: body.name }),
    ...(body.rules !== undefined && { rules }),
    ...(body.logicalOperator !== undefined && { logicalOperator: body.logicalOperator }),
    ...(body.scope !== undefined && { scope: body.scope }),
    ...(body.active !== undefined && { active: body.active }),
  };

  const now = new Date().toISOString();
  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: `SEGMENT_DEF#${segmentId}` },
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
    ExpressionAttributeValues: { ':desc': JSON.stringify(updatedDesc), ':now': now },
  }));

  return ok(event, { segmentId, ...updatedDesc, updatedAt: now });
}

async function deleteSegmentDef(event: APIGatewayProxyEvent, brandId: string) {
  const segmentId = event.pathParameters?.segmentId ?? event.path.split('/segments/')[1]?.split('/')[0];
  if (!segmentId) return err(event, 400, 'segmentId is required');

  const tenantId = await getTenantIdForBrand(brandId);
  if (!tenantId) return err(event, 403, 'Brand is not associated with a tenant');

  await dynamo.send(new DeleteCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: `SEGMENT_DEF#${segmentId}` },
    ConditionExpression: 'attribute_exists(pK)',
  })).catch(e => {
    if (e.name === 'ConditionalCheckFailedException') return null; // already gone — idempotent
    throw e;
  });

  return ok(event, { segmentId, deleted: true });
}

// ─── Card Config (loyalty template opt-in) ────────────────────────────────────
//
// DynamoDB key patterns (RefDataEvent):
//   pK: BRAND#<brandId>            sK: CARD_CONFIG#<templateId>   — brand's opt-in record
//   pK: CARDCONFIG#PENDING         sK: BRAND#<brandId>#<templateId>  — admin approval queue

type CardConfigStatus = 'PENDING' | 'APPROVED' | 'REJECTED' | 'REVOKED';

async function createCardConfig(event: APIGatewayProxyEvent, brandId: string) {
  const body = JSON.parse(event.body ?? '{}') as {
    templateId?: string;
    loyaltyProgramName?: string;  // brand's own name for the program, e.g. "Everyday Rewards"
    loyaltyProgramUrl?: string;   // optional: link to brand's loyalty T&Cs
    notes?: string;               // optional context for BeboCard admin reviewer
  };

  const templateId = (body.templateId ?? '').trim();
  const loyaltyProgramName = (body.loyaltyProgramName ?? '').trim();

  if (!templateId) return err(event, 400, 'templateId is required');
  if (!loyaltyProgramName) return err(event, 400, 'loyaltyProgramName is required');
  if (loyaltyProgramName.length > 100) return err(event, 400, 'loyaltyProgramName must be ≤ 100 characters');

  // Verify the template exists and is APPROVED
  const templateResult = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TEMPLATE#${templateId}`, sK: 'PROFILE' },
  }));

  if (!templateResult.Item) return err(event, 404, 'Template not found');
  if (templateResult.Item.status !== 'APPROVED') {
    return err(event, 409, 'Template is not approved for brand opt-in');
  }

  // Check for existing config (idempotency)
  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `CARD_CONFIG#${templateId}` },
  }));

  if (existing.Item && existing.Item.status !== 'REJECTED' && existing.Item.status !== 'REVOKED') {
    return err(event, 409, `Card config already exists with status: ${existing.Item.status}`);
  }

  const now = new Date().toISOString();

  const configItem = {
    pK: `BRAND#${brandId}`,
    sK: `CARD_CONFIG#${templateId}`,
    brandId,
    templateId,
    templateName: templateResult.Item.name,
    loyaltyProgramName,
    loyaltyProgramUrl: body.loyaltyProgramUrl?.trim() ?? null,
    notes: body.notes?.trim() ?? null,
    status: 'PENDING' as CardConfigStatus,
    createdAt: now,
    updatedAt: now,
  };

  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: configItem,
  }));

  // Write to the admin approval queue index
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK: 'CARDCONFIG#PENDING',
      sK: `BRAND#${brandId}#${templateId}`,
      brandId,
      templateId,
      templateName: templateResult.Item.name,
      loyaltyProgramName,
      status: 'PENDING',
      createdAt: now,
      updatedAt: now,
    },
  }));

  const { pK, sK, ...config } = configItem;
  console.info('[brand-api-handler] card config created', { brandId, templateId });
  return ok(event, config);
}

async function listCardConfigs(event: APIGatewayProxyEvent, brandId: string) {
  const result = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: {
      ':pk': `BRAND#${brandId}`,
      ':prefix': 'CARD_CONFIG#',
    },
  }));

  const configs = (result.Items ?? []).map(({ pK: _pK, sK: _sK, ...rest }) => rest);
  return ok(event, { configs, count: configs.length });
}

async function deleteCardConfig(event: APIGatewayProxyEvent, brandId: string, templateId: string) {
  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `CARD_CONFIG#${templateId}` },
  }));

  if (!existing.Item) return err(event, 404, 'Card config not found');
  if (existing.Item.status === 'APPROVED') {
    return err(event, 409, 'Cannot delete an approved card config. Request admin to revoke it instead.');
  }

  const now = new Date().toISOString();

  // Soft-delete: set status to REVOKED
  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `CARD_CONFIG#${templateId}` },
    UpdateExpression: 'SET #status = :status, updatedAt = :now',
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: { ':status': 'REVOKED', ':now': now },
  }));

  // Remove from pending queue if still there
  await dynamo.send(new DeleteCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: 'CARDCONFIG#PENDING', sK: `BRAND#${brandId}#${templateId}` },
  })).catch(() => null); // not in queue — ok

  return ok(event, { brandId, templateId, status: 'REVOKED', updatedAt: now });
}

// ─── Admin: Card Config Approval Queue ───────────────────────────────────────

async function adminListCardConfigs(event: APIGatewayProxyEvent) {
  const qp = (event.queryStringParameters ?? {}) as Record<string, string>;
  const filterStatus = (qp.status ?? 'PENDING').toUpperCase() as CardConfigStatus | 'ALL';

  if (filterStatus === 'PENDING') {
    // Fast path: read from the pending index
    const result = await dynamo.send(new QueryCommand({
      TableName: REFDATA_TABLE,
      KeyConditionExpression: 'pK = :pk',
      ExpressionAttributeValues: { ':pk': 'CARDCONFIG#PENDING' },
    }));
    const configs = (result.Items ?? []).map(({ pK: _pK, sK: _sK, ...rest }) => rest);
    return ok(event, { configs, count: configs.length });
  }

  // All statuses: scan BRAND# records for CARD_CONFIG# sort keys
  // At platform scale (< 1,000 brands initially) this is acceptable.
  const result = await dynamo.send(new ScanCommand({
    TableName: REFDATA_TABLE,
    FilterExpression: 'begins_with(sK, :prefix) AND (pK BETWEEN :lo AND :hi)',
    ExpressionAttributeValues: {
      ':prefix': 'CARD_CONFIG#',
      ':lo': 'BRAND#',
      ':hi': 'BRAND#\uFFFF',
    },
  }));

  let configs = (result.Items ?? []).map(({ pK: _pK, sK: _sK, ...rest }) => rest);
  if (filterStatus !== 'ALL') {
    configs = configs.filter(c => c.status === filterStatus);
  }

  return ok(event, { configs, count: configs.length });
}

async function adminActOnCardConfig(
  event: APIGatewayProxyEvent,
  brandId: string,
  templateId: string,
  action: 'approve' | 'reject' | 'revoke',
) {
  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `CARD_CONFIG#${templateId}` },
  }));

  if (!existing.Item) return err(event, 404, 'Card config not found');

  const current = existing.Item as { status: CardConfigStatus; loyaltyProgramName: string };
  const now = new Date().toISOString();

  const validTransitions: Record<string, CardConfigStatus[]> = {
    approve: ['PENDING'],
    reject:  ['PENDING'],
    revoke:  ['APPROVED'],
  };

  if (!validTransitions[action].includes(current.status)) {
    return err(event, 409, `Cannot ${action} a config with status ${current.status}`);
  }

  const newStatus: CardConfigStatus =
    action === 'approve' ? 'APPROVED' :
    action === 'reject'  ? 'REJECTED' :
    'REVOKED';

  const body = JSON.parse(event.body ?? '{}') as { rejectionReason?: string };
  const rejectionReason = action === 'reject' ? (body.rejectionReason?.trim() ?? '') : undefined;

  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: `CARD_CONFIG#${templateId}` },
    UpdateExpression: rejectionReason
      ? 'SET #status = :status, updatedAt = :now, rejectionReason = :reason'
      : 'SET #status = :status, updatedAt = :now',
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: rejectionReason
      ? { ':status': newStatus, ':now': now, ':reason': rejectionReason }
      : { ':status': newStatus, ':now': now },
  }));

  // If approving: write to the DISCOVERY index so the Flutter app can find it.
  // If rejecting/revoking: remove from DISCOVERY.
  if (action === 'approve') {
    // Fetch template details for the discovery record
    const templateResult = await dynamo.send(new GetCommand({
      TableName: REFDATA_TABLE,
      Key: { pK: `TEMPLATE#${templateId}`, sK: 'PROFILE' },
    }));
    const template = templateResult.Item ?? {};

    await dynamo.send(new PutCommand({
      TableName: REFDATA_TABLE,
      Item: {
        pK: `DISCOVERY#CARD_CONFIGS`,
        sK: `BRAND#${brandId}#TEMPLATE#${templateId}`,
        brandId,
        templateId,
        templateName: template.name ?? '',
        loyaltyProgramName: current.loyaltyProgramName,
        barcodeFormat: template.barcodeFormat ?? 'QR_CODE',
        primaryColor: template.primaryColor ?? '#1A1A2E',
        accentColor: template.accentColor ?? '#16213E',
        logoUrl: template.logoUrl ?? null,
        fieldLabels: template.fieldLabels ?? {},
        status: 'APPROVED',
        approvedAt: now,
        updatedAt: now,
      },
    }));
  } else {
    await dynamo.send(new DeleteCommand({
      TableName: REFDATA_TABLE,
      Key: { pK: 'DISCOVERY#CARD_CONFIGS', sK: `BRAND#${brandId}#TEMPLATE#${templateId}` },
    })).catch(() => null);
  }

  // Remove from pending queue regardless of action
  await dynamo.send(new DeleteCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: 'CARDCONFIG#PENDING', sK: `BRAND#${brandId}#${templateId}` },
  })).catch(() => null);

  console.info('[brand-api-handler] admin card config action', { brandId, templateId, action, newStatus, hasRejectionReason: !!rejectionReason });
  return ok(event, { brandId, templateId, status: newStatus, updatedAt: now });
}
