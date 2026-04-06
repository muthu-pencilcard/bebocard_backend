import type { APIGatewayProxyEvent, APIGatewayProxyHandler, APIGatewayProxyResult } from 'aws-lambda';
import { createHmac, createPublicKey, createVerify, randomUUID } from 'crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  checkTenantQuota,
  getTenantStateForBrand,
  incrementTenantUsageCounter,
} from '../../shared/tenant-billing';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const ADMIN_TABLE = process.env.ADMIN_TABLE!;
const COGNITO_REGION = process.env.COGNITO_REGION || process.env.AWS_REGION || 'ap-southeast-2';
const COGNITO_USER_POOL_ID = process.env.COGNITO_USER_POOL_ID ?? '';
const WIDGET_TOKEN_SECRET = process.env.WIDGET_TOKEN_SECRET ?? '';

type WidgetAction = 'invoice' | 'giftcard';
type WidgetClaims = {
  jti: string;
  tenantId: string;
  brandId: string;
  action: WidgetAction;
  permULID: string;
  origin: string;
  exp: number;
};

const CORS_HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
};

const JWKS_CACHE = new Map<string, JsonWebKey>();

export const handler: APIGatewayProxyHandler = async (event): Promise<APIGatewayProxyResult> => {
  const method = event.httpMethod;
  const path = event.path ?? '';

  if (method === 'OPTIONS') {
    return withCors(event, 204, {});
  }

  try {
    if (method === 'POST' && path.endsWith('/widget/auth')) return handleWidgetAuth(event);
    if (method === 'POST' && path.endsWith('/widget/invoice')) return handleWidgetInvoice(event);
    if (method === 'GET' && path.endsWith('/widget/giftcards')) return handleWidgetGiftCards(event);
    if (method === 'POST' && path.endsWith('/widget/giftcard/select')) return handleWidgetGiftCardSelect(event);
    return withCors(event, 404, { error: 'Not found' });
  } catch (error) {
    console.error('[widget-action-handler]', error);
    return withCors(event, 500, { error: 'Internal error' });
  }
};

async function handleWidgetAuth(event: APIGatewayProxyEvent) {
  const body = parseBody(event.body);
  const action = normalizeAction(body.action);
  const brandId = stringOrEmpty(body.brandId);
  if (!action || !brandId) return withCors(event, 400, { error: 'brandId and valid action are required' });
  if (!WIDGET_TOKEN_SECRET) return withCors(event, 503, { error: 'Widget token signing is not configured' });

  const origin = getRequestOrigin(event);
  const brand = await getWidgetBrandConfig(brandId);
  if (!brand) return withCors(event, 404, { error: 'Brand not found' });
  if (!isAllowedOrigin(origin, brand.allowedWidgetDomains)) return withCors(event, 403, { error: 'Origin not allowed for widget access' });
  if (!brand.widgetActions[action]) return withCors(event, 403, { error: 'Widget action is disabled for this brand' });

  const idToken = extractBearer(event.headers) ?? stringOrEmpty(body.idToken);
  if (!idToken) return withCors(event, 401, { error: 'Missing Cognito id_token' });

  const user = await verifyCognitoIdToken(idToken);
  if (!user?.permULID) return withCors(event, 401, { error: 'Invalid user token' });

  const expiresAt = new Date(Date.now() + 5 * 60 * 1000).toISOString();
  const claims: WidgetClaims = {
    jti: randomUUID(),
    tenantId: brand.tenantId,
    brandId,
    action,
    permULID: user.permULID,
    origin,
    exp: Math.floor(Date.now() / 1000) + 5 * 60,
  };

  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `WIDGETTOKEN#${claims.jti}`,
      sK: user.permULID,
      eventType: 'WIDGET_TOKEN',
      status: 'ACTIVE',
      desc: JSON.stringify({
        tenantId: claims.tenantId,
        brandId,
        action,
        origin,
        issuedAt: new Date().toISOString(),
        expiresAt,
        email: user.email ?? null,
      }),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  return withCors(event, 200, {
    token: signWidgetClaims(claims),
    expiresAt,
    tenantId: claims.tenantId,
    brandId,
    action,
    user: { permULID: user.permULID, email: user.email ?? null },
  });
}

async function handleWidgetInvoice(event: APIGatewayProxyEvent) {
  const auth = await authorizeWidgetRequest(event, 'invoice');
  if ('response' in auth) return auth.response;

  const tenantState = await getTenantStateForBrand(dynamo, REFDATA_TABLE, auth.claims.brandId);
  if (!tenantState.active) return withCors(event, 403, { error: 'Tenant billing is suspended' });

  const quotaCheck = await checkTenantQuota(dynamo, REFDATA_TABLE, tenantState, 'invoices');
  if (!quotaCheck.allowed) return withCors(event, 403, { error: quotaCheck.message ?? 'Tenant quota exceeded' });

  const body = parseBody(event.body);
  const supplier = stringOrEmpty(body.supplier);
  const dueDate = stringOrEmpty(body.dueDate);
  const amount = Number(body.amount);
  if (!supplier || !dueDate || !Number.isFinite(amount)) {
    return withCors(event, 400, { error: 'supplier, amount, and dueDate are required' });
  }

  const now = new Date().toISOString();
  const invoiceId = randomUUID();
  const invoiceSK = `INVOICE#${dueDate.slice(0, 10)}#${invoiceId}`;
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${auth.claims.permULID}`,
      sK: invoiceSK,
      eventType: 'INVOICE',
      status: 'ACTIVE',
      primaryCat: 'invoice',
      subCategory: stringOrEmpty(body.category) || 'widget',
      desc: JSON.stringify({
        supplier,
        amount,
        currency: stringOrEmpty(body.currency) || 'AUD',
        dueDate,
        invoiceNumber: stringOrEmpty(body.invoiceNumber) || null,
        notes: stringOrEmpty(body.notes) || null,
        status: 'unpaid',
        paidDate: null,
        brandId: auth.claims.brandId,
        tenantId: auth.claims.tenantId,
        source: 'widget',
        createdAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK) AND attribute_not_exists(sK)',
  }));

  await incrementTenantUsageCounter(dynamo, REFDATA_TABLE, tenantState.tenantId, auth.claims.brandId, 'invoices');
  await markWidgetTokenUsed(auth.claims);
  return withCors(event, 200, { success: true, invoiceSK, status: 'ACTIVE' });
}

async function handleWidgetGiftCards(event: APIGatewayProxyEvent) {
  const auth = await authorizeWidgetRequest(event, 'giftcard', { invalidateOnSuccess: false });
  if ('response' in auth) return auth.response;

  const res = await dynamo.send(new QueryCommand({
    TableName: USER_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: {
      ':pk': `USER#${auth.claims.permULID}`,
      ':prefix': 'GIFTCARD#',
    },
  }));

  const cards = (res.Items ?? [])
    .filter((item) => item.status === 'ACTIVE')
    .map((item) => {
      const desc = safeJson(item.desc);
      return {
        cardSK: item.sK,
        brandId: desc.brandId ?? item.subCategory ?? null,
        brandName: desc.brandName ?? 'Gift Card',
        balance: desc.balance ?? desc.giftCardValue ?? 0,
        currency: desc.currency ?? 'AUD',
        expiryDate: desc.expiryDate ?? null,
        maskedCardNumber: maskCardNumber(String(desc.cardNumber ?? '')),
      };
    })
    .filter((card) => card.brandId === auth.claims.brandId);

  return withCors(event, 200, { cards });
}

async function handleWidgetGiftCardSelect(event: APIGatewayProxyEvent) {
  const auth = await authorizeWidgetRequest(event, 'giftcard');
  if ('response' in auth) return auth.response;

  const body = parseBody(event.body);
  const cardSK = stringOrEmpty(body.cardSK);
  if (!cardSK) return withCors(event, 400, { error: 'cardSK is required' });

  const record = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${auth.claims.permULID}`, sK: cardSK },
  }));
  if (!record.Item || record.Item.status !== 'ACTIVE') return withCors(event, 404, { error: 'Gift card not found' });

  const desc = safeJson(record.Item.desc);
  if (String(desc.brandId ?? record.Item.subCategory ?? '') !== auth.claims.brandId) {
    return withCors(event, 403, { error: 'Gift card does not belong to this brand' });
  }

  await markWidgetTokenUsed(auth.claims);
  return withCors(event, 200, {
    success: true,
    card: {
      cardSK,
      cardNumber: desc.cardNumber ?? null,
      brandId: desc.brandId ?? null,
      brandName: desc.brandName ?? 'Gift Card',
      balance: desc.balance ?? desc.giftCardValue ?? 0,
      currency: desc.currency ?? 'AUD',
      expiryDate: desc.expiryDate ?? null,
    },
  });
}

async function authorizeWidgetRequest(
  event: APIGatewayProxyEvent,
  expectedAction: WidgetAction,
  options: { invalidateOnSuccess?: boolean } = {},
): Promise<{ claims: WidgetClaims } | { response: APIGatewayProxyResult }> {
  const token = extractBearer(event.headers);
  if (!token) return { response: withCors(event, 401, { error: 'Missing widget bearer token' }) };

  const claims = verifyWidgetClaims(token);
  if (!claims || claims.action !== expectedAction) {
    return { response: withCors(event, 401, { error: 'Invalid widget token' }) };
  }

  const origin = getRequestOrigin(event);
  if (claims.origin !== origin) return { response: withCors(event, 403, { error: 'Origin mismatch' }) };

  const tokenRes = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `WIDGETTOKEN#${claims.jti}`, sK: claims.permULID },
  }));
  if (!tokenRes.Item || tokenRes.Item.status !== 'ACTIVE') {
    return { response: withCors(event, 401, { error: 'Widget token expired or already used' }) };
  }

  if (options.invalidateOnSuccess === false) {
    return { claims };
  }

  return { claims };
}

async function markWidgetTokenUsed(claims: WidgetClaims) {
  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `WIDGETTOKEN#${claims.jti}`, sK: claims.permULID },
    UpdateExpression: 'SET #status = :used, updatedAt = :now',
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: {
      ':used': 'USED',
      ':now': new Date().toISOString(),
      ':active': 'ACTIVE',
    },
    ConditionExpression: '#status = :active',
  }));
}

async function getWidgetBrandConfig(brandId: string): Promise<{
  brandId: string;
  tenantId: string;
  allowedWidgetDomains: string[];
  widgetActions: Record<WidgetAction, boolean>;
} | null> {
  const brandRes = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'profile' },
  }));
  if (!brandRes.Item) return null;
  const brandDesc = safeJson(brandRes.Item.desc);
  const tenantId = String(brandDesc.tenantId ?? brandRes.Item.tenantId ?? '');
  if (!tenantId) return null;

  const tenantRes = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'PROFILE' },
  }));
  if (!tenantRes.Item) return null;

  const tenantDesc = safeJson(tenantRes.Item.desc);
  const allowedWidgetDomains = Array.isArray(tenantDesc.allowedWidgetDomains)
    ? tenantDesc.allowedWidgetDomains.map((domain) => String(domain)).filter(Boolean)
    : [];

  const widgetActions = safeJson(tenantDesc.widgetActions) as Record<string, unknown>;
  return {
    brandId,
    tenantId,
    allowedWidgetDomains,
    widgetActions: {
      invoice: widgetActions.invoice === true,
      giftcard: widgetActions.giftcard === true,
    },
  };
}

function withCors(event: APIGatewayProxyEvent, statusCode: number, body: unknown): APIGatewayProxyResult {
  const origin = getRequestOrigin(event);
  return {
    statusCode,
    headers: {
      ...CORS_HEADERS,
      'Access-Control-Allow-Origin': origin || '*',
    },
    body: statusCode === 204 ? '' : JSON.stringify(body),
  };
}

function getRequestOrigin(event: APIGatewayProxyEvent): string {
  return String(event.headers.origin ?? event.headers.Origin ?? '').trim();
}

function extractBearer(headers: APIGatewayProxyEvent['headers']): string | null {
  const header = headers.authorization ?? headers.Authorization ?? '';
  return header.startsWith('Bearer ') ? header.slice(7) : null;
}

function parseBody(body: string | null): Record<string, unknown> {
  if (!body) return {};
  try {
    return JSON.parse(body) as Record<string, unknown>;
  } catch {
    return {};
  }
}

function safeJson(value: unknown): Record<string, unknown> {
  if (typeof value === 'object' && value !== null) return value as Record<string, unknown>;
  if (typeof value !== 'string' || value.length === 0) return {};
  try {
    return JSON.parse(value) as Record<string, unknown>;
  } catch {
    return {};
  }
}

function stringOrEmpty(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizeAction(value: unknown): WidgetAction | null {
  return value === 'invoice' || value === 'giftcard' ? value : null;
}

function isAllowedOrigin(origin: string, allowedWidgetDomains: string[]): boolean {
  return !!origin && allowedWidgetDomains.includes(origin);
}

function maskCardNumber(cardNumber: string): string {
  const trimmed = cardNumber.replace(/\s+/g, '');
  if (trimmed.length <= 4) return trimmed;
  return `${'•'.repeat(Math.max(0, trimmed.length - 4))}${trimmed.slice(-4)}`;
}

function signWidgetClaims(claims: WidgetClaims): string {
  const encodedPayload = toBase64Url(JSON.stringify(claims));
  const signature = createHmac('sha256', WIDGET_TOKEN_SECRET).update(encodedPayload).digest('base64url');
  return `${encodedPayload}.${signature}`;
}

function verifyWidgetClaims(token: string): WidgetClaims | null {
  const [encodedPayload, signature] = token.split('.');
  if (!encodedPayload || !signature) return null;
  const expected = createHmac('sha256', WIDGET_TOKEN_SECRET).update(encodedPayload).digest('base64url');
  if (expected !== signature) return null;

  try {
    const claims = JSON.parse(fromBase64Url(encodedPayload)) as WidgetClaims;
    if (claims.exp * 1000 < Date.now()) return null;
    return claims;
  } catch {
    return null;
  }
}

async function verifyCognitoIdToken(idToken: string): Promise<{ permULID: string | null; email: string | null } | null> {
  const [encodedHeader, encodedPayload, encodedSignature] = idToken.split('.');
  if (!encodedHeader || !encodedPayload || !encodedSignature || !COGNITO_USER_POOL_ID) return null;

  const header = JSON.parse(fromBase64Url(encodedHeader)) as { kid?: string; alg?: string };
  const payload = JSON.parse(fromBase64Url(encodedPayload)) as Record<string, unknown>;
  if (header.alg !== 'RS256' || !header.kid) return null;

  const jwk = await getCognitoJwk(header.kid);
  if (!jwk) return null;

  const publicKey = createPublicKey({ key: jwk as any, format: 'jwk' });
  const verifier = createVerify('RSA-SHA256');
  verifier.update(`${encodedHeader}.${encodedPayload}`);
  verifier.end();
  const valid = verifier.verify(publicKey, Buffer.from(encodedSignature, 'base64url'));
  if (!valid) return null;

  const issuer = `https://cognito-idp.${COGNITO_REGION}.amazonaws.com/${COGNITO_USER_POOL_ID}`;
  if (payload.iss !== issuer || payload.token_use !== 'id') return null;
  if (typeof payload.exp !== 'number' || payload.exp * 1000 < Date.now()) return null;

  return {
    permULID: typeof payload['custom:permULID'] === 'string' ? payload['custom:permULID'] : null,
    email: typeof payload.email === 'string' ? payload.email : null,
  };
}

async function getCognitoJwk(kid: string): Promise<JsonWebKey | null> {
  const cached = JWKS_CACHE.get(kid);
  if (cached) return cached;

  const url = `https://cognito-idp.${COGNITO_REGION}.amazonaws.com/${COGNITO_USER_POOL_ID}/.well-known/jwks.json`;
  const payload = await fetchJson(url) as { keys?: (JsonWebKey & { kid?: string })[] };
  for (const jwk of payload.keys ?? []) {
    if (jwk.kid) JWKS_CACHE.set(String(jwk.kid), jwk);
  }
  return JWKS_CACHE.get(kid) ?? null;
}

function fetchJson(url: string): Promise<unknown> {
  return fetch(url).then((res) => {
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  });
}

function toBase64Url(value: string): string {
  return Buffer.from(value, 'utf8').toString('base64url');
}

function fromBase64Url(value: string): string {
  return Buffer.from(value, 'base64url').toString('utf8');
}
