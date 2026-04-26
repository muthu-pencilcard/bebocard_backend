import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { APIGatewayProxyEvent } from 'aws-lambda';

// ── Hoisted mocks ─────────────────────────────────────────────────────────────

const {
  mockDdbSend,
  mockSqsSend,
  mockKmsSend,
  mockFcmSend,
  mockValidateApiKey,
  mockExtractApiKey,
  mockGetTenantState,
  mockCheckQuota,
  mockIncrementCounter,
} = vi.hoisted(() => ({
  mockDdbSend: vi.fn(),
  mockSqsSend: vi.fn().mockResolvedValue({}),
  mockKmsSend: vi.fn(),
  mockFcmSend: vi.fn().mockResolvedValue('fcm-msg-id'),
  mockValidateApiKey: vi.fn(),
  mockExtractApiKey: vi.fn(),
  mockGetTenantState: vi.fn(),
  mockCheckQuota: vi.fn(),
  mockIncrementCounter: vi.fn().mockResolvedValue(undefined),
}));

// ── Module mocks ──────────────────────────────────────────────────────────────

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: object) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn(() => ({ send: mockDdbSend })) },
  GetCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetCommand', input });
  }),
  PutCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutCommand', input });
  }),
  QueryCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'QueryCommand', input });
  }),
  UpdateCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'UpdateCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-sqs', () => ({
  SQSClient: vi.fn(function (this: Record<string, unknown>) { this.send = mockSqsSend; }),
  SendMessageCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'SendMessageCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-kms', () => ({
  KMSClient: vi.fn(function (this: Record<string, unknown>) { this.send = mockKmsSend; }),
  GetPublicKeyCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetPublicKeyCommand', input });
  }),
}));

vi.mock('firebase-admin/app', () => ({
  initializeApp: vi.fn(),
  getApps: vi.fn(() => [{}]),
  cert: vi.fn((x: unknown) => x),
}));

vi.mock('firebase-admin/messaging', () => ({
  getMessaging: vi.fn(() => ({ send: mockFcmSend })),
}));

vi.mock('ulid', () => ({
  monotonicFactory: () => {
    let c = 0;
    return () => `ULID${String(c++).padStart(6, '0')}`;
  },
}));

vi.mock('../../shared/api-key-auth', () => ({
  validateApiKey: mockValidateApiKey,
  extractApiKey: mockExtractApiKey,
}));

vi.mock('../../shared/tenant-billing', () => ({
  getTenantStateForBrand: mockGetTenantState,
  checkTenantQuota: mockCheckQuota,
  incrementTenantUsageCounter: mockIncrementCounter,
}));

vi.mock('../../shared/audit-logger', () => ({
  withAuditLog: vi.fn((_ddb: unknown, h: unknown) => h),
}));

// ── Env vars (read at module level in handler) ─────────────────────────────────

process.env.ADMIN_TABLE = 'admin-table';
process.env.USER_TABLE = 'user-table';
process.env.REFDATA_TABLE = 'ref-table';
process.env.RECEIPT_QUEUE_URL = 'https://sqs.example.com/receipt-queue';
process.env.RECEIPT_ANALYTICS_QUEUE_URL = '';
process.env.RECEIPT_SIGNING_KEY_ID = 'arn:aws:kms:us-east-1:123:key/test-key';
process.env.FIREBASE_SERVICE_ACCOUNT_JSON = JSON.stringify({ project_id: 'test' });

// ── Handler import (after all mocks) ──────────────────────────────────────────

import { handler } from './handler.js';

// ── Constants ─────────────────────────────────────────────────────────────────

const BRAND_ID = 'woolworths';
const PERM_ULID = 'PERM0001ABCDEF0001ABCDEF01';
const SECONDARY_ULID = '01J0000SECONDARY000000001';

const VALID_KEY = {
  brandId: BRAND_ID,
  tenantId: 'wg',
  keyId: 'TESTKEY01234567890123456',
  rateLimit: 1000,
  scopes: ['scan', 'receipt'],
  isSandbox: false,
};

const SANDBOX_KEY = { ...VALID_KEY, isSandbox: true };

const ACTIVE_TENANT_STATE = {
  tenantId: 'wg',
  tier: 'base' as const,
  active: true,
  includedEventsPerMonth: 250,
  notifCap: 3,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeScanEvent(overrides: Partial<APIGatewayProxyEvent> = {}): APIGatewayProxyEvent {
  return {
    path: '/v1/scan',
    httpMethod: 'POST',
    headers: { 'x-api-key': 'bebo_test.secret' },
    queryStringParameters: null,
    body: JSON.stringify({ secondaryULID: SECONDARY_ULID, storeBrandLoyaltyName: BRAND_ID }),
    multiValueHeaders: {},
    multiValueQueryStringParameters: null,
    isBase64Encoded: false,
    pathParameters: null,
    stageVariables: null,
    requestContext: {} as APIGatewayProxyEvent['requestContext'],
    resource: '',
    ...overrides,
  } as APIGatewayProxyEvent;
}

function makeReceiptEvent(bodyOverrides: Record<string, unknown> = {}): APIGatewayProxyEvent {
  return makeScanEvent({
    path: '/v1/receipt',
    body: JSON.stringify({
      secondaryULID: SECONDARY_ULID,
      merchant: 'Woolworths Bondi',
      amount: 45.99,
      purchaseDate: '2026-04-25',
      ...bodyOverrides,
    }),
  });
}

function makeScanItem(overrides: Record<string, unknown> = {}) {
  return {
    pK: `SCAN#${SECONDARY_ULID}`,
    sK: PERM_ULID,
    status: 'ACTIVE',
    desc: JSON.stringify({
      cards: [{ brand: BRAND_ID, cardId: 'CARD001', isDefault: true }],
    }),
    ...overrides,
  };
}

function makeSegmentDesc(overrides: Record<string, unknown> = {}) {
  return JSON.stringify({
    spendBucket: '100-200',
    visitFrequency: 'frequent',
    totalSpend: 150,
    visitCount: 5,
    lastVisit: '2026-04-01',
    computedAt: '2026-04-01T00:00:00Z',
    ...overrides,
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Routing
// ─────────────────────────────────────────────────────────────────────────────

describe('routing', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns 308 permanent redirect with /v1 prefix for unversioned paths', async () => {
    const res = await handler(makeScanEvent({ path: '/scan' }), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 308 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.suggestedPath).toBe('/v1/scan');
    const headers = (res as { headers: Record<string, string> }).headers;
    expect(headers['Location']).toBe('/v1/scan');
    expect(headers['Deprecation']).toBe('true');
  });

  it('returns 404 for an unknown v1 route', async () => {
    mockExtractApiKey.mockReturnValue(null);
    const res = await handler(makeScanEvent({ path: '/v1/unknown' }), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 404 });
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /v1/health
// ─────────────────────────────────────────────────────────────────────────────

describe('GET /v1/health', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns 200 OPERATIONAL without auth', async () => {
    const res = await handler(
      makeScanEvent({ path: '/v1/health', httpMethod: 'GET', body: null }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 200 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.status).toBe('OPERATIONAL');
    expect(body).toHaveProperty('timestamp');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /v1/security/receipt-public-key
// ─────────────────────────────────────────────────────────────────────────────

describe('GET /v1/security/receipt-public-key', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns 200 with base64 public key from KMS', async () => {
    mockKmsSend.mockResolvedValue({ PublicKey: new Uint8Array([1, 2, 3, 4]) });
    const res = await handler(
      makeScanEvent({ path: '/v1/security/receipt-public-key', httpMethod: 'GET', body: null }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 200 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body).toHaveProperty('publicKey');
    expect(body.algorithm).toBe('RSASSA_PSS_SHA_256');
    expect(body.format).toBe('DER');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// POST /v1/scan — handleLoyaltyCheck
// ─────────────────────────────────────────────────────────────────────────────

describe('POST /v1/scan — auth + validation', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns 401 when extractApiKey returns null', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(makeScanEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 401 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.error).toMatch(/api key/i);
  });

  it('returns 401 when validateApiKey rejects (bad key)', async () => {
    mockExtractApiKey.mockReturnValue('bebo_bad.key');
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(makeScanEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 401 });
  });

  it('returns 400 when secondaryULID is missing', async () => {
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeScanEvent({ body: JSON.stringify({ storeBrandLoyaltyName: BRAND_ID }) }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
  });

  it('returns 400 when storeBrandLoyaltyName is missing', async () => {
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeScanEvent({ body: JSON.stringify({ secondaryULID: SECONDARY_ULID }) }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
  });

  it('returns 403 when storeBrandLoyaltyName does not match API key brandId', async () => {
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeScanEvent({
        body: JSON.stringify({ secondaryULID: SECONDARY_ULID, storeBrandLoyaltyName: 'coles' }),
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 403 });
  });
});

describe('POST /v1/scan — sandbox mode', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(SANDBOX_KEY);
  });

  it('SANDBOX_USER_SUCCESS returns 200 with hasLoyaltyCard: true and loyaltyId', async () => {
    const res = await handler(
      makeScanEvent({ body: JSON.stringify({ secondaryULID: 'SANDBOX_USER_SUCCESS', storeBrandLoyaltyName: BRAND_ID }) }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 200 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.hasLoyaltyCard).toBe(true);
    expect(body.loyaltyId).toBe('MOCK_CARD_001');
    expect(body.tier).toBe('frequent');
  });

  it('SANDBOX_USER_SUCCESS with requestedFields returns email_alias attribute', async () => {
    const res = await handler(
      makeScanEvent({
        body: JSON.stringify({
          secondaryULID: 'SANDBOX_USER_SUCCESS',
          storeBrandLoyaltyName: BRAND_ID,
          requestedFields: ['email_alias'],
        }),
      }),
      {} as never,
      {} as never,
    );
    const body = JSON.parse((res as { body: string }).body);
    expect(body.attributes?.email_alias).toMatch(/@bebocard\.me$/);
  });

  it('SANDBOX_USER_NO_CARD returns 200 with hasLoyaltyCard: false', async () => {
    const res = await handler(
      makeScanEvent({ body: JSON.stringify({ secondaryULID: 'SANDBOX_USER_NO_CARD', storeBrandLoyaltyName: BRAND_ID }) }),
      {} as never,
      {} as never,
    );
    const body = JSON.parse((res as { body: string }).body);
    expect(body.hasLoyaltyCard).toBe(false);
  });

  it('SANDBOX_USER_EXPIRED returns 404', async () => {
    const res = await handler(
      makeScanEvent({ body: JSON.stringify({ secondaryULID: 'SANDBOX_USER_EXPIRED', storeBrandLoyaltyName: BRAND_ID }) }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 404 });
  });

  it('SANDBOX_USER_REVOKED returns 404', async () => {
    const res = await handler(
      makeScanEvent({ body: JSON.stringify({ secondaryULID: 'SANDBOX_USER_REVOKED', storeBrandLoyaltyName: BRAND_ID }) }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 404 });
  });

  it('SANDBOX_USER_CONSENT returns 200 with consentRequired and requestId', async () => {
    const res = await handler(
      makeScanEvent({ body: JSON.stringify({ secondaryULID: 'SANDBOX_USER_CONSENT', storeBrandLoyaltyName: BRAND_ID }) }),
      {} as never,
      {} as never,
    );
    const body = JSON.parse((res as { body: string }).body);
    expect(body.hasLoyaltyCard).toBe(true);
    expect(body.consentRequired).toBe(true);
    expect(body.requestId).toBe('SANDBOX_REQ_789');
  });
});

describe('POST /v1/scan — identity resolution', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockGetTenantState.mockResolvedValue(ACTIVE_TENANT_STATE);
  });

  it('returns 404 when SCAN# record does not exist', async () => {
    mockDdbSend.mockResolvedValueOnce({ Items: [] }); // QueryCommand SCAN#
    const res = await handler(makeScanEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 404 });
  });

  it('returns 404 when SCAN# record has status REVOKED', async () => {
    mockDdbSend.mockResolvedValueOnce({ Items: [makeScanItem({ status: 'REVOKED' })] });
    const res = await handler(makeScanEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 404 });
  });

  it('returns 200 hasLoyaltyCard: false when user has no card for this brand', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [makeScanItem({ desc: JSON.stringify({ cards: [] }) })] }) // SCAN# lookup
      .mockResolvedValue({ Item: undefined }); // dedup check + any other calls
    const res = await handler(makeScanEvent(), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.hasLoyaltyCard).toBe(false);
  });

  it('returns 200 hasLoyaltyCard: true with loyaltyId when user has a card', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [makeScanItem()] }) // SCAN# lookup
      .mockResolvedValueOnce({ Item: undefined })          // SUBSCRIPTION# → not subscribed
      .mockResolvedValueOnce({ Item: undefined });         // SEGMENT#
    const res = await handler(makeScanEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 200 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.hasLoyaltyCard).toBe(true);
    expect(body.loyaltyId).toBe('CARD001');
    expect(body.tier).toBeUndefined();
    expect(body.spendBucket).toBeUndefined();
  });

  it('includes tier and spendBucket when user is subscribed and segment exists', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [makeScanItem()] })                     // SCAN#
      .mockResolvedValueOnce({ Item: { status: 'ACTIVE' } })                  // SUBSCRIPTION#
      .mockResolvedValueOnce({ Item: { desc: makeSegmentDesc() } });          // SEGMENT#
    const res = await handler(makeScanEvent(), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.tier).toBe('frequent');
    expect(body.spendBucket).toBe('100-200');
  });

  it('omits tier and spendBucket when subscription is not ACTIVE', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [makeScanItem()] })
      .mockResolvedValueOnce({ Item: { status: 'REVOKED' } }) // not ACTIVE
      .mockResolvedValueOnce({ Item: { desc: makeSegmentDesc() } });
    const res = await handler(makeScanEvent(), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.tier).toBeUndefined();
    expect(body.spendBucket).toBeUndefined();
  });
});

describe('POST /v1/scan — consent-gated identity release', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockGetTenantState.mockResolvedValue(ACTIVE_TENANT_STATE);
    mockCheckQuota.mockResolvedValue({ allowed: true });
  });

  it('returns consentRequired + requestId when no consent exists and requestedFields provided', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [makeScanItem()] }) // SCAN#
      .mockResolvedValueOnce({ Item: { status: 'ACTIVE' } }) // SUBSCRIPTION#
      .mockResolvedValueOnce({ Item: undefined })             // SEGMENT#
      .mockResolvedValueOnce({ Items: [] })                   // consent GSI → no consent
      .mockResolvedValueOnce({})                              // PutCommand consent record
      .mockResolvedValueOnce({ Item: null });                 // device token → null (no FCM)
    const res = await handler(
      makeScanEvent({
        body: JSON.stringify({
          secondaryULID: SECONDARY_ULID,
          storeBrandLoyaltyName: BRAND_ID,
          requestedFields: ['email_alias'],
          purpose: 'Send digital receipt',
        }),
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 200 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.consentRequired).toBe(true);
    expect(body.requestId).toBeDefined();
    expect(body.attributes).toBeUndefined();
  });

  it('returns releasedAttributes and marks consent CONSUMED when active consent exists', async () => {
    const consentItem = {
      pK: 'CONSENT#REQ001',
      sK: PERM_ULID,
      status: 'APPROVED',
      desc: JSON.stringify({ approvedFields: ['email_alias', 'first_name'] }),
    };
    mockDdbSend
      .mockResolvedValueOnce({ Items: [makeScanItem()] })  // SCAN#
      .mockResolvedValueOnce({ Item: { status: 'ACTIVE' } }) // SUBSCRIPTION#
      .mockResolvedValueOnce({ Item: undefined })              // SEGMENT#
      .mockResolvedValueOnce({ Items: [consentItem] })         // consent GSI → APPROVED
      .mockResolvedValueOnce({ Item: { desc: JSON.stringify({ first_name: 'Test' }) } }) // IDENTITY (resolveAttributes)
      .mockResolvedValueOnce({})                               // UpdateCommand CONSUMED
      .mockResolvedValueOnce({});                              // PutCommand quota log
    const res = await handler(
      makeScanEvent({
        body: JSON.stringify({
          secondaryULID: SECONDARY_ULID,
          storeBrandLoyaltyName: BRAND_ID,
          requestedFields: ['email_alias'],
        }),
      }),
      {} as never,
      {} as never,
    );
    const body = JSON.parse((res as { body: string }).body);
    expect(body.attributes).toBeDefined();
    expect(body.attributes.email_alias).toMatch(/@bebocard\.me$/);
    expect(body.consentRequired).toBeUndefined();
    // Verify the UpdateCommand to mark CONSUMED was issued
    const updateCalls = mockDdbSend.mock.calls.filter(
      ([cmd]: [{ __type: string }]) => cmd.__type === 'UpdateCommand',
    );
    expect(updateCalls.length).toBeGreaterThanOrEqual(1);
  });

  it('returns 403 when tenant quota is exceeded for consent', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [makeScanItem()] }) // SCAN#
      .mockResolvedValueOnce({ Item: { status: 'ACTIVE' } }) // SUBSCRIPTION#
      .mockResolvedValueOnce({ Item: undefined });              // SEGMENT#
    mockCheckQuota.mockResolvedValue({ allowed: false, message: 'Quota exceeded' });
    const res = await handler(
      makeScanEvent({
        body: JSON.stringify({
          secondaryULID: SECONDARY_ULID,
          storeBrandLoyaltyName: BRAND_ID,
          requestedFields: ['email_alias'],
        }),
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 403 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.error).toBe('Quota exceeded');
  });

  it('returns consentRequired with existing requestId when PENDING consent exists', async () => {
    const pendingItem = {
      pK: 'CONSENT#EXISTING001',
      sK: PERM_ULID,
      status: 'PENDING',
    };
    mockDdbSend
      .mockResolvedValueOnce({ Items: [makeScanItem()] })
      .mockResolvedValueOnce({ Item: { status: 'ACTIVE' } })
      .mockResolvedValueOnce({ Item: undefined })
      .mockResolvedValueOnce({ Items: [pendingItem] }); // consent GSI → PENDING
    const res = await handler(
      makeScanEvent({
        body: JSON.stringify({
          secondaryULID: SECONDARY_ULID,
          storeBrandLoyaltyName: BRAND_ID,
          requestedFields: ['email_alias'],
        }),
      }),
      {} as never,
      {} as never,
    );
    const body = JSON.parse((res as { body: string }).body);
    expect(body.consentRequired).toBe(true);
    expect(body.requestId).toBe('EXISTING001');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// POST /v1/receipt — handleReceipt
// ─────────────────────────────────────────────────────────────────────────────

describe('POST /v1/receipt — auth + validation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(null);
  });

  it('returns 401 when validateApiKey returns null', async () => {
    const res = await handler(makeReceiptEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 401 });
  });

  it('returns 400 when merchant is missing', async () => {
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeReceiptEvent({ merchant: undefined }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
  });

  it('returns 400 when amount is missing', async () => {
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeReceiptEvent({ amount: undefined }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
  });

  it('returns 400 when purchaseDate is missing', async () => {
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeReceiptEvent({ purchaseDate: undefined }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
  });

  it('returns 400 when neither secondaryULID nor anonymousMode is present', async () => {
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeScanEvent({
        path: '/v1/receipt',
        body: JSON.stringify({ merchant: 'Test', amount: 10, purchaseDate: '2026-04-25' }),
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.error).toMatch(/secondaryULID/i);
  });

  it('returns 400 when supplierTaxIdType is an invalid value', async () => {
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeReceiptEvent({ supplierTaxIdType: 'INVALID' }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.error).toMatch(/supplierTaxIdType/i);
  });

  it('returns 403 when body.brandId does not match API key brandId', async () => {
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeReceiptEvent({ brandId: 'coles' }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 403 });
  });
});

describe('POST /v1/receipt — identity resolution + async enqueue', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockGetTenantState.mockResolvedValue(ACTIVE_TENANT_STATE);
    mockSqsSend.mockResolvedValue({});
  });

  it('anonymousMode: returns 202 with claimToken + claimQRPayload', async () => {
    const res = await handler(
      makeReceiptEvent({ secondaryULID: undefined, anonymousMode: true }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 202 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.claimToken).toBeDefined();
    expect(body.claimQRPayload).toMatch(/^bebocard:\/\/claim/);
    expect(body.receiptId).toBeDefined();
  });

  it('BeboCard user: resolves permULID via SCAN# index and enqueues to SQS', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [makeScanItem()] }); // SCAN# lookup
    const res = await handler(makeReceiptEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 202 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.success).toBe(true);
    expect(body.receiptId).toBeDefined();
    // SQS message should contain the resolved permULID
    const sqsArgs = mockSqsSend.mock.calls[0]?.[0] as { input: { MessageBody: string } };
    const payload = JSON.parse(sqsArgs.input.MessageBody);
    expect(payload.permULID).toBe(PERM_ULID);
    expect(payload.isInvoice).toBe(false);
  });

  it('creates GHOST profile for unknown secondaryULID and enqueues', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [] })  // SCAN# not found → ghost path
      .mockResolvedValueOnce({});            // PutCommand ghost record
    const res = await handler(makeReceiptEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 202 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.success).toBe(true);
    // SQS message permULID should be GHOST# prefixed
    const sqsArgs = mockSqsSend.mock.calls[0]?.[0] as { input: { MessageBody: string } };
    const payload = JSON.parse(sqsArgs.input.MessageBody);
    expect(payload.permULID).toMatch(/^GHOST#/);
  });

  it('returns 404 when SCAN# record has status REVOKED', async () => {
    mockDdbSend.mockResolvedValueOnce({ Items: [{ sK: PERM_ULID, status: 'REVOKED' }] });
    const res = await handler(makeReceiptEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 404 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.error).toMatch(/revoked/i);
  });

  it('returns 403 when tenant account is suspended', async () => {
    mockDdbSend.mockResolvedValueOnce({ Items: [makeScanItem()] });
    mockGetTenantState.mockResolvedValue({ ...ACTIVE_TENANT_STATE, active: false });
    const res = await handler(makeReceiptEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 403 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.error).toMatch(/suspended/i);
  });

  it('sandbox user maps to SANDBOX_IDENTITY_123 and enqueues', async () => {
    mockValidateApiKey.mockResolvedValue(SANDBOX_KEY);
    const res = await handler(
      makeReceiptEvent({ secondaryULID: 'SANDBOX_USER_123' }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 202 });
    const sqsArgs = mockSqsSend.mock.calls[0]?.[0] as { input: { MessageBody: string } };
    const payload = JSON.parse(sqsArgs.input.MessageBody);
    expect(payload.permULID).toBe('SANDBOX_IDENTITY_123');
    expect(payload.isSandbox).toBe(true);
  });

  it('invoice path sets isInvoice: true in the SQS message', async () => {
    mockDdbSend.mockResolvedValueOnce({ Items: [makeScanItem()] });
    const res = await handler(
      makeScanEvent({
        path: '/v1/invoice',
        body: JSON.stringify({
          secondaryULID: SECONDARY_ULID,
          merchant: 'Test Supplier',
          amount: 100,
          purchaseDate: '2026-04-25',
        }),
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 202 });
    const sqsArgs = mockSqsSend.mock.calls[0]?.[0] as { input: { MessageBody: string } };
    const payload = JSON.parse(sqsArgs.input.MessageBody);
    expect(payload.isInvoice).toBe(true);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /v1/receipt — handleGetReceipt
// ─────────────────────────────────────────────────────────────────────────────

describe('GET /v1/receipt — handleGetReceipt', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
  });

  it('returns 401 when API key is invalid', async () => {
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(
      makeScanEvent({
        path: '/v1/receipt',
        httpMethod: 'GET',
        body: null,
        queryStringParameters: { receiptId: 'REC001', permULID: PERM_ULID },
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 401 });
  });

  it('returns 400 when receiptId is missing', async () => {
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeScanEvent({
        path: '/v1/receipt',
        httpMethod: 'GET',
        body: null,
        queryStringParameters: { permULID: PERM_ULID },
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
  });

  it('returns 400 when permULID is missing', async () => {
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeScanEvent({
        path: '/v1/receipt',
        httpMethod: 'GET',
        body: null,
        queryStringParameters: { receiptId: 'REC001' },
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
  });

  it('returns 404 when receipt not found', async () => {
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockDdbSend.mockResolvedValueOnce({ Item: undefined });
    const res = await handler(
      makeScanEvent({
        path: '/v1/receipt',
        httpMethod: 'GET',
        body: null,
        queryStringParameters: { receiptId: 'REC001', permULID: PERM_ULID },
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 404 });
  });

  it('returns 403 when receipt belongs to a different brand', async () => {
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockDdbSend.mockResolvedValueOnce({
      Item: { desc: JSON.stringify({ brandId: 'coles', merchant: 'Coles', amount: 20 }) },
    });
    const res = await handler(
      makeScanEvent({
        path: '/v1/receipt',
        httpMethod: 'GET',
        body: null,
        queryStringParameters: { receiptId: 'REC001', permULID: PERM_ULID },
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 403 });
  });

  it('returns 200 with receipt data when brand matches', async () => {
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockDdbSend.mockResolvedValueOnce({
      Item: {
        desc: JSON.stringify({
          brandId: BRAND_ID,
          merchant: 'Woolworths Bondi',
          amount: 45.99,
          purchaseDate: '2026-04-25',
          signature: 'sig123',
          signingAlgorithm: 'RSASSA_PSS_SHA_256',
        }),
      },
    });
    const res = await handler(
      makeScanEvent({
        path: '/v1/receipt',
        httpMethod: 'GET',
        body: null,
        queryStringParameters: { receiptId: 'REC001', permULID: PERM_ULID },
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 200 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.merchant).toBe('Woolworths Bondi');
    expect(body.amount).toBe(45.99);
    expect(body.brandId).toBe(BRAND_ID);
    expect(body).toHaveProperty('publicKeyUrl');
  });
});
