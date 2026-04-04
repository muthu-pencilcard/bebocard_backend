import { vi, describe, it, expect, beforeEach } from 'vitest';

// vi.hoisted ensures these mocks are available inside vi.mock() factories,
// which are hoisted before all variable declarations.
const { mockSend, mockValidateApiKey, mockExtractApiKey, mockFcmSend } = vi.hoisted(() => ({
  mockSend: vi.fn(),
  mockValidateApiKey: vi.fn(),
  mockExtractApiKey: vi.fn(),
  mockFcmSend: vi.fn(),
}));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockSend }) },
  GetCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetCommand', input });
  }),
  PutCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutCommand', input });
  }),
  QueryCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'QueryCommand', input });
  }),
}));

vi.mock('../../shared/api-key-auth.js', () => ({
  validateApiKey: mockValidateApiKey,
  extractApiKey: mockExtractApiKey,
}));

vi.mock('../../shared/audit-logger.js', () => ({
  withAuditLog: (_ddb: unknown, h: unknown) => h,
}));

vi.mock('firebase-admin/app', () => ({
  initializeApp: vi.fn(),
  getApps: vi.fn(() => [{ name: 'already-init' }]),
  cert: vi.fn(function (obj: unknown) { return obj; }),
}));

vi.mock('firebase-admin/messaging', () => ({
  getMessaging: vi.fn(() => ({ send: mockFcmSend })),
}));

vi.mock('ulid', () => ({
  monotonicFactory: () => () => 'TEST-ULID-0001',
}));

// ── Import handler after mocks ─────────────────────────────────────────────────
import { handler } from './handler.js';

// ── Helpers ────────────────────────────────────────────────────────────────────

type Headers = Record<string, string>;

function makeEvent(
  path: string,
  body: Record<string, unknown>,
  headers: Headers = {},
) {
  return {
    path,
    httpMethod: 'POST',
    headers,
    body: JSON.stringify(body),
    multiValueHeaders: {},
    isBase64Encoded: false,
    queryStringParameters: null,
    multiValueQueryStringParameters: null,
    pathParameters: null,
    stageVariables: null,
    requestContext: {} as never,
    resource: '',
  } as Parameters<typeof handler>[0];
}

const SCAN_ITEM = {
  pK: 'SCAN#SECONDARY-001',
  sK: 'PERM-001',
  status: 'ACTIVE',
  desc: JSON.stringify({
    cards: [
      { brand: 'woolworths', cardId: 'CARD-001', isDefault: true },
    ],
  }),
};

// ── POST /scan tests ──────────────────────────────────────────────────────────

describe('POST /scan', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.USER_TABLE = 'test-user-table';
    process.env.ADMIN_TABLE = 'test-admin-table';
    process.env.REFDATA_TABLE = 'test-refdata-table';
    process.env.FIREBASE_SERVICE_ACCOUNT_JSON = JSON.stringify({ type: 'service_account' });
    mockFcmSend.mockResolvedValue('msg-id');
  });

  it('returns 401 if API key is missing', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);

    const event = makeEvent('/scan', { secondaryULID: 'SEC-001', storeBrandLoyaltyName: 'woolworths' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(401);
    expect(JSON.parse(res!.body)).toMatchObject({ error: expect.any(String) });
  });

  it('returns 401 if API key is invalid', async () => {
    mockExtractApiKey.mockReturnValue('bebo_bad.key');
    mockValidateApiKey.mockResolvedValue(null);

    const event = makeEvent('/scan', { secondaryULID: 'SEC-001', storeBrandLoyaltyName: 'woolworths' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(401);
  });

  it('returns 400 if secondaryULID is missing', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['scan'] });

    const event = makeEvent('/scan', { storeBrandLoyaltyName: 'woolworths' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(400);
  });

  it('returns 400 if storeBrandLoyaltyName is missing', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['scan'] });

    const event = makeEvent('/scan', { secondaryULID: 'SEC-001' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(400);
  });

  it('returns 403 if brand name does not match API key brandId', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['scan'] });

    const event = makeEvent('/scan', { secondaryULID: 'SEC-001', storeBrandLoyaltyName: 'coles' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(403);
    expect(JSON.parse(res!.body)).toMatchObject({ error: 'Unauthorized' });
  });

  it('returns 404 if SCAN# record not found in AdminDataEvent', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['scan'] });

    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [] });
      return Promise.resolve({});
    });

    const event = makeEvent('/scan', { secondaryULID: 'SEC-MISSING', storeBrandLoyaltyName: 'woolworths' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(404);
  });

  it('returns 404 if SCAN# record is REVOKED', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['scan'] });

    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({
          Items: [{ ...SCAN_ITEM, status: 'REVOKED' }],
        });
      }
      return Promise.resolve({});
    });

    const event = makeEvent('/scan', { secondaryULID: 'SEC-001', storeBrandLoyaltyName: 'woolworths' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(404);
  });

  it('returns { hasLoyaltyCard: false } if user has no cards for this brand', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['scan'] });

    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({
          Items: [{
            ...SCAN_ITEM,
            desc: JSON.stringify({ cards: [{ brand: 'coles', cardId: 'OTHER-001', isDefault: true }] }),
          }],
        });
      }
      return Promise.resolve({});
    });

    const event = makeEvent('/scan', { secondaryULID: 'SEC-001', storeBrandLoyaltyName: 'woolworths' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(200);
    expect(JSON.parse(res!.body)).toEqual({ hasLoyaltyCard: false });
  });

  it('sends CARD_SUGGESTION push when no card exists and suggestion is not deduped', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['scan'] });

    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: { sK?: string; pK?: string } } }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({
          Items: [{
            ...SCAN_ITEM,
            desc: JSON.stringify({ cards: [{ brand: 'coles', cardId: 'OTHER-001', isDefault: true }] }),
          }],
        });
      }
      if (cmd.__type === 'GetCommand' && cmd.input?.Key?.sK === 'CARD_SUGGESTION#woolworths') {
        return Promise.resolve({ Item: undefined });
      }
      if (cmd.__type === 'GetCommand' && cmd.input?.Key?.sK === 'DEVICE_TOKEN') {
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'device-token-1' }) } });
      }
      if (cmd.__type === 'GetCommand' && cmd.input?.Key?.pK === 'BRAND#woolworths') {
        return Promise.resolve({ Item: { desc: JSON.stringify({ brandName: 'Woolworths', brandColor: '#007837', supportsDirectEnrollment: true }) } });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event = makeEvent('/scan', { secondaryULID: 'SEC-001', storeBrandLoyaltyName: 'woolworths' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(200);
    expect(JSON.parse(res!.body)).toEqual({ hasLoyaltyCard: false });
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(mockFcmSend).toHaveBeenCalledWith(expect.objectContaining({
      data: expect.objectContaining({ type: 'CARD_SUGGESTION', brandId: 'woolworths' }),
    }));
  });

  it('returns loyaltyId for the default card', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['scan'] });

    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: { sK?: string } } }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [SCAN_ITEM] });
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: null });
      return Promise.resolve({});
    });

    const event = makeEvent('/scan', { secondaryULID: 'SEC-001', storeBrandLoyaltyName: 'woolworths' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(200);
    const body = JSON.parse(res!.body);
    expect(body.hasLoyaltyCard).toBe(true);
    expect(body.loyaltyId).toBe('CARD-001');
  });

  it('includes tier and spendBucket when SUBSCRIPTION is ACTIVE', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['scan'] });

    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: { sK?: string } } }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [SCAN_ITEM] });
      if (cmd.__type === 'GetCommand') {
        const sk = cmd.input?.Key?.sK ?? '';
        if (sk.startsWith('SUBSCRIPTION#')) {
          return Promise.resolve({ Item: { pK: 'USER#PERM-001', sK: sk, status: 'ACTIVE' } });
        }
        if (sk.startsWith('SEGMENT#')) {
          return Promise.resolve({
            Item: {
              pK: 'USER#PERM-001',
              sK: sk,
              desc: JSON.stringify({ visitFrequency: 'frequent', spendBucket: '100-200' }),
            },
          });
        }
        return Promise.resolve({ Item: null });
      }
      return Promise.resolve({});
    });

    const event = makeEvent('/scan', { secondaryULID: 'SEC-001', storeBrandLoyaltyName: 'woolworths' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(200);
    const body = JSON.parse(res!.body);
    expect(body.tier).toBe('frequent');
    expect(body.spendBucket).toBe('100-200');
  });

  it('omits tier and spendBucket when no subscription exists', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['scan'] });

    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [SCAN_ITEM] });
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: null });
      return Promise.resolve({});
    });

    const event = makeEvent('/scan', { secondaryULID: 'SEC-001', storeBrandLoyaltyName: 'woolworths' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(200);
    const body = JSON.parse(res!.body);
    expect(body).not.toHaveProperty('tier');
    expect(body).not.toHaveProperty('spendBucket');
  });
});

// ── POST /receipt tests ───────────────────────────────────────────────────────

describe('POST /receipt', () => {
  const validReceiptBody = {
    secondaryULID: 'SEC-001',
    merchant: 'Woolworths Bondi',
    amount: 42.5,
    purchaseDate: '2026-03-20',
  };

  beforeEach(() => {
    vi.clearAllMocks();
    process.env.USER_TABLE = 'test-user-table';
    process.env.ADMIN_TABLE = 'test-admin-table';
    process.env.FIREBASE_SERVICE_ACCOUNT_JSON = JSON.stringify({ type: 'service_account' });
    mockFcmSend.mockResolvedValue('msg-id');
  });

  it('returns 401 if API key is missing', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);

    const event = makeEvent('/receipt', validReceiptBody);
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(401);
  });

  it('returns 400 if merchant is missing', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['receipt'] });

    const event = makeEvent('/receipt', { secondaryULID: 'SEC-001', amount: 42.5, purchaseDate: '2026-03-20' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(400);
  });

  it('returns 400 if amount is missing', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['receipt'] });

    const event = makeEvent('/receipt', {
      secondaryULID: 'SEC-001',
      merchant: 'Woolworths',
      purchaseDate: '2026-03-20',
    });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(400);
  });

  it('returns 403 if receipt brandId does not match key brandId', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['receipt'] });

    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [SCAN_ITEM] });
      return Promise.resolve({});
    });

    const event = makeEvent('/receipt', { ...validReceiptBody, brandId: 'coles' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(403);
  });

  it('returns 200 idempotent response if RECEIPT_IDEM# already exists', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['receipt'] });

    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: { sK?: string } } }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [SCAN_ITEM] });
      if (cmd.__type === 'GetCommand') {
        const sk = cmd.input?.Key?.sK ?? '';
        if (sk.startsWith('RECEIPT_IDEM#')) {
          return Promise.resolve({ Item: { receiptSK: 'RECEIPT#2026-03-20#EXISTING' } });
        }
        return Promise.resolve({ Item: null });
      }
      return Promise.resolve({});
    });

    const event = makeEvent('/receipt', validReceiptBody);
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(200);
    const body = JSON.parse(res!.body);
    expect(body.receiptSK).toBe('RECEIPT#2026-03-20#EXISTING');
  });

  it('saves receipt with correct schema to UserDataEvent', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['receipt'] });

    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [SCAN_ITEM] });
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: null });
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event = makeEvent('/receipt', { ...validReceiptBody, currency: 'AUD', category: 'grocery' });
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(200);

    const putCalls = mockSend.mock.calls.filter(
      ([cmd]) => cmd.__type === 'PutCommand',
    );
    const receiptPut = putCalls.find(([cmd]) => cmd.input?.Item?.eventType === 'RECEIPT');
    expect(receiptPut).toBeDefined();
    const item = receiptPut![0].input.Item;
    expect(item.pK).toBe('USER#PERM-001');
    expect(item.sK).toMatch(/^RECEIPT#2026-03-20#/);
    expect(item.eventType).toBe('RECEIPT');
    expect(item.primaryCat).toBe('receipt');
    const desc = JSON.parse(item.desc);
    expect(desc.merchant).toBe('Woolworths Bondi');
    expect(desc.amount).toBe(42.5);
    expect(desc.brandId).toBe('woolworths');
    expect(desc.currency).toBe('AUD');
    expect(desc.source).toBe('brand_push');
  });

  it('FCM failure does not fail the receipt save', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue({ brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['receipt'] });

    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: { sK?: string } } }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [SCAN_ITEM] });
      if (cmd.__type === 'GetCommand') {
        const sk = cmd.input?.Key?.sK ?? '';
        if (sk === 'DEVICE_TOKEN') {
          return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'device-token-abc' }) } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });
    mockFcmSend.mockRejectedValue(new Error('FCM unavailable'));

    const event = makeEvent('/receipt', validReceiptBody);
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(200);
    const body = JSON.parse(res!.body);
    expect(body.success).toBe(true);
  });
});

// ── Unknown route ─────────────────────────────────────────────────────────────

describe('unknown route', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.USER_TABLE = 'test-user-table';
    process.env.ADMIN_TABLE = 'test-admin-table';
  });

  it('returns 404 for unknown path', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);

    const event = makeEvent('/unknown-path', {});
    const res = await handler(event, {} as never, () => {});
    expect(res!.statusCode).toBe(404);
    expect(JSON.parse(res!.body)).toMatchObject({ error: 'Unknown route' });
  });
});
