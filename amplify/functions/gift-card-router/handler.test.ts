import { vi, describe, it, expect, beforeEach } from 'vitest';

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
  getApps: vi.fn(() => [{ name: 'app' }]),
  cert: vi.fn((x: unknown) => x),
}));

vi.mock('firebase-admin/messaging', () => ({
  getMessaging: vi.fn(() => ({ send: mockFcmSend })),
}));

vi.mock('ulid', () => ({
  monotonicFactory: () => () => 'DELIVERY-ULID-001',
}));

import { handler } from './handler.js';

function makeEvent(
  path: string,
  method: string,
  body: Record<string, unknown> = {},
) {
  return {
    path,
    httpMethod: method,
    headers: {},
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

const VALID_KEY = { brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['gift_card'] };

const DELIVER_BODY = {
  secondaryULID: 'SEC-001',
  token: 'opaque-brand-token-abc',
  giftCardValue: 50,
  currency: 'AUD',
  expiryDate: '2027-01-01',
  cardLabel: 'Woolworths Gift Card',
};

beforeEach(() => {
  vi.clearAllMocks();
  process.env.USER_TABLE  = 'test-user-table';
  process.env.ADMIN_TABLE = 'test-admin-table';
  process.env.REF_TABLE   = 'test-ref-table';
  process.env.FIREBASE_SERVICE_ACCOUNT_JSON = JSON.stringify({ type: 'service_account' });
  mockFcmSend.mockResolvedValue('msg-id');
});

// ── POST /gift-card/deliver ───────────────────────────────────────────────────

describe('POST /gift-card/deliver', () => {
  it('returns 401 when API key is missing', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(makeEvent('/gift-card/deliver', 'POST', DELIVER_BODY), {} as never, () => {});
    expect(res!.statusCode).toBe(401);
  });

  it('returns 401 when API key is invalid', async () => {
    mockExtractApiKey.mockReturnValue('bebo_bad.key');
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(makeEvent('/gift-card/deliver', 'POST', DELIVER_BODY), {} as never, () => {});
    expect(res!.statusCode).toBe(401);
  });

  it('returns 400 when required fields are missing', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeEvent('/gift-card/deliver', 'POST', { secondaryULID: 'SEC-001' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(400);
    expect(JSON.parse(res!.body).error).toContain('Missing required fields');
  });

  it('returns 404 when secondaryULID cannot be resolved', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockResolvedValue({ Items: [] });
    const res = await handler(makeEvent('/gift-card/deliver', 'POST', DELIVER_BODY), {} as never, () => {});
    expect(res!.statusCode).toBe(404);
    expect(JSON.parse(res!.body).error).toContain('not found');
  });

  it('returns 201 with deliveryId on success', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') {
        const key = cmd.input?.Key ?? {};
        if (String(key['sK'] ?? '') === 'DEVICE_TOKEN') {
          return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'fcm-token' }) } });
        }
        // brand profile
        return Promise.resolve({ Item: { desc: JSON.stringify({ brandName: 'Woolworths', brandColor: '#007837' }) } });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const res = await handler(makeEvent('/gift-card/deliver', 'POST', DELIVER_BODY), {} as never, () => {});
    expect(res!.statusCode).toBe(201);
    const body = JSON.parse(res!.body);
    expect(body.deliveryId).toBe('DELIVERY-ULID-001');
    expect(body.status).toBe('DELIVERED');
    expect(mockFcmSend).toHaveBeenCalledOnce();

    // Verify the push payload
    const fcmCall = mockFcmSend.mock.calls[0][0] as { data: Record<string, string> };
    expect(fcmCall.data.type).toBe('GIFT_CARD_DELIVERY');
    expect(fcmCall.data.deliveryId).toBe('DELIVERY-ULID-001');
  });

  it('stores the token as cardNumber in the DynamoDB record', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: { desc: JSON.stringify({ brandName: 'Woolworths' }) } });
      return Promise.resolve({});
    });

    await handler(makeEvent('/gift-card/deliver', 'POST', DELIVER_BODY), {} as never, () => {});

    const putCall = mockSend.mock.calls.find(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    const item = (putCall![0] as { input: { Item: Record<string, unknown> } }).input.Item;
    const desc = JSON.parse(item.desc as string);
    expect(desc.cardNumber).toBe(DELIVER_BODY.token);
    expect(desc.source).toBe('brand');
    expect(desc.balance).toBe(50);
  });

  it('proceeds successfully even when FCM push fails', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockFcmSend.mockRejectedValue(new Error('FCM down'));
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: { desc: JSON.stringify({ brandName: 'Woolworths' }) } });
      return Promise.resolve({});
    });

    const res = await handler(makeEvent('/gift-card/deliver', 'POST', DELIVER_BODY), {} as never, () => {});
    expect(res!.statusCode).toBe(201);
  });
});

// ── GET /gift-card/{deliveryId}/status ────────────────────────────────────────

describe('GET /gift-card/{deliveryId}/status', () => {
  it('returns 401 without valid API key', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(makeEvent('/gift-card/DELIVERY-001/status', 'GET'), {} as never, () => {});
    expect(res!.statusCode).toBe(401);
  });

  it('returns 404 when delivery index record is not found', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockResolvedValue({ Items: [] });
    const res = await handler(makeEvent('/gift-card/DELIVERY-MISSING/status', 'GET'), {} as never, () => {});
    expect(res!.statusCode).toBe(404);
  });

  it('returns 404 when UserDataEvent record is not found', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: undefined });
      return Promise.resolve({});
    });
    const res = await handler(makeEvent('/gift-card/DELIVERY-001/status', 'GET'), {} as never, () => {});
    expect(res!.statusCode).toBe(404);
  });

  it('returns 200 with delivery status', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: { status: 'ACTIVE', desc: '{}' } });
      return Promise.resolve({});
    });

    const res = await handler(makeEvent('/gift-card/DELIVERY-001/status', 'GET'), {} as never, () => {});
    expect(res!.statusCode).toBe(200);
    expect(JSON.parse(res!.body)).toMatchObject({ status: 'ACTIVE' });
  });
});

// ── Unknown routes ─────────────────────────────────────────────────────────────

describe('unknown routes', () => {
  it('returns 404', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(makeEvent('/gift-card/unknown', 'PATCH'), {} as never, () => {});
    expect(res!.statusCode).toBe(404);
  });
});
