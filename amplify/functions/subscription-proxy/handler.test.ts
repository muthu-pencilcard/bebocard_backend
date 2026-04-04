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
  UpdateCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'UpdateCommand', input });
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
  monotonicFactory: () => () => 'SUB-ULID-0001',
}));

import { handler } from './handler.js';

function makeEvent(
  path: string,
  method: string,
  body: Record<string, unknown> = {},
  headers: Record<string, string> = {},
) {
  return {
    path,
    httpMethod: method,
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

const VALID_KEY = { brandId: 'woolworths', keyId: 'k1', rateLimit: 1000, scopes: ['recurring'] };

const REGISTER_BODY = {
  secondaryULID: 'SEC-001',
  productName: 'Woolworths Plus',
  amount: 9.99,
  currency: 'AUD',
  frequency: 'monthly',
  nextBillingDate: '2026-05-01',
};

beforeEach(() => {
  vi.clearAllMocks();
  process.env.USER_TABLE  = 'test-user-table';
  process.env.ADMIN_TABLE = 'test-admin-table';
  process.env.REF_TABLE   = 'test-ref-table';
  process.env.FIREBASE_SERVICE_ACCOUNT_JSON = JSON.stringify({ type: 'service_account' });
  mockFcmSend.mockResolvedValue('msg-id');
});

// ── POST /recurring/register ──────────────────────────────────────────────────

describe('POST /recurring/register', () => {
  it('returns 401 when API key is missing', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(makeEvent('/recurring/register', 'POST', REGISTER_BODY), {} as never, () => {});
    expect(res!.statusCode).toBe(401);
  });

  it('returns 401 when API key is invalid', async () => {
    mockExtractApiKey.mockReturnValue('bebo_bad.key');
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(makeEvent('/recurring/register', 'POST', REGISTER_BODY), {} as never, () => {});
    expect(res!.statusCode).toBe(401);
  });

  it('returns 400 when required fields are missing', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeEvent('/recurring/register', 'POST', { secondaryULID: 'SEC-001' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(400);
    expect(JSON.parse(res!.body).error).toContain('Missing required fields');
  });

  it('returns 400 for invalid frequency', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(
      makeEvent('/recurring/register', 'POST', { ...REGISTER_BODY, frequency: 'biennially' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(400);
    expect(JSON.parse(res!.body).error).toContain('Invalid frequency');
  });

  it('returns 404 when secondaryULID cannot be resolved', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockResolvedValue({ Items: [] }); // SCAN# lookup returns nothing
    const res = await handler(makeEvent('/recurring/register', 'POST', REGISTER_BODY), {} as never, () => {});
    expect(res!.statusCode).toBe(404);
    expect(JSON.parse(res!.body).error).toContain('not found');
  });

  it('returns 201 with subId on success', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') {
        const key = cmd.input?.Key ?? {};
        if (String(key['sK'] ?? '') === 'DEVICE_TOKEN') return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'fcm-token' }) } });
        // brand profile
        return Promise.resolve({ Item: { desc: JSON.stringify({ brandName: 'Woolworths', recurringWebhookUrl: 'https://example.com/hook' }) } });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const res = await handler(makeEvent('/recurring/register', 'POST', REGISTER_BODY), {} as never, () => {});
    expect(res!.statusCode).toBe(201);
    const body = JSON.parse(res!.body);
    expect(body.subId).toBe('SUB-ULID-0001');
    expect(body.status).toBe('ACTIVE');
    expect(mockFcmSend).toHaveBeenCalledOnce();
  });

  it('proceeds successfully even when FCM push fails', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockFcmSend.mockRejectedValue(new Error('FCM down'));
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') {
        const key = cmd.input?.Key ?? {};
        if (String(key['sK'] ?? '') === 'DEVICE_TOKEN') return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'fcm-token' }) } });
        return Promise.resolve({ Item: { desc: JSON.stringify({ brandName: 'Woolworths' }) } });
      }
      return Promise.resolve({});
    });

    const res = await handler(makeEvent('/recurring/register', 'POST', REGISTER_BODY), {} as never, () => {});
    expect(res!.statusCode).toBe(201);
  });
});

// ── DELETE /recurring/{subId} ─────────────────────────────────────────────────

describe('DELETE /recurring/{subId}', () => {
  it('returns 401 without valid API key', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(makeEvent('/recurring/SUB-001', 'DELETE'), {} as never, () => {});
    expect(res!.statusCode).toBe(401);
  });

  it('returns 404 when subscription index record not found', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockResolvedValue({ Items: [] });
    const res = await handler(makeEvent('/recurring/SUB-MISSING', 'DELETE'), {} as never, () => {});
    expect(res!.statusCode).toBe(404);
  });

  it('returns 200 on successful brand cancellation', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') {
        return Promise.resolve({ Item: {
          status: 'ACTIVE',
          desc: JSON.stringify({ subId: 'SUB-001', brandId: 'woolworths', webhookUrl: null }),
        }});
      }
      return Promise.resolve({});
    });

    const res = await handler(makeEvent('/recurring/SUB-001', 'DELETE'), {} as never, () => {});
    expect(res!.statusCode).toBe(200);
    expect(JSON.parse(res!.body)).toMatchObject({ status: 'CANCELLED_BY_BRAND' });
  });
});

// ── GET /recurring/{subId}/status ─────────────────────────────────────────────

describe('GET /recurring/{subId}/status', () => {
  it('returns 401 without valid API key', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(makeEvent('/recurring/SUB-001/status', 'GET'), {} as never, () => {});
    expect(res!.statusCode).toBe(401);
  });

  it('returns 404 when index record not found', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockResolvedValue({ Items: [] });
    const res = await handler(makeEvent('/recurring/SUB-MISSING/status', 'GET'), {} as never, () => {});
    expect(res!.statusCode).toBe(404);
  });

  it('returns 200 with status and productName', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') {
        return Promise.resolve({ Item: {
          status: 'ACTIVE',
          desc: JSON.stringify({ productName: 'Woolworths Plus' }),
        }});
      }
      return Promise.resolve({});
    });

    const res = await handler(makeEvent('/recurring/SUB-001/status', 'GET'), {} as never, () => {});
    expect(res!.statusCode).toBe(200);
    expect(JSON.parse(res!.body)).toMatchObject({ status: 'ACTIVE', productName: 'Woolworths Plus' });
  });
});

// ── POST /recurring/{subId}/amount-change ─────────────────────────────────────

describe('POST /recurring/{subId}/amount-change', () => {
  it('returns 401 without valid API key', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(
      makeEvent('/recurring/SUB-001/amount-change', 'POST', { newAmount: 12.99, effectiveDate: '2026-05-01' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(401);
  });

  it('returns 400 when newAmount or effectiveDate is missing', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockResolvedValue({ Items: [{ sK: 'PERM-001' }] });
    const res = await handler(
      makeEvent('/recurring/SUB-001/amount-change', 'POST', { newAmount: 12.99 }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(400);
  });

  it('returns 200 with NO_CHANGE when amount is unchanged', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') {
        return Promise.resolve({ Item: {
          status: 'ACTIVE',
          desc: JSON.stringify({ amount: 9.99, currency: 'AUD', brandName: 'Woolworths' }),
        }});
      }
      return Promise.resolve({});
    });

    const res = await handler(
      makeEvent('/recurring/SUB-001/amount-change', 'POST', { newAmount: 9.99, effectiveDate: '2026-05-01' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(200);
    expect(JSON.parse(res!.body).status).toBe('NO_CHANGE');
  });

  it('returns 200 with AMOUNT_CHANGED on a real change', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') {
        return Promise.resolve({ Item: {
          status: 'ACTIVE',
          desc: JSON.stringify({ amount: 9.99, currency: 'AUD', brandName: 'Woolworths' }),
        }});
      }
      return Promise.resolve({});
    });

    const res = await handler(
      makeEvent('/recurring/SUB-001/amount-change', 'POST', { newAmount: 14.99, effectiveDate: '2026-05-01' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(200);
    const body = JSON.parse(res!.body);
    expect(body.status).toBe('AMOUNT_CHANGED');
    expect(body.oldAmount).toBe(9.99);
    expect(body.newAmount).toBe(14.99);
  });
});

// ── Unknown routes ─────────────────────────────────────────────────────────────

describe('unknown routes', () => {
  it('returns 404 for unmatched path', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    const res = await handler(makeEvent('/recurring/unknown/deep/path', 'GET'), {} as never, () => {});
    expect(res!.statusCode).toBe(404);
  });
});
