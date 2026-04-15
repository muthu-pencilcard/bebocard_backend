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

  it('returns 201 with subId on success and writes RECURRING_IDX# reverse index', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') {
        const key = cmd.input?.Key ?? {};
        if (String(key['sK'] ?? '') === 'DEVICE_TOKEN') return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'fcm-token' }) } });
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

    // Verify RECURRING_IDX# written to AdminDataEvent so brand operations can resolve permULID
    const puts = mockSend.mock.calls.filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand');
    expect(puts).toHaveLength(2); // RECURRING# (user) + RECURRING_IDX# (admin index)
    const idxPut = (puts[1][0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(String(idxPut.pK ?? '')).toBe('RECURRING_IDX#SUB-ULID-0001');
    expect(idxPut.sK).toBe('PERM-001');
    expect(idxPut.status).toBe('ACTIVE');
    expect(idxPut.brandId).toBe('woolworths');
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

  it('returns 200 on successful brand cancellation, updates RECURRING_IDX# and writes RECEIPT#', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') {
        return Promise.resolve({ Item: {
          status: 'ACTIVE',
          desc: JSON.stringify({ subId: 'SUB-001', brandId: 'woolworths', brandName: 'Woolworths', currency: 'AUD', webhookUrl: null }),
        }});
      }
      return Promise.resolve({});
    });

    const res = await handler(makeEvent('/recurring/SUB-001', 'DELETE'), {} as never, () => {});
    expect(res!.statusCode).toBe(200);
    expect(JSON.parse(res!.body)).toMatchObject({ status: 'CANCELLED_BY_BRAND' });

    const updates = mockSend.mock.calls.filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'UpdateCommand');
    // UpdateCommand 1: RECURRING# status update; UpdateCommand 2: RECURRING_IDX# index update
    expect(updates).toHaveLength(2);
    const idxUpdate = (updates[1][0] as { input: { Key: Record<string, unknown> } }).input;
    expect(String(idxUpdate.Key.pK ?? '')).toBe('RECURRING_IDX#SUB-001');

    // RECEIPT# written for cancellation
    const puts = mockSend.mock.calls.filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand');
    expect(puts).toHaveLength(1);
    const receiptItem = (puts[0][0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(String(receiptItem.sK ?? '')).toMatch(/^RECEIPT#/);
    expect(JSON.parse(receiptItem.desc as string).receiptType).toBe('SUBSCRIPTION_CANCELLATION');
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

  it('writes RECEIPT# with receiptType SUBSCRIPTION_AMOUNT_CHANGE', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: {
        status: 'ACTIVE',
        desc: JSON.stringify({ amount: 9.99, currency: 'AUD', brandName: 'Woolworths' }),
      }});
      return Promise.resolve({});
    });

    await handler(
      makeEvent('/recurring/SUB-001/amount-change', 'POST', { newAmount: 14.99, effectiveDate: '2026-05-01', reason: 'Annual review' }),
      {} as never, () => {},
    );

    // Audit PutCommand + RECEIPT# PutCommand = 2 puts
    const puts = mockSend.mock.calls.filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand');
    expect(puts).toHaveLength(2);
    const receiptItem = (puts[1][0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(String(receiptItem.sK ?? '')).toMatch(/^RECEIPT#/);
    expect(receiptItem.eventType).toBe('RECEIPT');
    const desc = JSON.parse(receiptItem.desc as string);
    expect(desc.receiptType).toBe('SUBSCRIPTION_AMOUNT_CHANGE');
    expect(desc.oldAmount).toBe(9.99);
    expect(desc.amount).toBe(14.99);
    expect(desc.reason).toBe('Annual review');
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

// ── POST /recurring/{subId}/invoice ─────────────────────────────────────────

const ACTIVE_SUB_DESC = JSON.stringify({
  subId: 'SUB-001',
  brandId: 'woolworths',
  brandName: 'Woolworths',
  productName: 'Woolworths Plus',
  amount: 9.99,
  currency: 'AUD',
  frequency: 'monthly',
  nextBillingDate: '2026-05-01',
  category: 'subscription',
  status: 'ACTIVE',
});

function makeActiveSubMock(opts: { noDevice?: boolean } = {}) {
  return (cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
    if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
    if (cmd.__type === 'GetCommand') {
      const sK = String((cmd.input?.Key ?? {})['sK'] ?? '');
      if (sK === 'DEVICE_TOKEN') {
        return opts.noDevice
          ? Promise.resolve({ Item: undefined })
          : Promise.resolve({ Item: { desc: JSON.stringify({ token: 'fcm-tok' }) } });
      }
      return Promise.resolve({ Item: { status: 'ACTIVE', desc: ACTIVE_SUB_DESC } });
    }
    return Promise.resolve({});
  };
}

describe('POST /recurring/{subId}/invoice', () => {
  it('returns 401 when API key is invalid', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(
      makeEvent('/recurring/SUB-001/invoice', 'POST', { amount: 9.99, dueDate: '2026-05-01' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(401);
  });

  it('returns 400 when amount or dueDate is missing', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation(makeActiveSubMock());
    const res = await handler(
      makeEvent('/recurring/SUB-001/invoice', 'POST', { amount: 9.99 }), // no dueDate
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(400);
  });

  it('returns 404 when subId is not indexed', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockResolvedValue({ Items: [] }); // empty RECURRING_IDX
    const res = await handler(
      makeEvent('/recurring/SUB-999/invoice', 'POST', { amount: 9.99, dueDate: '2026-05-01' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(404);
  });

  it('returns 201 and writes INVOICE# with correct shape', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation(makeActiveSubMock());

    const res = await handler(
      makeEvent('/recurring/SUB-001/invoice', 'POST', {
        amount: 9.99,
        dueDate: '2026-05-01',
        billingPeriod: '2026-04-01/2026-04-30',
        invoiceNumber: 'WW-INV-001',
        paymentUrl: 'https://pay.example.com/invoice/WW-INV-001',
      }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(201);
    const body = JSON.parse(res!.body);
    expect(body.status).toBe('PENDING');
    expect(body.invoiceSK).toMatch(/^INVOICE#2026-05-01#/);

    const put = mockSend.mock.calls.find(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    expect(put).toBeDefined();
    const item = (put![0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(item.primaryCat).toBe('invoice');
    const desc = JSON.parse(item.desc as string);
    expect(desc.invoiceType).toBe('SUBSCRIPTION_BILLING');
    expect(desc.linkedSubscriptionSk).toBe('RECURRING#woolworths#SUB-001');
    expect(desc.billingPeriod).toBe('2026-04-01/2026-04-30');
    expect(desc.invoiceNumber).toBe('WW-INV-001');
    expect(desc.paymentUrl).toBe('https://pay.example.com/invoice/WW-INV-001');
    expect(desc.status).toBe('unpaid');

    expect(mockFcmSend).toHaveBeenCalledOnce();
    const fcm = mockFcmSend.mock.calls[0][0] as { notification: { title: string }; data: Record<string, string> };
    expect(fcm.notification.title).toBe('Invoice from Woolworths');
    expect(fcm.data.type).toBe('SUBSCRIPTION_INVOICE');
  });

  it('returns 409 when subscription is not active', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: { status: 'CANCELLED_BY_USER', desc: ACTIVE_SUB_DESC } });
      return Promise.resolve({});
    });
    const res = await handler(
      makeEvent('/recurring/SUB-001/invoice', 'POST', { amount: 9.99, dueDate: '2026-05-01' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(409);
  });

  it('succeeds even when FCM push fails', async () => {
    mockFcmSend.mockRejectedValue(new Error('FCM down'));
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation(makeActiveSubMock());
    const res = await handler(
      makeEvent('/recurring/SUB-001/invoice', 'POST', { amount: 9.99, dueDate: '2026-05-01' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(201);
  });
});

// ── POST /recurring/{subId}/billing-confirmed ─────────────────────────────────

describe('POST /recurring/{subId}/billing-confirmed', () => {
  it('returns 401 when API key is invalid', async () => {
    mockExtractApiKey.mockReturnValue(null);
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(
      makeEvent('/recurring/SUB-001/billing-confirmed', 'POST', { amount: 9.99, billedAt: '2026-05-01T00:00:00Z' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(401);
  });

  it('returns 400 when amount or billedAt is missing', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation(makeActiveSubMock());
    const res = await handler(
      makeEvent('/recurring/SUB-001/billing-confirmed', 'POST', { amount: 9.99 }), // no billedAt
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(400);
  });

  it('advances nextBillingDate by one month and writes RECEIPT#', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation(makeActiveSubMock());

    const res = await handler(
      makeEvent('/recurring/SUB-001/billing-confirmed', 'POST', {
        amount: 9.99,
        billedAt: '2026-05-01T00:00:00Z',
      }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(200);
    const body = JSON.parse(res!.body);
    expect(body.status).toBe('PAYMENT_CONFIRMED');
    expect(body.nextBillingDate).toBe('2026-06-01');
    expect(body.receiptSK).toMatch(/^RECEIPT#2026-05-01#/);

    // UpdateCommand advances nextBillingDate
    const updates = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'UpdateCommand',
    );
    expect(updates.length).toBeGreaterThan(0);
    const updateDesc = JSON.parse(
      (updates[0][0] as { input: { ExpressionAttributeValues: Record<string, string> } })
        .input.ExpressionAttributeValues[':desc'],
    );
    expect(updateDesc.nextBillingDate).toBe('2026-06-01');

    // RECEIPT# written
    const puts = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    expect(puts).toHaveLength(1);
    const receiptDesc = JSON.parse(
      (puts[0][0] as { input: { Item: Record<string, unknown> } }).input.Item.desc as string,
    );
    expect(receiptDesc.receiptType).toBe('SUBSCRIPTION_PAYMENT');
    expect(receiptDesc.linkedSubscriptionSk).toBe('RECURRING#woolworths#SUB-001');

    expect(mockFcmSend).toHaveBeenCalledOnce();
  });

  it('marks linked invoice PAID when invoiceSK is provided', async () => {
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') {
        const sK = String((cmd.input?.Key ?? {})['sK'] ?? '');
        if (sK.startsWith('INVOICE#')) return Promise.resolve({ Item: { desc: JSON.stringify({ status: 'unpaid', amount: 9.99 }) } });
        if (sK === 'DEVICE_TOKEN') return Promise.resolve({ Item: undefined });
        return Promise.resolve({ Item: { status: 'ACTIVE', desc: ACTIVE_SUB_DESC } });
      }
      return Promise.resolve({});
    });

    const res = await handler(
      makeEvent('/recurring/SUB-001/billing-confirmed', 'POST', {
        amount: 9.99,
        billedAt: '2026-05-01T00:00:00Z',
        invoiceSK: 'INVOICE#2026-05-01#INV-001',
      }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(200);

    // Two UpdateCommands: one for subscription, one for invoice
    const updates = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'UpdateCommand',
    );
    expect(updates).toHaveLength(2);

    const invoiceUpdate = (updates[1][0] as {
      input: { ExpressionAttributeValues: Record<string, string> };
    }).input.ExpressionAttributeValues;
    const invDesc = JSON.parse(invoiceUpdate[':desc']);
    expect(invDesc.status).toBe('paid');
    expect(invDesc.paidDate).toBe('2026-05-01T00:00:00Z');
  });

  it('billing-confirmed for quarterly subscription advances by 3 months', async () => {
    const quarterlyDesc = JSON.stringify({ ...JSON.parse(ACTIVE_SUB_DESC), frequency: 'quarterly', nextBillingDate: '2026-04-01' });
    mockExtractApiKey.mockReturnValue('bebo_valid.key');
    mockValidateApiKey.mockResolvedValue(VALID_KEY);
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
      if (cmd.__type === 'GetCommand') {
        const sK = String((cmd.input?.Key ?? {})['sK'] ?? '');
        if (sK === 'DEVICE_TOKEN') return Promise.resolve({ Item: undefined });
        return Promise.resolve({ Item: { status: 'ACTIVE', desc: quarterlyDesc } });
      }
      return Promise.resolve({});
    });

    const res = await handler(
      makeEvent('/recurring/SUB-001/billing-confirmed', 'POST', { amount: 30, billedAt: '2026-04-01T00:00:00Z' }),
      {} as never, () => {},
    );
    expect(res!.statusCode).toBe(200);
    expect(JSON.parse(res!.body).nextBillingDate).toBe('2026-07-01');
  });
});
