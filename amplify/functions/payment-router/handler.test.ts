import { describe, it, expect, vi, beforeEach } from 'vitest';

// ── Hoisted mock functions ────────────────────────────────────────────────────

const { mockSend, mockValidateApiKey, mockExtractApiKey, mockFcmSend, mockSqsSend } =
  vi.hoisted(() => ({
    mockSend: vi.fn(),
    mockValidateApiKey: vi.fn(),
    mockExtractApiKey: vi.fn(),
    mockFcmSend: vi.fn(),
    mockSqsSend: vi.fn(),
  }));

// ── Mocks ─────────────────────────────────────────────────────────────────────

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) { }),
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

vi.mock('@aws-sdk/client-sqs', () => ({
  SQSClient: vi.fn(function (this: Record<string, unknown>) { this.send = mockSqsSend; }),
  SendMessageCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'SendMessageCommand', input });
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

vi.mock('../../shared/api-key-auth', () => ({
  validateApiKey: mockValidateApiKey,
  extractApiKey: mockExtractApiKey,
}));

vi.mock('../../shared/audit-logger', () => ({
  withAuditLog: vi.fn((_ddb: unknown, h: unknown) => h),
}));

vi.mock('https', () => ({
  default: {
    request: vi.fn((_url: unknown, _opts: unknown, cb: (res: { resume: () => void; on: (e: string, fn: () => void) => void }) => void) => {
      cb({ resume: vi.fn(), on: vi.fn((e: string, fn: () => void) => { if (e === 'end') fn(); }) });
      return { on: vi.fn(), write: vi.fn(), end: vi.fn(), setTimeout: vi.fn() };
    }),
  },
}));

// ── Set env vars before importing handler (top-level consts are captured at import time) ──

process.env.ADMIN_TABLE = 'admin-table';
process.env.USER_TABLE = 'user-table';
process.env.REF_TABLE = 'ref-table';
process.env.TIMEOUT_QUEUE_URL = 'https://sqs.example.com/queue';
process.env.FIREBASE_SERVICE_ACCOUNT_JSON = JSON.stringify({ project_id: 'test' });

// ── Import handler AFTER all mocks ────────────────────────────────────────────

const { handler } = await import('./handler.js');

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeApiEvent(overrides: Record<string, unknown> = {}) {
  return {
    path: '/checkout',
    httpMethod: 'POST',
    headers: { 'x-api-key': 'bebo_test.secret' },
    body: JSON.stringify({
      secondaryULID: 'SEC001',
      amount: 49.95,
      currency: 'AUD',
      merchantName: 'Woolworths',
      orderId: 'ORDER001',
    }),
    queryStringParameters: null,
    ...overrides,
  };
}

function makeValidKey() {
  return { brandId: 'woolworths', keyId: 'KEYID001', rateLimit: 1000, scopes: ['payment'] };
}

function makeSqsEvent(body: object) {
  return {
    Records: [{ eventSource: 'aws:sqs', body: JSON.stringify(body) }],
  };
}

/**
 * Sets up mockSend to return appropriate responses for the happy-path
 * POST /checkout call sequence:
 *   1. QueryCommand  → resolve secondaryULID
 *   2. GetCommand    → duplicate check (no item)
 *   3. GetCommand    → brand profile
 *   4. PutCommand    → store checkout record
 *   5. GetCommand    → device token
 */
function setupSuccessMocks() {
  mockSend.mockImplementation((cmd: { __type: string; input: Record<string, unknown> }) => {
    if (cmd.__type === 'QueryCommand') {
      const pk = (cmd.input as { ExpressionAttributeValues?: Record<string, string> })
        .ExpressionAttributeValues?.[':pk'] ?? '';
      if (pk.startsWith('SCAN#')) {
        return Promise.resolve({ Items: [{ sK: 'PERM001' }] });
      }
      // GET /status QueryCommand
      return Promise.resolve({ Items: [{ status: 'PENDING', desc: JSON.stringify({ expiresAt: '2026-01-01T00:00:00.000Z' }) }] });
    }
    if (cmd.__type === 'GetCommand') {
      const key = (cmd.input as { Key?: Record<string, string> }).Key ?? {};
      if (key.pK?.startsWith('TENANT#')) {
        return Promise.resolve({
          Item: {
            status: 'ACTIVE',
            createdAt: '2026-01-01T00:00:00.000Z',
            desc: JSON.stringify({
              tier: 'intelligence',
              billingStatus: 'ACTIVE',
              stripeCustomerId: 'cus_123',
              stripeSubscriptionId: 'sub_123',
            }),
          },
        });
      }
      if (key.pK?.startsWith('CHECKOUT#')) {
        return Promise.resolve({ Item: undefined });
      }
      if (key.pK?.startsWith('BRAND#')) {
        return Promise.resolve({
          Item: {
            desc: JSON.stringify({
              tenantId: 'tenant-001',
              paymentWebhookUrl: 'https://example.com/webhook',
            }),
          },
        });
      }
      if (key.sK === 'DEVICE_TOKEN') {
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'fcm-token' }) } });
      }
      return Promise.resolve({ Item: undefined });
    }
    return Promise.resolve({});
  });
}

// ── Setup ─────────────────────────────────────────────────────────────────────

beforeEach(() => {
  vi.clearAllMocks();
  mockSqsSend.mockResolvedValue({});
  mockFcmSend.mockResolvedValue('msg-id');
});

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('POST /checkout — auth', () => {
  it('returns 401 if extractApiKey returns null', async () => {
    mockExtractApiKey.mockReturnValue(null);
    const res = await handler(makeApiEvent()) as { statusCode: number };
    expect(res.statusCode).toBe(401);
  });

  it('returns 401 if validateApiKey returns null (invalid key)', async () => {
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(makeApiEvent()) as { statusCode: number };
    expect(res.statusCode).toBe(401);
  });
});

describe('POST /checkout — validation', () => {
  beforeEach(() => {
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(makeValidKey());
  });

  it('returns 400 if required body fields are missing', async () => {
    const res = await handler(makeApiEvent({ body: JSON.stringify({ secondaryULID: 'SEC001' }) })) as { statusCode: number };
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 if secondaryULID is not found', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input: Record<string, unknown> }) => {
      if (cmd.__type === 'GetCommand') {
        const key = (cmd.input as { Key?: Record<string, string> }).Key ?? {};
        if (key.pK?.startsWith('BRAND#')) {
          return Promise.resolve({ Item: { desc: JSON.stringify({ tenantId: 'tenant-001' }) } });
        }
        if (key.pK?.startsWith('TENANT#')) {
          return Promise.resolve({
            Item: {
              status: 'ACTIVE',
              createdAt: '2026-01-01T00:00:00.000Z',
              desc: JSON.stringify({
                tier: 'intelligence',
                billingStatus: 'ACTIVE',
                stripeCustomerId: 'cus_123',
                stripeSubscriptionId: 'sub_123',
              }),
            },
          });
        }
      }
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [] });
      return Promise.resolve({});
    });
    const res = await handler(makeApiEvent()) as { statusCode: number };
    expect(res.statusCode).toBe(404);
  });

  it('returns 409 if duplicate orderId exists', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input: Record<string, unknown> }) => {
      if (cmd.__type === 'GetCommand') {
        const key = (cmd.input as { Key?: Record<string, string> }).Key ?? {};
        if (key.pK?.startsWith('BRAND#')) {
          return Promise.resolve({ Item: { desc: JSON.stringify({ tenantId: 'tenant-001' }) } });
        }
        if (key.pK?.startsWith('TENANT#')) {
          return Promise.resolve({
            Item: {
              status: 'ACTIVE',
              createdAt: '2026-01-01T00:00:00.000Z',
              desc: JSON.stringify({
                tier: 'intelligence',
                billingStatus: 'ACTIVE',
                stripeCustomerId: 'cus_123',
                stripeSubscriptionId: 'sub_123',
              }),
            },
          });
        }
        if (key.pK?.startsWith('CHECKOUT#')) {
          return Promise.resolve({ Item: { status: 'PENDING', desc: JSON.stringify({ status: 'PENDING' }) } });
        }
      }
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [{ sK: 'PERM001' }] });
      return Promise.resolve({});
    });
    const res = await handler(makeApiEvent()) as { statusCode: number };
    expect(res.statusCode).toBe(409);
  });

  it('returns 403 when tenant is not eligible for payments', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input: Record<string, unknown> }) => {
      if (cmd.__type === 'GetCommand') {
        const key = (cmd.input as { Key?: Record<string, string> }).Key ?? {};
        if (key.pK?.startsWith('BRAND#')) {
          return Promise.resolve({ Item: { desc: JSON.stringify({ tenantId: 'tenant-001' }) } });
        }
        if (key.pK?.startsWith('TENANT#')) {
          return Promise.resolve({
            Item: {
              status: 'ACTIVE',
              createdAt: '2026-01-01T00:00:00.000Z',
              desc: JSON.stringify({
                tier: 'engagement',
                billingStatus: 'ACTIVE',
                stripeCustomerId: 'cus_123',
                stripeSubscriptionId: 'sub_123',
              }),
            },
          });
        }
      }
      return Promise.resolve({});
    });
    const res = await handler(makeApiEvent()) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(403);
    expect(JSON.parse(res.body).error).toContain('intelligence tier');
  });
});

describe('POST /checkout — success', () => {
  beforeEach(() => {
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(makeValidKey());
    setupSuccessMocks();
  });

  it('returns 202 with correct body on success', async () => {
    const res = await handler(makeApiEvent()) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(202);
    const body = JSON.parse(res.body);
    expect(body.orderId).toBe('ORDER001');
    expect(body.status).toBe('PENDING');
    expect(body.expiresAt).toBeDefined();
  });

  it('PutCommand has correct pK and status PENDING', async () => {
    const { PutCommand } = await import('@aws-sdk/lib-dynamodb');
    await handler(makeApiEvent());
    const putCalls = (PutCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
    const checkoutPut = putCalls.find(
      (args) => (args[0] as { Item: { pK: string } }).Item?.pK === 'CHECKOUT#ORDER001',
    );
    expect(checkoutPut).toBeDefined();
    expect((checkoutPut![0] as { Item: { status: string } }).Item.status).toBe('PENDING');
  });

  it('SQS SendMessageCommand has DelaySeconds 90 and correct body', async () => {
    const { SendMessageCommand } = await import('@aws-sdk/client-sqs');
    await handler(makeApiEvent());
    const sqsCalls = (SendMessageCommand as unknown as ReturnType<typeof vi.fn>).mock.calls;
    expect(sqsCalls.length).toBeGreaterThan(0);
    const sqsInput = sqsCalls[0][0] as { DelaySeconds: number; MessageBody: string };
    expect(sqsInput.DelaySeconds).toBe(90);
    const msgBody = JSON.parse(sqsInput.MessageBody);
    expect(msgBody.orderId).toBe('ORDER001');
    expect(msgBody.permULID).toBe('PERM001');
  });

  it('FCM send is called with correct token and data.type CHECKOUT', async () => {
    await handler(makeApiEvent());
    expect(mockFcmSend).toHaveBeenCalledOnce();
    const fcmArg = mockFcmSend.mock.calls[0][0] as { token: string; data: { type: string } };
    expect(fcmArg.token).toBe('fcm-token');
    expect(fcmArg.data.type).toBe('CHECKOUT');
  });

  it('proceeds and returns 202 even if FCM throws', async () => {
    mockFcmSend.mockRejectedValue(new Error('FCM unavailable'));
    const res = await handler(makeApiEvent()) as { statusCode: number };
    expect(res.statusCode).toBe(202);
  });

  it('increments tenant payment usage after checkout creation', async () => {
    const { UpdateCommand } = await import('@aws-sdk/lib-dynamodb');
    await handler(makeApiEvent());
    const updateCalls = (UpdateCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
    const usageUpdate = updateCalls.find((args) =>
      (args[0] as { Key?: { pK?: string; sK?: string } }).Key?.pK === 'TENANT#tenant-001' && (args[0] as { Key?: { pK?: string; sK?: string } }).Key?.sK?.endsWith('#payments'));
    expect(usageUpdate).toBeDefined();
  });
});

describe('GET /checkout/{orderId}/status', () => {
  beforeEach(() => {
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(makeValidKey());
  });

  it('returns 200 with status from item', async () => {
    mockSend.mockResolvedValue({
      Items: [{ status: 'APPROVED', desc: JSON.stringify({ expiresAt: '2026-01-01T00:00:00.000Z' }) }],
    });
    const res = await handler({
      path: '/checkout/ORDER001/status',
      httpMethod: 'GET',
      headers: {},
      body: null,
      queryStringParameters: null,
    }) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.orderId).toBe('ORDER001');
    expect(body.status).toBe('APPROVED');
  });

  it('returns 404 for unknown orderId', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const res = await handler({
      path: '/checkout/unknown/status',
      httpMethod: 'GET',
      headers: {},
      body: null,
      queryStringParameters: null,
    }) as { statusCode: number };
    expect(res.statusCode).toBe(404);
  });
});

describe('SQS timeout handler', () => {
  it('UpdateCommand sets status to TIMEOUT when checkout is PENDING', async () => {
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'GetCommand') {
        return Promise.resolve({
          Item: {
            status: 'PENDING',
            desc: JSON.stringify({ brandWebhookUrl: 'https://example.com/webhook' }),
          },
        });
      }
      return Promise.resolve({});
    });
    const { UpdateCommand } = await import('@aws-sdk/lib-dynamodb');
    await handler(makeSqsEvent({ orderId: 'ORDER001', permULID: 'PERM001' }));
    const updateCalls = (UpdateCommand as unknown as ReturnType<typeof vi.fn>).mock.calls;
    expect(updateCalls.length).toBeGreaterThan(0);
    const updateInput = updateCalls[0][0] as {
      ExpressionAttributeValues: Record<string, string>;
    };
    expect(updateInput.ExpressionAttributeValues[':s']).toBe('TIMEOUT');
  });

  it('skips if checkout record is not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    const { UpdateCommand } = await import('@aws-sdk/lib-dynamodb');
    await handler(makeSqsEvent({ orderId: 'MISSING', permULID: 'PERM001' }));
    expect((UpdateCommand as unknown as ReturnType<typeof vi.fn>).mock.calls.length).toBe(0);
  });

  it('skips if checkout is already resolved (status !== PENDING)', async () => {
    mockSend.mockResolvedValue({
      Item: { status: 'APPROVED', desc: JSON.stringify({}) },
    });
    const { UpdateCommand } = await import('@aws-sdk/lib-dynamodb');
    await handler(makeSqsEvent({ orderId: 'ORDER001', permULID: 'PERM001' }));
    expect((UpdateCommand as unknown as ReturnType<typeof vi.fn>).mock.calls.length).toBe(0);
  });
});
