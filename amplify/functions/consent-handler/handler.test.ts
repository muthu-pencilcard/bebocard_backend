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
process.env.CONSENT_TIMEOUT_QUEUE_URL = 'https://sqs.example.com/consent-queue';
process.env.FIREBASE_SERVICE_ACCOUNT_JSON = JSON.stringify({ project_id: 'test' });

// ── Import handler AFTER all mocks ────────────────────────────────────────────

const { handler } = await import('./handler.js');

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeApiEvent(overrides: Record<string, unknown> = {}) {
  return {
    path: '/consent-request',
    httpMethod: 'POST',
    headers: { 'x-api-key': 'bebo_test.secret' },
    body: JSON.stringify({
      secondaryULID: 'SEC001',
      requestedFields: ['email', 'phone'],
      purpose: 'Loyalty program enrollment',
    }),
    queryStringParameters: null,
    ...overrides,
  };
}

function makeValidKey() {
  return { brandId: 'woolworths', keyId: 'KEYID001', rateLimit: 1000, scopes: ['consent'] };
}

function makeSqsEvent(body: object) {
  return {
    Records: [{ eventSource: 'aws:sqs', body: JSON.stringify(body) }],
  };
}

/**
 * Sets up mockSend for the happy-path POST /consent-request sequence:
 *   1. QueryCommand → resolve secondaryULID
 *   2. GetCommand   → brand profile (PROFILE sK)
 *   3. PutCommand   → store consent record
 *   4. GetCommand   → device token
 */
function setupSuccessMocks() {
  mockSend.mockImplementation((cmd: { __type: string; input: Record<string, unknown> }) => {
    if (cmd.__type === 'QueryCommand') {
      const pk = (cmd.input as { ExpressionAttributeValues?: Record<string, string> })
        .ExpressionAttributeValues?.[':pk'] ?? '';
      if (pk.startsWith('SCAN#')) {
        return Promise.resolve({ Items: [{ sK: 'PERM001' }] });
      }
      // GET /status QueryCommand — return a consent item
      return Promise.resolve({
        Items: [{
          status: 'APPROVED',
          desc: JSON.stringify({ approvedFields: ['email'], expiresAt: '2026-01-01T00:00:00.000Z' }),
        }],
      });
    }
    if (cmd.__type === 'GetCommand') {
      const key = (cmd.input as { Key?: Record<string, string> }).Key ?? {};
      if (key.pK?.startsWith('BRAND#')) {
        return Promise.resolve({
          Item: { desc: JSON.stringify({ brandName: 'Woolworths', consentWebhookUrl: 'https://example.com/consent-webhook' }) },
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

describe('POST /consent-request — auth', () => {
  it('returns 401 if no API key (extractApiKey returns null)', async () => {
    mockExtractApiKey.mockReturnValue(null);
    const res = await handler(makeApiEvent()) as { statusCode: number };
    expect(res.statusCode).toBe(401);
  });

  it('returns 401 if invalid API key (validateApiKey returns null)', async () => {
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(null);
    const res = await handler(makeApiEvent()) as { statusCode: number };
    expect(res.statusCode).toBe(401);
  });
});

describe('POST /consent-request — validation', () => {
  beforeEach(() => {
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(makeValidKey());
  });

  it('returns 400 if missing required fields', async () => {
    const res = await handler(makeApiEvent({ body: JSON.stringify({ secondaryULID: 'SEC001' }) })) as { statusCode: number };
    expect(res.statusCode).toBe(400);
  });

  it('returns 400 if requestedFields contains an invalid field (ssn)', async () => {
    const res = await handler(makeApiEvent({
      body: JSON.stringify({ secondaryULID: 'SEC001', requestedFields: ['ssn'], purpose: 'test' }),
    })) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body).error).toMatch(/invalid fields/i);
  });

  it('returns 400 only for disallowed fields, not for valid ones like email/phone', async () => {
    // Mix of valid and invalid
    const res = await handler(makeApiEvent({
      body: JSON.stringify({ secondaryULID: 'SEC001', requestedFields: ['email', 'ssn'], purpose: 'test' }),
    })) as { statusCode: number };
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 if secondaryULID is not found', async () => {
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [] });
      return Promise.resolve({});
    });
    const res = await handler(makeApiEvent()) as { statusCode: number };
    expect(res.statusCode).toBe(404);
  });
});

describe('POST /consent-request — success', () => {
  beforeEach(() => {
    mockExtractApiKey.mockReturnValue('bebo_test.secret');
    mockValidateApiKey.mockResolvedValue(makeValidKey());
    setupSuccessMocks();
  });

  it('returns 202 with requestId and expiresAt', async () => {
    const res = await handler(makeApiEvent()) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(202);
    const body = JSON.parse(res.body);
    expect(body.requestId).toBeDefined();
    expect(body.status).toBe('PENDING');
    expect(body.expiresAt).toBeDefined();
  });

  it('PutCommand has pK CONSENT#<requestId>, sK permULID, status PENDING', async () => {
    const { PutCommand } = await import('@aws-sdk/lib-dynamodb');
    await handler(makeApiEvent());
    const putCalls = (PutCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
    const consentPut = putCalls.find(
      (args) => (args[0] as { Item: { pK: string } }).Item?.pK?.startsWith('CONSENT#'),
    );
    expect(consentPut).toBeDefined();
    expect((consentPut![0] as { Item: { sK: string } }).Item.sK).toBe('PERM001');
    expect((consentPut![0] as { Item: { status: string } }).Item.status).toBe('PENDING');
  });

  it('SQS DelaySeconds is 60 (not 90)', async () => {
    const { SendMessageCommand } = await import('@aws-sdk/client-sqs');
    await handler(makeApiEvent());
    const sqsCalls = (SendMessageCommand as unknown as ReturnType<typeof vi.fn>).mock.calls;
    expect(sqsCalls.length).toBeGreaterThan(0);
    const sqsInput = sqsCalls[0][0] as { DelaySeconds: number };
    expect(sqsInput.DelaySeconds).toBe(60);
  });

  it('FCM notification title contains brand name', async () => {
    await handler(makeApiEvent());
    expect(mockFcmSend).toHaveBeenCalledOnce();
    const fcmArg = mockFcmSend.mock.calls[0][0] as {
      notification: { title: string };
      data: { type: string };
    };
    expect(fcmArg.notification.title).toMatch(/Woolworths/i);
  });

  it('FCM data.type is CONSENT_REQUEST', async () => {
    await handler(makeApiEvent());
    const fcmArg = mockFcmSend.mock.calls[0][0] as { data: { type: string } };
    expect(fcmArg.data.type).toBe('CONSENT_REQUEST');
  });
});

describe('GET /consent-request/{requestId}/status', () => {
  it('returns correct shape including approvedFields', async () => {
    mockSend.mockResolvedValue({
      Items: [{
        status: 'APPROVED',
        desc: JSON.stringify({ approvedFields: ['email'], expiresAt: '2026-01-01T00:00:00.000Z' }),
      }],
    });
    const res = await handler({
      path: '/consent-request/REQ001/status',
      httpMethod: 'GET',
      headers: {},
      body: null,
      queryStringParameters: null,
    }) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.requestId).toBe('REQ001');
    expect(body.status).toBe('APPROVED');
    expect(body.approvedFields).toEqual(['email']);
    expect(body.expiresAt).toBeDefined();
  });

  it('returns 404 for unknown requestId', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const res = await handler({
      path: '/consent-request/UNKNOWN/status',
      httpMethod: 'GET',
      headers: {},
      body: null,
      queryStringParameters: null,
    }) as { statusCode: number };
    expect(res.statusCode).toBe(404);
  });
});

describe('SQS timeout handler', () => {
  it('marks consent as TIMEOUT if still PENDING', async () => {
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
    await handler(makeSqsEvent({ requestId: 'REQ001', permULID: 'PERM001' }));
    const updateCalls = (UpdateCommand as unknown as ReturnType<typeof vi.fn>).mock.calls;
    expect(updateCalls.length).toBeGreaterThan(0);
    const updateInput = updateCalls[0][0] as {
      ExpressionAttributeValues: Record<string, string>;
    };
    expect(updateInput.ExpressionAttributeValues[':s']).toBe('TIMEOUT');
  });

  it('is a no-op if consent record is already resolved', async () => {
    mockSend.mockResolvedValue({ Item: { status: 'APPROVED', desc: JSON.stringify({}) } });
    const { UpdateCommand } = await import('@aws-sdk/lib-dynamodb');
    await handler(makeSqsEvent({ requestId: 'REQ001', permULID: 'PERM001' }));
    expect((UpdateCommand as unknown as ReturnType<typeof vi.fn>).mock.calls.length).toBe(0);
  });

  it('is a no-op if consent record is not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    const { UpdateCommand } = await import('@aws-sdk/lib-dynamodb');
    await handler(makeSqsEvent({ requestId: 'MISSING', permULID: 'PERM001' }));
    expect((UpdateCommand as unknown as ReturnType<typeof vi.fn>).mock.calls.length).toBe(0);
  });
});
