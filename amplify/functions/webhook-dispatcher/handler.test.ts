import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { SQSEvent, SQSRecord } from 'aws-lambda';

// ── Hoisted mocks ─────────────────────────────────────────────────────────────

const {
  mockDdbSend,
  mockSecretsSend,
  mockFetch,
  mockGetTenantState,
  mockCheckQuota,
  mockIncrementCounter,
} = vi.hoisted(() => {
  process.env.REFDATA_TABLE = 'ref-table';
  return {
    mockDdbSend: vi.fn(),
    mockSecretsSend: vi.fn(),
    mockFetch: vi.fn(),
    mockGetTenantState: vi.fn(),
    mockCheckQuota: vi.fn(),
    mockIncrementCounter: vi.fn().mockResolvedValue(undefined),
  };
});

// ── Module mocks ──────────────────────────────────────────────────────────────

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: object) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn(() => ({ send: mockDdbSend })) },
  GetCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-secrets-manager', () => ({
  SecretsManagerClient: vi.fn(function (this: Record<string, unknown>) { this.send = mockSecretsSend; }),
  GetSecretValueCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetSecretValueCommand', input });
  }),
}));

vi.mock('../../shared/tenant-billing', () => ({
  getTenantStateForBrand: mockGetTenantState,
  checkTenantQuota: mockCheckQuota,
  incrementTenantUsageCounter: mockIncrementCounter,
}));

// Mock global fetch (used in handler for HTTP delivery)
vi.stubGlobal('fetch', mockFetch);

// ── Handler import ────────────────────────────────────────────────────────────

import { handler } from './handler.js';

// ── Constants ─────────────────────────────────────────────────────────────────

const BRAND_ID = 'woolworths';
const WEBHOOK_URL = 'https://pos.woolworths.com.au/webhooks/bebocard';
const ACTIVE_TENANT_STATE = {
  tenantId: 'wg',
  tier: 'base' as const,
  active: true,
  includedEventsPerMonth: 250,
  notifCap: 3,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeSQSEvent(records: Partial<SQSRecord>[]): SQSEvent {
  return {
    Records: records.map((r, i) => ({
      messageId: `msg-${i + 1}`,
      receiptHandle: `receipt-${i + 1}`,
      body: r.body ?? JSON.stringify({ brandId: BRAND_ID, type: 'NEW_OFFER', data: { offerId: 'OFF001' } }),
      attributes: {} as SQSRecord['attributes'],
      messageAttributes: {},
      md5OfBody: '',
      eventSource: 'aws:sqs',
      eventSourceARN: 'arn:aws:sqs:us-east-1:123:queue',
      awsRegion: 'us-east-1',
      ...r,
    })),
  };
}

function makeBrandProfile(overrides: Record<string, unknown> = {}) {
  return {
    Item: {
      pK: `BRAND#${BRAND_ID}`,
      sK: 'profile',
      desc: JSON.stringify({
        brandName: 'Woolworths',
        webhookUrl: WEBHOOK_URL,
        ...overrides,
      }),
    },
  };
}

function setupSuccessPath(webhookUrlOverride?: string) {
  mockDdbSend.mockResolvedValue(makeBrandProfile(webhookUrlOverride ? { webhookUrl: webhookUrlOverride } : {}));
  mockGetTenantState.mockResolvedValue(ACTIVE_TENANT_STATE);
  mockCheckQuota.mockResolvedValue({ allowed: true });
  mockFetch.mockResolvedValue({ ok: true, text: async () => 'OK', status: 200 });
}

// ─────────────────────────────────────────────────────────────────────────────
// webhook-dispatcher
// ─────────────────────────────────────────────────────────────────────────────

describe('webhook-dispatcher — delivery', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockIncrementCounter.mockResolvedValue(undefined);
    mockFetch.mockResolvedValue({ ok: true, text: async () => 'OK', status: 200 });
  });

  it('delivers webhook payload to brand webhookUrl', async () => {
    setupSuccessPath();
    await handler(makeSQSEvent([{}]), {} as never, {} as never);
    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, opts] = mockFetch.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(WEBHOOK_URL);
    expect(opts.method).toBe('POST');
    expect(opts.headers).toMatchObject({ 'X-BeboCard-Event': 'NEW_OFFER' });
    const body = JSON.parse(opts.body as string);
    expect(body.type).toBe('NEW_OFFER');
    expect(body.brandId).toBe(BRAND_ID);
    expect(body.version).toBe('v1');
  });

  it('skips delivery when brand has no webhookUrl configured', async () => {
    mockDdbSend.mockResolvedValue(makeBrandProfile({ webhookUrl: undefined }));
    mockGetTenantState.mockResolvedValue(ACTIVE_TENANT_STATE);
    await handler(makeSQSEvent([{}]), {} as never, {} as never);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('skips delivery when brand has empty webhookUrl', async () => {
    mockDdbSend.mockResolvedValue(makeBrandProfile({ webhookUrl: '' }));
    mockGetTenantState.mockResolvedValue(ACTIVE_TENANT_STATE);
    await handler(makeSQSEvent([{}]), {} as never, {} as never);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('skips delivery when tenant is suspended', async () => {
    mockDdbSend.mockResolvedValue(makeBrandProfile());
    mockGetTenantState.mockResolvedValue({ ...ACTIVE_TENANT_STATE, active: false });
    await handler(makeSQSEvent([{}]), {} as never, {} as never);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('skips delivery when quota is exceeded', async () => {
    mockDdbSend.mockResolvedValue(makeBrandProfile());
    mockGetTenantState.mockResolvedValue(ACTIVE_TENANT_STATE);
    mockCheckQuota.mockResolvedValue({ allowed: false, message: 'Quota exceeded' });
    await handler(makeSQSEvent([{}]), {} as never, {} as never);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('increments usage counter after successful delivery', async () => {
    setupSuccessPath();
    await handler(makeSQSEvent([{}]), {} as never, {} as never);
    expect(mockIncrementCounter).toHaveBeenCalledTimes(1);
    expect(mockIncrementCounter).toHaveBeenCalledWith(
      expect.anything(),
      expect.any(String),
      'wg',
      BRAND_ID,
      'offers',
    );
  });

  it('does not increment counter when delivery is skipped (no webhook URL)', async () => {
    mockDdbSend.mockResolvedValue(makeBrandProfile({ webhookUrl: undefined }));
    mockGetTenantState.mockResolvedValue(ACTIVE_TENANT_STATE);
    await handler(makeSQSEvent([{}]), {} as never, {} as never);
    expect(mockIncrementCounter).not.toHaveBeenCalled();
  });

  it('throws on non-2xx HTTP response so SQS retries the message', async () => {
    mockDdbSend.mockResolvedValue(makeBrandProfile());
    mockGetTenantState.mockResolvedValue(ACTIVE_TENANT_STATE);
    mockCheckQuota.mockResolvedValue({ allowed: true });
    mockFetch.mockResolvedValue({ ok: false, status: 503, text: async () => 'Service Unavailable' });
    await expect(
      handler(makeSQSEvent([{}]), {} as never, {} as never),
    ).rejects.toThrow();
  });

  it('processes multiple SQS records in one invocation', async () => {
    setupSuccessPath();
    const event = makeSQSEvent([
      { body: JSON.stringify({ brandId: BRAND_ID, type: 'NEW_OFFER', data: { offerId: 'A' } }) },
      { body: JSON.stringify({ brandId: BRAND_ID, type: 'NEW_OFFER', data: { offerId: 'B' } }) },
    ]);
    await handler(event, {} as never, {} as never);
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it('skips record with missing brandId (logs and continues)', async () => {
    const event = makeSQSEvent([
      { body: JSON.stringify({ type: 'NEW_OFFER', data: {} }) }, // no brandId
    ]);
    // Should not throw even though brandId is missing
    await handler(event, {} as never, {} as never);
    expect(mockFetch).not.toHaveBeenCalled();
  });
});

describe('webhook-dispatcher — HMAC signing', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockIncrementCounter.mockResolvedValue(undefined);
  });

  it('adds X-Bebocard-Signature header when brand has webhookSigningSecretArn', async () => {
    mockDdbSend.mockResolvedValue(
      makeBrandProfile({ webhookSigningSecretArn: 'arn:aws:secretsmanager:us-east-1:123:secret/wh-secret' }),
    );
    mockGetTenantState.mockResolvedValue(ACTIVE_TENANT_STATE);
    mockCheckQuota.mockResolvedValue({ allowed: true });
    mockSecretsSend.mockResolvedValue({ SecretString: 'my-signing-secret' });
    mockFetch.mockResolvedValue({ ok: true, text: async () => 'OK', status: 200 });

    await handler(makeSQSEvent([{}]), {} as never, {} as never);

    const [, opts] = mockFetch.mock.calls[0] as [string, RequestInit];
    expect((opts.headers as Record<string, string>)['X-Bebocard-Signature']).toMatch(/^sha256=[0-9a-f]{64}$/);
  });

  it('delivers unsigned when webhookSigningSecretArn is absent', async () => {
    setupSuccessPath();
    await handler(makeSQSEvent([{}]), {} as never, {} as never);
    const [, opts] = mockFetch.mock.calls[0] as [string, RequestInit];
    expect((opts.headers as Record<string, string>)['X-Bebocard-Signature']).toBeUndefined();
  });
});

describe('webhook-dispatcher — event type routing to usage counter', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockIncrementCounter.mockResolvedValue(undefined);
    mockFetch.mockResolvedValue({ ok: true, text: async () => 'OK', status: 200 });
  });

  it.each([
    ['NEW_OFFER', 'offers'],
    ['NEWSLETTER_SENT', 'newsletters'],
    ['CATALOGUE_PUBLISHED', 'catalogues'],
    ['INVOICE_CREATED', 'invoices'],
  ])('maps event type %s to usage type %s', async (eventType, expectedUsageType) => {
    mockDdbSend.mockResolvedValue(makeBrandProfile());
    mockGetTenantState.mockResolvedValue(ACTIVE_TENANT_STATE);
    mockCheckQuota.mockResolvedValue({ allowed: true });

    const event = makeSQSEvent([
      { body: JSON.stringify({ brandId: BRAND_ID, type: eventType, data: {} }) },
    ]);
    await handler(event, {} as never, {} as never);

    expect(mockCheckQuota).toHaveBeenCalledWith(
      expect.anything(),
      expect.any(String),
      ACTIVE_TENANT_STATE,
      expectedUsageType,
    );
  });
});
