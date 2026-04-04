/**
 * Tests for smb-handler — Phase 11 (SMB Loyalty-as-a-Service)
 *
 * Tests grouped by route:
 *   1. POST /smb/stamp    — auth, validation, quota, normal stamp, REDEEMABLE trigger
 *   2. POST /smb/redeem   — auth, not found, not redeemable, success
 *   3. GET  /smb/card     — auth, not found, success
 *   4. GET  /smb/analytics — auth, success
 *   5. validateStampQuota — unit tests for quota logic
 *   6. validateStampRecord — unit tests for record validation
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { validateStampQuota, validateStampRecord } from './handler';

// ── Mock AWS SDK ──────────────────────────────────────────────────────────────
// vi.mock is hoisted — use vi.hoisted so mockSend is available inside the factory.

const { mockSend } = vi.hoisted(() => ({ mockSend: vi.fn() }));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function () { return {}; }),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn(() => ({ send: mockSend })) },
  GetCommand:    vi.fn(function (input: unknown) { return { input }; }),
  PutCommand:    vi.fn(function (input: unknown) { return { input }; }),
  UpdateCommand: vi.fn(function (input: unknown) { return { input }; }),
  QueryCommand:  vi.fn(function (input: unknown) { return { input }; }),
}));

vi.mock('../../shared/api-key-auth', () => ({
  extractApiKey:  vi.fn(),
  validateApiKey: vi.fn(),
}));

vi.mock('../../shared/audit-logger', () => ({
  withAuditLog: (_ddb: unknown, fn: unknown) => fn,
}));

import { extractApiKey, validateApiKey } from '../../shared/api-key-auth';

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeEvent(overrides: Record<string, unknown> = {}) {
  return {
    path:       '/smb/stamp',
    httpMethod: 'POST',
    headers:    { 'x-api-key': 'bebo_test.key' },
    body:       JSON.stringify({ secondaryULID: 'SEC123' }),
    queryStringParameters: null,
    pathParameters:        null,
    multiValueHeaders:     {},
    multiValueQueryStringParameters: null,
    isBase64Encoded: false,
    requestContext:  {} as never,
    resource:        '',
    stageVariables:  null,
    ...overrides,
  };
}

const VALID_KEY = { brandId: 'woolworths', keyId: 'key1', rateLimit: 1000, scopes: ['smb'] };

const VALID_CONFIG_DESC = JSON.stringify({
  goal: 5,
  rewardDescription: 'Free coffee',
  tier: 'starter',
  monthlyQuota: 500,
  stampsThisMonth: 0,
  quotaMonthKey: new Date().toISOString().slice(0, 7),
});

// ── 5. validateStampQuota (pure unit tests) ───────────────────────────────────

describe('validateStampQuota', () => {
  const currentMonth = new Date().toISOString().slice(0, 7);

  it('returns null when under quota', () => {
    const config = {
      goal: 10, rewardDescription: 'Free item', tier: 'starter',
      monthlyQuota: 500, stampsThisMonth: 100, quotaMonthKey: currentMonth,
    };
    expect(validateStampQuota(config)).toBeNull();
  });

  it('returns error string when at quota limit', () => {
    const config = {
      goal: 10, rewardDescription: 'Free item', tier: 'starter',
      monthlyQuota: 500, stampsThisMonth: 500, quotaMonthKey: currentMonth,
    };
    expect(validateStampQuota(config)).toMatch(/quota/i);
  });

  it('returns error string when over quota', () => {
    const config = {
      goal: 10, rewardDescription: 'Free item', tier: 'growth',
      monthlyQuota: 2000, stampsThisMonth: 2001, quotaMonthKey: currentMonth,
    };
    expect(validateStampQuota(config)).toMatch(/quota/i);
  });

  it('returns null when month key is different (quota resets)', () => {
    const config = {
      goal: 10, rewardDescription: 'Free item', tier: 'starter',
      monthlyQuota: 500, stampsThisMonth: 500, quotaMonthKey: '2025-01',
    };
    expect(validateStampQuota(config)).toBeNull();
  });

  it('returns null for business tier well over starter quota', () => {
    const config = {
      goal: 10, rewardDescription: 'Free item', tier: 'business',
      monthlyQuota: 999_999, stampsThisMonth: 50000, quotaMonthKey: currentMonth,
    };
    expect(validateStampQuota(config)).toBeNull();
  });
});

// ── 6. validateStampRecord (pure unit tests) ─────────────────────────────────

describe('validateStampRecord', () => {
  it('returns true for valid record', () => {
    expect(validateStampRecord({
      brandId: 'woolworths', stamps: 3, goal: 10, status: 'ACTIVE', rewardDescription: 'Free item',
    })).toBe(true);
  });

  it('returns false when brandId missing', () => {
    expect(validateStampRecord({ stamps: 3, goal: 10, status: 'ACTIVE', rewardDescription: 'x' })).toBe(false);
  });

  it('returns false when stamps is not a number', () => {
    expect(validateStampRecord({ brandId: 'b', stamps: '3', goal: 10, status: 'ACTIVE', rewardDescription: 'x' })).toBe(false);
  });

  it('returns false when goal missing', () => {
    expect(validateStampRecord({ brandId: 'b', stamps: 3, status: 'ACTIVE', rewardDescription: 'x' })).toBe(false);
  });
});

// ── 1. POST /smb/stamp ────────────────────────────────────────────────────────

describe('POST /smb/stamp', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.ADMIN_TABLE = 'AdminDataEvent';
    process.env.USER_TABLE  = 'UserDataEvent';
    process.env.REF_TABLE   = 'RefDataEvent';
  });

  it('returns 401 when API key is missing', async () => {
    vi.mocked(extractApiKey).mockReturnValue(null);
    vi.mocked(validateApiKey).mockResolvedValue(null);

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent({ headers: {} }));

    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.body).error).toMatch(/invalid|missing/i);
  });

  it('returns 401 when API key is invalid', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_bad.key');
    vi.mocked(validateApiKey).mockResolvedValue(null);

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent());

    expect(res.statusCode).toBe(401);
  });

  it('returns 400 when secondaryULID is missing', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent({ body: JSON.stringify({}) }));

    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body).error).toContain('secondaryULID');
  });

  it('returns 404 when user not found', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    // SCAN# lookup → empty
    mockSend.mockResolvedValueOnce({ Items: [] });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent());

    expect(res.statusCode).toBe(404);
    expect(JSON.parse(res.body).error).toContain('User not found');
  });

  it('returns 429 when monthly quota exceeded', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    // SCAN# → permULID
    mockSend.mockResolvedValueOnce({ Items: [{ sK: 'PERM001' }] });
    // SMB_CONFIG → quota exceeded
    const quotaMonthKey = new Date().toISOString().slice(0, 7);
    mockSend.mockResolvedValueOnce({
      Item: {
        desc: JSON.stringify({
          goal: 5, rewardDescription: 'Free coffee', tier: 'starter',
          monthlyQuota: 500, stampsThisMonth: 500, quotaMonthKey,
        }),
      },
    });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent());

    expect(res.statusCode).toBe(429);
    expect(JSON.parse(res.body).error).toMatch(/quota/i);
  });

  it('returns 200 with stamp data on normal stamp', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    // SCAN# → permULID
    mockSend.mockResolvedValueOnce({ Items: [{ sK: 'PERM001' }] });
    // SMB_CONFIG
    mockSend.mockResolvedValueOnce({ Item: { desc: VALID_CONFIG_DESC } });
    // Brand PROFILE
    mockSend.mockResolvedValueOnce({ Item: { desc: JSON.stringify({ brandName: 'Woolworths', brandColor: '#007837' }) } });
    // PutCommand (create stamp record) — no-op (throws ConditionalCheck, swallowed)
    mockSend.mockRejectedValueOnce({ name: 'ConditionalCheckFailedException' });
    // UpdateCommand (ADD stamps :one) → returns new stamp count = 2
    mockSend.mockResolvedValueOnce({
      Attributes: {
        stamps: 2,
        desc: JSON.stringify({ stamps: 2, goal: 5, status: 'ACTIVE', rewardDescription: 'Free coffee', brandId: 'woolworths', brandName: 'Woolworths', brandColor: '#007837', redemptions: 0 }),
      },
    });
    // incrementMonthlyStamp UpdateCommand
    mockSend.mockResolvedValueOnce({});

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent());

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.stamps).toBe(2);
    expect(body.goal).toBe(5);
    expect(body.status).toBe('ACTIVE');
    expect(body.rewardDescription).toBe('Free coffee');
  });

  it('returns 200 with REDEEMABLE status when stamps reach goal', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    // SCAN# → permULID
    mockSend.mockResolvedValueOnce({ Items: [{ sK: 'PERM001' }] });
    // SMB_CONFIG (goal=5)
    mockSend.mockResolvedValueOnce({ Item: { desc: VALID_CONFIG_DESC } });
    // Brand PROFILE
    mockSend.mockResolvedValueOnce({ Item: { desc: JSON.stringify({ brandName: 'Woolworths', brandColor: '#007837' }) } });
    // PutCommand (create) — swallowed
    mockSend.mockRejectedValueOnce({ name: 'ConditionalCheckFailedException' });
    // UpdateCommand ADD stamps → stamps now = 5 (== goal)
    mockSend.mockResolvedValueOnce({
      Attributes: {
        stamps: 5,
        desc: JSON.stringify({ stamps: 5, goal: 5, status: 'ACTIVE', rewardDescription: 'Free coffee', brandId: 'woolworths', brandName: 'Woolworths', brandColor: '#007837', redemptions: 0 }),
      },
    });
    // UpdateCommand SET status = REDEEMABLE
    mockSend.mockResolvedValueOnce({});
    // getDeviceToken → no token (skip FCM)
    mockSend.mockResolvedValueOnce({ Item: null });
    // incrementMonthlyStamp
    mockSend.mockResolvedValueOnce({});

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent());

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.status).toBe('REDEEMABLE');
    expect(body.stamps).toBe(5);
  });
});

// ── 2. POST /smb/redeem ───────────────────────────────────────────────────────

describe('POST /smb/redeem', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.ADMIN_TABLE = 'AdminDataEvent';
    process.env.USER_TABLE  = 'UserDataEvent';
    process.env.REF_TABLE   = 'RefDataEvent';
  });

  const redeemEvent = (overrides: Record<string, unknown> = {}) =>
    makeEvent({ path: '/smb/redeem', body: JSON.stringify({ secondaryULID: 'SEC123' }), ...overrides });

  it('returns 401 with no API key', async () => {
    vi.mocked(extractApiKey).mockReturnValue(null);
    vi.mocked(validateApiKey).mockResolvedValue(null);

    const { handler } = await import('./handler');
    const res = await (handler as Function)(redeemEvent({ headers: {} }));

    expect(res.statusCode).toBe(401);
  });

  it('returns 404 when user not found', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    mockSend.mockResolvedValueOnce({ Items: [] });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(redeemEvent());

    expect(res.statusCode).toBe(404);
    expect(JSON.parse(res.body).error).toContain('User not found');
  });

  it('returns 404 when stamp card does not exist', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    mockSend.mockResolvedValueOnce({ Items: [{ sK: 'PERM001' }] }); // SCAN# → permULID
    mockSend.mockResolvedValueOnce({ Item: undefined });              // GetCommand stamp card → missing

    const { handler } = await import('./handler');
    const res = await (handler as Function)(redeemEvent());

    expect(res.statusCode).toBe(404);
    expect(JSON.parse(res.body).error).toContain('Stamp card not found');
  });

  it('returns 409 when stamp card is not redeemable', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    mockSend.mockResolvedValueOnce({ Items: [{ sK: 'PERM001' }] }); // SCAN# → permULID
    mockSend.mockResolvedValueOnce({ Item: { status: 'ACTIVE', desc: '{}' } }); // stamp card with ACTIVE status

    const { handler } = await import('./handler');
    const res = await (handler as Function)(redeemEvent());

    expect(res.statusCode).toBe(409);
    expect(JSON.parse(res.body).error).toMatch(/not redeemable/i);
  });

  it('returns 200 on successful redemption', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    mockSend.mockResolvedValueOnce({ Items: [{ sK: 'PERM001' }] });               // SCAN# → permULID
    mockSend.mockResolvedValueOnce({ Item: { status: 'REDEEMABLE', desc: '{}' } }); // stamp card REDEEMABLE
    mockSend.mockResolvedValueOnce({});                                             // UpdateCommand reset stamps
    mockSend.mockResolvedValueOnce({});                                             // PutCommand redemption record

    const { handler } = await import('./handler');
    const res = await (handler as Function)(redeemEvent());

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.success).toBe(true);
    expect(body.redemptionId).toBeTruthy();
  });
});

// ── 3. GET /smb/card ──────────────────────────────────────────────────────────

describe('GET /smb/card', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.ADMIN_TABLE = 'AdminDataEvent';
    process.env.USER_TABLE  = 'UserDataEvent';
    process.env.REF_TABLE   = 'RefDataEvent';
  });

  const cardEvent = (overrides: Record<string, unknown> = {}) =>
    makeEvent({
      path:       '/smb/card',
      httpMethod: 'GET',
      body:       null,
      queryStringParameters: { secondaryULID: 'SEC123' },
      ...overrides,
    });

  it('returns 401 with no API key', async () => {
    vi.mocked(extractApiKey).mockReturnValue(null);
    vi.mocked(validateApiKey).mockResolvedValue(null);

    const { handler } = await import('./handler');
    const res = await (handler as Function)(cardEvent({ headers: {} }));

    expect(res.statusCode).toBe(401);
  });

  it('returns 404 when user not found', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    mockSend.mockResolvedValueOnce({ Items: [] }); // SCAN# → empty

    const { handler } = await import('./handler');
    const res = await (handler as Function)(cardEvent());

    expect(res.statusCode).toBe(404);
    expect(JSON.parse(res.body).error).toContain('User not found');
  });

  it('returns 200 with card state', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    mockSend.mockResolvedValueOnce({ Items: [{ sK: 'PERM001' }] }); // SCAN# → permULID
    // stamp card exists
    mockSend.mockResolvedValueOnce({
      Item: {
        status: 'ACTIVE',
        desc: JSON.stringify({ stamps: 3, goal: 10, status: 'ACTIVE', rewardDescription: 'Free burger', redemptions: 1 }),
      },
    });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(cardEvent());

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.stamps).toBe(3);
    expect(body.goal).toBe(10);
    expect(body.rewardDescription).toBe('Free burger');
    expect(body.redemptions).toBe(1);
  });
});

// ── 4. GET /smb/analytics ─────────────────────────────────────────────────────

describe('GET /smb/analytics', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.ADMIN_TABLE = 'AdminDataEvent';
    process.env.USER_TABLE  = 'UserDataEvent';
    process.env.REF_TABLE   = 'RefDataEvent';
  });

  const analyticsEvent = (overrides: Record<string, unknown> = {}) =>
    makeEvent({
      path:       '/smb/analytics',
      httpMethod: 'GET',
      body:       null,
      ...overrides,
    });

  it('returns 401 with no API key', async () => {
    vi.mocked(extractApiKey).mockReturnValue(null);
    vi.mocked(validateApiKey).mockResolvedValue(null);

    const { handler } = await import('./handler');
    const res = await (handler as Function)(analyticsEvent({ headers: {} }));

    expect(res.statusCode).toBe(401);
  });

  it('returns 200 with analytics data', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    // getSmbConfig → valid config
    mockSend.mockResolvedValueOnce({ Item: { desc: VALID_CONFIG_DESC } });
    // REDEEM# QueryCommand → Count = 3
    mockSend.mockResolvedValueOnce({ Count: 3 });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(analyticsEvent());

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.totalRedemptions).toBe(3);
    expect(body.tier).toBe('starter');
    expect(body.quota).toBe(500);
    expect(typeof body.totalStamps).toBe('number');
  });
});
