/**
 * tenant-analytics handler tests
 *
 * ESM-compatible mock patterns:
 * - All shared mock fns are hoisted via vi.hoisted()
 * - All constructor mocks use regular `function` (not arrow functions)
 * - Command mocks store { __type, input } on `this`
 * - Handler is imported AFTER all vi.mock() calls
 *
 * validateTenantApiKey() is internal — tested indirectly by driving DDB mock
 * responses that simulate the byKeyId GSI query.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createHash } from 'crypto';
import type { APIGatewayProxyEvent } from 'aws-lambda';

// ─── Hoisted shared mocks ─────────────────────────────────────────────────────

const { mockSend, mockExtractApiKey } = vi.hoisted(() => ({
  mockSend:          vi.fn(),
  mockExtractApiKey: vi.fn(),
}));

// ─── Module mocks (must precede handler import) ───────────────────────────────

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: object) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: {
    from: vi.fn(() => ({ send: mockSend })),
  },
  GetCommand: vi.fn(function (this: { __type: string; input: unknown }, input: unknown) {
    this.__type = 'GetCommand';
    this.input = input;
  }),
  QueryCommand: vi.fn(function (this: { __type: string; input: unknown }, input: unknown) {
    this.__type = 'QueryCommand';
    this.input = input;
  }),
  PutCommand: vi.fn(function (this: { __type: string; input: unknown }, input: unknown) {
    this.__type = 'PutCommand';
    this.input = input;
  }),
}));

vi.mock('../../shared/audit-logger', () => ({
  withAuditLog: vi.fn((_ddb: unknown, h: unknown) => h),
}));

vi.mock('../../shared/api-key-auth', () => ({
  extractApiKey: mockExtractApiKey,
}));

vi.mock('ulid', () => ({
  monotonicFactory: () => {
    let c = 0;
    return () => `ULID${c++}`;
  },
}));

// ─── Handler import (after all vi.mock calls) ─────────────────────────────────

import { handler } from './handler.js';

// ─── Test constants ───────────────────────────────────────────────────────────

const RAW_KEY = 'bebo_TESTID000000000000000000.secret123';

// Pre-computed SHA256 of RAW_KEY — used to build tenant items
const VALID_KEY_HASH = createHash('sha256').update(RAW_KEY).digest('hex');

// ─── Helper: build a valid tenant DDB item ────────────────────────────────────

function makeTenantItem(rawKey: string, overrides: Record<string, unknown> = {}) {
  const withoutPrefix = rawKey.slice(5);                          // strip 'bebo_'
  const keyId = withoutPrefix.slice(0, withoutPrefix.indexOf('.'));
  const keyHash = createHash('sha256').update(rawKey).digest('hex');

  return {
    pK: 'TENANT#wg',
    sK: `APIKEY#${keyId}`,
    status: 'ACTIVE',
    keyId,
    brandIds: ['woolworths', 'bigw'],
    allowedScopes: ['segments', 'receipts_aggregate'],
    minCohortThreshold: 2,
    desc: JSON.stringify({
      keyHash,
      brandIds: ['woolworths', 'bigw'],
      allowedScopes: ['segments', 'receipts_aggregate'],
      minCohortThreshold: 2,
    }),
    ...overrides,
  };
}

// ─── Helper: APIGateway event ─────────────────────────────────────────────────

function makeEvent(overrides: Partial<APIGatewayProxyEvent> = {}): APIGatewayProxyEvent {
  return {
    path: '/v1/analytics/segments',
    httpMethod: 'GET',
    headers: { 'x-api-key': RAW_KEY },
    queryStringParameters: { brandId: 'woolworths', period: '2026-03' },
    body: null,
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

// ─── Helper: segment item ─────────────────────────────────────────────────────

function makeSegmentItem(overrides: Record<string, unknown> = {}) {
  return {
    pK: 'USER#PERM001',
    sK: 'SEGMENT#woolworths',
    desc: JSON.stringify({
      spendBucket: '100-200',
      visitFrequency: 'frequent',
      subscribed: true,
      totalSpend: 150,
      visitCount: 5,
      lastVisit: '2026-03-01',
      persona: ['high_value'],
      computedAt: '2026-03-01T00:00:00Z',
      ...overrides,
    }),
  };
}

/**
 * Sets up mockSend so that:
 *   - First call (byKeyId QueryCommand → REFDATA) returns the tenant item
 *   - Subsequent calls (sK-pK-index QueryCommand → USER_TABLE) return segmentItems
 */
function setupMocks(
  segmentItems: unknown[],
  tenantOverrides: Record<string, unknown> = {},
) {
  let call = 0;
  mockSend.mockImplementation(() => {
    const n = call++;
    if (n === 0) {
      // validateTenantApiKey → byKeyId GSI on REFDATA_TABLE
      return Promise.resolve({
        Items: [makeTenantItem(RAW_KEY, tenantOverrides)],
      });
    }
    // handleSegments → sK-pK-index on USER_TABLE (no pagination)
    return Promise.resolve({
      Items: segmentItems,
      LastEvaluatedKey: undefined,
    });
  });
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('tenant-analytics handler', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.USER_TABLE            = 'user-table';
    process.env.REFDATA_TABLE         = 'ref-table';
    process.env.MIN_COHORT_THRESHOLD  = '2';
    mockExtractApiKey.mockReturnValue(RAW_KEY);
  });

  // 1 ── missing API key ─────────────────────────────────────────────────────────
  it('returns 401 when extractApiKey returns null', async () => {
    mockExtractApiKey.mockReturnValue(null);
    const res = await handler(makeEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 401 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.error).toMatch(/missing api key/i);
  });

  // 2 ── invalid tenant key (no DDB item) ───────────────────────────────────────
  it('returns 401 when the byKeyId GSI returns no items', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const res = await handler(makeEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 401 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.error).toMatch(/invalid api key/i);
  });

  // 3 ── brandId not in tenant's allowlist ──────────────────────────────────────
  it('returns 403 when requested brandId is not in tenant brandIds', async () => {
    setupMocks([]);
    const res = await handler(
      makeEvent({ queryStringParameters: { brandId: 'coles', period: '2026-03' } }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 403 });
  });

  // 4 ── 'segments' scope not in tenant's allowedScopes ─────────────────────────
  it('returns 403 when "segments" is not in tenant allowedScopes', async () => {
    let call = 0;
    mockSend.mockImplementation(() => {
      const n = call++;
      if (n === 0) {
        return Promise.resolve({
          Items: [makeTenantItem(RAW_KEY, {
            desc: JSON.stringify({
              keyHash: VALID_KEY_HASH,
              brandIds: ['woolworths'],
              allowedScopes: ['receipts_aggregate'], // no 'segments'
              minCohortThreshold: 2,
            }),
          })],
        });
      }
      return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
    });

    const res = await handler(makeEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 403 });
  });

  // 5 ── unknown route ───────────────────────────────────────────────────────────
  it('returns 404 for an unknown route', async () => {
    setupMocks([]);
    const res = await handler(
      makeEvent({ path: '/v1/analytics/unknown' }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 404 });
  });

  // 6 ── suppressed: subscriberCount below threshold ────────────────────────────
  it('returns 200 with zeroed distributions when subscriberCount < minCohortThreshold', async () => {
    // threshold = 2, only 1 subscribed user → suppressed
    setupMocks([makeSegmentItem()]);

    const res = await handler(makeEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 200 });

    const body = JSON.parse((res as { body: string }).body);
    expect(body.subscriberCount).toBe(0);
    expect(body.spendDistribution).toEqual({ '<100': 0, '100-200': 0, '200-500': 0, '500+': 0 });
    expect(body.visitFrequency).toEqual({ new: 0, occasional: 0, frequent: 0, lapsed: 0 });
  });

  // 7 ── above threshold: correct proportions ────────────────────────────────────
  it('returns 200 with correct proportions when subscriberCount >= minCohortThreshold', async () => {
    // 2 subscribed users: both '100-200' spend, both 'frequent' visit → 100%
    setupMocks([
      makeSegmentItem({ spendBucket: '100-200', visitFrequency: 'frequent' }),
      makeSegmentItem({ pK: 'USER#PERM002', spendBucket: '100-200', visitFrequency: 'frequent' }),
    ]);

    const res = await handler(makeEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 200 });

    const body = JSON.parse((res as { body: string }).body);
    expect(body.subscriberCount).toBe(2);
    expect(body.spendDistribution['100-200']).toBe(1);
    expect(body.visitFrequency['frequent']).toBe(1);
    expect(body.brandId).toBe('woolworths');
    expect(body.period).toBe('2026-03');
  });

  // 8 ── non-subscribed users are excluded from distributions ───────────────────
  it('filters out items where subscribed=false from distributions', async () => {
    // 2 items in DDB; one subscribed=false → only 1 counted → suppressed (< threshold 2)
    setupMocks([
      makeSegmentItem({ subscribed: true }),
      makeSegmentItem({ pK: 'USER#PERM002', subscribed: false }),
    ]);

    const res = await handler(makeEvent(), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    // Only 1 subscribed → below threshold of 2 → suppressed
    expect(body.subscriberCount).toBe(0);
  });

  // 9 ── pagination ──────────────────────────────────────────────────────────────
  it('follows pagination (LastEvaluatedKey) to collect all segment items', async () => {
    let call = 0;
    mockSend.mockImplementation(() => {
      const n = call++;
      if (n === 0) {
        // validateTenantApiKey
        return Promise.resolve({ Items: [makeTenantItem(RAW_KEY)] });
      }
      if (n === 1) {
        // First page — contains 1 subscribed item, signals more pages
        return Promise.resolve({
          Items: [makeSegmentItem()],
          LastEvaluatedKey: { sK: 'SEGMENT#woolworths', pK: 'USER#PERM001' },
        });
      }
      // Second (final) page — 1 more subscribed item, no more pages
      return Promise.resolve({
        Items: [makeSegmentItem({ pK: 'USER#PERM002' })],
        LastEvaluatedKey: undefined,
      });
    });

    const res = await handler(makeEvent(), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    // 2 subscribed users >= threshold 2 → not suppressed
    expect(body.subscriberCount).toBe(2);
    expect(res).toMatchObject({ statusCode: 200 });
    // QueryCommand for segments should have been called twice (2 pages)
    const queryCalls = (mockSend.mock.calls as Array<[{ __type: string; input: Record<string, unknown> }]>).filter(
      ([cmd]) => cmd.__type === 'QueryCommand',
    );
    expect(queryCalls.length).toBeGreaterThanOrEqual(2);
  });

  // 10 ── missing brandId query param ───────────────────────────────────────────
  it('returns 400 when brandId query param is absent', async () => {
    setupMocks([]);
    const res = await handler(
      makeEvent({ queryStringParameters: { period: '2026-03' } }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.error).toMatch(/brandId/i);
  });

  // 11 ── tenant key with REVOKED status ────────────────────────────────────────
  it('returns 401 when tenant key has status REVOKED', async () => {
    mockSend.mockResolvedValue({
      Items: [makeTenantItem(RAW_KEY, { status: 'REVOKED' })],
    });

    const res = await handler(makeEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 401 });
  });

  // 12 ── keyHash mismatch in tenant desc ───────────────────────────────────────
  it('returns 401 when desc.keyHash does not match SHA256 of rawKey', async () => {
    mockSend.mockResolvedValue({
      Items: [makeTenantItem(RAW_KEY, {
        desc: JSON.stringify({
          keyHash: 'a'.repeat(64), // wrong hash
          brandIds: ['woolworths'],
          allowedScopes: ['segments'],
          minCohortThreshold: 2,
        }),
      })],
    });

    const res = await handler(makeEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 401 });
  });

  // 13 ── pK must be TENANT# prefix ──────────────────────────────────────────────
  it('returns 401 when resolved key record has a BRAND# pK instead of TENANT#', async () => {
    mockSend.mockResolvedValue({
      Items: [makeTenantItem(RAW_KEY, { pK: 'BRAND#woolworths' })],
    });

    const res = await handler(makeEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 401 });
  });

  // 14 ── response shape for successful request ──────────────────────────────────
  it('response body includes brandId, period, subscriberCount, spendDistribution, visitFrequency', async () => {
    setupMocks([
      makeSegmentItem({ spendBucket: '<100', visitFrequency: 'new' }),
      makeSegmentItem({ pK: 'USER#PERM002', spendBucket: '500+', visitFrequency: 'lapsed' }),
    ]);

    const res = await handler(makeEvent(), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);

    expect(body).toHaveProperty('brandId', 'woolworths');
    expect(body).toHaveProperty('period', '2026-03');
    expect(body).toHaveProperty('subscriberCount');
    expect(body).toHaveProperty('spendDistribution');
    expect(body).toHaveProperty('visitFrequency');
  });

  // 15 ── rawKey without bebo_ prefix ───────────────────────────────────────────
  it('returns 401 when rawKey does not start with bebo_', async () => {
    mockExtractApiKey.mockReturnValue('noprefix.secret123');

    const res = await handler(makeEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 401 });
  });

  // 16 ── rawKey without dot separator ──────────────────────────────────────────
  it('returns 401 when rawKey has no dot separator (invalid format)', async () => {
    mockExtractApiKey.mockReturnValue('bebo_NODOTKEY');

    const res = await handler(makeEvent(), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 401 });
  });
});

// ── Tests: GET /analytics/subscriber-count ────────────────────────────────────

describe('GET /analytics/subscriber-count', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.USER_TABLE           = 'user-table';
    process.env.REFDATA_TABLE        = 'ref-table';
    process.env.MIN_COHORT_THRESHOLD = '2';
    mockExtractApiKey.mockReturnValue(RAW_KEY);
  });

  // 1 ── scope not permitted ────────────────────────────────────────────────────
  it('returns 403 when subscriber_count is not in tenant allowedScopes', async () => {
    let call = 0;
    mockSend.mockImplementation(() => {
      const n = call++;
      if (n === 0) {
        return Promise.resolve({
          Items: [makeTenantItem(RAW_KEY, {
            desc: JSON.stringify({
              keyHash: VALID_KEY_HASH,
              brandIds: ['woolworths'],
              allowedScopes: ['segments'],   // no subscriber_count
              minCohortThreshold: 2,
            }),
          })],
        });
      }
      return Promise.resolve({ Count: 0 });
    });

    const res = await handler(
      makeEvent({ path: '/v1/analytics/subscriber-count', queryStringParameters: { brandId: 'woolworths' } }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 403 });
  });

  // 2 ── brandId not in allowlist ───────────────────────────────────────────────
  it('returns 403 when brandId is not in tenant brandIds', async () => {
    setupMocksSubscriberCount(99);
    const res = await handler(
      makeEvent({ path: '/v1/analytics/subscriber-count', queryStringParameters: { brandId: 'not-allowed' } }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 403 });
  });

  // 3 ── brandId missing ────────────────────────────────────────────────────────
  it('returns 400 when brandId query param is absent', async () => {
    setupMocksSubscriberCount(0);
    const res = await handler(
      makeEvent({ path: '/v1/analytics/subscriber-count', queryStringParameters: null }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.error).toMatch(/brandId/i);
  });

  // 4 ── success ────────────────────────────────────────────────────────────────
  it('returns 200 with subscriberCount from COUNT response', async () => {
    setupMocksSubscriberCount(42);
    const res = await handler(
      makeEvent({ path: '/v1/analytics/subscriber-count', queryStringParameters: { brandId: 'woolworths', period: '2026-03' } }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 200 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.subscriberCount).toBe(42);
    expect(body.brandId).toBe('woolworths');
    expect(body.period).toBe('2026-03');
  });
});

// ── Shared helper for subscriber-count tests ──────────────────────────────────

function setupMocksSubscriberCount(count: number) {
  let call = 0;
  mockSend.mockImplementation(() => {
    const n = call++;
    if (n === 0) {
      return Promise.resolve({
        Items: [makeTenantItem(RAW_KEY, {
          desc: JSON.stringify({
            keyHash: VALID_KEY_HASH,
            brandIds: ['woolworths', 'bigw'],
            allowedScopes: ['segments', 'subscriber_count'],
            minCohortThreshold: 2,
          }),
        })],
      });
    }
    return Promise.resolve({ Count: count, LastEvaluatedKey: undefined });
  });
}
