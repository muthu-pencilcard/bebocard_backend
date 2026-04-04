/**
 * Tests for enrollment-handler — Phase 9 (Enrollment Marketplace)
 *
 * Tests are grouped into three areas:
 *   1. generateAlias           — determinism and format
 *   2. POST /enroll            — auth, validation, happy path, duplicate
 *   3. GET /enroll/{id}/status — auth, not found, wrong brand, pending/accepted
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { generateAlias } from './handler';

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
  extractApiKey: vi.fn(),
  validateApiKey: vi.fn(),
}));

vi.mock('../../shared/audit-logger', () => ({
  withAuditLog: (_ddb: unknown, fn: unknown) => fn,
}));

// ── Import mocked modules (vi.mock is hoisted so these resolve to mocked versions) ──
import { extractApiKey, validateApiKey } from '../../shared/api-key-auth';

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeEvent(overrides: Record<string, unknown> = {}) {
  return {
    path:       '/enroll',
    httpMethod: 'POST',
    headers:    { 'x-api-key': 'bebo_test.key' },
    body:       JSON.stringify({
      secondaryULID: 'SEC123',
      programName:   'Woolies Rewards',
    }),
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

const VALID_KEY = { brandId: 'woolworths', keyId: 'key1', rateLimit: 1000, scopes: ['enrollment'] };

// ── 1. generateAlias ──────────────────────────────────────────────────────────

describe('generateAlias', () => {
  it('produces a valid relay email format', () => {
    const alias = generateAlias('PERM001', 'woolworths');
    expect(alias).toMatch(/^bebo_[0-9a-f]{16}@relay\.bebocard\.com$/);
  });

  it('is deterministic — same inputs produce same alias', () => {
    const a1 = generateAlias('PERM001', 'woolworths');
    const a2 = generateAlias('PERM001', 'woolworths');
    expect(a1).toBe(a2);
  });

  it('is unique per user — different permULIDs produce different aliases', () => {
    const a1 = generateAlias('PERM001', 'woolworths');
    const a2 = generateAlias('PERM002', 'woolworths');
    expect(a1).not.toBe(a2);
  });

  it('is unique per brand — different brandIds produce different aliases', () => {
    const a1 = generateAlias('PERM001', 'woolworths');
    const a2 = generateAlias('PERM001', 'bigw');
    expect(a1).not.toBe(a2);
  });

  it('alias is 16 hex chars long (first 16 of sha256)', () => {
    const alias = generateAlias('PERM001', 'woolworths');
    const localPart = alias.split('@')[0].replace('bebo_', '');
    expect(localPart).toHaveLength(16);
    expect(localPart).toMatch(/^[0-9a-f]+$/);
  });
});

// ── 2. POST /enroll ───────────────────────────────────────────────────────────

describe('POST /enroll', () => {
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
    const res = await (handler as Function)(makeEvent({
      body: JSON.stringify({ programName: 'Woolies Rewards' }),
    }));

    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body).error).toContain('secondaryULID');
  });

  it('returns 400 when programName is missing', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent({
      body: JSON.stringify({ secondaryULID: 'SEC123' }),
    }));

    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body).error).toContain('programName');
  });

  it('returns 404 when secondaryULID cannot be resolved', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    // QueryCommand for SCAN# returns empty — user not found
    mockSend.mockResolvedValueOnce({ Items: [] });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent());

    expect(res.statusCode).toBe(404);
    expect(JSON.parse(res.body).error).toContain('User not found');
  });

  it('returns 201 with enrollmentId on happy path', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    // SCAN# lookup → permULID
    mockSend.mockResolvedValueOnce({ Items: [{ sK: 'PERM001' }] });
    // findExistingEnrollment (USER# query) → no existing
    mockSend.mockResolvedValueOnce({ Items: [] });
    // PutCommand for ENROLL# in AdminDataEvent
    mockSend.mockResolvedValueOnce({});
    // getDeviceToken → no token (skip FCM)
    mockSend.mockResolvedValueOnce({ Item: null });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent());

    expect(res.statusCode).toBe(201);
    const body = JSON.parse(res.body);
    expect(body.status).toBe('PENDING');
    expect(body.enrollmentId).toBeTruthy();
    expect(typeof body.enrollmentId).toBe('string');
  });

  it('returns 409 when user is already enrolled (ACCEPTED)', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    // SCAN# → permULID
    mockSend.mockResolvedValueOnce({ Items: [{ sK: 'PERM001' }] });
    // findExistingEnrollment → already ACCEPTED
    mockSend.mockResolvedValueOnce({ Items: [{ status: 'ACCEPTED' }] });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent());

    expect(res.statusCode).toBe(409);
    expect(JSON.parse(res.body).error).toContain('already enrolled');
  });

  it('returns 404 for unknown routes', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);

    const { handler } = await import('./handler');
    const res = await (handler as Function)(makeEvent({ path: '/unknown', httpMethod: 'DELETE' }));

    expect(res.statusCode).toBe(404);
  });
});

// ── 3. GET /enroll/{id}/status ────────────────────────────────────────────────

describe('GET /enroll/{enrollmentId}/status', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.ADMIN_TABLE = 'AdminDataEvent';
    process.env.USER_TABLE  = 'UserDataEvent';
    process.env.REF_TABLE   = 'RefDataEvent';
  });

  const statusEvent = (enrollmentId = 'ENROLL01') => makeEvent({
    path:       `/enroll/${enrollmentId}/status`,
    httpMethod: 'GET',
    body:       null,
  });

  it('returns 401 with no API key', async () => {
    vi.mocked(extractApiKey).mockReturnValue(null);
    vi.mocked(validateApiKey).mockResolvedValue(null);

    const { handler } = await import('./handler');
    const res = await (handler as Function)(statusEvent());

    expect(res.statusCode).toBe(401);
  });

  it('returns 404 when enrollment does not exist', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    mockSend.mockResolvedValueOnce({ Items: [] });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(statusEvent('MISSING'));

    expect(res.statusCode).toBe(404);
  });

  it('returns 403 when enrollment belongs to a different brand', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    mockSend.mockResolvedValueOnce({
      Items: [{ pK: 'ENROLL#ENROLL01', sK: 'PERM001', brandId: 'bigw', status: 'PENDING' }],
    });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(statusEvent());

    expect(res.statusCode).toBe(403);
  });

  it('returns 200 with status PENDING and no alias', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    mockSend.mockResolvedValueOnce({
      Items: [{
        pK: 'ENROLL#ENROLL01', sK: 'PERM001',
        brandId: 'woolworths', status: 'PENDING',
        createdAt: '2026-03-30T10:00:00Z', updatedAt: '2026-03-30T10:00:00Z',
      }],
    });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(statusEvent());

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.status).toBe('PENDING');
    expect(body.alias).toBeUndefined();
  });

  it('returns 200 with alias when enrollment is ACCEPTED', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    mockSend.mockResolvedValueOnce({
      Items: [{
        pK: 'ENROLL#ENROLL01', sK: 'PERM001',
        brandId: 'woolworths', status: 'ACCEPTED',
        alias: 'bebo_abc123def456abcd@relay.bebocard.com',
        createdAt: '2026-03-30T10:00:00Z', updatedAt: '2026-03-30T10:05:00Z',
      }],
    });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(statusEvent());

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.status).toBe('ACCEPTED');
    expect(body.alias).toBe('bebo_abc123def456abcd@relay.bebocard.com');
  });

  it('does not expose alias when enrollment is DECLINED', async () => {
    vi.mocked(extractApiKey).mockReturnValue('bebo_valid.key');
    vi.mocked(validateApiKey).mockResolvedValue(VALID_KEY as never);
    mockSend.mockResolvedValueOnce({
      Items: [{
        pK: 'ENROLL#ENROLL01', sK: 'PERM001',
        brandId: 'woolworths', status: 'DECLINED',
        alias: 'bebo_abc123def456abcd@relay.bebocard.com',
        createdAt: '2026-03-30T10:00:00Z', updatedAt: '2026-03-30T10:02:00Z',
      }],
    });

    const { handler } = await import('./handler');
    const res = await (handler as Function)(statusEvent());

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.status).toBe('DECLINED');
    expect(body.alias).toBeUndefined();
  });
});
