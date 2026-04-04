import { createHash } from 'crypto';
import { vi, describe, it, expect, beforeEach } from 'vitest';

const { mockSend } = vi.hoisted(() => ({ mockSend: vi.fn() }));

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

vi.mock('ulid', () => ({
  monotonicFactory: () => {
    let c = 0;
    return () => `ULID${String(c++).padStart(26, '0')}`;
  },
}));

import { generateApiKey, extractApiKey, validateApiKey } from './api-key-auth.js';
import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

const fakeDdb = { send: mockSend } as unknown as DynamoDBDocumentClient;

// ── generateApiKey ────────────────────────────────────────────────────────────

describe('generateApiKey', () => {
  it("returns a key starting with 'bebo_'", () => {
    const { rawKey } = generateApiKey();
    expect(rawKey.startsWith('bebo_')).toBe(true);
  });

  it('format is bebo_<keyId>.<secret>', () => {
    const { rawKey, keyId } = generateApiKey();
    const withoutPrefix = rawKey.slice(5);
    const dotIdx = withoutPrefix.indexOf('.');
    expect(dotIdx).toBeGreaterThan(0);
    expect(withoutPrefix.slice(0, dotIdx)).toBe(keyId);
    expect(withoutPrefix.slice(dotIdx + 1).length).toBeGreaterThan(0);
  });

  it('keyHash is the SHA-256 of rawKey', () => {
    const { rawKey, keyHash } = generateApiKey();
    const expected = createHash('sha256').update(rawKey).digest('hex');
    expect(keyHash).toBe(expected);
  });

  it('produces unique keys on each call', () => {
    const k1 = generateApiKey();
    const k2 = generateApiKey();
    expect(k1.rawKey).not.toBe(k2.rawKey);
    expect(k1.keyId).not.toBe(k2.keyId);
    expect(k1.keyHash).not.toBe(k2.keyHash);
  });
});

// ── extractApiKey ─────────────────────────────────────────────────────────────

describe('extractApiKey', () => {
  it('reads from x-api-key header (lowercase)', () => {
    expect(extractApiKey({ 'x-api-key': 'bebo_testkey123' })).toBe('bebo_testkey123');
  });

  it('reads from X-Api-Key header (mixed case)', () => {
    expect(extractApiKey({ 'X-Api-Key': 'bebo_testkey456' })).toBe('bebo_testkey456');
  });

  it('reads from Authorization: Bearer bebo_... header', () => {
    expect(extractApiKey({ authorization: 'Bearer bebo_abcdef.xyz' })).toBe('bebo_abcdef.xyz');
  });

  it('returns null if no key header present', () => {
    expect(extractApiKey({ 'content-type': 'application/json' })).toBeNull();
  });

  it('returns null for non-bebo_ Bearer tokens', () => {
    expect(extractApiKey({ authorization: 'Bearer sk_live_notbebo' })).toBeNull();
  });
});

// ── validateApiKey ────────────────────────────────────────────────────────────

describe('validateApiKey', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.REFDATA_TABLE = 'test-ref-table';
  });

  function makeItem(keyId: string, keyHash: string, overrides: Record<string, unknown> = {}) {
    return {
      pK: 'BRAND#woolworths',
      sK: `APIKEY#${keyId}`,
      brandId: 'woolworths',
      keyId,
      keyHash,
      scopes: ['scan', 'receipt'],
      rateLimit: 1000,
      status: 'ACTIVE',
      createdAt: new Date().toISOString(),
      createdBy: 'admin',
      ...overrides,
    };
  }

  it('returns null for keys not starting with bebo_', async () => {
    const result = await validateApiKey(fakeDdb, 'invalid-key', 'scan');
    expect(result).toBeNull();
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('returns null if key has no dot separator', async () => {
    const result = await validateApiKey(fakeDdb, 'bebo_nodot', 'scan');
    expect(result).toBeNull();
  });

  it('returns null if DynamoDB returns no item', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const result = await validateApiKey(fakeDdb, generateApiKey().rawKey, 'scan');
    expect(result).toBeNull();
  });

  it('returns null if key status is REVOKED', async () => {
    const { rawKey, keyId, keyHash } = generateApiKey();
    mockSend.mockResolvedValue({ Items: [makeItem(keyId, keyHash, { status: 'REVOKED' })] });
    expect(await validateApiKey(fakeDdb, rawKey, 'scan')).toBeNull();
  });

  it('returns null if key is GRACE and graceUntil has expired', async () => {
    const { rawKey, keyId, keyHash } = generateApiKey();
    const expired = new Date(Date.now() - 60_000).toISOString();
    mockSend.mockResolvedValue({
      Items: [makeItem(keyId, keyHash, { status: 'GRACE', graceUntil: expired })],
    });
    expect(await validateApiKey(fakeDdb, rawKey, 'scan')).toBeNull();
  });

  it('returns ValidatedKey if key is GRACE and graceUntil is in the future', async () => {
    const { rawKey, keyId, keyHash } = generateApiKey();
    const future = new Date(Date.now() + 3_600_000).toISOString();
    mockSend.mockResolvedValue({
      Items: [makeItem(keyId, keyHash, { status: 'GRACE', graceUntil: future })],
    });
    const result = await validateApiKey(fakeDdb, rawKey, 'scan');
    expect(result).not.toBeNull();
    expect(result!.brandId).toBe('woolworths');
  });

  it('returns null if hash does not match', async () => {
    const { keyId } = generateApiKey();
    const { keyHash: wrongHash } = generateApiKey(); // different key's hash
    mockSend.mockResolvedValue({ Items: [makeItem(keyId, wrongHash)] });
    // Use the correct keyId but wrong secret — hash won't match
    const result = await validateApiKey(fakeDdb, `bebo_${keyId}.wrongsecret`, 'scan');
    expect(result).toBeNull();
  });

  it('returns null if required scope is not in key scopes', async () => {
    const { rawKey, keyId, keyHash } = generateApiKey();
    mockSend.mockResolvedValue({
      Items: [makeItem(keyId, keyHash, { scopes: ['receipt'] })], // no 'scan'
    });
    expect(await validateApiKey(fakeDdb, rawKey, 'scan')).toBeNull();
  });

  it('returns ValidatedKey with correct brandId and scopes on success', async () => {
    const { rawKey, keyId, keyHash } = generateApiKey();
    mockSend.mockResolvedValue({
      Items: [makeItem(keyId, keyHash, { scopes: ['scan', 'receipt', 'offers'], rateLimit: 500 })],
    });
    const result = await validateApiKey(fakeDdb, rawKey, 'scan');
    expect(result).not.toBeNull();
    expect(result!.brandId).toBe('woolworths');
    expect(result!.keyId).toBe(keyId);
    expect(result!.rateLimit).toBe(500);
    expect(result!.scopes).toContain('scan');
    expect(result!.scopes).toContain('offers');
  });
});
