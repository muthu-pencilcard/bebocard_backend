import { describe, it, expect, vi, beforeEach } from 'vitest';

// ── Hoisted mocks ─────────────────────────────────────────────────────────────

const { mockCognitoSend, mockS3Send } = vi.hoisted(() => {
  // Set module-level env vars here — these are read as constants at handler import time
  process.env.USER_POOL_ID = 'ap-southeast-2_TestPool';
  process.env.EXPORT_BUCKET = 'bebocard-cognito-exports-test';
  return {
    mockCognitoSend: vi.fn(),
    mockS3Send: vi.fn().mockResolvedValue({}),
  };
});

// ── Module mocks ──────────────────────────────────────────────────────────────

vi.mock('@aws-sdk/client-cognito-identity-provider', () => ({
  CognitoIdentityProviderClient: vi.fn(function (this: Record<string, unknown>) {
    this.send = mockCognitoSend;
  }),
  ListUsersCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'ListUsersCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-s3', () => ({
  S3Client: vi.fn(function (this: Record<string, unknown>) { this.send = mockS3Send; }),
  // Spread input props directly onto instance so they're accessible without .input wrapper
  PutObjectCommand: vi.fn(function (this: Record<string, unknown>, input: Record<string, unknown>) {
    this.__type = 'PutObjectCommand';
    Object.assign(this, input);
  }),
}));

// ── Handler import ────────────────────────────────────────────────────────────

import { handler } from './handler.js';

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeCognitoUser(username: string, attributes: Record<string, string> = {}) {
  return {
    Username: username,
    UserStatus: 'CONFIRMED',
    Enabled: true,
    UserCreateDate: new Date('2026-01-01T00:00:00Z'),
    UserLastModifiedDate: new Date('2026-04-01T00:00:00Z'),
    Attributes: [
      { Name: 'email', Value: `${username}@test.com` },
      { Name: 'custom:permULID', Value: `PERM${username.toUpperCase()}` },
      ...Object.entries(attributes).map(([Name, Value]) => ({ Name, Value })),
    ],
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// cognito-export
// ─────────────────────────────────────────────────────────────────────────────

describe('cognito-export handler', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockS3Send.mockResolvedValue({});
  });

  it('exports all users from a single page and uploads to S3 with correct key', async () => {
    const users = [makeCognitoUser('alice'), makeCognitoUser('bob')];
    mockCognitoSend.mockResolvedValueOnce({ Users: users, PaginationToken: undefined });
    mockS3Send.mockResolvedValue({});

    const result = await handler({} as never, {} as never, {} as never);

    expect(result).toMatchObject({ userCount: 2 });
    expect((result as { s3Key: string }).s3Key).toMatch(/^pool-exports\/\d{4}-\d{2}-\d{2}\/users\.json$/);

    // Verify S3 PutObject was called once
    expect(mockS3Send).toHaveBeenCalledTimes(1);
    // Props are spread directly on the mock instance (Bucket, Key, Body, etc.)
    const putCmd = mockS3Send.mock.calls[0][0] as Record<string, unknown>;
    const payload = JSON.parse(putCmd['Body'] as string);
    expect(payload.userCount).toBe(2);
    expect(payload.userPoolId).toBe('ap-southeast-2_TestPool');
    expect(payload.users).toHaveLength(2);
  });

  it('paginates through all users when multiple pages are returned', async () => {
    mockCognitoSend
      .mockResolvedValueOnce({ Users: [makeCognitoUser('alice')], PaginationToken: 'page2token' })
      .mockResolvedValueOnce({ Users: [makeCognitoUser('bob'), makeCognitoUser('carol')], PaginationToken: undefined });

    const result = await handler({} as never, {} as never, {} as never);

    expect((result as { userCount: number }).userCount).toBe(3);
    expect(mockCognitoSend).toHaveBeenCalledTimes(2);
    // Second call must include the pagination token
    const secondCallArgs = mockCognitoSend.mock.calls[1][0] as {
      input: { PaginationToken: string };
    };
    expect(secondCallArgs.input.PaginationToken).toBe('page2token');
  });

  it('exports user attributes including custom:permULID', async () => {
    mockCognitoSend.mockResolvedValueOnce({
      Users: [makeCognitoUser('dave', { 'custom:userId': 'USER001' })],
      PaginationToken: undefined,
    });

    await handler({} as never, {} as never, {} as never);

    const putCmd = mockS3Send.mock.calls[0][0] as Record<string, unknown>;
    const payload = JSON.parse(putCmd['Body'] as string);
    const user = payload.users[0];
    expect(user.Attributes['custom:permULID']).toBe('PERMDAVE');
    expect(user.Attributes['custom:userId']).toBe('USER001');
    expect(user.Username).toBe('dave');
    expect(user.UserStatus).toBe('CONFIRMED');
    expect(user.Enabled).toBe(true);
  });

  it('handles empty user pool gracefully (zero users)', async () => {
    mockCognitoSend.mockResolvedValueOnce({ Users: [], PaginationToken: undefined });

    const result = await handler({} as never, {} as never, {} as never);

    expect((result as { userCount: number }).userCount).toBe(0);
    const putCmd = mockS3Send.mock.calls[0][0] as Record<string, unknown>;
    const payload = JSON.parse(putCmd['Body'] as string);
    expect(payload.users).toHaveLength(0);
  });

  it('includes exportedAt timestamp in the S3 payload', async () => {
    mockCognitoSend.mockResolvedValueOnce({ Users: [makeCognitoUser('eve')], PaginationToken: undefined });

    await handler({} as never, {} as never, {} as never);

    const putCmd = mockS3Send.mock.calls[0][0] as Record<string, unknown>;
    const payload = JSON.parse(putCmd['Body'] as string);
    expect(payload.exportedAt).toBeDefined();
    expect(new Date(payload.exportedAt as string).getTime()).toBeGreaterThan(0);
  });

  it('returns s3Key, userCount, and exportedAt from the handler', async () => {
    mockCognitoSend.mockResolvedValueOnce({ Users: [makeCognitoUser('frank')], PaginationToken: undefined });

    const result = await handler({} as never, {} as never, {} as never) as Record<string, unknown>;

    expect(result).toHaveProperty('s3Key');
    expect(result).toHaveProperty('userCount', 1);
    expect(result).toHaveProperty('exportedAt');
  });
});
