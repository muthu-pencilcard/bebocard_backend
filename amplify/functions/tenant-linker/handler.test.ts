import { vi, describe, it, expect, beforeEach } from 'vitest';

// ── Mocks ─────────────────────────────────────────────────────────────────────

const { mockDynSend, mockCognitoSend, mockFetch } = vi.hoisted(() => ({
  mockDynSend: vi.fn(),
  mockCognitoSend: vi.fn(),
  mockFetch: vi.fn(),
}));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockDynSend }) },
  GetCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetCommand', input });
  }),
  PutCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutCommand', input });
  }),
  UpdateCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'UpdateCommand', input });
  }),
  DeleteCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'DeleteCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-cognito-identity-provider', () => ({
  CognitoIdentityProviderClient: vi.fn(function (this: Record<string, unknown>) {}),
  GetUserCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetUserCommand', input });
  }),
}));

vi.mock('ulid', () => ({
  monotonicFactory: () => () => 'STATE-ULID-001',
}));

import { handler } from './handler.js';

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeEvent(
  path: string,
  queryStringParameters: Record<string, string> = {},
  headers: Record<string, string> = {},
) {
  return {
    rawPath: path,
    rawQueryString: '',
    queryStringParameters,
    headers: { 'user-agent': 'TestBrowser/1.0', ...headers },
    requestContext: {} as never,
    version: '2.0',
    routeKey: '$default',
    isBase64Encoded: false,
    body: undefined,
  } as unknown as Parameters<typeof handler>[0];
}

beforeEach(() => {
  vi.clearAllMocks();
  process.env.USER_TABLE  = 'test-user-table';
  process.env.ADMIN_TABLE = 'test-admin-table';
  process.env.API_BASE_URL = 'https://api.bebocard.app';
  process.env.APP_SUCCESS_URL = 'https://bebocard.app/link-success';
  process.env.APP_FAILURE_URL = 'https://bebocard.app/link-failed';
  process.env.WOOLWORTHS_CLIENT_ID = 'ww-client-id';
  process.env.WOOLWORTHS_CLIENT_SECRET = 'ww-client-secret';
  process.env.FLYBUYS_CLIENT_ID = 'fb-client-id';
  process.env.FLYBUYS_CLIENT_SECRET = 'fb-client-secret';
  global.fetch = mockFetch;
});

// ── GET /auth/link/{brandId} ──────────────────────────────────────────────────

describe('GET /auth/link/{brandId}', () => {
  it('returns 400 for an unknown brandId', async () => {
    mockDynSend.mockResolvedValue({});
    const res = await handler(makeEvent('/auth/link/unknownbrand', { permULID: 'P1', authToken: 'tok' }), {} as never, () => {});
    expect(res.statusCode).toBe(400);
    expect(res.body).toContain('Unknown brand');
  });

  it('returns 400 when permULID is absent', async () => {
    const res = await handler(makeEvent('/auth/link/woolworths', { authToken: 'tok' }), {} as never, () => {});
    expect(res.statusCode).toBe(400);
  });

  it('returns 400 when authToken is absent', async () => {
    const res = await handler(makeEvent('/auth/link/woolworths', { permULID: 'PERM-001' }), {} as never, () => {});
    expect(res.statusCode).toBe(400);
  });

  it('returns 503 when the brand client ID env var is not set', async () => {
    delete process.env.WOOLWORTHS_CLIENT_ID;
    // Cognito succeeds
    mockCognitoSend.mockResolvedValue({
      UserAttributes: [{ Name: 'custom:permULID', Value: 'PERM-001' }],
    });
    // We need to mock the CognitoClient.send — patch it on the module
    const { CognitoIdentityProviderClient } = await import('@aws-sdk/client-cognito-identity-provider');
    (CognitoIdentityProviderClient as unknown as { prototype: { send: typeof mockCognitoSend } }).prototype.send = mockCognitoSend;

    const res = await handler(makeEvent('/auth/link/woolworths', { permULID: 'PERM-001', authToken: 'tok' }), {} as never, () => {});
    expect(res.statusCode).toBe(503);
  });

  it('redirects to failure URL when Cognito auth token is invalid', async () => {
    const { CognitoIdentityProviderClient } = await import('@aws-sdk/client-cognito-identity-provider');
    (CognitoIdentityProviderClient as unknown as { prototype: { send: typeof mockCognitoSend } }).prototype.send = mockCognitoSend;
    mockCognitoSend.mockRejectedValue(new Error('NotAuthorizedException'));

    const res = await handler(makeEvent('/auth/link/woolworths', { permULID: 'PERM-001', authToken: 'bad-token' }), {} as never, () => {});
    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('link-failed');
  });

  it('redirects to OAuth authorize URL on success', async () => {
    const { CognitoIdentityProviderClient } = await import('@aws-sdk/client-cognito-identity-provider');
    (CognitoIdentityProviderClient as unknown as { prototype: { send: typeof mockCognitoSend } }).prototype.send = mockCognitoSend;
    mockCognitoSend.mockResolvedValue({
      UserAttributes: [{ Name: 'custom:permULID', Value: 'PERM-001' }],
    });
    mockDynSend.mockResolvedValue({}); // PutCommand for OAuth state

    const res = await handler(makeEvent('/auth/link/woolworths', { permULID: 'PERM-001', authToken: 'valid-token' }), {} as never, () => {});
    expect(res.statusCode).toBe(302);
    const location = res.headers?.Location as string;
    expect(location).toContain('woolworthsrewards.com.au');
    expect(location).toContain('code_challenge');
    expect(location).toContain('STATE-ULID-001');
  });
});

// ── GET /auth/callback/{brandId} ──────────────────────────────────────────────

describe('GET /auth/callback/{brandId}', () => {
  it('redirects to failure URL for unknown brandId', async () => {
    const res = await handler(makeEvent('/auth/callback/unknownbrand', { code: 'c', state: 's' }), {} as never, () => {});
    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('link-failed');
  });

  it('redirects to failure URL when code is absent', async () => {
    const res = await handler(makeEvent('/auth/callback/woolworths', { state: 's' }), {} as never, () => {});
    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('link-failed');
  });

  it('redirects to failure URL when state record is not found in DynamoDB', async () => {
    mockDynSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: undefined });
      return Promise.resolve({});
    });
    const res = await handler(makeEvent('/auth/callback/woolworths', { code: 'auth-code', state: 'BAD-STATE' }), {} as never, () => {});
    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('invalid_state');
  });

  it('redirects to failure URL when brandId in state does not match path', async () => {
    const stateDesc = JSON.stringify({
      brandId: 'flybuys', // mismatch
      permULID: 'PERM-001',
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
      codeVerifier: 'verifier-abc',
      uaHash: '',
    });
    mockDynSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: { desc: stateDesc } });
      return Promise.resolve({});
    });

    const res = await handler(makeEvent('/auth/callback/woolworths', { code: 'auth-code', state: 'STATE-ULID-001' }), {} as never, () => {});
    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('invalid_state');
  });

  it('redirects to failure URL when token exchange fails', async () => {
    const stateDesc = JSON.stringify({
      brandId: 'woolworths',
      permULID: 'PERM-001',
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
      codeVerifier: 'verifier-abc',
      uaHash: '',
    });
    mockDynSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: { desc: stateDesc } });
      return Promise.resolve({});
    });
    mockFetch.mockResolvedValue({ ok: false, status: 400, text: () => Promise.resolve('bad_code') });

    const res = await handler(makeEvent('/auth/callback/woolworths', { code: 'bad-code', state: 'STATE-001' }), {} as never, () => {});
    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('token_exchange_failed');
  });

  it('redirects to failure URL when card fetch fails', async () => {
    const stateDesc = JSON.stringify({
      brandId: 'woolworths',
      permULID: 'PERM-001',
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
      codeVerifier: 'verifier-abc',
      uaHash: '',
    });
    mockDynSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: { desc: stateDesc } });
      return Promise.resolve({});
    });
    mockFetch
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve({ access_token: 'at-001' }) }) // token exchange
      .mockResolvedValueOnce({ ok: false, status: 404, text: () => Promise.resolve('not found') }); // card fetch

    const res = await handler(makeEvent('/auth/callback/woolworths', { code: 'code', state: 'STATE-001' }), {} as never, () => {});
    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('card_fetch_failed');
  });

  it('stores the linked card and redirects to success URL on happy path', async () => {
    const stateDesc = JSON.stringify({
      brandId: 'woolworths',
      permULID: 'PERM-001',
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
      codeVerifier: 'verifier-abc',
      uaHash: '',
    });
    mockDynSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'GetCommand') {
        const c = cmd as unknown as { input: { Key: Record<string, unknown> } };
        const pk = String(c.input?.Key?.pK ?? '');
        if (pk.startsWith('OAUTHSTATE#')) return Promise.resolve({ Item: { desc: stateDesc } });
        if (pk.startsWith('USER#')) return Promise.resolve({ Item: { secondaryULID: 'SEC-001' } }); // IDENTITY record
        if (pk.startsWith('SCAN#')) return Promise.resolve({ Item: { desc: '{"cards":[]}' } }); // SCAN index
        return Promise.resolve({ Item: undefined });
      }
      return Promise.resolve({});
    });
    mockFetch
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve({ access_token: 'at-001' }) })
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve({ cardNumber: 'WW-CARD-9876' }) });

    const res = await handler(makeEvent('/auth/callback/woolworths', { code: 'good-code', state: 'STATE-001' }), {} as never, () => {});
    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('link-success');
    expect(res.headers?.Location).toContain('brand=woolworths');

    // Verify card was written to UserDataEvent
    const putCalls = mockDynSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    const cardPut = putCalls.find((c: unknown[]) => {
      const item = (c[0] as { input: { Item: Record<string, unknown> } }).input.Item;
      return String(item.sK ?? '').startsWith('CARD#woolworths#');
    });
    expect(cardPut).toBeTruthy();
    const cardDesc = JSON.parse((cardPut![0] as { input: { Item: { desc: string } } }).input.Item.desc);
    expect(cardDesc.cardNumber).toBe('WW-CARD-9876');
    expect(cardDesc.isLinked).toBe(true);
  });
});

// ── Unknown routes ─────────────────────────────────────────────────────────────

describe('unknown routes', () => {
  it('returns 404 for unmatched path', async () => {
    const res = await handler(makeEvent('/auth/unknown/path'), {} as never, () => {});
    expect(res.statusCode).toBe(404);
  });
});

// ── extractCardNumber — brand-specific field extraction ───────────────────────

describe('card number extraction via happy-path responses', () => {
  it('extracts cardNumber from Flybuys response shape', async () => {
    const stateDesc = JSON.stringify({
      brandId: 'flybuys',
      permULID: 'PERM-001',
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
      codeVerifier: 'verifier-abc',
      uaHash: '',
    });
    mockDynSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'GetCommand') {
        const c = cmd as unknown as { input: { Key: Record<string, unknown> } };
        const pk = String(c.input?.Key?.pK ?? '');
        if (pk.startsWith('OAUTHSTATE#')) return Promise.resolve({ Item: { desc: stateDesc } });
        if (pk.startsWith('USER#')) return Promise.resolve({ Item: { secondaryULID: 'SEC-001' } });
        if (pk.startsWith('SCAN#')) return Promise.resolve({ Item: { desc: '{"cards":[]}' } });
        return Promise.resolve({ Item: undefined });
      }
      return Promise.resolve({});
    });
    mockFetch
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve({ access_token: 'at-001' }) })
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve({ flybuysCardNumber: 'FB-9999' }) });

    const res = await handler(makeEvent('/auth/callback/flybuys', { code: 'good-code', state: 'STATE-001' }), {} as never, () => {});
    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('link-success');
  });
});

// ── GET /auth/link/{brandId}?scope=subscriptions ──────────────────────────────

describe('subscription consent linking (scope=subscriptions)', () => {
  beforeEach(() => {
    // Default: Cognito verifies the permULID successfully
    mockCognitoSend.mockResolvedValue({
      UserAttributes: [{ Name: 'custom:permULID', Value: 'PERM-001' }],
    });
  });

  it('returns 302 to link-success after writing SUBSCRIPTION# consent record', async () => {
    mockDynSend.mockResolvedValue({});

    const res = await handler(
      makeEvent('/auth/link/woolworths', {
        permULID: 'PERM-001',
        authToken: 'valid-token',
        scope: 'subscriptions',
      }),
      {} as never, () => {},
    );

    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('link-success');
    expect(res.headers?.Location).toContain('scope=subscriptions');
    expect(res.headers?.Location).toContain('brand=woolworths');

    // Verify SUBSCRIPTION# PutCommand was written
    const puts = mockDynSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    // Two PutCommands: one for SUBSCRIPTION# consent, one may be the brand ref lookup
    // At minimum one PutCommand for the consent record
    const consentPut = puts.find((c: unknown[]) => {
      const item = (c[0] as { input?: { Item?: Record<string, unknown> } }).input?.Item;
      return String(item?.sK ?? '').startsWith('SUBSCRIPTION#');
    });
    expect(consentPut).toBeDefined();

    const item = (consentPut![0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(item.sK).toBe('SUBSCRIPTION#woolworths');
    expect(item.primaryCat).toBe('subscription_consent');
    expect(item.status).toBe('ACTIVE');
    const desc = JSON.parse(item.desc as string);
    expect(desc.brandId).toBe('woolworths');
    expect(desc.scope).toBe('recurring,invoices');
    expect(desc.source).toBe('tenant_linked');
  });

  it('returns 302 to failure when authToken cannot be verified', async () => {
    mockCognitoSend.mockRejectedValue(new Error('Invalid token'));

    const res = await handler(
      makeEvent('/auth/link/woolworths', {
        permULID: 'PERM-001',
        authToken: 'bad-token',
        scope: 'subscriptions',
      }),
      {} as never, () => {},
    );

    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('link-failed');
  });

  it('returns 302 to failure when permULID does not match Cognito claim', async () => {
    mockCognitoSend.mockResolvedValue({
      UserAttributes: [{ Name: 'custom:permULID', Value: 'DIFFERENT-PERM' }],
    });

    const res = await handler(
      makeEvent('/auth/link/telstra', {
        permULID: 'PERM-001',
        authToken: 'valid-token',
        scope: 'subscriptions',
      }),
      {} as never, () => {},
    );

    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('link-failed');
  });

  it('returns 400 when permULID or authToken is missing', async () => {
    const res = await handler(
      makeEvent('/auth/link/woolworths', { scope: 'subscriptions' }), // no permULID or authToken
      {} as never, () => {},
    );
    expect(res.statusCode).toBe(400);
  });

  it('still succeeds for unknown brandId not in BRAND_CONFIG (tenant-registered brand)', async () => {
    // Brand not in BRAND_CONFIG but registered in RefDataEvent
    mockDynSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'GetCommand') {
        return Promise.resolve({ Item: { desc: JSON.stringify({ brandName: 'AGL Energy' }) } });
      }
      return Promise.resolve({});
    });

    const res = await handler(
      makeEvent('/auth/link/agl', {
        permULID: 'PERM-001',
        authToken: 'valid-token',
        scope: 'subscriptions',
      }),
      {} as never, () => {},
    );

    expect(res.statusCode).toBe(302);
    expect(res.headers?.Location).toContain('link-success');

    const puts = mockDynSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    const consentPut = puts.find((c: unknown[]) => {
      const item = (c[0] as { input?: { Item?: Record<string, unknown> } }).input?.Item;
      return String(item?.sK ?? '').startsWith('SUBSCRIPTION#');
    });
    expect(consentPut).toBeDefined();
    const desc = JSON.parse(
      (consentPut![0] as { input: { Item: { desc: string } } }).input.Item.desc,
    );
    expect(desc.brandName).toBe('AGL Energy');
  });
});
