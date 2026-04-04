import { afterAll, beforeEach, describe, expect, it, vi } from 'vitest';

const { mockSend, mockVerify, mockCreatePublicKey, mockRandomUUID } = vi.hoisted(() => ({
  mockSend: vi.fn(),
  mockVerify: vi.fn(),
  mockCreatePublicKey: vi.fn(),
  mockRandomUUID: vi.fn(() => 'jti-123'),
}));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function () { }),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockSend }) },
  GetCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) { Object.assign(this, { input }); }),
  PutCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) { Object.assign(this, { input }); }),
  QueryCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) { Object.assign(this, { input }); }),
  UpdateCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) { Object.assign(this, { input }); }),
}));

vi.mock('crypto', async () => {
  const actual = await vi.importActual<typeof import('crypto')>('crypto');
  return {
    ...actual,
    randomUUID: mockRandomUUID,
    createPublicKey: mockCreatePublicKey,
    createVerify: vi.fn(() => ({
      update: vi.fn(),
      end: vi.fn(),
      verify: mockVerify,
    })),
  };
});

process.env.USER_TABLE = 'user-table';
process.env.REFDATA_TABLE = 'ref-table';
process.env.ADMIN_TABLE = 'admin-table';
process.env.COGNITO_REGION = 'ap-southeast-2';
process.env.COGNITO_USER_POOL_ID = 'ap-southeast-2_TEST';
process.env.WIDGET_TOKEN_SECRET = 'widget-secret';

const originalFetch = global.fetch;
global.fetch = vi.fn();

const { handler } = await import('./handler.js');

function makeIdToken(payload: Record<string, unknown>) {
  const header = Buffer.from(JSON.stringify({ alg: 'RS256', kid: 'kid-1' })).toString('base64url');
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  return `${header}.${body}.signature`;
}

beforeEach(() => {
  vi.clearAllMocks();
  mockVerify.mockReturnValue(true);
  mockCreatePublicKey.mockReturnValue('public-key');
  (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValue({
    ok: true,
    json: async () => ({ keys: [{ kid: 'kid-1', kty: 'RSA', n: 'abc', e: 'AQAB' }] }),
  });
});

describe('widget-action-handler', () => {
  it('issues a widget token for an allowed origin and enabled action', async () => {
    mockSend
      // 1st GetCommand: brand profile → returns tenantId
      .mockResolvedValueOnce({
        Item: {
          pK: 'BRAND#brand-1',
          sK: 'profile',
          tenantId: 'tenant-1',
          desc: JSON.stringify({ tenantId: 'tenant-1' }),
        },
      })
      // 2nd GetCommand: tenant profile → returns widget config
      .mockResolvedValueOnce({
        Item: {
          pK: 'TENANT#tenant-1',
          sK: 'PROFILE',
          desc: JSON.stringify({
            allowedWidgetDomains: ['https://brand.example.com'],
            widgetActions: { invoice: true, giftcard: true },
          }),
        },
      })
      // 3rd call: PutCommand for token storage
      .mockResolvedValueOnce({});

    const res = await handler({
      httpMethod: 'POST',
      path: '/widget/auth',
      headers: {
        origin: 'https://brand.example.com',
        authorization: `Bearer ${makeIdToken({
          iss: 'https://cognito-idp.ap-southeast-2.amazonaws.com/ap-southeast-2_TEST',
          token_use: 'id',
          exp: Math.floor(Date.now() / 1000) + 600,
          email: 'user@example.com',
          'custom:permULID': 'perm-123',
        })}`,
      },
      body: JSON.stringify({ brandId: 'brand-1', action: 'invoice' }),
    } as any, {} as any, {} as any) as { statusCode: number; body: string };

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.action).toBe('invoice');
    expect(body.user.permULID).toBe('perm-123');
    expect(body.token).toBeTruthy();
  });

  it('rejects widget auth for a disallowed origin', async () => {
    mockSend
      // 1st GetCommand: brand profile → returns tenantId
      .mockResolvedValueOnce({
        Item: {
          pK: 'BRAND#brand-1',
          sK: 'profile',
          tenantId: 'tenant-1',
          desc: JSON.stringify({ tenantId: 'tenant-1' }),
        },
      })
      // 2nd GetCommand: tenant profile → returns widget config with allowed domain
      .mockResolvedValueOnce({
        Item: {
          pK: 'TENANT#tenant-1',
          sK: 'PROFILE',
          desc: JSON.stringify({
            allowedWidgetDomains: ['https://brand.example.com'],
            widgetActions: { invoice: true, giftcard: true },
          }),
        },
      });

    const res = await handler({
      httpMethod: 'POST',
      path: '/widget/auth',
      headers: { origin: 'https://evil.example.com' },
      body: JSON.stringify({ brandId: 'brand-1', action: 'invoice', idToken: 'x.y.z' }),
    } as any, {} as any, {} as any) as { statusCode: number };

    expect(res.statusCode).toBe(403);
  });
});

afterAll(() => {
  global.fetch = originalFetch;
});
