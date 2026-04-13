import { vi, describe, it, expect, beforeEach } from 'vitest';

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: vi.fn() }) },
  GetCommand: vi.fn(),
  PutCommand: vi.fn(),
  QueryCommand: vi.fn(),
}));

vi.mock('../../shared/api-key-auth.js', () => ({
  validateApiKey: vi.fn(),
  extractApiKey: vi.fn(),
}));

vi.mock('../../shared/audit-logger.js', () => ({
  withAuditLog: (_ddb: unknown, h: unknown) => h,
}));

vi.mock('firebase-admin/app', () => ({
  initializeApp: vi.fn(),
  getApps: vi.fn(() => []),
  cert: vi.fn(),
}));

vi.mock('ulid', () => ({
  monotonicFactory: () => () => 'TEST-ULID',
}));

import { handler } from './handler.js';

describe('API Versioning Redirects (P2-7)', () => {
  const makeEvent = (path: string) => ({
    path,
    httpMethod: 'POST',
    headers: { 'x-api-key': 'test-key' },
    body: JSON.stringify({ test: 'data' }),
  } as any);

  it('redirects legacy /scan to /v1/scan with 308', async () => {
    const event = makeEvent('/scan');
    const res = await handler(event, {} as any, () => {});
    
    expect(res!.statusCode).toBe(308);
    expect(res!.headers?.['Location']).toBe('/v1/scan');
    const body = JSON.parse(res!.body);
    expect(body.error).toBe('Deprecated Endpoint');
  });

  it('redirects legacy /receipt to /v1/receipt with 308', async () => {
    const event = makeEvent('/receipt');
    const res = await handler(event, {} as any, () => {});
    
    expect(res!.statusCode).toBe(308);
    expect(res!.headers?.['Location']).toBe('/v1/receipt');
  });

  it('does NOT redirect /v1/scan', async () => {
    // This should proceed to normal logic (e.g., 401 if missing auth in mock)
    const event = makeEvent('/v1/scan');
    const res = await handler(event, {} as any, () => {});
    
    expect(res!.statusCode).not.toBe(308);
  });
});
