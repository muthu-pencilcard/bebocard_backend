import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createHmac } from 'crypto';

const { mockSend } = vi.hoisted(() => {
  process.env.REFDATA_TABLE = 'RefDataEvent-test';
  process.env.INTERNAL_SIGNING_SECRET = 'test-signing-secret';
  process.env.PORTAL_ORIGIN = 'https://business.bebocard.com.au';
  return { mockSend: vi.fn() };
});

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: () => ({ send: mockSend }) },
  PutCommand:           class PutCommand           { __type = 'PutCommand';           constructor(public input: any) {} },
  GetCommand:           class GetCommand           { __type = 'GetCommand';           constructor(public input: any) {} },
  UpdateCommand:        class UpdateCommand        { __type = 'UpdateCommand';        constructor(public input: any) {} },
  QueryCommand:         class QueryCommand         { __type = 'QueryCommand';         constructor(public input: any) {} },
  DeleteCommand:        class DeleteCommand        { __type = 'DeleteCommand';        constructor(public input: any) {} },
  TransactWriteCommand: class TransactWriteCommand { __type = 'TransactWriteCommand'; constructor(public input: any) {} },
}));

import { handler } from './handler';
import type { APIGatewayProxyEvent } from 'aws-lambda';

const ACTOR = 'admin@bebocard.com';
const SECRET = 'test-signing-secret';

function makeEvent(
  method: string,
  path: string,
  body: unknown = null,
  overrideAuth?: { email?: string; secret?: string; expiredTimestamp?: boolean },
): APIGatewayProxyEvent {
  const email = overrideAuth?.email ?? ACTOR;
  const timestamp = overrideAuth?.expiredTimestamp
    ? String(Date.now() - 10 * 60 * 1000) // 10 min ago — outside 5-min window
    : String(Date.now());
  const secret = overrideAuth?.secret ?? SECRET;
  const sig = createHmac('sha256', secret).update(`${email}:${timestamp}`).digest('hex');

  return {
    httpMethod: method,
    path,
    headers: { origin: 'https://business.bebocard.com.au' },
    body: body !== null ? JSON.stringify(body) : null,
    _internalActorEmail: email,
    _internalTimestamp: timestamp,
    _internalSig: sig,
  } as unknown as APIGatewayProxyEvent;
}

function makeUnauthEvent(method: string, path: string, body: unknown = null): APIGatewayProxyEvent {
  return {
    httpMethod: method,
    path,
    headers: { origin: 'https://business.bebocard.com.au' },
    body: body !== null ? JSON.stringify(body) : null,
    _internalActorEmail: '',
    _internalTimestamp: '',
    _internalSig: '',
  } as unknown as APIGatewayProxyEvent;
}

const DRAFT_TEMPLATE = {
  pK: 'TEMPLATE#abc123',
  sK: 'PROFILE',
  templateId: 'abc123',
  name: 'Standard QR',
  description: 'Generic QR loyalty card',
  barcodeFormat: 'QR_CODE',
  primaryColor: '#1A1A2E',
  accentColor: '#16213E',
  fieldLabels: {},
  requiredScopes: ['scan'],
  status: 'DRAFT',
  createdAt: '2026-01-01T00:00:00.000Z',
  updatedAt: '2026-01-01T00:00:00.000Z',
  createdBy: ACTOR,
};

const APPROVED_TEMPLATE = { ...DRAFT_TEMPLATE, status: 'APPROVED', approvedAt: '2026-01-02T00:00:00.000Z', approvedBy: ACTOR };
const ARCHIVED_TEMPLATE = { ...DRAFT_TEMPLATE, status: 'ARCHIVED' };

beforeEach(() => {
  mockSend.mockReset();
});

// ─── Auth ─────────────────────────────────────────────────────────────────────

describe('HMAC auth', () => {
  it('rejects request with no auth fields', async () => {
    const res = await handler(makeUnauthEvent('GET', '/templates'), {} as any, {} as any);
    expect(res?.statusCode).toBe(401);
  });

  it('rejects request with wrong secret', async () => {
    const res = await handler(makeEvent('GET', '/templates', null, { secret: 'wrong' }), {} as any, {} as any);
    expect(res?.statusCode).toBe(401);
  });

  it('rejects request with expired timestamp (>5 min)', async () => {
    const res = await handler(makeEvent('GET', '/templates', null, { expiredTimestamp: true }), {} as any, {} as any);
    expect(res?.statusCode).toBe(401);
  });
});

// ─── OPTIONS preflight ────────────────────────────────────────────────────────

describe('OPTIONS preflight', () => {
  it('returns 200 with CORS headers', async () => {
    const event = { httpMethod: 'OPTIONS', path: '/templates', headers: { origin: 'https://business.bebocard.com.au' } } as unknown as APIGatewayProxyEvent;
    const res = await handler(event, {} as any, {} as any);
    expect(res?.statusCode).toBe(200);
    expect(res?.headers?.['Access-Control-Allow-Methods']).toContain('POST');
  });
});

// ─── POST /templates ─────────────────────────────────────────────────────────

describe('POST /templates — createTemplate', () => {
  it('creates a template and returns 201', async () => {
    mockSend.mockResolvedValueOnce({});
    const res = await handler(makeEvent('POST', '/templates', {
      name: 'My Card',
      description: 'A card',
      barcodeFormat: 'QR_CODE',
      primaryColor: '#AABBCC',
      accentColor: '#112233',
    }), {} as any, {} as any);
    expect(res?.statusCode).toBe(201);
    const body = JSON.parse(res!.body);
    expect(body.name).toBe('My Card');
    expect(body.status).toBe('DRAFT');
    expect(body.createdBy).toBe(ACTOR);
    const call = mockSend.mock.calls[0][0];
    expect(call.input.ConditionExpression).toBe('attribute_not_exists(pK)');
  });

  it('returns 400 when name is missing', async () => {
    const res = await handler(makeEvent('POST', '/templates', { description: 'No name' }), {} as any, {} as any);
    expect(res?.statusCode).toBe(400);
    expect(JSON.parse(res!.body).error).toMatch(/name/i);
  });

  it('returns 400 for invalid primaryColor', async () => {
    const res = await handler(makeEvent('POST', '/templates', {
      name: 'X', primaryColor: 'red', accentColor: '#112233',
    }), {} as any, {} as any);
    expect(res?.statusCode).toBe(400);
    expect(JSON.parse(res!.body).error).toMatch(/primaryColor/i);
  });

  it('returns 400 for invalid barcodeFormat', async () => {
    const res = await handler(makeEvent('POST', '/templates', {
      name: 'X', primaryColor: '#AABBCC', accentColor: '#112233', barcodeFormat: 'INVALID',
    }), {} as any, {} as any);
    expect(res?.statusCode).toBe(400);
    expect(JSON.parse(res!.body).error).toMatch(/barcodeFormat/i);
  });

  it('returns 400 when name exceeds 100 characters', async () => {
    const res = await handler(makeEvent('POST', '/templates', {
      name: 'A'.repeat(101), primaryColor: '#AABBCC', accentColor: '#112233',
    }), {} as any, {} as any);
    expect(res?.statusCode).toBe(400);
  });
});

// ─── GET /templates ───────────────────────────────────────────────────────────

describe('GET /templates — listTemplates', () => {
  it('returns templates sorted by createdAt descending', async () => {
    mockSend.mockResolvedValueOnce({
      Items: [
        { pK: 'TEMPLATE#a', sK: 'PROFILE', templateId: 'a', name: 'A', createdAt: '2026-01-01T00:00:00.000Z' },
        { pK: 'TEMPLATE#b', sK: 'PROFILE', templateId: 'b', name: 'B', createdAt: '2026-02-01T00:00:00.000Z' },
      ],
    });
    const res = await handler(makeEvent('GET', '/templates'), {} as any, {} as any);
    expect(res?.statusCode).toBe(200);
    const body = JSON.parse(res!.body);
    expect(body.templates[0].name).toBe('B');
    expect(body.templates[1].name).toBe('A');
    expect(body.count).toBe(2);
  });

  it('returns empty list when no templates exist', async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    const res = await handler(makeEvent('GET', '/templates'), {} as any, {} as any);
    const body = JSON.parse(res!.body);
    expect(body.count).toBe(0);
  });
});

// ─── GET /templates/:id ───────────────────────────────────────────────────────

describe('GET /templates/:id — getTemplate', () => {
  it('returns template when found', async () => {
    mockSend.mockResolvedValueOnce({ Item: DRAFT_TEMPLATE });
    const res = await handler(makeEvent('GET', '/templates/abc123'), {} as any, {} as any);
    expect(res?.statusCode).toBe(200);
    const body = JSON.parse(res!.body);
    expect(body.templateId).toBe('abc123');
    expect(body.pK).toBeUndefined();
    expect(body.sK).toBeUndefined();
  });

  it('returns 404 when not found', async () => {
    mockSend.mockResolvedValueOnce({ Item: undefined });
    const res = await handler(makeEvent('GET', '/templates/missing'), {} as any, {} as any);
    expect(res?.statusCode).toBe(404);
  });
});

// ─── PUT /templates/:id ───────────────────────────────────────────────────────

describe('PUT /templates/:id — updateTemplate', () => {
  it('updates a DRAFT template', async () => {
    mockSend.mockResolvedValueOnce({ Item: DRAFT_TEMPLATE }).mockResolvedValueOnce({});
    const res = await handler(makeEvent('PUT', '/templates/abc123', { name: 'Updated' }), {} as any, {} as any);
    expect(res?.statusCode).toBe(200);
    expect(JSON.parse(res!.body).name).toBe('Updated');
  });

  it('rejects update on ARCHIVED template', async () => {
    mockSend.mockResolvedValueOnce({ Item: ARCHIVED_TEMPLATE });
    const res = await handler(makeEvent('PUT', '/templates/abc123', { name: 'X' }), {} as any, {} as any);
    expect(res?.statusCode).toBe(409);
    expect(JSON.parse(res!.body).error).toMatch(/archived/i);
  });

  it('syncs DISCOVERY#TEMPLATES entry when template is APPROVED', async () => {
    mockSend.mockResolvedValueOnce({ Item: APPROVED_TEMPLATE }).mockResolvedValueOnce({}).mockResolvedValueOnce({});
    await handler(makeEvent('PUT', '/templates/abc123', { name: 'New Name' }), {} as any, {} as any);
    const discoveryCall = mockSend.mock.calls[2][0];
    expect(discoveryCall.input.Key).toEqual({ pK: 'DISCOVERY#TEMPLATES', sK: 'TEMPLATE#abc123' });
  });

  it('returns 400 when no updatable fields provided', async () => {
    mockSend.mockResolvedValueOnce({ Item: DRAFT_TEMPLATE });
    const res = await handler(makeEvent('PUT', '/templates/abc123', {}), {} as any, {} as any);
    expect(res?.statusCode).toBe(400);
  });

  it('returns 404 when template not found', async () => {
    mockSend.mockResolvedValueOnce({ Item: undefined });
    const res = await handler(makeEvent('PUT', '/templates/missing', { name: 'X' }), {} as any, {} as any);
    expect(res?.statusCode).toBe(404);
  });
});

// ─── DELETE /templates/:id ────────────────────────────────────────────────────

describe('DELETE /templates/:id — deleteTemplate', () => {
  it('archives a DRAFT template without touching discovery index', async () => {
    mockSend.mockResolvedValueOnce({ Item: DRAFT_TEMPLATE }).mockResolvedValueOnce({});
    const res = await handler(makeEvent('DELETE', '/templates/abc123'), {} as any, {} as any);
    expect(res?.statusCode).toBe(200);
    expect(JSON.parse(res!.body).status).toBe('ARCHIVED');
    const transact = mockSend.mock.calls[1][0];
    expect(transact.input.TransactItems).toHaveLength(1);
    expect(transact.input.TransactItems[0].Update.Key.pK).toBe('TEMPLATE#abc123');
  });

  it('archives an APPROVED template and deletes discovery index entry', async () => {
    mockSend.mockResolvedValueOnce({ Item: APPROVED_TEMPLATE }).mockResolvedValueOnce({});
    await handler(makeEvent('DELETE', '/templates/abc123'), {} as any, {} as any);
    const transact = mockSend.mock.calls[1][0];
    expect(transact.input.TransactItems).toHaveLength(2);
    const deleteItem = transact.input.TransactItems[1].Delete;
    expect(deleteItem.Key).toEqual({ pK: 'DISCOVERY#TEMPLATES', sK: 'TEMPLATE#abc123' });
  });

  it('returns 404 when template not found', async () => {
    mockSend.mockResolvedValueOnce({ Item: undefined });
    const res = await handler(makeEvent('DELETE', '/templates/missing'), {} as any, {} as any);
    expect(res?.statusCode).toBe(404);
  });
});

// ─── POST /templates/:id/approve ─────────────────────────────────────────────

describe('POST /templates/:id/approve — approveTemplate', () => {
  it('approves a DRAFT template with TransactWrite and writes discovery index entry', async () => {
    mockSend.mockResolvedValueOnce({ Item: DRAFT_TEMPLATE }).mockResolvedValueOnce({});
    const res = await handler(makeEvent('POST', '/templates/abc123/approve'), {} as any, {} as any);
    expect(res?.statusCode).toBe(200);
    const body = JSON.parse(res!.body);
    expect(body.status).toBe('APPROVED');
    expect(body.approvedAt).toBeDefined();

    const transact = mockSend.mock.calls[1][0];
    expect(transact.input.TransactItems).toHaveLength(2);

    const updateItem = transact.input.TransactItems[0].Update;
    expect(updateItem.Key.pK).toBe('TEMPLATE#abc123');
    expect(updateItem.ExpressionAttributeValues[':status']).toBe('APPROVED');
    expect(updateItem.ExpressionAttributeValues[':actor']).toBe(ACTOR);

    const putItem = transact.input.TransactItems[1].Put;
    expect(putItem.Item.pK).toBe('DISCOVERY#TEMPLATES');
    expect(putItem.Item.sK).toBe('TEMPLATE#abc123');
    expect(putItem.Item.status).toBe('APPROVED');
  });

  it('approves a WITHDRAWN template', async () => {
    mockSend.mockResolvedValueOnce({ Item: { ...DRAFT_TEMPLATE, status: 'WITHDRAWN' } }).mockResolvedValueOnce({});
    const res = await handler(makeEvent('POST', '/templates/abc123/approve'), {} as any, {} as any);
    expect(res?.statusCode).toBe(200);
  });

  it('returns 409 when template is already APPROVED', async () => {
    mockSend.mockResolvedValueOnce({ Item: APPROVED_TEMPLATE });
    const res = await handler(makeEvent('POST', '/templates/abc123/approve'), {} as any, {} as any);
    expect(res?.statusCode).toBe(409);
    expect(JSON.parse(res!.body).error).toMatch(/already approved/i);
  });

  it('returns 409 when template is ARCHIVED', async () => {
    mockSend.mockResolvedValueOnce({ Item: ARCHIVED_TEMPLATE });
    const res = await handler(makeEvent('POST', '/templates/abc123/approve'), {} as any, {} as any);
    expect(res?.statusCode).toBe(409);
    expect(JSON.parse(res!.body).error).toMatch(/archived/i);
  });

  it('returns 404 when template not found', async () => {
    mockSend.mockResolvedValueOnce({ Item: undefined });
    const res = await handler(makeEvent('POST', '/templates/missing/approve'), {} as any, {} as any);
    expect(res?.statusCode).toBe(404);
  });
});

// ─── POST /templates/:id/withdraw ────────────────────────────────────────────

describe('POST /templates/:id/withdraw — withdrawTemplate', () => {
  it('withdraws an APPROVED template and deletes discovery index entry', async () => {
    mockSend.mockResolvedValueOnce({ Item: APPROVED_TEMPLATE }).mockResolvedValueOnce({});
    const res = await handler(makeEvent('POST', '/templates/abc123/withdraw'), {} as any, {} as any);
    expect(res?.statusCode).toBe(200);
    const body = JSON.parse(res!.body);
    expect(body.status).toBe('WITHDRAWN');
    expect(body.withdrawnAt).toBeDefined();

    const transact = mockSend.mock.calls[1][0];
    expect(transact.input.TransactItems).toHaveLength(2);

    const updateItem = transact.input.TransactItems[0].Update;
    expect(updateItem.ExpressionAttributeValues[':status']).toBe('WITHDRAWN');

    const deleteItem = transact.input.TransactItems[1].Delete;
    expect(deleteItem.Key).toEqual({ pK: 'DISCOVERY#TEMPLATES', sK: 'TEMPLATE#abc123' });
  });

  it('returns 409 when template is not APPROVED', async () => {
    mockSend.mockResolvedValueOnce({ Item: DRAFT_TEMPLATE });
    const res = await handler(makeEvent('POST', '/templates/abc123/withdraw'), {} as any, {} as any);
    expect(res?.statusCode).toBe(409);
    expect(JSON.parse(res!.body).error).toMatch(/APPROVED/);
  });

  it('returns 404 when template not found', async () => {
    mockSend.mockResolvedValueOnce({ Item: undefined });
    const res = await handler(makeEvent('POST', '/templates/missing/withdraw'), {} as any, {} as any);
    expect(res?.statusCode).toBe(404);
  });
});

// ─── 404 fallthrough ──────────────────────────────────────────────────────────

describe('unknown routes', () => {
  it('returns 404 for unrecognised path', async () => {
    const res = await handler(makeEvent('GET', '/unknown'), {} as any, {} as any);
    expect(res?.statusCode).toBe(404);
  });
});
