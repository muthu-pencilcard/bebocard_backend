import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createHmac } from 'crypto';
import type { APIGatewayProxyEvent } from 'aws-lambda';

// ── Hoisted mocks ─────────────────────────────────────────────────────────────

const { mockDdbSend, mockSsmSend } = vi.hoisted(() => ({
  mockDdbSend: vi.fn(),
  mockSsmSend: vi.fn(),
}));

// ── Module mocks ──────────────────────────────────────────────────────────────

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: object) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn(() => ({ send: mockDdbSend })) },
  GetCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetCommand', input });
  }),
  UpdateCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'UpdateCommand', input });
  }),
  PutCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutCommand', input });
  }),
  QueryCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'QueryCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-ssm', () => ({
  SSMClient: vi.fn(function (this: Record<string, unknown>) { this.send = mockSsmSend; }),
  GetParameterCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetParameterCommand', input });
  }),
}));

// ── Env vars ──────────────────────────────────────────────────────────────────

process.env.REFDATA_TABLE = 'ref-table';

// ── Handler import ────────────────────────────────────────────────────────────

import { handler } from './handler.js';

// ── Constants ─────────────────────────────────────────────────────────────────

const WEBHOOK_SECRET = 'whsec_test_secret_for_billing_tests';
const TENANT_ID = 'wg-001';

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeStripeSignature(body: string, secret: string, timestamp = '1714000000'): string {
  const signedPayload = `${timestamp}.${body}`;
  const hmac = createHmac('sha256', secret).update(signedPayload).digest('hex');
  return `t=${timestamp},v1=${hmac}`;
}

function makeEvent(body: object, overrides: Partial<APIGatewayProxyEvent> = {}): APIGatewayProxyEvent {
  const bodyStr = JSON.stringify(body);
  return {
    path: '/webhooks/stripe',
    httpMethod: 'POST',
    headers: {
      'stripe-signature': makeStripeSignature(bodyStr, WEBHOOK_SECRET),
    },
    body: bodyStr,
    queryStringParameters: null,
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

function mockSsmWithSecret(secret = WEBHOOK_SECRET) {
  mockSsmSend.mockResolvedValue({ Parameter: { Value: secret } });
}

function makeTenantProfileItem(desc: Record<string, unknown> = {}) {
  return {
    pK: `TENANT#${TENANT_ID}`,
    sK: 'PROFILE',
    desc: JSON.stringify({
      tenantName: 'Woolworths Group',
      tier: 'base',
      billingStatus: 'ACTIVE',
      ...desc,
    }),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Signature verification
// ─────────────────────────────────────────────────────────────────────────────

describe('Stripe signature verification', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns 401 when stripe-signature header is absent', async () => {
    mockSsmSend.mockResolvedValue({ Parameter: { Value: WEBHOOK_SECRET } });
    const res = await handler(
      makeEvent({}, { headers: {} }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 401 });
  });

  it('returns 401 when signature is invalid (wrong secret)', async () => {
    mockSsmSend.mockResolvedValue({ Parameter: { Value: WEBHOOK_SECRET } });
    const bodyStr = JSON.stringify({ type: 'checkout.session.completed', data: { object: {} } });
    const res = await handler(
      {
        path: '/webhooks/stripe',
        httpMethod: 'POST',
        headers: { 'stripe-signature': makeStripeSignature(bodyStr, 'wrong_secret') },
        body: bodyStr,
        queryStringParameters: null,
        multiValueHeaders: {},
        multiValueQueryStringParameters: null,
        isBase64Encoded: false,
        pathParameters: null,
        stageVariables: null,
        requestContext: {} as APIGatewayProxyEvent['requestContext'],
        resource: '',
      } as APIGatewayProxyEvent,
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 401 });
  });

  it('returns 400 when body is not valid JSON (after passing signature)', async () => {
    const rawBody = 'not-json{{{';
    mockSsmSend.mockResolvedValue({ Parameter: { Value: WEBHOOK_SECRET } });
    const sig = makeStripeSignature(rawBody, WEBHOOK_SECRET);
    const res = await handler(
      {
        path: '/webhooks/stripe',
        httpMethod: 'POST',
        headers: { 'stripe-signature': sig },
        body: rawBody,
        queryStringParameters: null,
        multiValueHeaders: {},
        multiValueQueryStringParameters: null,
        isBase64Encoded: false,
        pathParameters: null,
        stageVariables: null,
        requestContext: {} as APIGatewayProxyEvent['requestContext'],
        resource: '',
      } as APIGatewayProxyEvent,
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 400 });
  });

  it('returns 200 received: true for unknown event type', async () => {
    mockSsmWithSecret();
    const res = await handler(
      makeEvent({ type: 'some.unknown.event', data: { object: {} } }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 200 });
    const body = JSON.parse((res as { body: string }).body);
    expect(body.received).toBe(true);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// checkout.session.completed
// ─────────────────────────────────────────────────────────────────────────────

describe('checkout.session.completed', () => {
  beforeEach(() => vi.clearAllMocks());

  it('links tenant to Stripe customer + subscription and sets billingStatus ACTIVE', async () => {
    mockSsmWithSecret();
    mockDdbSend
      .mockResolvedValueOnce({ Item: makeTenantProfileItem() }) // GetCommand
      .mockResolvedValueOnce({});                               // UpdateCommand

    const session = {
      customer: 'cus_abc123',
      subscription: 'sub_xyz789',
      metadata: { tenantId: TENANT_ID },
    };

    const res = await handler(
      makeEvent({ type: 'checkout.session.completed', data: { object: session } }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 200 });

    // Verify UpdateCommand was called with correct values
    const updateArgs = mockDdbSend.mock.calls.find(
      ([cmd]: [{ __type: string; input: { ExpressionAttributeValues?: Record<string, string> } }]) =>
        cmd.__type === 'UpdateCommand',
    );
    expect(updateArgs).toBeDefined();
    const descUpdated = JSON.parse(
      (updateArgs![0] as { input: { ExpressionAttributeValues: Record<string, string> } })
        .input.ExpressionAttributeValues[':desc'],
    );
    expect(descUpdated.stripeCustomerId).toBe('cus_abc123');
    expect(descUpdated.stripeSubscriptionId).toBe('sub_xyz789');
    expect(descUpdated.billingStatus).toBe('ACTIVE');
  });

  it('skips update when tenantId is absent from metadata', async () => {
    mockSsmWithSecret();
    const res = await handler(
      makeEvent({
        type: 'checkout.session.completed',
        data: { object: { customer: 'cus_abc', subscription: 'sub_123', metadata: {} } },
      }),
      {} as never,
      {} as never,
    );
    expect(res).toMatchObject({ statusCode: 200 });
    expect(mockDdbSend).not.toHaveBeenCalled();
  });

  it('preserves existing desc fields when merging', async () => {
    mockSsmWithSecret();
    const existingDesc = { tenantName: 'Woolworths Group', tier: 'engagement', tierStartDate: '2026-01-01T00:00:00Z' };
    mockDdbSend
      .mockResolvedValueOnce({ Item: { desc: JSON.stringify(existingDesc) } })
      .mockResolvedValueOnce({});

    await handler(
      makeEvent({
        type: 'checkout.session.completed',
        data: { object: { customer: 'cus_new', subscription: 'sub_new', metadata: { tenantId: TENANT_ID } } },
      }),
      {} as never,
      {} as never,
    );

    const updateArgs = mockDdbSend.mock.calls.find(
      ([cmd]: [{ __type: string }]) => cmd.__type === 'UpdateCommand',
    );
    const desc = JSON.parse(
      (updateArgs![0] as { input: { ExpressionAttributeValues: Record<string, string> } })
        .input.ExpressionAttributeValues[':desc'],
    );
    // Existing tier preserved
    expect(desc.tier).toBe('engagement');
    // tierStartDate preserved (not overwritten)
    expect(desc.tierStartDate).toBe('2026-01-01T00:00:00Z');
    // New fields added
    expect(desc.stripeCustomerId).toBe('cus_new');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// customer.subscription.updated / deleted
// ─────────────────────────────────────────────────────────────────────────────

describe('customer.subscription.updated / deleted', () => {
  beforeEach(() => vi.clearAllMocks());

  it('sets billingStatus ACTIVE when Stripe status is "active"', async () => {
    mockSsmWithSecret();
    mockDdbSend
      .mockResolvedValueOnce({ Item: makeTenantProfileItem() })
      .mockResolvedValueOnce({});

    await handler(
      makeEvent({
        type: 'customer.subscription.updated',
        data: { object: { customer: 'cus_abc', status: 'active', metadata: { tenantId: TENANT_ID } } },
      }),
      {} as never,
      {} as never,
    );

    const updateArgs = mockDdbSend.mock.calls.find(
      ([cmd]: [{ __type: string }]) => cmd.__type === 'UpdateCommand',
    );
    const desc = JSON.parse(
      (updateArgs![0] as { input: { ExpressionAttributeValues: Record<string, string> } })
        .input.ExpressionAttributeValues[':desc'],
    );
    expect(desc.billingStatus).toBe('ACTIVE');
    expect(desc.stripeSubscriptionStatus).toBe('active');
  });

  it('sets billingStatus ACTIVE when Stripe status is "trialing"', async () => {
    mockSsmWithSecret();
    mockDdbSend
      .mockResolvedValueOnce({ Item: makeTenantProfileItem() })
      .mockResolvedValueOnce({});

    await handler(
      makeEvent({
        type: 'customer.subscription.updated',
        data: { object: { status: 'trialing', metadata: { tenantId: TENANT_ID } } },
      }),
      {} as never,
      {} as never,
    );

    const updateArgs = mockDdbSend.mock.calls.find(
      ([cmd]: [{ __type: string }]) => cmd.__type === 'UpdateCommand',
    );
    const desc = JSON.parse(
      (updateArgs![0] as { input: { ExpressionAttributeValues: Record<string, string> } })
        .input.ExpressionAttributeValues[':desc'],
    );
    expect(desc.billingStatus).toBe('ACTIVE');
  });

  it('sets billingStatus SUSPENDED when Stripe status is "canceled"', async () => {
    mockSsmWithSecret();
    mockDdbSend
      .mockResolvedValueOnce({ Item: makeTenantProfileItem() })
      .mockResolvedValueOnce({});

    await handler(
      makeEvent({
        type: 'customer.subscription.deleted',
        data: { object: { status: 'canceled', metadata: { tenantId: TENANT_ID } } },
      }),
      {} as never,
      {} as never,
    );

    const updateArgs = mockDdbSend.mock.calls.find(
      ([cmd]: [{ __type: string }]) => cmd.__type === 'UpdateCommand',
    );
    const desc = JSON.parse(
      (updateArgs![0] as { input: { ExpressionAttributeValues: Record<string, string> } })
        .input.ExpressionAttributeValues[':desc'],
    );
    expect(desc.billingStatus).toBe('SUSPENDED');
  });

  it('sets billingStatus SUSPENDED when Stripe status is "past_due"', async () => {
    mockSsmWithSecret();
    mockDdbSend
      .mockResolvedValueOnce({ Item: makeTenantProfileItem() })
      .mockResolvedValueOnce({});

    await handler(
      makeEvent({
        type: 'customer.subscription.updated',
        data: { object: { status: 'past_due', metadata: { tenantId: TENANT_ID } } },
      }),
      {} as never,
      {} as never,
    );

    const updateArgs = mockDdbSend.mock.calls.find(
      ([cmd]: [{ __type: string }]) => cmd.__type === 'UpdateCommand',
    );
    const desc = JSON.parse(
      (updateArgs![0] as { input: { ExpressionAttributeValues: Record<string, string> } })
        .input.ExpressionAttributeValues[':desc'],
    );
    expect(desc.billingStatus).toBe('SUSPENDED');
  });

  it('skips update when tenantId absent from metadata', async () => {
    mockSsmWithSecret();
    await handler(
      makeEvent({
        type: 'customer.subscription.updated',
        data: { object: { status: 'active', metadata: {} } },
      }),
      {} as never,
      {} as never,
    );
    expect(mockDdbSend).not.toHaveBeenCalled();
  });

  it('skips update when tenant profile does not exist in DynamoDB', async () => {
    mockSsmWithSecret();
    mockDdbSend.mockResolvedValueOnce({ Item: undefined }); // GetCommand → no item
    await handler(
      makeEvent({
        type: 'customer.subscription.updated',
        data: { object: { status: 'canceled', metadata: { tenantId: TENANT_ID } } },
      }),
      {} as never,
      {} as never,
    );
    // Only 1 DDB call (GetCommand), no UpdateCommand
    expect(mockDdbSend).toHaveBeenCalledTimes(1);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// invoice.payment_failed
// ─────────────────────────────────────────────────────────────────────────────

describe('invoice.payment_failed', () => {
  beforeEach(() => vi.clearAllMocks());

  it('sets billingStatus OVERDUE on payment failure', async () => {
    mockSsmWithSecret();
    mockDdbSend
      .mockResolvedValueOnce({ Item: makeTenantProfileItem() })
      .mockResolvedValueOnce({});

    await handler(
      makeEvent({
        type: 'invoice.payment_failed',
        data: {
          object: {
            subscription_details: { metadata: { tenantId: TENANT_ID } },
          },
        },
      }),
      {} as never,
      {} as never,
    );

    const updateArgs = mockDdbSend.mock.calls.find(
      ([cmd]: [{ __type: string }]) => cmd.__type === 'UpdateCommand',
    );
    const desc = JSON.parse(
      (updateArgs![0] as { input: { ExpressionAttributeValues: Record<string, string> } })
        .input.ExpressionAttributeValues[':desc'],
    );
    expect(desc.billingStatus).toBe('OVERDUE');
  });

  it('skips update when tenantId not resolvable from invoice metadata', async () => {
    mockSsmWithSecret();
    await handler(
      makeEvent({
        type: 'invoice.payment_failed',
        data: { object: { subscription_details: { metadata: {} }, metadata: {} } },
      }),
      {} as never,
      {} as never,
    );
    expect(mockDdbSend).not.toHaveBeenCalled();
  });
});
