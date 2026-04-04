/**
 * gift-card-handler tests — Phase 13 (Gift Card Marketplace + Gifting)
 *
 * Coverage:
 *   AppSync:  purchaseForSelf, purchaseAsGift, syncGiftCardBalance
 *   REST:     POST /webhook (Stripe) — self + gift flows, idempotency
 *             GET  /gift/:token — valid claim, already-claimed, expired, not-found
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createHmac } from 'crypto';

// ── Hoisted mock factories ────────────────────────────────────────────────────

const { mockDdbSend, mockKmsSend, mockSesSend, mockFcmSend, mockFetch } = vi.hoisted(() => ({
  mockDdbSend: vi.fn(),
  mockKmsSend: vi.fn(),
  mockSesSend: vi.fn(),
  mockFcmSend: vi.fn(),
  mockFetch:   vi.fn(),
}));

// ── AWS SDK mocks ─────────────────────────────────────────────────────────────

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockDdbSend }) },
  GetCommand:    vi.fn(function (this: Record<string, unknown>, input: unknown) { Object.assign(this, { __type: 'GetCommand', input }); }),
  PutCommand:    vi.fn(function (this: Record<string, unknown>, input: unknown) { Object.assign(this, { __type: 'PutCommand', input }); }),
  UpdateCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) { Object.assign(this, { __type: 'UpdateCommand', input }); }),
  QueryCommand:  vi.fn(function (this: Record<string, unknown>, input: unknown) { Object.assign(this, { __type: 'QueryCommand', input }); }),
}));

vi.mock('@aws-sdk/client-kms', () => ({
  KMSClient:      vi.fn(function (this: Record<string, unknown>) { this.send = mockKmsSend; }),
  EncryptCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) { Object.assign(this, { __type: 'EncryptCommand', input }); }),
  DecryptCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) { Object.assign(this, { __type: 'DecryptCommand', input }); }),
}));

vi.mock('@aws-sdk/client-ses', () => ({
  SESClient:        vi.fn(function (this: Record<string, unknown>) { this.send = mockSesSend; }),
  SendEmailCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) { Object.assign(this, { __type: 'SendEmailCommand', input }); }),
}));

vi.mock('firebase-admin/app', () => ({
  initializeApp: vi.fn(),
  getApps:       vi.fn(() => [{ name: 'default' }]),
  cert:          vi.fn((obj: unknown) => obj),
}));

vi.mock('firebase-admin/messaging', () => ({
  getMessaging: vi.fn(() => ({ send: mockFcmSend })),
}));

vi.mock('ulid', () => ({
  monotonicFactory: () => {
    let n = 0;
    return () => `TEST-ULID-${String(n++).padStart(4, '0')}`;
  },
}));

// ── Import handler after mocks ────────────────────────────────────────────────

import { handler } from './handler.js';

// ── Helpers ───────────────────────────────────────────────────────────────────

// Module captures GIFT_TOKEN_SECRET at import time; default is 'dev-secret' when env is not set.
// Tests must generate tokens with the same secret the module loaded with.
const TEST_SECRET = 'dev-secret';

/** Generate a valid gift token matching handler's generateGiftToken() */
function makeGiftToken(sessionId = 'SESSION-001'): string {
  const payload = Buffer.from(JSON.stringify({ s: sessionId, t: Date.now() })).toString('base64url');
  const sig     = createHmac('sha256', TEST_SECRET).update(payload).digest('base64url');
  return `${payload}.${sig}`;
}

/** Build an APIGateway-style event for REST tests */
function makeRestEvent(method: string, path: string, body = '', headers: Record<string, string> = {}) {
  return { httpMethod: method, path, body, headers, queryStringParameters: null };
}

/** Build an AppSync resolver event */
function makeAppsyncEvent(fieldName: string, args: Record<string, unknown>, sub = 'cognito-sub-001') {
  return {
    info:      { fieldName },
    identity:  { sub },
    arguments: args,
  };
}

const CATALOG_ITEM_DESC = {
  brandName:      'Woolworths',
  brandColor:     '#00A651',
  distributorSku: 'WOW-50',
  currency:       'AUD',
};
const CATALOG_ITEM = {
  distributorId: 'reloadly',
  desc:           JSON.stringify(CATALOG_ITEM_DESC),
};

const SESSION_SELF = {
  pK:             'GIFT_SESSION#TEST-ULID-0000',
  sK:             'metadata',
  type:           'self',
  permULID:       'perm-001',
  brandId:        'woolworths',
  skuId:          'WOW-50',
  denomination:   50,
  currency:       'AUD',
  distributorId:  'reloadly',
  distributorSku: 'WOW-50',
  brandName:      'Woolworths',
  brandColor:     '#00A651',
  status:         'pending',
  createdAt:      '2026-04-01T00:00:00.000Z',
};

const SESSION_GIFT = {
  ...SESSION_SELF,
  type:               'gift',
  senderPermULID:     'perm-sender',
  recipientEmail:     'hashed-email',
  recipientEmailRaw:  'recipient@example.com',
  senderDisplayName:  'Alice',
  message:            'Happy Birthday!',
};

/** Reloadly mock responses */
const RELOADLY_TOKEN_RESPONSE = { ok: true, json: async () => ({ access_token: 'reloadly-token' }) };
const RELOADLY_FULFIL_RESPONSE = {
  ok:   true,
  json: async () => ({
    transactions: [{ cardNumber: 'GIFT-CARD-NUMBER', pinCode: '9876', expiryDate: '2028-06-30' }],
  }),
};
const RELOADLY_BALANCE_RESPONSE = { ok: true, json: async () => ({ balance: 42.5 }) };
const STRIPE_SESSION_RESPONSE   = { ok: true, json: async () => ({ url: 'https://checkout.stripe.com/pay/test' }) };

// ── Tests ─────────────────────────────────────────────────────────────────────

beforeEach(() => {
  vi.resetAllMocks();
  process.env.USER_TABLE            = 'UserDataEvent';
  process.env.ADMIN_TABLE           = 'AdminDataEvent';
  process.env.REFDATA_TABLE         = 'RefDataEvent';
  process.env.REF_TABLE             = 'RefDataEvent';
  process.env.STRIPE_SECRET_KEY     = 'sk_test_placeholder';
  // Note: GIFT_TOKEN_SECRET and STRIPE_WEBHOOK_SECRET are captured as module-level constants
  // at import time, so setting them here has no effect. Tests are written to work with the
  // module defaults ('dev-secret' and '' respectively).
  process.env.GIFT_CARD_KMS_KEY_ARN = 'arn:aws:kms:us-east-1:123456789:key/test-key';
  process.env.APP_BASE_URL          = 'https://app.bebocard.com';
  delete process.env.RELOADLY_CLIENT_ID;
  delete process.env.RELOADLY_CLIENT_SECRET;
});

// ─────────────────────────────────────────────────────────────────────────────
// AppSync — purchaseForSelf
// ─────────────────────────────────────────────────────────────────────────────

describe('purchaseForSelf', () => {
  it('creates GIFT_SESSION in AdminDataEvent and returns checkoutUrl + sessionId', async () => {
    // 1: resolvePermULID (QueryCommand) → 2: fetchCatalogItem (GetCommand) → 3: PutCommand session
    mockDdbSend
      .mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-001' }] })  // resolvePermULID
      .mockResolvedValueOnce({ Item: CATALOG_ITEM })                  // fetchCatalogItem
      .mockResolvedValueOnce({});                                       // PutCommand GIFT_SESSION

    mockFetch.mockResolvedValueOnce(STRIPE_SESSION_RESPONSE);
    vi.stubGlobal('fetch', mockFetch);

    const res = await handler(makeAppsyncEvent('purchaseForSelf', {
      brandId: 'woolworths', skuId: 'WOW-50', denomination: 50, currency: 'AUD',
    })) as { checkoutUrl: string; sessionId: string };

    expect(res.checkoutUrl).toBe('https://checkout.stripe.com/pay/test');
    expect(res.sessionId).toBeDefined();

    // Verify GIFT_SESSION PutCommand shape
    const putCall = mockDdbSend.mock.calls.find(([cmd]: any[]) =>
      cmd.__type === 'PutCommand' && String(cmd.input?.Item?.pK).startsWith('GIFT_SESSION#'),
    );
    expect(putCall).toBeDefined();
    const item = (putCall![0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(item.type).toBe('self');
    expect(item.denomination).toBe(50);
    expect(item.status).toBe('pending');
    expect(item.distributorId).toBe('reloadly');
  });

  it('throws when catalog item not found', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-001' }] })
      .mockResolvedValueOnce({ Item: undefined }); // catalog miss

    vi.stubGlobal('fetch', mockFetch);

    await expect(
      handler(makeAppsyncEvent('purchaseForSelf', { brandId: 'unknown', skuId: 'X', denomination: 10, currency: 'AUD' })),
    ).rejects.toThrow('Catalog item not found');
  });

  it('throws when user sub cannot be resolved to a permULID', async () => {
    mockDdbSend.mockResolvedValueOnce({ Items: [] }); // no match in GSI

    vi.stubGlobal('fetch', mockFetch);

    await expect(
      handler(makeAppsyncEvent('purchaseForSelf', { brandId: 'woolworths', skuId: 'WOW-50', denomination: 50, currency: 'AUD' })),
    ).rejects.toThrow('No permULID found');
  });

  it('throws when Stripe checkout creation fails', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-001' }] })
      .mockResolvedValueOnce({ Item: CATALOG_ITEM })
      .mockResolvedValueOnce({});

    mockFetch.mockResolvedValueOnce({ ok: false, status: 402, text: async () => 'Card declined' });
    vi.stubGlobal('fetch', mockFetch);

    await expect(
      handler(makeAppsyncEvent('purchaseForSelf', { brandId: 'woolworths', skuId: 'WOW-50', denomination: 50, currency: 'AUD' })),
    ).rejects.toThrow('Stripe checkout session failed');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// AppSync — purchaseAsGift
// ─────────────────────────────────────────────────────────────────────────────

describe('purchaseAsGift', () => {
  it('creates GIFT_SESSION with gift type and hashed recipientEmail', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-sender' }] })
      .mockResolvedValueOnce({ Item: CATALOG_ITEM })
      .mockResolvedValueOnce({});

    mockFetch.mockResolvedValueOnce(STRIPE_SESSION_RESPONSE);
    vi.stubGlobal('fetch', mockFetch);

    const res = await handler(makeAppsyncEvent('purchaseAsGift', {
      brandId: 'woolworths', skuId: 'WOW-50', denomination: 50, currency: 'AUD',
      recipientEmail: 'friend@example.com', senderDisplayName: 'Alice', message: 'Happy Birthday!',
    })) as { checkoutUrl: string; sessionId: string };

    expect(res.checkoutUrl).toBe('https://checkout.stripe.com/pay/test');

    const putCall = mockDdbSend.mock.calls.find(([cmd]: any[]) =>
      cmd.__type === 'PutCommand' && String(cmd.input?.Item?.pK).startsWith('GIFT_SESSION#'),
    );
    const item = (putCall![0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(item.type).toBe('gift');
    expect(item.senderDisplayName).toBe('Alice');
    expect(item.message).toBe('Happy Birthday!');
    // recipientEmail should be hashed (not plain text)
    expect(item.recipientEmail).not.toBe('friend@example.com');
    expect(item.recipientEmail).toHaveLength(64); // SHA-256 hex
    // Raw email stored for SES delivery (to be removed post-send)
    expect(item.recipientEmailRaw).toBe('friend@example.com');
  });

  it('throws when recipientEmail is missing', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-sender' }] })
      .mockResolvedValueOnce({ Item: CATALOG_ITEM })
      .mockResolvedValueOnce({});

    vi.stubGlobal('fetch', mockFetch);

    await expect(
      handler(makeAppsyncEvent('purchaseAsGift', {
        brandId: 'woolworths', skuId: 'WOW-50', denomination: 50, currency: 'AUD',
        recipientEmail: '',
      })),
    ).rejects.toThrow('recipientEmail is required');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// REST — POST /webhook (Stripe)
// ─────────────────────────────────────────────────────────────────────────────

describe('Stripe webhook', () => {
  function makeWebhookBody(type: string, metadata: Record<string, string>) {
    return JSON.stringify({
      type,
      data: { object: { metadata } },
    });
  }

  it('returns 200 immediately for non-checkout events', async () => {
    const event = makeRestEvent('POST', '/webhook', makeWebhookBody('payment_intent.created', {}));
    const res = await handler(event) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body).received).toBe(true);
    expect(mockDdbSend).not.toHaveBeenCalled();
  });

  it('returns 400 when sessionId missing from metadata', async () => {
    const event = makeRestEvent('POST', '/webhook', makeWebhookBody('checkout.session.completed', {}));
    const res = await handler(event) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body).error).toMatch(/sessionId/i);
  });

  it('returns 404 when session not found in AdminDataEvent', async () => {
    mockDdbSend.mockResolvedValueOnce({ Item: undefined }); // session miss
    const event = makeRestEvent('POST', '/webhook', makeWebhookBody('checkout.session.completed', { sessionId: 'SESSION-X' }));
    const res = await handler(event) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(404);
  });

  it('skips idempotently when session is already fulfilled', async () => {
    mockDdbSend.mockResolvedValueOnce({ Item: { ...SESSION_SELF, status: 'fulfilled' } });
    const event = makeRestEvent('POST', '/webhook', makeWebhookBody('checkout.session.completed', { sessionId: 'SESSION-X' }));
    const res = await handler(event) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body).skipped).toBe(true);
    // No PutCommand to USER_TABLE
    const putCalls = mockDdbSend.mock.calls.filter(([cmd]: any[]) => cmd.__type === 'PutCommand');
    expect(putCalls).toHaveLength(0);
  });

  it('fulfils self-purchase: writes card to USER_TABLE and marks session fulfilled', async () => {
    // 1: GetCommand session → 2: UpdateCommand mark processing → 3+4: fetch (reloadly auth + fulfil)
    // → 5: PutCommand card to USER_TABLE → 6: GetCommand DEVICE_TOKEN → 7: UpdateCommand mark fulfilled
    mockDdbSend
      .mockResolvedValueOnce({ Item: SESSION_SELF })    // GetCommand session
      .mockResolvedValueOnce({})                         // UpdateCommand mark processing
      .mockResolvedValueOnce({})                         // PutCommand card to USER_TABLE
      .mockResolvedValueOnce({ Item: null })             // GetCommand DEVICE_TOKEN (no token = skip FCM)
      .mockResolvedValueOnce({});                        // UpdateCommand mark fulfilled

    // Reloadly: auth token then fulfil
    mockFetch
      .mockResolvedValueOnce(RELOADLY_TOKEN_RESPONSE)
      .mockResolvedValueOnce(RELOADLY_FULFIL_RESPONSE);
    vi.stubGlobal('fetch', mockFetch);

    // Set Reloadly env so the client doesn't throw
    process.env.RELOADLY_CLIENT_ID     = 'rl-id';
    process.env.RELOADLY_CLIENT_SECRET = 'rl-secret';

    const event = makeRestEvent('POST', '/webhook', makeWebhookBody('checkout.session.completed', { sessionId: 'SESSION-001' }));
    const res = await handler(event) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body).received).toBe(true);

    // Card written to USER_TABLE
    const cardPut = mockDdbSend.mock.calls.find(([cmd]: any[]) =>
      cmd.__type === 'PutCommand' && String(cmd.input?.Item?.pK).startsWith('USER#'),
    );
    expect(cardPut).toBeDefined();
    const cardItem = (cardPut![0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(cardItem.eventType).toBe('GIFTCARD');
    expect(JSON.parse(cardItem.desc as string).cardNumber).toBe('GIFT-CARD-NUMBER');

    // Session marked fulfilled
    const fulfilledUpdate = mockDdbSend.mock.calls.find(([cmd]: any[]) =>
      cmd.__type === 'UpdateCommand' &&
      cmd.input?.ExpressionAttributeValues?.[':fulfilled'] === 'fulfilled',
    );
    expect(fulfilledUpdate).toBeDefined();
  });

  it('fulfils gift-purchase: creates GIFT# record, sends SES email, removes raw email', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Item: SESSION_GIFT })   // GetCommand session
      .mockResolvedValueOnce({})                        // UpdateCommand mark processing
      .mockResolvedValueOnce({})                        // KMS encrypt via kms.send
      .mockResolvedValueOnce({})                        // PutCommand GIFT# AdminDataEvent
      .mockResolvedValueOnce({})                        // UpdateCommand remove recipientEmailRaw
      .mockResolvedValueOnce({});                       // UpdateCommand mark fulfilled

    // KMS encrypt returns a fake ciphertext
    mockKmsSend.mockResolvedValueOnce({
      CiphertextBlob: Buffer.from('encrypted-card-data'),
    });
    mockSesSend.mockResolvedValueOnce({});

    mockFetch
      .mockResolvedValueOnce(RELOADLY_TOKEN_RESPONSE)
      .mockResolvedValueOnce(RELOADLY_FULFIL_RESPONSE);
    vi.stubGlobal('fetch', mockFetch);

    process.env.RELOADLY_CLIENT_ID     = 'rl-id';
    process.env.RELOADLY_CLIENT_SECRET = 'rl-secret';

    const event = makeRestEvent('POST', '/webhook', makeWebhookBody('checkout.session.completed', { sessionId: 'SESSION-001' }));
    const res = await handler(event) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);

    // GIFT# record written to AdminDataEvent
    const giftPut = mockDdbSend.mock.calls.find(([cmd]: any[]) =>
      cmd.__type === 'PutCommand' && String(cmd.input?.Item?.pK).startsWith('GIFT#'),
    );
    expect(giftPut).toBeDefined();
    const giftItem = (giftPut![0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(giftItem.status).toBe('pending');
    expect(giftItem.encryptedCard).toBeDefined();
    expect(giftItem.senderDisplayName).toBe('Alice');

    // SES email sent
    expect(mockSesSend).toHaveBeenCalledOnce();

    // recipientEmailRaw removed (UpdateCommand with REMOVE expression)
    const rawEmailRemoval = mockDdbSend.mock.calls.find(([cmd]: any[]) =>
      cmd.__type === 'UpdateCommand' &&
      String(cmd.input?.UpdateExpression ?? '').includes('REMOVE recipientEmailRaw'),
    );
    expect(rawEmailRemoval).toBeDefined();
  });

  it('rejects with SyntaxError for malformed (non-JSON) webhook body', async () => {
    // STRIPE_WEBHOOK_SECRET is a module-level constant (empty at test import time) so sig check
    // is skipped. handleRest uses `return` (not `await`) on handleStripeWebhook, so the catch
    // block doesn't intercept async rejections — SyntaxError from JSON.parse propagates.
    const event = makeRestEvent('POST', '/webhook', 'not-valid-json');
    await expect(handler(event)).rejects.toThrow(SyntaxError);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// REST — GET /gift/:token (gift claim)
// ─────────────────────────────────────────────────────────────────────────────

describe('gift claim (GET /gift/:token)', () => {
  it('returns 401 for a token with wrong HMAC signature', async () => {
    const badToken = 'validpayload.invalidsignature';
    const event = makeRestEvent('GET', `/gift/${badToken}`);
    const res = await handler(event) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(401);
    expect(JSON.parse(res.body).error).toMatch(/invalid/i);
  });

  it('returns 401 for a token with only one segment', async () => {
    const event = makeRestEvent('GET', '/gift/onlyone');
    const res = await handler(event) as { statusCode: number };
    expect(res.statusCode).toBe(401);
  });

  it('returns 404 when gift record not found', async () => {
    const token = makeGiftToken('session-unknown');
    mockDdbSend.mockResolvedValueOnce({ Item: undefined });
    const event = makeRestEvent('GET', `/gift/${token}`);
    const res = await handler(event) as { statusCode: number };
    expect(res.statusCode).toBe(404);
  });

  it('returns 409 when gift has already been claimed', async () => {
    const token = makeGiftToken('session-claimed');
    mockDdbSend.mockResolvedValueOnce({
      Item: { status: 'claimed', expiresAt: new Date(Date.now() + 1e9).toISOString() },
    });
    const event = makeRestEvent('GET', `/gift/${token}`);
    const res = await handler(event) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(409);
    expect(JSON.parse(res.body).error).toMatch(/claimed/i);
  });

  it('returns 410 when gift has expired', async () => {
    const token = makeGiftToken('session-expired');
    mockDdbSend.mockResolvedValueOnce({
      Item: {
        status:    'pending',
        expiresAt: new Date(Date.now() - 1000).toISOString(), // in the past
      },
    });
    const event = makeRestEvent('GET', `/gift/${token}`);
    const res = await handler(event) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(410);
    expect(JSON.parse(res.body).error).toMatch(/expired/i);
  });

  it('returns 200 with decrypted card details and marks gift as claimed', async () => {
    const token   = makeGiftToken('session-valid');
    const cardPayload = JSON.stringify({ cardNumber: 'CLAIM-CARD', pin: '4321' });

    mockDdbSend
      .mockResolvedValueOnce({             // GetCommand GIFT# record
        Item: {
          status:            'pending',
          expiresAt:         new Date(Date.now() + 1e9).toISOString(),
          brandName:         'Woolworths',
          brandId:           'woolworths',
          denomination:      50,
          currency:          'AUD',
          encryptedCard:     Buffer.from('cipher').toString('base64'),
          senderDisplayName: 'Alice',
          message:           'For you!',
        },
      })
      .mockResolvedValueOnce({});          // UpdateCommand mark claimed

    // KMS decrypt returns the card payload
    mockKmsSend.mockResolvedValueOnce({
      Plaintext: Buffer.from(cardPayload, 'utf-8'),
    });

    const event = makeRestEvent('GET', `/gift/${token}`);
    const res   = await handler(event) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);

    const body = JSON.parse(res.body) as Record<string, unknown>;
    expect(body.cardNumber).toBe('CLAIM-CARD');
    expect(body.pin).toBe('4321');
    expect(body.brandName).toBe('Woolworths');
    expect(body.senderDisplayName).toBe('Alice');
    expect(body.cardSK).toMatch(/^GIFTCARD#/);

    // Verify UpdateCommand removes encryptedCard
    const claimUpdate = mockDdbSend.mock.calls.find(([cmd]: any[]) =>
      cmd.__type === 'UpdateCommand' &&
      String(cmd.input?.UpdateExpression ?? '').includes('REMOVE encryptedCard'),
    );
    expect(claimUpdate).toBeDefined();

    // UpdateCommand should set status = 'claimed'
    const vals = (claimUpdate![0] as { input: { ExpressionAttributeValues: Record<string, unknown> } })
      .input.ExpressionAttributeValues;
    expect(vals[':claimed']).toBe('claimed');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// AppSync — syncGiftCardBalance
// ─────────────────────────────────────────────────────────────────────────────

describe('syncGiftCardBalance', () => {
  it('fetches live balance from distributor and updates USER_TABLE', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-001' }] })  // resolvePermULID
      .mockResolvedValueOnce({                                         // GetCommand card
        Item: {
          desc: JSON.stringify({
            cardNumber:     'GIFT-CARD-NUMBER',
            balance:        50,
            distributorId:  'reloadly',
            distributorSku: 'WOW-50',
          }),
        },
      })
      .mockResolvedValueOnce({});                                      // UpdateCommand balance

    mockFetch
      .mockResolvedValueOnce(RELOADLY_TOKEN_RESPONSE)
      .mockResolvedValueOnce(RELOADLY_BALANCE_RESPONSE);
    vi.stubGlobal('fetch', mockFetch);

    process.env.RELOADLY_CLIENT_ID     = 'rl-id';
    process.env.RELOADLY_CLIENT_SECRET = 'rl-secret';

    const res = await handler(makeAppsyncEvent('syncGiftCardBalance', {
      cardSK: 'GIFTCARD#session-001',
    })) as { balance: number; lastSyncAt: string };

    expect(res.balance).toBe(42.5);
    expect(res.lastSyncAt).toBeDefined();

    // UpdateCommand writes new balance back
    const balanceUpdate = mockDdbSend.mock.calls.find(([cmd]: any[]) =>
      cmd.__type === 'UpdateCommand',
    );
    const desc = JSON.parse(
      ((balanceUpdate![0] as { input: { ExpressionAttributeValues: Record<string, unknown> } })
        .input.ExpressionAttributeValues[':desc'] as string),
    ) as Record<string, unknown>;
    expect(desc.balance).toBe(42.5);
  });

  it('throws when card has no distributor info', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-001' }] })
      .mockResolvedValueOnce({
        Item: { desc: JSON.stringify({ cardNumber: 'MANUAL-CARD', balance: 20 }) }, // no distributorId
      });

    vi.stubGlobal('fetch', mockFetch);

    await expect(
      handler(makeAppsyncEvent('syncGiftCardBalance', { cardSK: 'GIFTCARD#manual-001' })),
    ).rejects.toThrow('no distributor info');
  });

  it('throws when card record not found', async () => {
    mockDdbSend
      .mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-001' }] })
      .mockResolvedValueOnce({ Item: undefined });

    vi.stubGlobal('fetch', mockFetch);

    await expect(
      handler(makeAppsyncEvent('syncGiftCardBalance', { cardSK: 'GIFTCARD#missing' })),
    ).rejects.toThrow('Card not found');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// REST — unknown path
// ─────────────────────────────────────────────────────────────────────────────

describe('REST routing', () => {
  it('returns 404 for unknown REST path', async () => {
    const event = makeRestEvent('GET', '/unknown/path');
    const res = await handler(event) as { statusCode: number };
    expect(res.statusCode).toBe(404);
  });
});
