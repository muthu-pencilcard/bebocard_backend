import { vi, describe, it, expect, beforeEach } from 'vitest';

// ── Mocks ─────────────────────────────────────────────────────────────────────

const { mockDynSend, mockKmsSend, mockFcmSend } = vi.hoisted(() => ({
  mockDynSend: vi.fn(),
  mockKmsSend: vi.fn(),
  mockFcmSend: vi.fn(),
}));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockDynSend }) },
  ScanCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'ScanCommand', input });
  }),
  PutCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutCommand', input });
  }),
  UpdateCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'UpdateCommand', input });
  }),
  GetCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-kms', () => ({
  KMSClient: vi.fn(function (this: Record<string, unknown>) { this.send = mockKmsSend; }),
  DecryptCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'DecryptCommand', input });
  }),
}));

vi.mock('firebase-admin/app', () => ({
  initializeApp: vi.fn(),
  getApps: vi.fn(() => [{ name: 'app' }]),
  cert: vi.fn((x: unknown) => x),
}));

vi.mock('firebase-admin/messaging', () => ({
  getMessaging: vi.fn(() => ({ send: mockFcmSend })),
}));

import { handler } from './handler.js';

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeExpiredGift(overrides: Partial<Record<string, unknown>> = {}): Record<string, unknown> {
  return {
    pK: 'GIFT#gift-001',
    sK: 'metadata',
    status: 'pending',
    expiresAt: '2026-01-01T00:00:00.000Z', // in the past
    senderPermULID: 'PERM-SENDER-001',
    brandId: 'woolworths',
    brandName: 'Woolworths',
    denomination: 50,
    currency: 'AUD',
    encryptedCard: Buffer.from(JSON.stringify({ cardNumber: '1234-5678', pin: '9999' })).toString('base64'),
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  process.env.ADMIN_TABLE = 'test-admin-table';
  process.env.USER_TABLE  = 'test-user-table';
  process.env.FIREBASE_SERVICE_ACCOUNT_JSON = JSON.stringify({ type: 'service_account' });
  mockFcmSend.mockResolvedValue('msg-id');
});

// ── No expired gifts ───────────────────────────────────────────────────────────

describe('no expired gifts', () => {
  it('does not call KMS or write anything when the scan returns no results', async () => {
    mockDynSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });
    await (handler as () => Promise<void>)();
    expect(mockKmsSend).not.toHaveBeenCalled();
    const puts = mockDynSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    expect(puts).toHaveLength(0);
  });
});

// ── Expired gift processing ────────────────────────────────────────────────────

describe('expired gift refund', () => {
  it('decrypts the card, writes it back to sender wallet, marks GIFT record as refunded, and notifies sender', async () => {
    const decryptedPayload = { cardNumber: '1234-5678', pin: '9999' };

    mockKmsSend.mockResolvedValue({
      Plaintext: Buffer.from(JSON.stringify(decryptedPayload)),
    });

    mockDynSend.mockImplementation((cmd: { __type: string; input?: { ExclusiveStartKey?: unknown } }) => {
      if (cmd.__type === 'ScanCommand' && !cmd.input?.ExclusiveStartKey) {
        return Promise.resolve({ Items: [makeExpiredGift()], LastEvaluatedKey: undefined });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      if (cmd.__type === 'UpdateCommand') return Promise.resolve({});
      if (cmd.__type === 'GetCommand') {
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'fcm-token' }) } });
      }
      return Promise.resolve({ Items: [] });
    });

    await (handler as () => Promise<void>)();

    // KMS decrypt called once
    expect(mockKmsSend).toHaveBeenCalledOnce();

    // Two PutCommands: GIFTCARD# (wallet) + RECEIPT# (Finance tab)
    const putCalls = mockDynSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    expect(putCalls).toHaveLength(2);

    // First PutCommand: GIFTCARD# back to sender wallet
    const putItem = (putCalls[0][0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(putItem.pK).toBe('USER#PERM-SENDER-001');
    expect(putItem.eventType).toBe('GIFTCARD');
    const desc = JSON.parse(putItem.desc as string);
    expect(desc.cardNumber).toBe('1234-5678');
    expect(desc.source).toBe('refunded_gift');

    // Second PutCommand: RECEIPT# in Finance tab
    const receiptItem = (putCalls[1][0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(receiptItem.pK).toBe('USER#PERM-SENDER-001');
    expect(receiptItem.eventType).toBe('RECEIPT');
    const receiptDesc = JSON.parse(receiptItem.desc as string);
    expect(receiptDesc.receiptType).toBe('GIFT_CARD_REFUND');
    expect(receiptDesc.brandId).toBe('woolworths');

    // UpdateCommand to mark GIFT# record as refunded
    const updateCalls = mockDynSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'UpdateCommand',
    );
    expect(updateCalls).toHaveLength(1);

    // FCM notification to sender
    expect(mockFcmSend).toHaveBeenCalledOnce();
    const fcmPayload = mockFcmSend.mock.calls[0][0] as { notification: { title: string } };
    expect(fcmPayload.notification.title).toContain('Unclaimed Gift Returned');
  });

  it('continues processing remaining gifts after one KMS failure', async () => {
    mockKmsSend
      .mockRejectedValueOnce(new Error('KMS unavailable'))
      .mockResolvedValue({ Plaintext: Buffer.from(JSON.stringify({ cardNumber: '9999', pin: '0000' })) });

    mockDynSend.mockImplementation((cmd: { __type: string; input?: { ExclusiveStartKey?: unknown } }) => {
      if (cmd.__type === 'ScanCommand' && !cmd.input?.ExclusiveStartKey) {
        return Promise.resolve({
          Items: [makeExpiredGift({ pK: 'GIFT#gift-001' }), makeExpiredGift({ pK: 'GIFT#gift-002' })],
          LastEvaluatedKey: undefined,
        });
      }
      if (cmd.__type === 'GetCommand') {
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'fcm-token' }) } });
      }
      return Promise.resolve({});
    });

    // Should not throw even though gift-001 fails
    await (handler as () => Promise<void>)();

    // Only gift-002 is processed successfully — 2 PutCommands (GIFTCARD# + RECEIPT#)
    const putCalls = mockDynSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    expect(putCalls).toHaveLength(2);
  });

  it('handles pagination (LastEvaluatedKey)', async () => {
    let page = 0;
    mockKmsSend.mockResolvedValue({
      Plaintext: Buffer.from(JSON.stringify({ cardNumber: '1234', pin: '5678' })),
    });
    mockDynSend.mockImplementation((cmd: { __type: string; input?: { ExclusiveStartKey?: unknown } }) => {
      if (cmd.__type === 'ScanCommand') {
        page++;
        if (page === 1) {
          return Promise.resolve({
            Items: [makeExpiredGift({ pK: 'GIFT#gift-page1' })],
            LastEvaluatedKey: { pK: 'GIFT#gift-page1' },
          });
        }
        return Promise.resolve({ Items: [makeExpiredGift({ pK: 'GIFT#gift-page2' })], LastEvaluatedKey: undefined });
      }
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'tok' }) } });
      return Promise.resolve({});
    });

    await (handler as () => Promise<void>)();

    expect(page).toBe(2); // Two scan pages
    const puts = mockDynSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    // 2 gifts × 2 PutCommands each (GIFTCARD# + RECEIPT#) = 4
    expect(puts).toHaveLength(4);
  });

  it('does not send FCM when sender has no device token', async () => {
    mockKmsSend.mockResolvedValue({
      Plaintext: Buffer.from(JSON.stringify({ cardNumber: '1234', pin: '5678' })),
    });
    mockDynSend.mockImplementation((cmd: { __type: string; input?: { ExclusiveStartKey?: unknown } }) => {
      if (cmd.__type === 'ScanCommand' && !cmd.input?.ExclusiveStartKey) {
        return Promise.resolve({ Items: [makeExpiredGift()], LastEvaluatedKey: undefined });
      }
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: undefined }); // no token
      return Promise.resolve({});
    });

    await (handler as () => Promise<void>)();
    expect(mockFcmSend).not.toHaveBeenCalled();
  });
});
