import { vi, describe, it, expect, beforeEach } from 'vitest';

const { mockSend } = vi.hoisted(() => {
  process.env.RECEIPT_SIGNING_KEY_ID = 'test-key-id';
  process.env.USER_TABLE = 'test-user-table';
  return { mockSend: vi.fn() };
});

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockImplementation(() => ({ send: mockSend })) },
  PutCommand: vi.fn().mockImplementation(function (input: any) { return { __type: 'PutCommand', input }; }),
  GetCommand: vi.fn().mockImplementation(function (input: any) { return { __type: 'GetCommand', input }; }),
}));
vi.mock('@aws-sdk/client-kms', () => ({
  KMSClient: vi.fn().mockImplementation(function (this: any) { this.send = mockSend; }),
  SignCommand: vi.fn().mockImplementation(function (input: any) { return { __type: 'SignCommand', input }; }),
}));
vi.mock('../shared/idempotency', () => ({
  idempotentPut: vi.fn().mockResolvedValue({ success: true }),
}));
vi.mock('firebase-admin/app', () => ({ initializeApp: vi.fn(), getApps: vi.fn(() => []), cert: vi.fn() }));
vi.mock('firebase-admin/messaging', () => ({ getMessaging: vi.fn(() => ({ send: vi.fn() })) }));
vi.mock('ulid', () => ({ monotonicFactory: () => () => 'TEST-ULID' }));

import { handler } from './handler.js';

describe('Receipt Processor Signing (P3-4)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockResolvedValue({ 
      Signature: Buffer.from('test-signature'),
      Item: { desc: JSON.stringify({ token: 'test-token' }) }
    });
  });

  const makeSqsEvent = (body: any) => ({
    Records: [{ body: JSON.stringify(body), messageId: 'msg-1' }]
  } as any);

  it('signs receipt with KMS and stores signature in DynamoDB', async () => {
    const task = {
      permULID: 'USER-123',
      brandId: 'woolworths',
      merchant: 'Woolies Metro',
      amount: 50.0,
      purchaseDate: '2026-04-10',
      isInvoice: false,
      secondaryULID: 'SCAN-123'
    };

    const event = makeSqsEvent(task);
    await handler(event, {} as any, () => {});

    // Verify KMS Sign check
    const kmsCall = mockSend.mock.calls.find(c => c[0].__type === 'SignCommand');
    expect(kmsCall).toBeDefined();
    expect(kmsCall![0].input.KeyId).toBe('test-key-id');
    expect(kmsCall![0].input.SigningAlgorithm).toBe('RSASSA_PSS_SHA_256');

    // Verify DynamoDB Put check
    const ddbCall = mockSend.mock.calls.find(c => c[0].__type === 'PutCommand' && c[0].input.Item.sK.startsWith('RECEIPT#'));
    expect(ddbCall).toBeDefined();
    const desc = JSON.parse(ddbCall![0].input.Item.desc);
    expect(desc.signature).toBe(Buffer.from('test-signature').toString('base64'));
    expect(desc.signingAlgorithm).toBe('RSASSA_PSS_SHA_256');
  });
});
