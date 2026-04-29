import { vi, describe, it, expect, beforeEach } from 'vitest';

const { mockS3Send, mockSecretsSend, mockSsmSend } = vi.hoisted(() => {
  process.env.ANALYTICS_BUCKET_PARAM = '/bebocard/test/ANALYTICS_BUCKET';
  process.env.GLOBAL_ANALYTICS_SALT = 'test-global-salt';
  return {
    mockS3Send: vi.fn().mockResolvedValue({}),
    mockSecretsSend: vi.fn(),
    mockSsmSend: vi.fn().mockResolvedValue({ Parameter: { Value: 'test-analytics-bucket' } }),
  };
});

vi.mock('@aws-sdk/client-s3', () => ({
  S3Client: vi.fn().mockImplementation(function (this: any) { this.send = mockS3Send; }),
  PutObjectCommand: vi.fn().mockImplementation(function (input: any) { return { __type: 'PutObjectCommand', input }; }),
}));
vi.mock('@aws-sdk/client-secrets-manager', () => ({
  SecretsManagerClient: vi.fn().mockImplementation(function (this: any) { this.send = mockSecretsSend; }),
  GetSecretValueCommand: vi.fn().mockImplementation(function (input: any) { return { __type: 'GetSecretValueCommand', input }; }),
}));
vi.mock('@aws-sdk/client-ssm', () => ({
  SSMClient: vi.fn().mockImplementation(function (this: any) { this.send = mockSsmSend; }),
  GetParameterCommand: vi.fn().mockImplementation(function (input: any) { return { __type: 'GetParameterCommand', input }; }),
}));

import { handler } from './handler.js';

const makeSqsEvent = (messages: any[]) => ({
  Records: messages.map((m, i) => ({ body: JSON.stringify(m), messageId: `msg-${i}` })),
} as any);

describe('receipt-analytics-processor', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSecretsSend.mockResolvedValue({ SecretString: 'tenant-specific-salt' });
  });

  it('writes analytics row with dual visitor hashes for a BeboCard user', async () => {
    await handler(makeSqsEvent([{
      permULID: 'PERM-USER-123',
      brandId: 'woolworths',
      tenantId: 'woolworths-group',
      merchant: 'Woolies Metro',
      amount: 45.50,
      currency: 'AUD',
      purchaseDate: '2026-04-10',
      category: 'grocery',
      items: [{ name: 'Milk', qty: 2, unit_price: 2.50 }],
    }]), {} as any, vi.fn() as any);

    expect(mockS3Send).toHaveBeenCalledOnce();
    const [putCall] = mockS3Send.mock.calls;
    const putInput = putCall[0].input;

    expect(putInput.Bucket).toBe('test-analytics-bucket');
    expect(putInput.Key).toMatch(/^receipts\/raw\/woolworths-group\/woolworths\/2026-04-10\//);

    const rows = putInput.Body.split('\n').map(JSON.parse);
    expect(rows).toHaveLength(1);
    const row = rows[0];
    expect(row.is_bebocard).toBe(true);
    expect(row.visitor_hash).toBeTypeOf('string');
    expect(row.visitor_hash_tenant).toBeTypeOf('string');
    expect(row.visitor_hash).not.toBe(row.visitor_hash_tenant);
    expect(row.amount).toBe(45.50);
    expect(row.merchant).toBe('Woolies Metro');
    expect(row.visitor_hash).not.toContain('PERM-USER-123');
  });

  it('writes null visitor hashes for anonymous walk-ins', async () => {
    await handler(makeSqsEvent([{
      permULID: null,
      brandId: 'myer',
      merchant: 'Myer City',
      amount: 120,
      purchaseDate: '2026-04-10',
    }]), {} as any, vi.fn() as any);

    const rows = mockS3Send.mock.calls[0][0].input.Body.split('\n').map(JSON.parse);
    const row = rows[0];
    expect(row.is_bebocard).toBe(false);
    expect(row.visitor_hash).toBeNull();
    expect(row.visitor_hash_tenant).toBeNull();
  });

  it('treats ANON# permULID as anonymous', async () => {
    await handler(makeSqsEvent([{
      permULID: 'ANON#abc123',
      brandId: 'coles',
      merchant: 'Coles Express',
      amount: 30,
      purchaseDate: '2026-04-10',
    }]), {} as any, vi.fn() as any);

    const rows = mockS3Send.mock.calls[0][0].input.Body.split('\n').map(JSON.parse);
    expect(rows[0].is_bebocard).toBe(false);
    expect(rows[0].visitor_hash).toBeNull();
  });

  it('groups records from the same partition into a single S3 write', async () => {
    await handler(makeSqsEvent([
      { permULID: 'PERM-A', brandId: 'target', merchant: 'Target CBD', amount: 50, purchaseDate: '2026-04-10' },
      { permULID: 'PERM-B', brandId: 'target', merchant: 'Target Bondi', amount: 30, purchaseDate: '2026-04-10' },
    ]), {} as any, vi.fn() as any);

    expect(mockS3Send).toHaveBeenCalledOnce(); // same tenant/brand/date → single write
    const rows = mockS3Send.mock.calls[0][0].input.Body.split('\n').map(JSON.parse);
    expect(rows).toHaveLength(2);
  });

  it('uses fallback currency AUD when not provided', async () => {
    await handler(makeSqsEvent([{
      permULID: null, brandId: 'kmart', merchant: 'Kmart', amount: 15, purchaseDate: '2026-04-10',
    }]), {} as any, vi.fn() as any);

    const row = JSON.parse(mockS3Send.mock.calls[0][0].input.Body);
    expect(row.currency).toBe('AUD');
  });
});
