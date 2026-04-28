import { vi, describe, it, expect, beforeEach } from 'vitest';

const { mockDdbSend, mockSsmSend, mockFetch } = vi.hoisted(() => ({
  mockDdbSend: vi.fn(),
  mockSsmSend: vi.fn(),
  mockFetch: vi.fn(),
}));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));
vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockDdbSend }) },
  BatchWriteCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'BatchWriteCommand', input });
  }),
  ScanCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'ScanCommand', input });
  }),
}));
vi.mock('@aws-sdk/client-ssm', () => ({
  SSMClient: vi.fn(function (this: Record<string, unknown>) {
    this.send = mockSsmSend;
  }),
  GetParameterCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetParameterCommand', input });
  }),
}));

// Mock global fetch
vi.stubGlobal('fetch', mockFetch);

import { handler } from './handler.js';

// ── Helpers ──────────────────────────────────────────────────────────────────

function setupSsmTableName(tableName = 'RefDataEvent-test') {
  mockSsmSend.mockResolvedValue({ Parameter: { Value: tableName } });
}

function makeAffiliateFeed(overrides: Partial<{
  Id: number; Name: string; MerchantName: string; Description: string;
  TrackingUrl: string; MerchantLogoUrl: string; EndDate: string; Category: string;
}>[] = [{}]) {
  return overrides.map((o, i) => ({
    Id: o.Id ?? 1001 + i,
    Name: o.Name ?? `Offer ${i}`,
    MerchantName: o.MerchantName ?? 'TestBrand',
    Description: o.Description ?? 'A test offer',
    TrackingUrl: o.TrackingUrl ?? `https://t.cf.com/test${i}`,
    MerchantLogoUrl: o.MerchantLogoUrl ?? 'https://cdn.bebocard.com/test/logo.png',
    EndDate: o.EndDate ?? '2026-12-31T23:59:59Z',
    Category: o.Category ?? 'general',
  }));
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('affiliate-feed-sync handler', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.REFDATA_TABLE_PARAM = '/bebo/test/REFDATA_TABLE';
    delete process.env.AFFILIATE_API_KEY;
    setupSsmTableName();
  });

  describe('SSM table name lookup', () => {
    it('fetches table name from SSM via REFDATA_TABLE_PARAM on first invocation', async () => {
      mockDdbSend.mockResolvedValue({});
      // No API key → mock sync path, but SSM is still called first
      await handler({} as never, {} as never, () => {});

      // The module caches the table name after the first call, so SSM is only hit once per Lambda lifetime.
      // We only assert the param name is correct here — the throw-on-empty path is a cold-start concern.
      expect(mockSsmSend).toHaveBeenCalled();
      const ssmCall = mockSsmSend.mock.calls[0][0] as { input: { Name: string } };
      expect(ssmCall.input.Name).toBe('/bebo/test/REFDATA_TABLE');
    });
  });

  describe('mock sync fallback (no AFFILIATE_API_KEY)', () => {
    it('writes 7 mock offers to DynamoDB when no API key is set', async () => {
      mockDdbSend.mockResolvedValue({});

      const result = await handler({} as never, {} as never, () => {});
      const res = result as { statusCode: number; body: string };

      expect(res.statusCode).toBe(200);
      const body = JSON.parse(res.body);
      expect(body.count).toBe(7);

      const batchCalls = mockDdbSend.mock.calls as [{ __type: string; input: Record<string, unknown> }][];
      const writeCalls = batchCalls.filter(([cmd]) => cmd.__type === 'BatchWriteCommand');
      expect(writeCalls.length).toBeGreaterThan(0);

      // All items should use the SSM-resolved table name
      const firstCall = writeCalls[0][0].input as { RequestItems: Record<string, unknown[]> };
      expect(Object.keys(firstCall.RequestItems)[0]).toBe('RefDataEvent-test');
    });

    it('mock offers have correct schema fields', async () => {
      mockDdbSend.mockResolvedValue({});
      await handler({} as never, {} as never, () => {});

      const batchCalls = mockDdbSend.mock.calls as [{ __type: string; input: { RequestItems: Record<string, { PutRequest: { Item: Record<string, unknown> } }[]> } }][];
      const firstBatch = batchCalls.find(([cmd]) => cmd.__type === 'BatchWriteCommand')!;
      const item = firstBatch[0].input.RequestItems['RefDataEvent-test'][0].PutRequest.Item;

      expect(item.pK).toMatch(/^BEBO_OFFER#CF_/);
      expect(item.sK).toBe('offer');
      expect(item.eventType).toBe('AFFILIATE_OFFER');
      expect(item.primaryCat).toBe('curated_offer');
      expect(item.status).toBe('ACTIVE');
    });
  });

  describe('real sync (AFFILIATE_API_KEY present)', () => {
    beforeEach(() => {
      process.env.AFFILIATE_API_KEY = 'cf-real-api-key';
    });

    it('calls Commission Factory API with the correct header', async () => {
      mockDdbSend.mockResolvedValue({ Items: [] });
      mockFetch.mockResolvedValue({ ok: true, json: async () => makeAffiliateFeed() });

      await handler({} as never, {} as never, () => {});

      expect(mockFetch).toHaveBeenCalledOnce();
      const [url, opts] = mockFetch.mock.calls[0] as [string, RequestInit];
      expect(url).toContain('commissionfactory.com');
      expect((opts.headers as Record<string, string>)['X-ApiKey']).toBe('cf-real-api-key');
    });

    it('writes new offers that do not exist in the table', async () => {
      mockDdbSend.mockResolvedValueOnce({ Items: [] }); // ScanCommand — no existing offers
      mockDdbSend.mockResolvedValue({});                // BatchWriteCommand

      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => makeAffiliateFeed([{ Id: 2001, Name: '20% Off' }]),
      });

      const result = await handler({} as never, {} as never, () => {}) as { statusCode: number; body: string };
      expect(result.statusCode).toBe(200);

      const batchCalls = mockDdbSend.mock.calls as [{ __type: string }][];
      expect(batchCalls.some(([cmd]) => cmd.__type === 'BatchWriteCommand')).toBe(true);
    });

    it('skips offers whose contentHash is unchanged (delta-sync)', async () => {
      const { createHash } = await import('crypto');
      const offer = makeAffiliateFeed([{ Id: 3001, Name: '10% Off', Description: 'Save 10%', TrackingUrl: 'https://t.cf.com/x' }])[0];
      const contentHash = createHash('sha256')
        .update(JSON.stringify({ title: offer.Name, desc: offer.Description, url: offer.TrackingUrl }))
        .digest('hex');

      // Scan returns an existing offer with matching hash
      mockDdbSend.mockResolvedValueOnce({
        Items: [{ pK: `BEBO_OFFER#CF_3001`, desc: { contentHash } }],
      });
      mockDdbSend.mockResolvedValue({});

      mockFetch.mockResolvedValue({ ok: true, json: async () => [offer] });

      await handler({} as never, {} as never, () => {});

      // No BatchWriteCommand should be issued (nothing changed)
      const batchCalls = mockDdbSend.mock.calls as [{ __type: string }][];
      expect(batchCalls.some(([cmd]) => cmd.__type === 'BatchWriteCommand')).toBe(false);
    });

    it('marks removed offers as EXPIRED', async () => {
      // Scan returns an offer that is NOT in the live feed
      mockDdbSend.mockResolvedValueOnce({
        Items: [{ pK: 'BEBO_OFFER#CF_9999', desc: { contentHash: 'old-hash' } }],
      });
      mockDdbSend.mockResolvedValue({});

      // Feed returns a different offer (9999 is no longer present)
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => makeAffiliateFeed([{ Id: 8888, Name: 'New Offer' }]),
      });

      await handler({} as never, {} as never, () => {});

      const batchCalls = mockDdbSend.mock.calls as [{ __type: string; input: { RequestItems: Record<string, { PutRequest: { Item: Record<string, unknown> } }[]> } }][];
      const batchCall = batchCalls.find(([cmd]) => cmd.__type === 'BatchWriteCommand')!;
      const items = batchCall[0].input.RequestItems['RefDataEvent-test'];
      const expiredItem = items.find(r => r.PutRequest.Item.pK === 'BEBO_OFFER#CF_9999');
      expect(expiredItem).toBeDefined();
      expect(expiredItem!.PutRequest.Item.status).toBe('EXPIRED');
    });

    it('returns 500 and does not throw when Commission Factory API returns an error', async () => {
      mockDdbSend.mockResolvedValue({ Items: [] });
      mockFetch.mockResolvedValue({ ok: false, status: 503, statusText: 'Service Unavailable' });

      const result = await handler({} as never, {} as never, () => {}) as { statusCode: number };
      expect(result.statusCode).toBe(500);
    });

    it('uses the reserved word-safe FilterExpression with ExpressionAttributeNames for status', async () => {
      mockDdbSend.mockResolvedValueOnce({ Items: [] });
      mockDdbSend.mockResolvedValue({});
      mockFetch.mockResolvedValue({ ok: true, json: async () => [] });

      await handler({} as never, {} as never, () => {});

      const ddbCalls = mockDdbSend.mock.calls as [{ __type: string; input: Record<string, unknown> }][];
      const scanCall = ddbCalls.find(([cmd]) => cmd.__type === 'ScanCommand')!;
      expect(scanCall[0].input.ExpressionAttributeNames).toHaveProperty('#st', 'status');
      expect(scanCall[0].input.FilterExpression).toContain('#st');
    });
  });
});
