import { vi, describe, it, expect, beforeEach } from 'vitest';

const { mockSend } = vi.hoisted(() => ({
  mockSend: vi.fn(),
}));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockSend }) },
  PutCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutCommand', input });
  }),
  ScanCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'ScanCommand', input });
  }),
  UpdateCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'UpdateCommand', input });
  }),
}));

import { handler } from './handler.js';

beforeEach(() => {
  vi.clearAllMocks();
  process.env.REFDATA_TABLE = 'test-ref-table';
  // Clear all distributor API keys so distributors are skipped by default
  delete process.env.PREZZEE_API_KEY;
  delete process.env.TANGO_API_KEY;
  delete process.env.RUNA_API_KEY;
  delete process.env.YOUGOTAGIFT_API_KEY;
  delete process.env.RELOADLY_CLIENT_ID;
  delete process.env.RELOADLY_CLIENT_SECRET;
  // ScanCommand for deactivateRemovedSkus — return no active items
  mockSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });
});

describe('catalog-sync — no API keys configured', () => {
  it('skips all distributors and returns zero upserts when no keys are set', async () => {
    const results = await handler({} as never, {} as never, () => {});
    expect(results).toMatchObject({
      prezzee:     { upserted: 0, errors: 0 },
      tango:       { upserted: 0, errors: 0 },
      runa:        { upserted: 0, errors: 0 },
      yougotagift: { upserted: 0, errors: 0 },
      reloadly:    { upserted: 0, errors: 0 },
    });
    // No DynamoDB PutCommand calls when no items are returned
    const putCalls = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    expect(putCalls).toHaveLength(0);
  });
});

describe('catalog-sync — Prezzee', () => {
  beforeEach(() => {
    process.env.PREZZEE_API_KEY = 'test-prezzee-key';
  });

  it('upserts catalog items returned by Prezzee API', async () => {
    const mockProducts = {
      products: [
        {
          productId: 'prez-001',
          name: 'Woolworths',
          logoUrl: 'https://cdn.prezzee.com/woolworths.png',
          category: 'grocery',
          faceValue: 50,
          currency: 'AUD',
          expiryPolicy: '3 years',
          howToRedeem: 'Show at checkout',
        },
        {
          productId: 'prez-002',
          name: 'JB Hi-Fi',
          faceValue: 100,
          currency: 'AUD',
        },
      ],
    };

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(mockProducts),
    } as Response);
    mockSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });

    const results = await handler({} as never, {} as never, () => {});
    expect(results).toMatchObject({ prezzee: { upserted: 2, errors: 0 } });

    const putCalls = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    expect(putCalls).toHaveLength(2);

    // Verify the upserted record shape
    const firstPut = (putCalls[0][0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(firstPut.pK).toBe('GIFTCARD#woolworths#prez-001');
    expect(firstPut.sK).toBe('profile');
    expect(firstPut.status).toBe('ACTIVE');
    expect(firstPut.primaryCat).toBe('gift_card_catalog');
    const desc = JSON.parse(firstPut.desc as string);
    expect(desc.distributorId).toBe('prezzee');
    expect(desc.region).toBe('AU');
    expect(desc.denomination).toBe(50);
  });

  it('increments errors when Prezzee API returns non-OK status', async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 503,
    } as Response);
    mockSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });

    const results = await handler({} as never, {} as never, () => {});
    expect(results).toMatchObject({ prezzee: { upserted: 0, errors: 1 } });
  });

  it('increments errors on fetch network failure', async () => {
    global.fetch = vi.fn().mockRejectedValue(new Error('Network error'));
    mockSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });

    const results = await handler({} as never, {} as never, () => {});
    expect(results).toMatchObject({ prezzee: { upserted: 0, errors: 1 } });
  });
});

describe('catalog-sync — deactivateRemovedSkus', () => {
  it('marks previously active SKUs from the distributor as INACTIVE when no longer in catalog', async () => {
    process.env.PREZZEE_API_KEY = 'test-prezzee-key';

    // Prezzee returns only one product; DB has two active Prezzee records
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({
        products: [{ productId: 'prez-001', name: 'Woolworths', faceValue: 50, currency: 'AUD' }],
      }),
    } as Response);

    mockSend.mockImplementation((cmd: { __type: string; input?: { ExclusiveStartKey?: unknown } }) => {
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      if (cmd.__type === 'UpdateCommand') return Promise.resolve({});
      if (cmd.__type === 'ScanCommand' && !cmd.input?.ExclusiveStartKey) {
        return Promise.resolve({
          Items: [
            // Still active in catalog — should NOT be deactivated
            { pK: 'GIFTCARD#woolworths#prez-001', sK: 'profile', desc: JSON.stringify({ distributorId: 'prezzee' }) },
            // Removed from catalog — should be deactivated
            { pK: 'GIFTCARD#old-brand#prez-999', sK: 'profile', desc: JSON.stringify({ distributorId: 'prezzee' }) },
          ],
          LastEvaluatedKey: undefined,
        });
      }
      return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
    });

    await handler({} as never, {} as never, () => {});

    const updateCalls = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'UpdateCommand',
    );
    expect(updateCalls).toHaveLength(1);
    const updatedKey = (updateCalls[0][0] as { input: { Key: Record<string, unknown> } }).input.Key;
    expect(updatedKey.pK).toBe('GIFTCARD#old-brand#prez-999');
  });

  it('does not deactivate records from a different distributor', async () => {
    // Both Prezzee and Tango are configured.
    // Prezzee returns 0 products (all its old SKUs should be deactivated).
    // Tango returns its catalog (those SKUs should NOT be deactivated).
    process.env.PREZZEE_API_KEY = 'test-prezzee-key';
    process.env.TANGO_API_KEY   = 'test-tango-key';

    global.fetch = vi.fn()
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve({ products: [] }) })          // Prezzee: 0 items
      .mockResolvedValueOnce({ ok: true, json: () => Promise.resolve({                              // Tango: amazon item still active
        brands: [{ brandKey: 'amazon', brandName: 'Amazon', items: [{ utid: 'tango-001', faceValue: 50, currencyCode: 'USD' }] }],
      }) });

    // The scan finds one old Prezzee SKU and one active Tango SKU.
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      if (cmd.__type === 'UpdateCommand') return Promise.resolve({});
      if (cmd.__type === 'ScanCommand') {
        return Promise.resolve({
          Items: [
            // Old Prezzee SKU — not in prezzee catalog anymore → should be deactivated
            { pK: 'GIFTCARD#old-prezzee-brand#prez-old', sK: 'profile', desc: JSON.stringify({ distributorId: 'prezzee' }) },
            // Active Tango SKU — still in tango catalog → must NOT be deactivated
            { pK: 'GIFTCARD#amazon#tango-001', sK: 'profile', desc: JSON.stringify({ distributorId: 'tango' }) },
          ],
          LastEvaluatedKey: undefined,
        });
      }
      return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
    });

    await handler({} as never, {} as never, () => {});

    const updateCalls = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'UpdateCommand',
    );
    // Only the old Prezzee record should be deactivated, not the Tango record
    expect(updateCalls).toHaveLength(1);
    const deactivatedPk = (updateCalls[0][0] as { input: { Key: { pK: string } } }).input.Key.pK;
    expect(deactivatedPk).toBe('GIFTCARD#old-prezzee-brand#prez-old');
  });
});

describe('catalog-sync — Tango', () => {
  it('upserts items for each brand/SKU combination', async () => {
    process.env.TANGO_API_KEY = 'test-tango-key';

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({
        brands: [
          {
            brandKey: 'amazon',
            brandName: 'Amazon',
            items: [
              { utid: 'AMZN-25', faceValue: 25, currencyCode: 'USD' },
              { utid: 'AMZN-50', faceValue: 50, currencyCode: 'USD' },
            ],
          },
        ],
      }),
    } as Response);
    mockSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });

    const results = await handler({} as never, {} as never, () => {});
    expect(results).toMatchObject({ tango: { upserted: 2, errors: 0 } });
  });
});

describe('catalog-sync — category mapping', () => {
  it('maps known category keywords to canonical category strings', async () => {
    process.env.PREZZEE_API_KEY = 'test-prezzee-key';

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({
        products: [
          { productId: 'p1', name: 'KFC', category: 'restaurant', faceValue: 20, currency: 'AUD' },
          { productId: 'p2', name: 'Steam', category: 'gaming', faceValue: 20, currency: 'AUD' },
          { productId: 'p3', name: 'Coles', category: 'supermarket', faceValue: 50, currency: 'AUD' },
        ],
      }),
    } as Response);
    mockSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });

    await handler({} as never, {} as never, () => {});

    const putCalls = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    const categories = putCalls.map((c: unknown[]) => {
      const item = (c[0] as { input: { Item: Record<string, unknown> } }).input.Item;
      return JSON.parse(item.desc as string).category as string;
    });
    expect(categories).toContain('dining');
    expect(categories).toContain('gaming');
    expect(categories).toContain('grocery');
  });
});
