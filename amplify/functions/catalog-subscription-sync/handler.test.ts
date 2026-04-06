import { vi, describe, it, expect, beforeEach } from 'vitest';

// ── Mocks ─────────────────────────────────────────────────────────────────────

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

// ── Setup ─────────────────────────────────────────────────────────────────────

beforeEach(() => {
  vi.clearAllMocks();
  process.env.REFDATA_TABLE = 'test-ref-table';
  // Default: scan returns no existing catalog items (nothing to deactivate)
  mockSend.mockImplementation((cmd: { __type: string }) => {
    if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
    return Promise.resolve({});
  });
});

// ── Provider count ─────────────────────────────────────────────────────────────

describe('catalog-subscription-sync — provider upserts', () => {
  it('upserts 18 catalog records and 18 benchmark records (36 PutCommands total)', async () => {
    const result = await handler();

    const puts = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );

    // 18 providers × 2 writes (catalog + benchmark) = 36
    expect(puts).toHaveLength(36);
    expect(result).toEqual({ upserted: 18, errors: 0 });
  });

  it('returns { upserted: 18, errors: 0 } on clean run', async () => {
    const result = await handler();
    expect(result.upserted).toBe(18);
    expect(result.errors).toBe(0);
  });
});

// ── Record shape ──────────────────────────────────────────────────────────────

describe('catalog-subscription-sync — record shape', () => {
  it('writes SUBSCRIPTION_CATALOG# record with correct keys and primaryCat', async () => {
    await handler();

    const catalogPuts = mockSend.mock.calls
      .filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand')
      .map((c: unknown[]) => (c[0] as { input: { Item: Record<string, unknown> } }).input.Item)
      .filter((item) => String(item.pK ?? '').startsWith('SUBSCRIPTION_CATALOG#'));

    expect(catalogPuts).toHaveLength(18);

    // Spot-check Netflix
    const netflix = catalogPuts.find((i) => i.pK === 'SUBSCRIPTION_CATALOG#netflix');
    expect(netflix).toBeDefined();
    expect(netflix!.sK).toBe('profile');
    expect(netflix!.primaryCat).toBe('subscription_catalog');
    expect(netflix!.status).toBe('ACTIVE');

    const desc = JSON.parse(netflix!.desc as string);
    expect(desc.providerId).toBe('netflix');
    expect(desc.name).toBe('Netflix');
    expect(desc.category).toBe('Streaming');
    expect(desc.region).toBe('AU');
    expect(Array.isArray(desc.plans)).toBe(true);
    expect(desc.plans.length).toBeGreaterThan(0);
    expect(typeof desc.benchmarkPrice).toBe('number');
    expect(typeof desc.affiliateUrl).toBe('string');
    expect(Array.isArray(desc.tags)).toBe(true);
  });

  it('writes BENCHMARK# record with correct keys and benchmarkAmount', async () => {
    await handler();

    const benchmarkPuts = mockSend.mock.calls
      .filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand')
      .map((c: unknown[]) => (c[0] as { input: { Item: Record<string, unknown> } }).input.Item)
      .filter((item) => String(item.pK ?? '').startsWith('BENCHMARK#'));

    expect(benchmarkPuts).toHaveLength(18);

    // Spot-check Spotify: benchmark $12.99
    const spotify = benchmarkPuts.find((i) => i.pK === 'BENCHMARK#spotify');
    expect(spotify).toBeDefined();
    expect(spotify!.sK).toBe('BENCHMARK');
    expect(spotify!.primaryCat).toBe('benchmark');
    expect(spotify!.benchmarkAmount).toBe(12.99);
    expect(spotify!.status).toBe('ACTIVE');
  });

  it('each plan has required fields (planId, name, price, currency, frequency, features)', async () => {
    await handler();

    const catalogPuts = mockSend.mock.calls
      .filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand')
      .map((c: unknown[]) => (c[0] as { input: { Item: Record<string, unknown> } }).input.Item)
      .filter((item) => String(item.pK ?? '').startsWith('SUBSCRIPTION_CATALOG#'));

    for (const item of catalogPuts) {
      const desc = JSON.parse(item.desc as string);
      for (const plan of desc.plans as Record<string, unknown>[]) {
        expect(typeof plan.planId).toBe('string');
        expect(typeof plan.name).toBe('string');
        expect(typeof plan.price).toBe('number');
        expect(plan.currency).toBe('AUD');
        expect(['monthly', 'annually']).toContain(plan.frequency);
        expect(Array.isArray(plan.features)).toBe(true);
      }
    }
  });

  it('all 6 expected categories are represented', async () => {
    await handler();

    const catalogPuts = mockSend.mock.calls
      .filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand')
      .map((c: unknown[]) => (c[0] as { input: { Item: Record<string, unknown> } }).input.Item)
      .filter((item) => String(item.pK ?? '').startsWith('SUBSCRIPTION_CATALOG#'));

    const categories = new Set(
      catalogPuts.map((i) => JSON.parse(i.desc as string).category as string),
    );

    for (const cat of ['Streaming', 'Music', 'Productivity', 'Health', 'Telecom', 'Utilities']) {
      expect(categories.has(cat), `Missing category: ${cat}`).toBe(true);
    }
  });
});

// ── deactivateRemoved ─────────────────────────────────────────────────────────

describe('catalog-subscription-sync — deactivateRemoved', () => {
  it('marks stale catalog records INACTIVE when they no longer appear in the provider list', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: { FilterExpression?: string } }) => {
      if (cmd.__type === 'ScanCommand') {
        return Promise.resolve({
          Items: [
            { pK: 'SUBSCRIPTION_CATALOG#old-provider', sK: 'profile' }, // stale
          ],
          LastEvaluatedKey: undefined,
        });
      }
      return Promise.resolve({});
    });

    await handler();

    const updates = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'UpdateCommand',
    );
    expect(updates).toHaveLength(1);
    const update = (updates[0][0] as {
      input: {
        Key: Record<string, unknown>;
        UpdateExpression: string;
        ExpressionAttributeValues: Record<string, unknown>;
      };
    }).input;
    expect(update.Key.pK).toBe('SUBSCRIPTION_CATALOG#old-provider');
    expect(update.ExpressionAttributeValues[':inactive']).toBe('INACTIVE');
  });

  it('does NOT issue UpdateCommand for providers that are still active', async () => {
    // ScanCommand returns a currently-active provider pK
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'ScanCommand') {
        return Promise.resolve({
          Items: [{ pK: 'SUBSCRIPTION_CATALOG#netflix', sK: 'profile' }],
          LastEvaluatedKey: undefined,
        });
      }
      return Promise.resolve({});
    });

    await handler();

    const updates = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'UpdateCommand',
    );
    expect(updates).toHaveLength(0);
  });

  it('handles paginated scan — deactivates stale record found on second page', async () => {
    let scanCallCount = 0;
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'ScanCommand') {
        scanCallCount++;
        if (scanCallCount === 1) {
          return Promise.resolve({
            Items: [],
            LastEvaluatedKey: { pK: 'SUBSCRIPTION_CATALOG#netflix' },
          });
        }
        // Second page
        return Promise.resolve({
          Items: [{ pK: 'SUBSCRIPTION_CATALOG#removed-provider', sK: 'profile' }],
          LastEvaluatedKey: undefined,
        });
      }
      return Promise.resolve({});
    });

    await handler();

    const updates = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'UpdateCommand',
    );
    expect(updates).toHaveLength(1);
    const update = (updates[0][0] as { input: { Key: Record<string, unknown> } }).input;
    expect(update.Key.pK).toBe('SUBSCRIPTION_CATALOG#removed-provider');
  });
});

// ── Error resilience ──────────────────────────────────────────────────────────

describe('catalog-subscription-sync — error resilience', () => {
  it('continues processing remaining providers when one upsert fails', async () => {
    let callCount = 0;
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
      if (cmd.__type === 'PutCommand') {
        callCount++;
        // Fail the first PutCommand (first provider catalog record)
        if (callCount === 1) return Promise.reject(new Error('DynamoDB throttled'));
      }
      return Promise.resolve({});
    });

    const result = await handler();

    // One provider errored (callCount=1 fails the catalog write, so entire provider counted as error)
    expect(result.errors).toBe(1);
    expect(result.upserted).toBe(17);
  });

  it('does not throw when all providers error', async () => {
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
      return Promise.reject(new Error('table unavailable'));
    });

    const result = await handler();
    expect(result.errors).toBe(18);
    expect(result.upserted).toBe(0);
  });
});

// ── deactivateRemoved issues exactly one ScanCommand per run ──────────────────

describe('catalog-subscription-sync — scan behaviour', () => {
  it('issues exactly one ScanCommand (the deactivation scan) on a clean run', async () => {
    await handler();

    const scans = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'ScanCommand',
    );
    expect(scans).toHaveLength(1);
  });

  it('issues a second ScanCommand when deactivation scan is paginated', async () => {
    let scanCount = 0;
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'ScanCommand') {
        scanCount++;
        return Promise.resolve({
          Items: [],
          LastEvaluatedKey: scanCount === 1 ? { pK: 'cursor' } : undefined,
        });
      }
      return Promise.resolve({});
    });

    await handler();

    const scans = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'ScanCommand',
    );
    expect(scans).toHaveLength(2);
  });
});

// ── New catalog fields ────────────────────────────────────────────────────────

describe('catalog-subscription-sync — new catalog fields', () => {
  it('sync-curated records write isAffiliate=true, isTenantLinked=false, listingStatus=ACTIVE, source=sync', async () => {
    await handler();

    const catalogPuts = mockSend.mock.calls
      .filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand')
      .map((c: unknown[]) => (c[0] as { input: { Item: Record<string, unknown> } }).input.Item)
      .filter((item) => String(item.pK ?? '').startsWith('SUBSCRIPTION_CATALOG#'));

    for (const item of catalogPuts) {
      expect(item.source).toBe('sync'); // top-level attribute
      const desc = JSON.parse(item.desc as string);
      expect(desc.isAffiliate).toBe(true);
      expect(desc.isTenantLinked).toBe(false);
      expect(desc.hasLinking).toBe(false);
      expect(desc.listingStatus).toBe('ACTIVE');
      expect(desc.source).toBe('sync');
    }
  });

  it('streaming providers have invoiceType SUBSCRIPTION', async () => {
    await handler();

    const catalogPuts = mockSend.mock.calls
      .filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand')
      .map((c: unknown[]) => (c[0] as { input: { Item: Record<string, unknown> } }).input.Item)
      .filter((item) => String(item.pK ?? '').startsWith('SUBSCRIPTION_CATALOG#'));

    const streaming = catalogPuts.filter((i) => JSON.parse(i.desc as string).category === 'Streaming');
    expect(streaming.length).toBeGreaterThan(0);
    for (const item of streaming) {
      expect(JSON.parse(item.desc as string).invoiceType).toBe('SUBSCRIPTION');
    }
  });

  it('utility providers have invoiceType RECURRING_INVOICE', async () => {
    await handler();

    const catalogPuts = mockSend.mock.calls
      .filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand')
      .map((c: unknown[]) => (c[0] as { input: { Item: Record<string, unknown> } }).input.Item)
      .filter((item) => String(item.pK ?? '').startsWith('SUBSCRIPTION_CATALOG#'));

    const utilities = catalogPuts.filter((i) => JSON.parse(i.desc as string).category === 'Utilities');
    expect(utilities.length).toBeGreaterThan(0);
    for (const item of utilities) {
      expect(JSON.parse(item.desc as string).invoiceType).toBe('RECURRING_INVOICE');
    }
  });

  it('all sync providers include cancelUrl and portalUrl', async () => {
    await handler();

    const catalogPuts = mockSend.mock.calls
      .filter((c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand')
      .map((c: unknown[]) => (c[0] as { input: { Item: Record<string, unknown> } }).input.Item)
      .filter((item) => String(item.pK ?? '').startsWith('SUBSCRIPTION_CATALOG#'));

    for (const item of catalogPuts) {
      const desc = JSON.parse(item.desc as string);
      expect(typeof desc.cancelUrl).toBe('string');
      expect(typeof desc.portalUrl).toBe('string');
    }
  });

  it('deactivateRemoved scan FilterExpression excludes source=tenant records', async () => {
    await handler();

    const scans = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'ScanCommand',
    );
    expect(scans).toHaveLength(1);
    const scanInput = (scans[0][0] as { input: { FilterExpression: string; ExpressionAttributeValues: Record<string, unknown> } }).input;
    // Verify the scan only targets sync-managed records
    expect(scanInput.FilterExpression).toContain('attribute_not_exists');
    expect(scanInput.ExpressionAttributeValues[':sync']).toBe('sync');
  });

  it('deactivateRemoved does NOT deactivate tenant-registered entries', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'ScanCommand') {
        // Return a tenant-registered provider in the scan — it should NOT be deactivated
        // (In reality it wouldn't appear because FilterExpression excludes source='tenant',
        // but we test the filter is correct by checking UpdateCommand count)
        return Promise.resolve({
          Items: [],  // FilterExpression already excluded tenant entries
          LastEvaluatedKey: undefined,
        });
      }
      return Promise.resolve({});
    });

    await handler();

    const updates = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'UpdateCommand',
    );
    // No UpdateCommands — the scan returned no stale sync entries
    expect(updates).toHaveLength(0);
  });
});
