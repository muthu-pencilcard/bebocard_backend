import { vi, describe, it, expect, beforeEach } from 'vitest';

// ── Mocks ─────────────────────────────────────────────────────────────────────

const { mockSend, mockFcmSend } = vi.hoisted(() => ({
  mockSend: vi.fn(),
  mockFcmSend: vi.fn(),
}));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockSend }) },
  ScanCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'ScanCommand', input });
  }),
  GetCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetCommand', input });
  }),
  PutCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutCommand', input });
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

function makeSubscription(amount: number, brandId = 'netflix'): Record<string, unknown> {
  return {
    pK: 'USER#PERM-001',
    sK: `RECURRING#${brandId}#SUB-001`,
    eventType: 'RECURRING',
    status: 'ACTIVE',
    desc: JSON.stringify({ subId: 'SUB-001', brandId, brandName: brandId, amount, currency: 'AUD' }),
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  process.env.USER_TABLE = 'test-user-table';
  process.env.REF_TABLE  = 'test-ref-table';
  process.env.FIREBASE_SERVICE_ACCOUNT_JSON = JSON.stringify({ type: 'service_account' });
  mockFcmSend.mockResolvedValue('msg-id');
});

// ── No active subscriptions ────────────────────────────────────────────────────

describe('no active subscriptions', () => {
  it('does nothing when scan returns no items', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    await (handler as () => Promise<void>)();
    expect(mockFcmSend).not.toHaveBeenCalled();
  });
});

// ── Amount threshold filtering ─────────────────────────────────────────────────

describe('amount threshold', () => {
  it('skips subscriptions with amount ≤ 50', async () => {
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [makeSubscription(50)] });
      return Promise.resolve({});
    });
    await (handler as () => Promise<void>)();
    expect(mockFcmSend).not.toHaveBeenCalled();
  });

  it('processes subscriptions with amount > 50', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [makeSubscription(100)] });
      if (cmd.__type === 'GetCommand') {
        const key = cmd.input?.Key ?? {};
        if (String(key['pK'] ?? '').startsWith('BENCHMARK#')) {
          return Promise.resolve({ Item: { benchmarkAmount: 70 } }); // user pays $100, benchmark is $70 (43% above — triggers alert)
        }
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'fcm-tok' }) } });
      }
      return Promise.resolve({});
    });
    await (handler as () => Promise<void>)();
    expect(mockFcmSend).toHaveBeenCalledOnce();
  });
});

// ── Missing brand or benchmark ─────────────────────────────────────────────────

describe('missing data', () => {
  it('skips when desc has no brandId', async () => {
    const item = {
      pK: 'USER#PERM-001',
      sK: 'RECURRING#brand#S1',
      eventType: 'RECURRING',
      status: 'ACTIVE',
      desc: JSON.stringify({ amount: 200 }), // no brandId
    };
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [item] });
      return Promise.resolve({});
    });
    await (handler as () => Promise<void>)();
    expect(mockFcmSend).not.toHaveBeenCalled();
  });

  it('skips when no benchmark record exists and no catalog record exists for the brand', async () => {
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [makeSubscription(200)] });
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: undefined }); // no benchmark, no catalog
      return Promise.resolve({});
    });
    await (handler as () => Promise<void>)();
    expect(mockFcmSend).not.toHaveBeenCalled();
  });
});

// ── Catalog fallback (BENCHMARK# missing → SUBSCRIPTION_CATALOG# lookup) ──────

describe('catalog benchmark fallback', () => {
  it('uses SUBSCRIPTION_CATALOG# benchmarkPrice when BENCHMARK# record is absent', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [makeSubscription(100, 'netflix')] });
      if (cmd.__type === 'GetCommand') {
        const pk = String((cmd.input?.Key ?? {})['pK'] ?? '');
        if (pk.startsWith('BENCHMARK#')) {
          return Promise.resolve({ Item: undefined }); // no BENCHMARK# record
        }
        if (pk.startsWith('SUBSCRIPTION_CATALOG#')) {
          return Promise.resolve({
            Item: {
              desc: JSON.stringify({ benchmarkPrice: 7.99 }), // $100 vs $7.99 → way above 15% → alert
            },
          });
        }
        // DEVICE_TOKEN
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'tok-123' }) } });
      }
      return Promise.resolve({});
    });

    await (handler as () => Promise<void>)();

    const puts = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    expect(puts).toHaveLength(1);
    const item = (puts[0][0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(item.sK).toBe('SAVING_OPPORTUNITY#netflix');
    const desc = JSON.parse(item.desc as string);
    expect(desc.benchmarkAmount).toBe(7.99);
    expect(desc.potentialSaving).toBeCloseTo(92.01, 1);

    expect(mockFcmSend).toHaveBeenCalledOnce();
  });

  it('skips when SUBSCRIPTION_CATALOG# desc has no benchmarkPrice', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [makeSubscription(200, 'unknown-brand')] });
      if (cmd.__type === 'GetCommand') {
        const pk = String((cmd.input?.Key ?? {})['pK'] ?? '');
        if (pk.startsWith('BENCHMARK#')) return Promise.resolve({ Item: undefined });
        if (pk.startsWith('SUBSCRIPTION_CATALOG#')) {
          return Promise.resolve({
            Item: { desc: JSON.stringify({ name: 'Some Service' }) }, // no benchmarkPrice
          });
        }
        return Promise.resolve({ Item: undefined });
      }
      return Promise.resolve({});
    });

    await (handler as () => Promise<void>)();
    expect(mockFcmSend).not.toHaveBeenCalled();
  });

  it('does not alert via catalog fallback when amount is within 15% of catalog benchmark', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [makeSubscription(13, 'spotify')] });
      if (cmd.__type === 'GetCommand') {
        const pk = String((cmd.input?.Key ?? {})['pK'] ?? '');
        if (pk.startsWith('BENCHMARK#')) return Promise.resolve({ Item: undefined });
        if (pk.startsWith('SUBSCRIPTION_CATALOG#')) {
          return Promise.resolve({
            Item: { desc: JSON.stringify({ benchmarkPrice: 12.99 }) }, // $13 vs $12.99 — well within 15%
          });
        }
        return Promise.resolve({ Item: undefined });
      }
      return Promise.resolve({});
    });

    await (handler as () => Promise<void>)();
    expect(mockFcmSend).not.toHaveBeenCalled();
  });

  it('prefers BENCHMARK# record over SUBSCRIPTION_CATALOG# when both exist', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [makeSubscription(100, 'netflix')] });
      if (cmd.__type === 'GetCommand') {
        const pk = String((cmd.input?.Key ?? {})['pK'] ?? '');
        if (pk.startsWith('BENCHMARK#')) {
          return Promise.resolve({ Item: { benchmarkAmount: 90 } }); // within 15% → no alert
        }
        // If we reach this, the handler incorrectly fell through to catalog
        if (pk.startsWith('SUBSCRIPTION_CATALOG#')) {
          return Promise.resolve({
            Item: { desc: JSON.stringify({ benchmarkPrice: 50 }) }, // would trigger alert if used
          });
        }
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'tok' }) } });
      }
      return Promise.resolve({});
    });

    await (handler as () => Promise<void>)();

    // BENCHMARK# says $90 (within 15% of $100), so no alert — catalog's $50 is NOT used
    expect(mockFcmSend).not.toHaveBeenCalled();
  });
});

// ── Benchmark comparison logic ─────────────────────────────────────────────────

describe('benchmark comparison', () => {
  it('does NOT create SAVING_OPPORTUNITY when amount is within 15% of benchmark', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [makeSubscription(80)] });
      if (cmd.__type === 'GetCommand') {
        const key = cmd.input?.Key ?? {};
        if (String(key['pK'] ?? '').startsWith('BENCHMARK#')) {
          return Promise.resolve({ Item: { benchmarkAmount: 75 } }); // 80 / 75 ≈ 1.067 — within 15%
        }
        return Promise.resolve({ Item: undefined });
      }
      return Promise.resolve({});
    });
    await (handler as () => Promise<void>)();
    expect(mockFcmSend).not.toHaveBeenCalled();
  });

  it('creates SAVING_OPPORTUNITY when amount is > 15% above benchmark', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [makeSubscription(100, 'spotify')] });
      if (cmd.__type === 'GetCommand') {
        const key = cmd.input?.Key ?? {};
        if (String(key['pK'] ?? '').startsWith('BENCHMARK#')) {
          return Promise.resolve({ Item: { benchmarkAmount: 60 } }); // $100 vs $60 → 67% above
        }
        // device token for FCM
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'fcm-tok' }) } });
      }
      return Promise.resolve({});
    });

    await (handler as () => Promise<void>)();

    // PutCommand for SAVING_OPPORTUNITY
    const putCalls = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    expect(putCalls).toHaveLength(1);
    const putItem = (putCalls[0][0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(putItem.sK).toBe('SAVING_OPPORTUNITY#spotify');
    expect(putItem.eventType).toBe('SAVING_OPPORTUNITY');
    const desc = JSON.parse(putItem.desc as string);
    expect(desc.potentialSaving).toBe(40);

    // FCM push
    expect(mockFcmSend).toHaveBeenCalledOnce();
    const fcm = mockFcmSend.mock.calls[0][0] as { notification: { title: string; body: string }; data: Record<string, string> };
    expect(fcm.notification.title).toBe('Saving Opportunity');
    expect(fcm.notification.body).toContain('40.00');
    expect(fcm.data.type).toBe('SAVING_OPPORTUNITY');
  });

  it('does not throw when FCM push fails', async () => {
    mockFcmSend.mockRejectedValue(new Error('Firebase down'));
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: [makeSubscription(200, 'brand-x')] });
      if (cmd.__type === 'GetCommand') {
        const key = cmd.input?.Key ?? {};
        if (String(key['pK'] ?? '').startsWith('BENCHMARK#')) {
          return Promise.resolve({ Item: { benchmarkAmount: 100 } });
        }
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'tok' }) } });
      }
      return Promise.resolve({});
    });
    await expect(handler({} as never, {} as never, () => {})).resolves.not.toThrow();
  });

  it('processes multiple subscriptions in one run', async () => {
    const subs = [
      makeSubscription(200, 'brand-a'), // 200 vs benchmark 100 → alert
      makeSubscription(55, 'brand-b'),  // 55 ≤ 50 threshold? No, 55>50 but let's say benchmark 70 → 55/70 < 1.15 → no alert
      makeSubscription(30, 'brand-c'),  // ≤ 50 → skipped
    ];
    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: Record<string, unknown> } }) => {
      if (cmd.__type === 'ScanCommand') return Promise.resolve({ Items: subs });
      if (cmd.__type === 'GetCommand') {
        const key = cmd.input?.Key ?? {};
        const pk = String(key['pK'] ?? '');
        if (pk === 'BENCHMARK#brand-a') return Promise.resolve({ Item: { benchmarkAmount: 100 } });
        if (pk === 'BENCHMARK#brand-b') return Promise.resolve({ Item: { benchmarkAmount: 70 } });
        if (pk.startsWith('BENCHMARK#')) return Promise.resolve({ Item: undefined });
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'tok' }) } });
      }
      return Promise.resolve({});
    });

    await (handler as () => Promise<void>)();

    const putCalls = mockSend.mock.calls.filter(
      (c: unknown[]) => (c[0] as { __type?: string }).__type === 'PutCommand',
    );
    expect(putCalls).toHaveLength(1); // only brand-a triggers a saving opportunity
    expect(mockFcmSend).toHaveBeenCalledTimes(1);
  });
});
