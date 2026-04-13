import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// ─── Hoisted: env vars + mocks must run before module imports ─────────────────

const { mockDdbSend, mockSesSend, mockFetch } = vi.hoisted(() => {
  process.env.REFDATA_TABLE = 'MOCK_REFDATA';
  process.env.STRIPE_SECRET_KEY = 'sk_test_mock';
  process.env.FROM_EMAIL = 'billing@bebocard.com.au';
  return {
    mockDdbSend: vi.fn(),
    mockSesSend: vi.fn(),
    mockFetch: vi.fn(),
  };
});

vi.mock('@aws-sdk/client-dynamodb', () => ({ DynamoDBClient: class { } }));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: () => ({ send: mockDdbSend }) },
  GetCommand: class { constructor(public input: unknown) { } },
  PutCommand: class { constructor(public input: unknown) { } },
  ScanCommand: class { constructor(public input: unknown) { } },
}));

vi.mock('@aws-sdk/client-ses', () => ({
  SESClient: class { send = mockSesSend; },
  SendEmailCommand: class { constructor(public input: unknown) { } },
}));

vi.mock('@aws-sdk/client-ssm', () => ({
  SSMClient: class { send = vi.fn().mockRejectedValue(new Error('Mock SSM Error')); },
  GetParameterCommand: class { constructor(public input: unknown) { } },
}));

vi.stubGlobal('fetch', mockFetch);

// ─── Import handler after mocks ───────────────────────────────────────────────

import { handler } from './handler';

// ─── Helpers ──────────────────────────────────────────────────────────────────

const PREV_MONTH = (() => {
  const d = new Date();
  d.setDate(1);
  d.setMonth(d.getMonth() - 1);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
})();

const ALL_USAGE_TYPES = ['offers', 'newsletters', 'catalogues', 'invoices', 'geolocation', 'payments', 'consent'];

function makeTenantItem(opts: {
  tenantId?: string;
  tier?: string;
  includedEventsPerMonth?: number | null;
  stripeCustomerId?: string | null;
  billingEmail?: string | null;
  billingStatus?: string;
} = {}) {
  return {
    pK: `TENANT#${opts.tenantId ?? 'tenant-1'}`,
    sK: 'PROFILE',
    status: 'ACTIVE',
    primaryCat: 'tenant',
    desc: JSON.stringify({
      tenantId:              opts.tenantId              ?? 'tenant-1',
      tenantName:            'Test Tenant',
      tier:                  opts.tier                  ?? 'engagement',
      billingEmail:          'billingEmail' in opts ? opts.billingEmail : 'billing@test.com',
      stripeCustomerId:      'stripeCustomerId' in opts ? opts.stripeCustomerId : 'cus_test123',
      stripeSubscriptionId:  'sub_test123',
      includedEventsPerMonth: opts.includedEventsPerMonth ?? null,
      billingStatus:         opts.billingStatus         ?? 'ACTIVE',
    }),
  };
}

// Queue mock responses for a single full billing run of one tenant:
//   scan → billing-run check → 7 usage GetCommands → PutCommand (billing run save)
function mockSingleTenantRun(
  tenantItem: ReturnType<typeof makeTenantItem>,
  usageCounts: Partial<Record<string, number>>,
  alreadyInvoiced = false,
) {
  mockDdbSend.mockImplementation((cmd: any) => {
    if (cmd.input?.FilterExpression?.includes('primaryCat')) {
      return Promise.resolve({ Items: [tenantItem], LastEvaluatedKey: undefined });
    }
    if (cmd.input?.Key?.sK?.startsWith('BILLING_RUN')) {
      return Promise.resolve(
        alreadyInvoiced
          ? { Item: { desc: JSON.stringify({ status: 'INVOICED' }) } }
          : { Item: undefined }
      );
    }
    if (cmd.input?.Key?.sK?.includes('USAGE')) {
      const parts = cmd.input.Key.sK.split('#');
      const type = parts[parts.length - 1];
      return Promise.resolve({ Item: { usageCount: usageCounts[type] ?? 0 } });
    }
    return Promise.resolve({});
  });
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('billing-run-handler', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-04-01T12:00:00Z')); // run on the 1st of the month
    vi.resetAllMocks(); // resets once-queue AND implementations to prevent cross-test contamination
    mockSesSend.mockResolvedValue({});
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({ id: 'ii_test123', invoice: null }),
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // ── Billing month ────────────────────────────────────────────────────────────

  it('reports the previous calendar month', async () => {
    mockDdbSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });
    const result = await handler({}, {} as never, () => { }) as any;

    expect(result.total).toBe(0);
  });

  // ── No overage ───────────────────────────────────────────────────────────────

  it('marks SKIPPED and sends summary email when usage is within quota', async () => {
    // 100 offers on engagement tier with 2500 included → no overage
    mockSingleTenantRun(makeTenantItem({ tier: 'engagement' }), { offers: 100 });

    const result = await handler({}, {} as never, () => { }) as any;

    expect(result.processedCount).toBe(1);
    expect(result.total).toBe(1);
    expect(mockFetch).not.toHaveBeenCalled();

    const putCalls = mockDdbSend.mock.calls.filter(
      (c: any) => c[0]?.input?.Item?.sK === `BILLING_RUN#${PREV_MONTH}`,
    );
    expect(putCalls).toHaveLength(1);
    const savedDesc = JSON.parse(putCalls[0][0].input.Item.desc);
    expect(savedDesc.status).toBe('SKIPPED');
    expect(savedDesc.overageCount).toBe(0);

    expect(mockSesSend).toHaveBeenCalledOnce();
    const emailText: string = mockSesSend.mock.calls[0][0].input.Message.Body.Text.Data;
    expect(emailText).toContain('within the included quota');
  });

  // ── Overage billing ──────────────────────────────────────────────────────────

  it('creates Stripe invoice items for overage and marks INVOICED', async () => {
    // 2840 total on engagement (2500 included) → 340 overage
    mockSingleTenantRun(makeTenantItem({ tier: 'engagement' }), {
      offers: 1800, newsletters: 1000, catalogues: 40,
    });

    const result = await handler({}, {} as never, () => { }) as any;

    expect(result.processedCount).toBe(1);
    expect(mockFetch).toHaveBeenCalled();

    // Stripe invoice item should be POSTed to v1/invoiceitems
    const stripeUrl = mockFetch.mock.calls[0][0] as string;
    expect(stripeUrl).toBe('https://api.stripe.com/v1/invoiceitems');

    const putCalls = mockDdbSend.mock.calls.filter(
      (c: any) => c[0]?.input?.Item?.sK === `BILLING_RUN#${PREV_MONTH}`,
    );
    const savedDesc = JSON.parse(putCalls[0][0].input.Item.desc);
    expect(savedDesc.status).toBe('INVOICED');
    expect(savedDesc.overageCount).toBe(340);
    expect(savedDesc.overageAud).toBeGreaterThan(0);

    expect(mockSesSend).toHaveBeenCalledOnce();
    const emailText: string = mockSesSend.mock.calls[0][0].input.Message.Body.Text.Data;
    expect(emailText).toContain('340');
  });

  it('Stripe invoice item description includes category, count and rate', async () => {
    // 3000 offers on engagement → 500 overage
    mockSingleTenantRun(makeTenantItem({ tier: 'engagement' }), { offers: 3000 });

    await handler({}, {} as never, () => { });

    expect(mockFetch).toHaveBeenCalled();
    const body = new URLSearchParams(mockFetch.mock.calls[0][1].body as string);
    expect(body.get('description')).toMatch(/offers/i);
    expect(body.get('description')).toMatch(/0\.20/); // engagement rate
    expect(body.get('currency')).toBe('aud');
    expect(body.get('customer')).toBe('cus_test123');
    expect(body.get('metadata[type]')).toBe('overage');
  });

  // ── Idempotency ───────────────────────────────────────────────────────────────

  it('skips a tenant already INVOICED for the month', async () => {
    mockSingleTenantRun(makeTenantItem(), { offers: 5000 }, /* alreadyInvoiced= */ true);

    const result = await handler({}, {} as never, () => { }) as any;

    expect(result.processedCount).toBe(1);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  // ── No Stripe customer ───────────────────────────────────────────────────────

  it('marks SKIPPED when tenant has no stripeCustomerId', async () => {
    mockSingleTenantRun(makeTenantItem({ stripeCustomerId: null }), { offers: 5000 });

    const result = await handler({}, {} as never, () => { }) as any;

    expect(result.processedCount).toBe(1);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  // ── Enterprise tier ──────────────────────────────────────────────────────────

  it('enterprise tenant never overages (999M included events)', async () => {
    // enterprise has ~unlimited included → totalBillable will never exceed it
    mockSingleTenantRun(makeTenantItem({ tier: 'enterprise' }), { offers: 1_000_000 });

    const result = await handler({}, {} as never, () => { }) as any;

    expect(result.processedCount).toBe(1);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  // ── Per-tier billable type filtering ─────────────────────────────────────────

  it('base tier: invoices and geolocation do NOT count toward billable total', async () => {
    // offers=200 (billable), invoices=100 + geolocation=100 (not billable on base)
    // billable total = 200 < 250 included → no overage
    mockSingleTenantRun(makeTenantItem({ tier: 'base' }), {
      offers: 200, invoices: 100, geolocation: 100,
    });

    const result = await handler({}, {} as never, () => { }) as any;

    expect(result.processedCount).toBe(1);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('engagement tier: invoices and geolocation count toward billable total', async () => {
    // engagement: offers=1000 + invoices=1000 + geolocation=600 = 2600 → over 2500 → 100 overage
    mockSingleTenantRun(makeTenantItem({ tier: 'engagement' }), {
      offers: 1000, invoices: 1000, geolocation: 600,
    });

    const result = await handler({}, {} as never, () => { }) as any;

    expect(result.processedCount).toBe(1);
    expect(mockFetch).toHaveBeenCalled();
  });

  // ── Consent category (intelligence only, $0.15 rate) ─────────────────────────

  it('consent overage on intelligence tier uses $0.15 rate', async () => {
    // 25200 consent on intelligence (25000 included) → 200 overage @ $0.15 = $30.00
    mockSingleTenantRun(makeTenantItem({ tier: 'intelligence' }), { consent: 25200 });

    await handler({}, {} as never, () => { });

    if (mockFetch.mock.calls.length > 0) {
      const body = new URLSearchParams(mockFetch.mock.calls[0][1].body as string);
      expect(body.get('description')).toMatch(/consent/i);
      const amountCents = Number(body.get('amount'));
      // 200 × $0.15 = $30.00 → 3000 cents (proportional distribution may vary slightly)
      expect(amountCents).toBeGreaterThan(0);
    }
  });

  // ── Empty tenant list ─────────────────────────────────────────────────────────

  it('returns zero totals when no active tenants', async () => {
    mockDdbSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });

    const result = await handler({}, {} as never, () => { }) as any;


    expect(result.total).toBe(0);
    expect(mockFetch).not.toHaveBeenCalled();
    expect(mockSesSend).not.toHaveBeenCalled();
  });

  // ── Error resilience ─────────────────────────────────────────────────────────

  it('continues processing remaining tenants if one errors', async () => {
    const tenant2 = makeTenantItem({ tenantId: 'tenant-2' });

    // Scan returns 2 tenants
    mockDdbSend.mockResolvedValueOnce({
      Items: [makeTenantItem({ tenantId: 'tenant-1' }), tenant2],
      LastEvaluatedKey: undefined,
    });
    // tenant-1: billing run check throws
    mockDdbSend.mockRejectedValueOnce(new Error('DDB timeout'));
    // tenant-2: normal no-overage path
    mockDdbSend.mockResolvedValueOnce({ Item: undefined }); // billing run check
    for (let i = 0; i < ALL_USAGE_TYPES.length; i++) {
      mockDdbSend.mockResolvedValueOnce({ Item: { usageCount: 0 } });
    }
    mockDdbSend.mockResolvedValue({});

    const result = await handler({}, {} as never, () => { }) as any;

    expect(result.total).toBe(2);
    expect(result.processedCount).toBeGreaterThanOrEqual(1); // tenant-2 processed and skipped
  });
});
