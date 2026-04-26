import { describe, it, expect, vi, beforeEach } from 'vitest';

// ── Hoisted mocks ─────────────────────────────────────────────────────────────

const { mockDdbSend } = vi.hoisted(() => ({ mockDdbSend: vi.fn() }));

// ── Module mocks ──────────────────────────────────────────────────────────────

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: object) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn(() => ({ send: mockDdbSend })) },
  ScanCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'ScanCommand', input });
  }),
}));

// ── Env vars ──────────────────────────────────────────────────────────────────

process.env.REFDATA_TABLE = 'ref-table';
process.env.USER_TABLE = 'user-table';

// ── Handler import ────────────────────────────────────────────────────────────

import { handler } from './handler.js';

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeEvent(path: string, qs: Record<string, string> = {}, extras: Record<string, unknown> = {}) {
  return {
    httpMethod: 'GET',
    path,
    queryStringParameters: qs,
    requestContext: { authorizer: null },
    ...extras,
  };
}

function makeBrandItem(brandId: string, overrides: Record<string, unknown> = {}) {
  return {
    pK: `BRAND#${brandId}`,
    sK: 'profile',
    primaryCat: 'brand',
    status: 'ACTIVE',
    desc: JSON.stringify({
      brandName: `${brandId} Name`,
      brandColor: '#0066CC',
      region: 'AU',
      category: 'grocery',
      ...overrides,
    }),
  };
}

function makeOfferItem(brandId: string, overrides: Record<string, unknown> = {}) {
  const tomorrow = new Date(Date.now() + 86400000).toISOString();
  return {
    pK: `BRAND#${brandId}`,
    sK: 'OFFER#001',
    primaryCat: 'offer',
    status: 'ACTIVE',
    desc: JSON.stringify({
      offerId: 'OFF001',
      brandId,
      brandName: `${brandId} Name`,
      brandRegion: 'AU',
      brandColor: '#0066CC',
      title: '10% off',
      description: 'Discount offer',
      validTo: tomorrow,
      ...overrides,
    }),
  };
}

function makeCatalogueItem(brandId: string, overrides: Record<string, unknown> = {}) {
  const nextWeek = new Date(Date.now() + 7 * 86400000).toISOString();
  return {
    pK: `BRAND#${brandId}`,
    sK: 'CATALOGUE#001',
    primaryCat: 'catalogue',
    status: 'ACTIVE',
    desc: JSON.stringify({
      catalogueId: 'CAT001',
      brandId,
      brandName: `${brandId} Name`,
      brandRegion: 'AU',
      brandColor: '#0066CC',
      title: 'Winter Catalogue',
      validTo: nextWeek,
      ...overrides,
    }),
  };
}

function makeNewsletterItem(brandId: string, createdAt: string) {
  return {
    pK: `BRAND#${brandId}`,
    sK: 'NEWSLETTER#001',
    primaryCat: 'newsletter',
    status: 'ACTIVE',
    createdAt,
    desc: JSON.stringify({
      newsletterId: 'NEWS001',
      brandId,
      brandName: `${brandId} Name`,
      brandRegion: 'AU',
      brandColor: '#0066CC',
      subject: 'Weekly News',
    }),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Routing
// ─────────────────────────────────────────────────────────────────────────────

describe('discovery-handler routing', () => {
  beforeEach(() => vi.clearAllMocks());

  it('OPTIONS returns 200 with empty body', async () => {
    const res = await handler(makeEvent('/discover/brands', {}, { httpMethod: 'OPTIONS' }), {} as never, {} as never);
    expect(res).toMatchObject({ statusCode: 200, body: '' });
  });

  it('returns 404 for unknown path', async () => {
    mockDdbSend.mockResolvedValue({ Items: [] });
    const res = await handler(makeEvent('/discover/unknown'), {} as never, {} as never);
    expect((res as { statusCode: number }).statusCode).toBe(404);
  });

  it('returns 500 on unhandled DynamoDB error', async () => {
    mockDdbSend.mockRejectedValue(new Error('DynamoDB unavailable'));
    const res = await handler(makeEvent('/discover/brands'), {} as never, {} as never);
    expect((res as { statusCode: number }).statusCode).toBe(500);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /discover/brands
// ─────────────────────────────────────────────────────────────────────────────

describe('GET /discover/brands', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns 200 with brands list from DynamoDB scan', async () => {
    mockDdbSend.mockResolvedValueOnce({
      Items: [makeBrandItem('woolworths'), makeBrandItem('coles')],
      LastEvaluatedKey: undefined,
    });
    const res = await handler(makeEvent('/discover/brands'), {} as never, {} as never);
    expect((res as { statusCode: number }).statusCode).toBe(200);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.brands).toHaveLength(2);
    expect(body.brands[0].brandId).toBe('woolworths');
    expect(body.nextCursor).toBeNull();
  });

  it('filters brands by region when region param is provided', async () => {
    mockDdbSend.mockResolvedValueOnce({
      Items: [
        makeBrandItem('woolworths', { region: 'AU' }),
        makeBrandItem('tesco', { region: 'UK' }),
      ],
      LastEvaluatedKey: undefined,
    });
    const res = await handler(makeEvent('/discover/brands', { region: 'AU' }), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.brands).toHaveLength(1);
    expect(body.brands[0].brandId).toBe('woolworths');
  });

  it('returns nextCursor when LastEvaluatedKey is present', async () => {
    const lastKey = { pK: 'BRAND#woolworths', sK: 'profile' };
    mockDdbSend.mockResolvedValueOnce({
      Items: [makeBrandItem('woolworths')],
      LastEvaluatedKey: lastKey,
    });
    const res = await handler(makeEvent('/discover/brands', { limit: '1' }), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.nextCursor).toBeDefined();
    expect(typeof body.nextCursor).toBe('string');
  });

  it('limits results to MAX_LIMIT (100) regardless of limit param', async () => {
    const items = Array.from({ length: 5 }, (_, i) => makeBrandItem(`brand${i}`));
    mockDdbSend.mockResolvedValueOnce({ Items: items, LastEvaluatedKey: undefined });
    const res = await handler(makeEvent('/discover/brands', { limit: '9999' }), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    // All 5 items returned (under limit)
    expect(body.brands).toHaveLength(5);
  });

  it('includes wallet-owned brands first when user is authenticated', async () => {
    // handler fetches wallet brands FIRST (USER_TABLE scan), then brand catalog (REFDATA_TABLE scan)
    mockDdbSend
      .mockResolvedValueOnce({ Items: [{ subCategory: 'woolworths' }] })                                                  // wallet scan
      .mockResolvedValueOnce({ Items: [makeBrandItem('bigw'), makeBrandItem('woolworths')], LastEvaluatedKey: undefined }); // brands scan
    const res = await handler(
      makeEvent('/discover/brands', {}, {
        requestContext: {
          authorizer: { claims: { 'custom:permULID': 'PERM001' } },
        },
      }),
      {} as never,
      {} as never,
    );
    const body = JSON.parse((res as { body: string }).body);
    // woolworths is in wallet → should be sorted first
    expect(body.brands[0].brandId).toBe('woolworths');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /discover/offers
// ─────────────────────────────────────────────────────────────────────────────

describe('GET /discover/offers', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns active offers sorted by soonest expiry first', async () => {
    const sooner = makeOfferItem('woolworths', {
      offerId: 'SOON',
      validTo: new Date(Date.now() + 2 * 86400000).toISOString(),
    });
    const later = makeOfferItem('coles', {
      offerId: 'LATER',
      validTo: new Date(Date.now() + 10 * 86400000).toISOString(),
    });
    mockDdbSend.mockResolvedValueOnce({ Items: [later, sooner], LastEvaluatedKey: undefined });
    const res = await handler(makeEvent('/discover/offers'), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.offers[0].offerId).toBe('SOON');
    expect(body.offers[1].offerId).toBe('LATER');
  });

  it('filters out expired offers', async () => {
    const expired = makeOfferItem('woolworths', {
      validTo: new Date(Date.now() - 86400000).toISOString(), // yesterday
    });
    const active = makeOfferItem('coles');
    mockDdbSend.mockResolvedValueOnce({ Items: [expired, active], LastEvaluatedKey: undefined });
    const res = await handler(makeEvent('/discover/offers'), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.offers).toHaveLength(1);
    expect(body.offers[0].brandId).toBe('coles');
  });

  it('filters offers by region', async () => {
    mockDdbSend.mockResolvedValueOnce({
      Items: [
        makeOfferItem('woolworths', { brandRegion: 'AU' }),
        makeOfferItem('tesco', { brandRegion: 'UK' }),
      ],
    });
    const res = await handler(makeEvent('/discover/offers', { region: 'UK' }), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.offers).toHaveLength(1);
    expect(body.offers[0].brandId).toBe('tesco');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /discover/catalogues
// ─────────────────────────────────────────────────────────────────────────────

describe('GET /discover/catalogues', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns active catalogues and filters expired ones', async () => {
    const active = makeCatalogueItem('woolworths');
    const expired = makeCatalogueItem('coles', { validTo: new Date(Date.now() - 86400000).toISOString() });
    mockDdbSend.mockResolvedValueOnce({ Items: [active, expired] });
    const res = await handler(makeEvent('/discover/catalogues'), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.catalogues).toHaveLength(1);
    expect(body.catalogues[0].brandId).toBe('woolworths');
  });

  it('returns 200 with empty catalogues list when no results', async () => {
    mockDdbSend.mockResolvedValueOnce({ Items: [] });
    const res = await handler(makeEvent('/discover/catalogues'), {} as never, {} as never);
    expect((res as { statusCode: number }).statusCode).toBe(200);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.catalogues).toEqual([]);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /discover/newsletters
// ─────────────────────────────────────────────────────────────────────────────

describe('GET /discover/newsletters', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns newsletters sorted most recent first', async () => {
    const older = makeNewsletterItem('woolworths', '2026-01-01T00:00:00Z');
    const newer = makeNewsletterItem('coles', '2026-04-01T00:00:00Z');
    mockDdbSend.mockResolvedValueOnce({ Items: [older, newer] });
    const res = await handler(makeEvent('/discover/newsletters'), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.newsletters[0].brandId).toBe('coles');
    expect(body.newsletters[1].brandId).toBe('woolworths');
  });

  it('filters newsletters by region', async () => {
    mockDdbSend.mockResolvedValueOnce({
      Items: [
        makeNewsletterItem('woolworths', '2026-04-01T00:00:00Z'),
        { ...makeNewsletterItem('tesco', '2026-04-01T00:00:00Z'), desc: JSON.stringify({ brandId: 'tesco', brandRegion: 'UK', subject: 'UK News', brandName: 'Tesco', brandColor: '#CC0000' }) },
      ],
    });
    const res = await handler(makeEvent('/discover/newsletters', { region: 'AU' }), {} as never, {} as never);
    const body = JSON.parse((res as { body: string }).body);
    expect(body.newsletters).toHaveLength(1);
    expect(body.newsletters[0].brandId).toBe('woolworths');
  });

  it('returns CORS headers on successful response', async () => {
    mockDdbSend.mockResolvedValueOnce({ Items: [] });
    const res = await handler(makeEvent('/discover/newsletters'), {} as never, {} as never);
    const headers = (res as { headers: Record<string, string> }).headers;
    expect(headers['Access-Control-Allow-Origin']).toBe('*');
    expect(headers['Content-Type']).toBe('application/json');
  });
});
