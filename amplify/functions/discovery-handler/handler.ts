/**
 * Discovery Handler — public read-only catalogue of brands, offers, catalogues, newsletters.
 * Called by the mobile app to power the regional discovery feeds.
 *
 * Routes:
 *   GET /discover/brands?region=AU&limit=40&cursor=<base64>
 *   GET /discover/offers?region=AU&limit=20&cursor=<base64>
 *   GET /discover/catalogues?region=AU&limit=20&cursor=<base64>
 *   GET /discover/newsletters?region=AU&limit=20&cursor=<base64>
 */
import type { Handler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const MAX_LIMIT = 100;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Authorization,Content-Type',
  'Content-Type': 'application/json',
};

function ok(body: unknown) {
  return { statusCode: 200, headers: CORS, body: JSON.stringify(body) };
}
function notFound() {
  return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Not found' }) };
}
function serverError() {
  return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Internal error' }) };
}

function parseRecord(value: unknown): Record<string, unknown> {
  if (typeof value === 'object' && value !== null) return value as Record<string, unknown>;
  if (typeof value !== 'string' || !value) return {};
  try { return JSON.parse(value) as Record<string, unknown>; } catch { return {}; }
}

function encodeCursor(key: Record<string, unknown> | undefined): string | null {
  if (!key) return null;
  return Buffer.from(JSON.stringify(key)).toString('base64url');
}

function decodeCursor(cursor: string | undefined): Record<string, unknown> | undefined {
  if (!cursor) return undefined;
  try { return JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')); } catch { return undefined; }
}

// ── Brand discovery ────────────────────────────────────────────────────────────

async function discoverBrands(region: string | undefined, limit: number, cursor: string | undefined) {
  const scan = await dynamo.send(new ScanCommand({
    TableName: REFDATA_TABLE,
    FilterExpression: 'primaryCat = :cat AND sK = :sk AND #status = :active',
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: { ':cat': 'brand', ':sk': 'profile', ':active': 'ACTIVE' },
    ExclusiveStartKey: decodeCursor(cursor),
    Limit: limit * 3, // over-fetch to allow post-filter
  }));

  const now = Date.now();
  const brands = (scan.Items ?? []).map((item) => {
    const desc = parseRecord(item.desc);
    return {
      brandId: String(item.pK).replace('BRAND#', ''),
      brandName: desc.brandName as string ?? desc.name as string ?? 'Brand',
      brandColor: desc.brandColor as string ?? '#0066CC',
      logoUrl: desc.logoUrl as string ?? null,
      category: desc.category as string ?? null,
      region: desc.region as string ?? null,
      description: desc.description as string ?? null,
      website: desc.website as string ?? null,
    };
  }).filter((b) => !region || !b.region || b.region.toUpperCase() === region.toUpperCase())
    .slice(0, limit);

  return ok({ brands, nextCursor: encodeCursor(scan.LastEvaluatedKey as Record<string, unknown> | undefined) });
}

// ── Offer discovery ────────────────────────────────────────────────────────────

async function discoverOffers(region: string | undefined, limit: number, cursor: string | undefined) {
  const now = new Date().toISOString();
  const scan = await dynamo.send(new ScanCommand({
    TableName: REFDATA_TABLE,
    FilterExpression: '(primaryCat = :cat OR primaryCat = :curated) AND #status = :active',
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: { ':cat': 'offer', ':curated': 'curated_offer', ':active': 'ACTIVE' },
    ExclusiveStartKey: decodeCursor(cursor),
    Limit: limit * 6,
  }));

  const offers = (scan.Items ?? []).map((item) => {
    const desc = parseRecord(item.desc);
    const validTo = desc.validTo as string | undefined;
    if (validTo && new Date(validTo) < new Date()) return null; // expired
    return {
      offerId: desc.offerId as string ?? String(item.sK).replace('OFFER#', ''),
      brandId: desc.brandId as string ?? String(item.pK).replace('BRAND#', ''),
      brandName: desc.brandName as string ?? 'Brand',
      brandColor: desc.brandColor as string ?? '#0066CC',
      brandRegion: desc.brandRegion as string ?? null,
      title: desc.title as string ?? 'Offer',
      description: desc.description as string ?? '',
      imageUrl: desc.imageUrl as string ?? null,
      validFrom: desc.validFrom as string ?? null,
      validTo: validTo ?? null,
      category: desc.category as string ?? null,
      trackingUrl: desc.trackingUrl as string ?? null,
      isBeboCurated: desc.isBeboCurated as boolean ?? false,
      source: desc.source as string ?? null,
    };
  }).filter((o): o is NonNullable<typeof o> =>
    o !== null && (!region || !o.brandRegion || o.brandRegion.toUpperCase() === region.toUpperCase()),
  ).slice(0, limit);

  // Sort: soonest expiry first, then no-expiry
  offers.sort((a, b) => {
    if (!a.validTo) return 1;
    if (!b.validTo) return -1;
    return new Date(a.validTo).getTime() - new Date(b.validTo).getTime();
  });

  return ok({ offers, nextCursor: encodeCursor(scan.LastEvaluatedKey as Record<string, unknown> | undefined) });
}

// ── Catalogue discovery ────────────────────────────────────────────────────────

async function discoverCatalogues(region: string | undefined, limit: number, cursor: string | undefined) {
  const scan = await dynamo.send(new ScanCommand({
    TableName: REFDATA_TABLE,
    FilterExpression: 'primaryCat = :cat AND #status = :active',
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: { ':cat': 'catalogue', ':active': 'ACTIVE' },
    ExclusiveStartKey: decodeCursor(cursor),
    Limit: limit * 4,
  }));

  const catalogues = (scan.Items ?? []).map((item) => {
    const desc = parseRecord(item.desc);
    const validTo = desc.validTo as string | undefined;
    if (validTo && new Date(validTo) < new Date()) return null;
    return {
      catalogueId: desc.catalogueId as string ?? String(item.sK).replace('CATALOGUE#', ''),
      brandId: desc.brandId as string ?? String(item.pK).replace('BRAND#', ''),
      brandName: desc.brandName as string ?? 'Brand',
      brandColor: desc.brandColor as string ?? '#0066CC',
      brandRegion: desc.brandRegion as string ?? null,
      title: desc.title as string ?? 'Catalogue',
      description: desc.description as string ?? '',
      headerImageUrl: desc.headerImageUrl as string ?? null,
      pdfUrl: desc.pdfUrl as string ?? null,
      itemCount: (desc.items as unknown[] | undefined)?.length ?? 0,
      validFrom: desc.validFrom as string ?? null,
      validTo: validTo ?? null,
    };
  }).filter((c): c is NonNullable<typeof c> =>
    c !== null && (!region || !c.brandRegion || c.brandRegion.toUpperCase() === region.toUpperCase()),
  ).slice(0, limit);

  catalogues.sort((a, b) => {
    if (!a.validTo) return 1;
    if (!b.validTo) return -1;
    return new Date(b.validTo).getTime() - new Date(a.validTo).getTime(); // latest expiry first
  });

  return ok({ catalogues, nextCursor: encodeCursor(scan.LastEvaluatedKey as Record<string, unknown> | undefined) });
}

// ── Newsletter discovery ───────────────────────────────────────────────────────

async function discoverNewsletters(region: string | undefined, limit: number, cursor: string | undefined) {
  const scan = await dynamo.send(new ScanCommand({
    TableName: REFDATA_TABLE,
    FilterExpression: 'primaryCat = :cat AND #status = :active',
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: { ':cat': 'newsletter', ':active': 'ACTIVE' },
    ExclusiveStartKey: decodeCursor(cursor),
    Limit: limit * 4,
  }));

  const newsletters = (scan.Items ?? []).map((item) => {
    const desc = parseRecord(item.desc);
    return {
      newsletterId: desc.newsletterId as string ?? String(item.sK).replace('NEWSLETTER#', ''),
      brandId: desc.brandId as string ?? String(item.pK).replace('BRAND#', ''),
      brandName: desc.brandName as string ?? 'Brand',
      brandColor: desc.brandColor as string ?? '#0066CC',
      brandRegion: desc.brandRegion as string ?? null,
      subject: desc.subject as string ?? '(No subject)',
      imageUrl: desc.imageUrl as string ?? null,
      createdAt: item.createdAt as string ?? new Date().toISOString(),
    };
  }).filter((n): n is NonNullable<typeof n> =>
    n !== null && (!region || !n.brandRegion || n.brandRegion.toUpperCase() === region.toUpperCase()),
  ).slice(0, limit);

  // Most recent first
  newsletters.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());

  return ok({ newsletters, nextCursor: encodeCursor(scan.LastEvaluatedKey as Record<string, unknown> | undefined) });
}

// ── Main handler ───────────────────────────────────────────────────────────────
//
// personalization: if Authorizer claims are present, we boost brands already in wallet.
// (P2-14: Personalised relevance feed)

export const handler: Handler = async (event: any) => {
  const method = (event.httpMethod as string ?? 'GET').toUpperCase();
  const path = event.path as string ?? '/';
  const qs: Record<string, string> = event.queryStringParameters ?? {};

  if (method === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  // Resolve identity if available (AppSync/Cognito Authorizer)
  const permULID = event.requestContext?.authorizer?.claims?.['custom:permULID'] 
    || event.requestContext?.authorizer?.claims?.['permULID'];

  const region = qs.region;
  const limit = Math.min(Math.max(Number(qs.limit ?? 20), 1), MAX_LIMIT);
  const cursor = qs.cursor;

  console.info(`[discovery] ${method} ${path} region=${region ?? 'all'} user=${permULID ?? 'anon'}`);

  let walletBrands: string[] = [];
  if (permULID) {
    try {
      const walletRes = await dynamo.send(new ScanCommand({
        TableName: process.env.USER_TABLE!,
        FilterExpression: 'pK = :pk AND eventType = :cat',
        ExpressionAttributeValues: { ':pk': `USER#${permULID}`, ':cat': 'CARD' },
        ProjectionExpression: 'subCategory',
      }));
      walletBrands = (walletRes.Items ?? []).map(i => i.subCategory as string);
    } catch (err) {
      console.warn('[discovery] failed to fetch wallet brands for personalization:', err);
    }
  }

  try {
    let result: any;
    if (path.endsWith('/brands')) {
        result = await discoverBrands(region, limit, cursor);
        if (permULID && result.statusCode === 200) {
            const body = JSON.parse(result.body);
            body.brands.sort((a: any, b: any) => {
                const aIn = walletBrands.includes(a.brandId) ? 1 : 0;
                const bIn = walletBrands.includes(b.brandId) ? 1 : 0;
                return bIn - aIn;
            });
            result.body = JSON.stringify(body);
        }
        return result;
    }
    
    if (path.endsWith('/offers')) {
        result = await discoverOffers(region, limit, cursor);
        if (permULID && result.statusCode === 200) {
            const body = JSON.parse(result.body);
            body.offers.sort((a: any, b: any) => {
                const aIn = walletBrands.includes(a.brandId) ? 1 : 0;
                const bIn = walletBrands.includes(b.brandId) ? 1 : 0;
                return bIn - aIn;
            });
            result.body = JSON.stringify(body);
        }
        return result;
    }

    if (path.endsWith('/catalogues')) return await discoverCatalogues(region, limit, cursor);
    if (path.endsWith('/newsletters')) return await discoverNewsletters(region, limit, cursor);
    return notFound();
  } catch (err) {
    console.error('[discovery] unhandled error:', err);
    return serverError();
  }
};
