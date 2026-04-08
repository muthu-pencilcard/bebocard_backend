/**
 * catalog-sync — EventBridge cron, runs weekly (Sunday 02:00 UTC).
 *
 * Pulls the gift card product catalog from each configured distributor and
 * upserts GIFTCARD#<brandId>#<skuId> records into RefDataEvent so the
 * GiftCardMarketplacePage has live data.
 *
 * Record shape in RefDataEvent:
 *   pK:         "GIFTCARD#<brandId>#<skuId>"
 *   sK:         "profile"
 *   primaryCat: "gift_card_catalog"
 *   status:     "ACTIVE" | "INACTIVE"
 *   brandId:    <brandId>
 *   desc:       JSON { brandName, logoUrl, category, region, denomination, currency,
 *                      distributorId, distributorSku, discountPct, expiryPolicy,
 *                      redemptionInstructions }
 *   updatedAt:  ISO timestamp
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';

const dynamo   = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const REF_TABLE = process.env.REFDATA_TABLE ?? process.env.REF_TABLE!;

// ── Entry point ───────────────────────────────────────────────────────────────

export const handler = async (_event?: unknown, _context?: unknown, _callback?: unknown) => {
  const results: Record<string, { upserted: number; errors: number }> = {};

  await Promise.allSettled([
    runDistributor('prezzee',    syncPrezzee,    results),
    runDistributor('tango',      syncTango,      results),
    runDistributor('runa',       syncRuna,       results),
    runDistributor('yougotagift', syncYOUGotaGift, results),
    runDistributor('reloadly',   syncReloadly,   results),
  ]);

  console.log('[catalog-sync] completed', JSON.stringify(results));
  return results;
};

async function runDistributor(
  id: string,
  fn: () => Promise<CatalogItem[]>,
  results: Record<string, { upserted: number; errors: number }>,
) {
  results[id] = { upserted: 0, errors: 0 };
  try {
    const items = await fn();
    for (const item of items) {
      try {
        await upsert(item);
        results[id].upserted++;
      } catch (err) {
        console.error(`[catalog-sync] upsert error ${id}/${item.skuId}`, err);
        results[id].errors++;
      }
    }
    // Mark any previously-active SKUs from this distributor that are no longer
    // in the catalog as INACTIVE.
    await deactivateRemovedSkus(id, items.map(i => `GIFTCARD#${i.brandId}#${i.skuId}`));
  } catch (err) {
    console.error(`[catalog-sync] distributor fetch error ${id}`, err);
    results[id].errors++;
  }
}

// ── DynamoDB ──────────────────────────────────────────────────────────────────

interface CatalogItem {
  brandId: string;
  skuId: string;
  brandName: string;
  logoUrl?: string;
  category: string;
  region: string;
  denomination: number;
  currency: string;
  distributorId: string;
  distributorSku: string;
  discountPct?: number;
  expiryPolicy?: string;
  redemptionInstructions?: string;
}

async function upsert(item: CatalogItem) {
  const pK       = `GIFTCARD#${item.brandId}#${item.skuId}`;
  const now      = new Date().toISOString();
  await dynamo.send(new PutCommand({
    TableName: REF_TABLE,
    Item: {
      pK,
      sK:         'profile',
      primaryCat: 'gift_card_catalog',
      status:     'ACTIVE',
      brandId:    item.brandId,
      desc: JSON.stringify({
        brandName:              item.brandName,
        logoUrl:                item.logoUrl ?? null,
        category:               item.category,
        region:                 item.region,
        denomination:           item.denomination,
        currency:               item.currency,
        distributorId:          item.distributorId,
        distributorSku:         item.distributorSku,
        discountPct:            item.discountPct ?? null,
        expiryPolicy:           item.expiryPolicy ?? null,
        redemptionInstructions: item.redemptionInstructions ?? null,
      }),
      updatedAt: now,
    },
  }));
}

async function deactivateRemovedSkus(distributorId: string, activePKs: string[]) {
  // Scan for ACTIVE gift_card_catalog items for this distributor, mark removed ones INACTIVE.
  const activeSet = new Set(activePKs);
  let lastKey: Record<string, unknown> | undefined;
  do {
    const res = await dynamo.send(new ScanCommand({
      TableName: REF_TABLE,
      FilterExpression: 'primaryCat = :cat AND #s = :active',
      ExpressionAttributeNames: { '#s': 'status' },
      ExpressionAttributeValues: { ':cat': 'gift_card_catalog', ':active': 'ACTIVE' },
      ProjectionExpression: 'pK, sK, desc',
      ExclusiveStartKey: lastKey,
    }));
    for (const item of res.Items ?? []) {
      const pK  = item.pK as string;
      if (!pK.startsWith('GIFTCARD#')) continue;
      const desc = safeJson(item.desc);
      if (desc.distributorId !== distributorId) continue; // not this distributor's record
      if (!activeSet.has(pK)) {
        await dynamo.send(new UpdateCommand({
          TableName: REF_TABLE,
          Key: { pK, sK: 'profile' },
          UpdateExpression: 'SET #s = :inactive, updatedAt = :now',
          ExpressionAttributeNames: { '#s': 'status' },
          ExpressionAttributeValues: { ':inactive': 'INACTIVE', ':now': new Date().toISOString() },
        }));
      }
    }
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);
}

function safeJson(raw: unknown): Record<string, unknown> {
  if (typeof raw !== 'string') return {};
  try { return JSON.parse(raw) as Record<string, unknown>; } catch { return {}; }
}

// ── Distributor: Prezzee (AU) ─────────────────────────────────────────────────

async function syncPrezzee(): Promise<CatalogItem[]> {
  const key = process.env.PREZZEE_API_KEY;
  if (!key) { console.warn('[catalog-sync] PREZZEE_API_KEY not set — skipping'); return []; }

  const res = await fetch('https://api.prezzee.com/v1/catalog', {
    headers: { 'Authorization': `Bearer ${key}` },
  });
  if (!res.ok) throw new Error(`Prezzee catalog failed: ${res.status}`);
  const data = await res.json() as { products: Array<{
    productId: string;
    name: string;
    logoUrl?: string;
    category?: string;
    faceValue: number;
    currency: string;
    expiryPolicy?: string;
    howToRedeem?: string;
  }> };

  return (data.products ?? []).map(p => ({
    brandId:                slugify(p.name),
    skuId:                  p.productId,
    brandName:              p.name,
    logoUrl:                p.logoUrl,
    category:               mapCategory(p.category ?? ''),
    region:                 'AU',
    denomination:           p.faceValue,
    currency:               p.currency ?? 'AUD',
    distributorId:          'prezzee',
    distributorSku:         p.productId,
    expiryPolicy:           p.expiryPolicy,
    redemptionInstructions: p.howToRedeem,
  }));
}

// ── Distributor: Tango / BHN RaaS (US) ───────────────────────────────────────

async function syncTango(): Promise<CatalogItem[]> {
  const key = process.env.TANGO_API_KEY;
  if (!key) { console.warn('[catalog-sync] TANGO_API_KEY not set — skipping'); return []; }

  const res = await fetch('https://api.tangocard.com/raas/v2/catalog', {
    headers: { 'Authorization': `Bearer ${key}` },
  });
  if (!res.ok) throw new Error(`Tango catalog failed: ${res.status}`);
  const data = await res.json() as { brands: Array<{
    brandKey: string;
    brandName: string;
    imageUrls?: { 'square-26'?: string };
    items: Array<{
      utid: string;
      faceValue: number;
      currencyCode: string;
      rewardName?: string;
      disclaimer?: string;
    }>;
  }> };

  const items: CatalogItem[] = [];
  for (const brand of data.brands ?? []) {
    for (const sku of brand.items ?? []) {
      items.push({
        brandId:    slugify(brand.brandName),
        skuId:      sku.utid,
        brandName:  brand.brandName,
        logoUrl:    brand.imageUrls?.['square-26'],
        category:   'retail',
        region:     'US',
        denomination: sku.faceValue,
        currency:   sku.currencyCode ?? 'USD',
        distributorId:  'tango',
        distributorSku: sku.utid,
        expiryPolicy: sku.disclaimer,
      });
    }
  }
  return items;
}

// ── Distributor: Runa (UK) ────────────────────────────────────────────────────

async function syncRuna(): Promise<CatalogItem[]> {
  const key = process.env.RUNA_API_KEY;
  if (!key) { console.warn('[catalog-sync] RUNA_API_KEY not set — skipping'); return []; }

  const res = await fetch('https://api.runa.io/v1/products', {
    headers: { 'Authorization': `Bearer ${key}` },
  });
  if (!res.ok) throw new Error(`Runa catalog failed: ${res.status}`);
  const data = await res.json() as { products: Array<{
    product_id: string;
    name: string;
    logo_url?: string;
    category?: string;
    denomination: number;
    currency: string;
    expiry_policy?: string;
    how_to_redeem?: string;
  }> };

  return (data.products ?? []).map(p => ({
    brandId:                slugify(p.name),
    skuId:                  p.product_id,
    brandName:              p.name,
    logoUrl:                p.logo_url,
    category:               mapCategory(p.category ?? ''),
    region:                 'UK',
    denomination:           p.denomination,
    currency:               p.currency ?? 'GBP',
    distributorId:          'runa',
    distributorSku:         p.product_id,
    expiryPolicy:           p.expiry_policy,
    redemptionInstructions: p.how_to_redeem,
  }));
}

// ── Distributor: YOUGotaGift (UAE/GCC) ───────────────────────────────────────

async function syncYOUGotaGift(): Promise<CatalogItem[]> {
  const key = process.env.YOUGOTAGIFT_API_KEY;
  if (!key) { console.warn('[catalog-sync] YOUGOTAGIFT_API_KEY not set — skipping'); return []; }

  const res = await fetch('https://api.yougotagift.com/v2/products', {
    headers: { 'X-Api-Key': key },
  });
  if (!res.ok) throw new Error(`YOUGotaGift catalog failed: ${res.status}`);
  const data = await res.json() as { products: Array<{
    sku: string;
    name: string;
    logoUrl?: string;
    category?: string;
    amount: number;
    currency: string;
    expiryPolicy?: string;
    redeemInstructions?: string;
  }> };

  return (data.products ?? []).map(p => ({
    brandId:                slugify(p.name),
    skuId:                  p.sku,
    brandName:              p.name,
    logoUrl:                p.logoUrl,
    category:               mapCategory(p.category ?? ''),
    region:                 'UAE',
    denomination:           p.amount,
    currency:               p.currency ?? 'AED',
    distributorId:          'yougotagift',
    distributorSku:         p.sku,
    expiryPolicy:           p.expiryPolicy,
    redemptionInstructions: p.redeemInstructions,
  }));
}

// ── Distributor: Reloadly (global fallback) ───────────────────────────────────

async function syncReloadly(): Promise<CatalogItem[]> {
  const clientId     = process.env.RELOADLY_CLIENT_ID;
  const clientSecret = process.env.RELOADLY_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    console.warn('[catalog-sync] RELOADLY credentials not set — skipping');
    return [];
  }

  // Auth
  const authRes = await fetch('https://auth.reloadly.com/oauth/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: 'client_credentials',
      audience: 'https://giftcards.reloadly.com',
    }),
  });
  if (!authRes.ok) throw new Error(`Reloadly auth failed: ${authRes.status}`);
  const { access_token: token } = await authRes.json() as { access_token: string };

  // Fetch discounts map (per-product discount percentages)
  const discountMap = await fetchReloadlyDiscounts(token);

  // Paginate through products (page size 200)
  const products: CatalogItem[] = [];
  let page = 1;
  while (true) {
    const res = await fetch(`https://giftcards.reloadly.com/products?size=200&page=${page}`, {
      headers: { 'Authorization': `Bearer ${token}` },
    });
    if (!res.ok) throw new Error(`Reloadly products failed: ${res.status}`);
    const data = await res.json() as {
      content: Array<{
        productId: number;
        productName: string;
        logoUrls?: string[];
        category?: { name?: string };
        fixedRecipientDenominations?: number[];
        recipientCurrencyCode?: string;
        countryCode?: string;
        expiryAndValidity?: string;
        redeemInstruction?: { verbose?: string };
      }>;
      last: boolean;
    };

    for (const p of data.content ?? []) {
      // Only include products with fixed denominations (variable-denomination excluded)
      if (!p.fixedRecipientDenominations?.length) continue;
      for (const denom of p.fixedRecipientDenominations) {
        const skuId = `${p.productId}-${denom}`;
        products.push({
          brandId:                slugify(p.productName),
          skuId,
          brandName:              p.productName,
          logoUrl:                p.logoUrls?.[0],
          category:               mapCategory(p.category?.name ?? ''),
          region:                 p.countryCode ?? 'GLOBAL',
          denomination:           denom,
          currency:               p.recipientCurrencyCode ?? 'USD',
          distributorId:          'reloadly',
          distributorSku:         String(p.productId),
          discountPct:            discountMap[p.productId],
          expiryPolicy:           p.expiryAndValidity,
          redemptionInstructions: p.redeemInstruction?.verbose,
        });
      }
    }

    if (data.last) break;
    page++;
  }
  return products;
}

async function fetchReloadlyDiscounts(token: string): Promise<Record<number, number>> {
  try {
    const res = await fetch('https://giftcards.reloadly.com/discounts', {
      headers: { 'Authorization': `Bearer ${token}` },
    });
    if (!res.ok) return {};
    const data = await res.json() as { content: Array<{ product: { productId: number }; percentage: number }> };
    const map: Record<number, number> = {};
    for (const d of data.content ?? []) map[d.product.productId] = d.percentage;
    return map;
  } catch {
    return {};
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function slugify(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

const CATEGORY_MAP: Record<string, string> = {
  food:          'dining',
  restaurant:    'dining',
  dining:        'dining',
  grocery:       'grocery',
  supermarket:   'grocery',
  retail:        'retail',
  fashion:       'retail',
  clothing:      'retail',
  electronics:   'retail',
  entertainment: 'entertainment',
  movies:        'entertainment',
  gaming:        'gaming',
  games:         'gaming',
  health:        'health',
  wellness:      'health',
  beauty:        'health',
  travel:        'travel',
  hotels:        'travel',
  airlines:      'travel',
};

function mapCategory(raw: string): string {
  const lower = raw.toLowerCase();
  for (const [key, cat] of Object.entries(CATEGORY_MAP)) {
    if (lower.includes(key)) return cat;
  }
  return 'retail';
}
