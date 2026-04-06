/**
 * catalog-subscription-sync — EventBridge cron, weekly (Sunday 03:00 UTC).
 *
 * Maintains the subscription provider catalog in RefDataEvent so the
 * SubscriptionMarketplacePage has live data, and writes BENCHMARK# records
 * so subscription-negotiator can compare user costs against market rates.
 *
 * RefDataEvent record shape:
 *   pK:         "SUBSCRIPTION_CATALOG#<providerId>"
 *   sK:         "profile"
 *   primaryCat: "subscription_catalog"
 *   status:     "ACTIVE" | "INACTIVE"
 *   desc:       JSON { providerId, name, logoUrl, category, region,
 *                      plans[], affiliateUrl, benchmarkPrice,
 *                      benchmarkFrequency, tags[] }
 *   updatedAt:  ISO timestamp
 *
 * BENCHMARK# record shape (consumed by subscription-negotiator):
 *   pK:             "BENCHMARK#<providerId>"
 *   sK:             "BENCHMARK"
 *   benchmarkAmount: number (lowest popular plan monthly equivalent)
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';

const dynamo    = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const REF_TABLE = process.env.REFDATA_TABLE ?? process.env.REF_TABLE!;

// ── Provider catalog ──────────────────────────────────────────────────────────

interface SubscriptionPlan {
  planId:    string;
  name:      string;
  price:     number;        // monthly equivalent in AUD
  currency:  string;
  frequency: string;        // 'monthly' | 'annually'
  features:  string[];
}

interface Provider {
  providerId:         string;
  name:               string;
  logoUrl:            string | null;
  category:           string;
  region:             string;
  plans:              SubscriptionPlan[];
  affiliateUrl:       string;
  benchmarkPrice:     number;   // lowest popular plan monthly equivalent
  benchmarkFrequency: string;
  tags:               string[];
  cancelUrl?:         string;   // direct cancellation page URL
  portalUrl?:         string;   // account management portal URL
  invoiceType?:       'SUBSCRIPTION' | 'RECURRING_INVOICE' | 'BOTH';
}

// Prices current as of 2026-Q1 (AU). Update via re-deploy or add price API.
const PROVIDERS: Provider[] = [

  // ── Streaming ──────────────────────────────────────────────────────────────

  {
    providerId: 'netflix', name: 'Netflix', logoUrl: null, category: 'Streaming', region: 'AU',
    affiliateUrl: 'https://www.netflix.com/au/signup',
    cancelUrl: 'https://www.netflix.com/account/membership',
    portalUrl: 'https://www.netflix.com/account',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 7.99, benchmarkFrequency: 'monthly',
    tags: ['streaming', 'movies', 'tv'],
    plans: [
      { planId: 'netflix-ads',      name: 'Standard with Ads', price: 7.99,  currency: 'AUD', frequency: 'monthly', features: ['1080p', '2 screens', 'ads'] },
      { planId: 'netflix-standard', name: 'Standard',          price: 16.99, currency: 'AUD', frequency: 'monthly', features: ['1080p', '2 screens', 'no ads'] },
      { planId: 'netflix-premium',  name: 'Premium',           price: 22.99, currency: 'AUD', frequency: 'monthly', features: ['4K', '4 screens', 'spatial audio'] },
    ],
  },
  {
    providerId: 'disney-plus', name: 'Disney+', logoUrl: null, category: 'Streaming', region: 'AU',
    affiliateUrl: 'https://www.disneyplus.com/en-au/subscribe',
    cancelUrl: 'https://www.disneyplus.com/account/subscription',
    portalUrl: 'https://www.disneyplus.com/account',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 13.99, benchmarkFrequency: 'monthly',
    tags: ['streaming', 'movies', 'disney', 'marvel', 'star-wars'],
    plans: [
      { planId: 'disney-standard', name: 'Standard', price: 13.99, currency: 'AUD', frequency: 'monthly', features: ['Full library', '4 screens', '1080p'] },
      { planId: 'disney-premium',  name: 'Premium',  price: 17.99, currency: 'AUD', frequency: 'monthly', features: ['Full library', '4 screens', '4K'] },
    ],
  },
  {
    providerId: 'stan', name: 'Stan', logoUrl: null, category: 'Streaming', region: 'AU',
    affiliateUrl: 'https://www.stan.com.au/signup',
    cancelUrl: 'https://www.stan.com.au/settings/membership',
    portalUrl: 'https://www.stan.com.au/settings',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 12.00, benchmarkFrequency: 'monthly',
    tags: ['streaming', 'australian', 'movies', 'tv'],
    plans: [
      { planId: 'stan-basic',    name: 'Basic',    price: 12.00, currency: 'AUD', frequency: 'monthly', features: ['1 screen', 'HD'] },
      { planId: 'stan-standard', name: 'Standard', price: 16.00, currency: 'AUD', frequency: 'monthly', features: ['3 screens', 'HD'] },
      { planId: 'stan-premium',  name: 'Premium',  price: 19.00, currency: 'AUD', frequency: 'monthly', features: ['4 screens', '4K'] },
    ],
  },
  {
    providerId: 'binge', name: 'Binge', logoUrl: null, category: 'Streaming', region: 'AU',
    affiliateUrl: 'https://binge.com.au/signup',
    cancelUrl: 'https://binge.com.au/settings/account',
    portalUrl: 'https://binge.com.au/settings',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 10.00, benchmarkFrequency: 'monthly',
    tags: ['streaming', 'foxtel', 'hbo', 'tv'],
    plans: [
      { planId: 'binge-basic',    name: 'Basic',    price: 10.00, currency: 'AUD', frequency: 'monthly', features: ['1 screen', 'HD'] },
      { planId: 'binge-standard', name: 'Standard', price: 18.00, currency: 'AUD', frequency: 'monthly', features: ['2 screens', 'HD'] },
      { planId: 'binge-max',      name: 'Max',      price: 25.00, currency: 'AUD', frequency: 'monthly', features: ['4 screens', '4K HDR'] },
    ],
  },
  {
    providerId: 'apple-tv-plus', name: 'Apple TV+', logoUrl: null, category: 'Streaming', region: 'AU',
    affiliateUrl: 'https://tv.apple.com/au/subscribe',
    cancelUrl: 'https://support.apple.com/en-au/118223',
    portalUrl: 'https://appleid.apple.com/account/manage/subscriptions',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 12.99, benchmarkFrequency: 'monthly',
    tags: ['streaming', 'apple', 'originals'],
    plans: [
      { planId: 'apple-tv-individual', name: 'Individual', price: 12.99, currency: 'AUD', frequency: 'monthly', features: ['6 screens', '4K HDR'] },
    ],
  },
  {
    providerId: 'youtube-premium', name: 'YouTube Premium', logoUrl: null, category: 'Streaming', region: 'AU',
    affiliateUrl: 'https://www.youtube.com/premium',
    cancelUrl: 'https://www.youtube.com/paid_memberships',
    portalUrl: 'https://myaccount.google.com/subscriptions',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 14.99, benchmarkFrequency: 'monthly',
    tags: ['streaming', 'google', 'ad-free', 'music'],
    plans: [
      { planId: 'yt-premium-individual', name: 'Individual', price: 14.99, currency: 'AUD', frequency: 'monthly', features: ['Ad-free', 'Background play', 'YouTube Music'] },
      { planId: 'yt-premium-family',     name: 'Family',     price: 22.99, currency: 'AUD', frequency: 'monthly', features: ['Up to 5 members', 'Ad-free', 'YouTube Music'] },
    ],
  },

  // ── Music ──────────────────────────────────────────────────────────────────

  {
    providerId: 'spotify', name: 'Spotify', logoUrl: null, category: 'Music', region: 'AU',
    affiliateUrl: 'https://www.spotify.com/au/premium/',
    cancelUrl: 'https://www.spotify.com/au/account/subscription/cancel',
    portalUrl: 'https://www.spotify.com/au/account',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 12.99, benchmarkFrequency: 'monthly',
    tags: ['music', 'podcasts', 'streaming'],
    plans: [
      { planId: 'spotify-individual', name: 'Individual', price: 12.99, currency: 'AUD', frequency: 'monthly', features: ['Ad-free', 'Offline', 'High quality'] },
      { planId: 'spotify-duo',        name: 'Duo',        price: 17.99, currency: 'AUD', frequency: 'monthly', features: ['2 accounts', 'Offline'] },
      { planId: 'spotify-family',     name: 'Family',     price: 23.99, currency: 'AUD', frequency: 'monthly', features: ['6 accounts', 'Parental controls'] },
    ],
  },
  {
    providerId: 'apple-music', name: 'Apple Music', logoUrl: null, category: 'Music', region: 'AU',
    affiliateUrl: 'https://music.apple.com/au/subscribe',
    cancelUrl: 'https://support.apple.com/en-au/118223',
    portalUrl: 'https://appleid.apple.com/account/manage/subscriptions',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 11.99, benchmarkFrequency: 'monthly',
    tags: ['music', 'apple', 'streaming'],
    plans: [
      { planId: 'apple-music-individual', name: 'Individual', price: 11.99, currency: 'AUD', frequency: 'monthly', features: ['90M songs', 'Lossless', 'Spatial Audio'] },
      { planId: 'apple-music-family',     name: 'Family',     price: 18.99, currency: 'AUD', frequency: 'monthly', features: ['Up to 6 members', 'Lossless'] },
    ],
  },
  {
    providerId: 'audible', name: 'Audible', logoUrl: null, category: 'Music', region: 'AU',
    affiliateUrl: 'https://www.audible.com.au/ep/freetrial',
    cancelUrl: 'https://www.audible.com.au/account/cancel',
    portalUrl: 'https://www.audible.com.au/account',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 16.45, benchmarkFrequency: 'monthly',
    tags: ['audiobooks', 'amazon', 'podcasts'],
    plans: [
      { planId: 'audible-plus',    name: 'Audible Plus',    price: 9.99,  currency: 'AUD', frequency: 'monthly', features: ['Unlimited catalogue access', 'Podcasts'] },
      { planId: 'audible-premium', name: 'Audible Premium', price: 16.45, currency: 'AUD', frequency: 'monthly', features: ['1 credit/month', 'Unlimited catalogue'] },
    ],
  },

  // ── Productivity ───────────────────────────────────────────────────────────

  {
    providerId: 'microsoft-365', name: 'Microsoft 365', logoUrl: null, category: 'Productivity', region: 'AU',
    affiliateUrl: 'https://www.microsoft.com/en-au/microsoft-365/personal',
    cancelUrl: 'https://account.microsoft.com/services',
    portalUrl: 'https://account.microsoft.com/services',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 11.99, benchmarkFrequency: 'monthly',
    tags: ['office', 'word', 'excel', 'onedrive'],
    plans: [
      { planId: 'm365-personal', name: 'Personal', price: 11.99, currency: 'AUD', frequency: 'monthly', features: ['1 user', '1TB OneDrive', 'Word/Excel/PowerPoint'] },
      { planId: 'm365-family',   name: 'Family',   price: 17.99, currency: 'AUD', frequency: 'monthly', features: ['Up to 6 users', '6TB OneDrive'] },
    ],
  },
  {
    providerId: 'adobe-cc', name: 'Adobe Creative Cloud', logoUrl: null, category: 'Productivity', region: 'AU',
    affiliateUrl: 'https://www.adobe.com/au/creativecloud/plans.html',
    cancelUrl: 'https://account.adobe.com/plans',
    portalUrl: 'https://account.adobe.com/plans',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 87.98, benchmarkFrequency: 'monthly',
    tags: ['design', 'photo', 'video', 'photoshop'],
    plans: [
      { planId: 'adobe-photography', name: 'Photography',  price: 16.99, currency: 'AUD', frequency: 'monthly', features: ['Lightroom', 'Photoshop', '20GB cloud'] },
      { planId: 'adobe-all-apps',    name: 'All Apps',     price: 87.98, currency: 'AUD', frequency: 'monthly', features: ['All 20+ apps', '100GB cloud'] },
    ],
  },
  {
    providerId: 'dropbox', name: 'Dropbox', logoUrl: null, category: 'Productivity', region: 'AU',
    affiliateUrl: 'https://www.dropbox.com/plans',
    cancelUrl: 'https://www.dropbox.com/account/plan',
    portalUrl: 'https://www.dropbox.com/account/plan',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 13.99, benchmarkFrequency: 'monthly',
    tags: ['storage', 'cloud', 'backup'],
    plans: [
      { planId: 'dropbox-plus',       name: 'Plus',       price: 13.99, currency: 'AUD', frequency: 'monthly', features: ['2TB storage', 'Smart sync'] },
      { planId: 'dropbox-essentials', name: 'Essentials', price: 22.00, currency: 'AUD', frequency: 'monthly', features: ['3TB storage', 'eSign', 'Screen recorder'] },
    ],
  },

  // ── Health ─────────────────────────────────────────────────────────────────

  {
    providerId: 'headspace', name: 'Headspace', logoUrl: null, category: 'Health', region: 'AU',
    affiliateUrl: 'https://www.headspace.com/subscribe',
    cancelUrl: 'https://www.headspace.com/account',
    portalUrl: 'https://www.headspace.com/account',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 12.99, benchmarkFrequency: 'monthly',
    tags: ['meditation', 'sleep', 'mindfulness'],
    plans: [
      { planId: 'headspace-monthly', name: 'Monthly', price: 12.99, currency: 'AUD', frequency: 'monthly', features: ['Guided meditation', 'Sleep sounds', 'Mindfulness'] },
      { planId: 'headspace-annual',  name: 'Annual',  price: 7.99,  currency: 'AUD', frequency: 'monthly', features: ['All features', '2 months free'] },
    ],
  },
  {
    providerId: 'calm', name: 'Calm', logoUrl: null, category: 'Health', region: 'AU',
    affiliateUrl: 'https://www.calm.com/subscribe',
    cancelUrl: 'https://www.calm.com/account',
    portalUrl: 'https://www.calm.com/account',
    invoiceType: 'SUBSCRIPTION',
    benchmarkPrice: 14.99, benchmarkFrequency: 'monthly',
    tags: ['meditation', 'sleep', 'anxiety'],
    plans: [
      { planId: 'calm-premium', name: 'Premium', price: 14.99, currency: 'AUD', frequency: 'monthly', features: ['Sleep stories', 'Masterclasses', 'Music'] },
    ],
  },

  // ── Telecom ────────────────────────────────────────────────────────────────

  {
    providerId: 'telstra', name: 'Telstra', logoUrl: null, category: 'Telecom', region: 'AU',
    affiliateUrl: 'https://www.telstra.com.au/mobile-phones/mobile-phone-plans',
    cancelUrl: 'https://www.telstra.com.au/myaccount',
    portalUrl: 'https://www.telstra.com.au/myaccount',
    invoiceType: 'RECURRING_INVOICE',
    benchmarkPrice: 55.00, benchmarkFrequency: 'monthly',
    tags: ['mobile', 'data', '5g', 'telco'],
    plans: [
      { planId: 'telstra-basic',      name: 'Basic',       price: 55.00,  currency: 'AUD', frequency: 'monthly', features: ['30GB data', '5G', 'Unlimited calls'] },
      { planId: 'telstra-essential',  name: 'Essential',   price: 65.00,  currency: 'AUD', frequency: 'monthly', features: ['60GB data', '5G', 'Unlimited calls'] },
      { planId: 'telstra-unlimited',  name: 'Unlimited',   price: 85.00,  currency: 'AUD', frequency: 'monthly', features: ['Unlimited data', '5G', 'Global roaming'] },
    ],
  },
  {
    providerId: 'optus', name: 'Optus', logoUrl: null, category: 'Telecom', region: 'AU',
    affiliateUrl: 'https://www.optus.com.au/mobile/mobile-phone-plans',
    cancelUrl: 'https://www.optus.com.au/myaccount',
    portalUrl: 'https://www.optus.com.au/myaccount',
    invoiceType: 'RECURRING_INVOICE',
    benchmarkPrice: 49.00, benchmarkFrequency: 'monthly',
    tags: ['mobile', 'data', '5g', 'telco'],
    plans: [
      { planId: 'optus-30gb',        name: '30GB',         price: 49.00,  currency: 'AUD', frequency: 'monthly', features: ['30GB data', '5G', 'Unlimited calls'] },
      { planId: 'optus-60gb',        name: '60GB',         price: 59.00,  currency: 'AUD', frequency: 'monthly', features: ['60GB data', '5G', 'Unlimited calls'] },
      { planId: 'optus-unlimited',   name: 'Unlimited',    price: 79.00,  currency: 'AUD', frequency: 'monthly', features: ['Unlimited data', '5G'] },
    ],
  },

  // ── Utilities ──────────────────────────────────────────────────────────────

  {
    providerId: 'agl', name: 'AGL', logoUrl: null, category: 'Utilities', region: 'AU',
    affiliateUrl: 'https://www.agl.com.au/residential/electricity',
    cancelUrl: 'https://www.agl.com.au/myaccount',
    portalUrl: 'https://www.agl.com.au/myaccount',
    invoiceType: 'RECURRING_INVOICE',
    benchmarkPrice: 180.00, benchmarkFrequency: 'monthly',
    tags: ['electricity', 'gas', 'energy'],
    plans: [
      { planId: 'agl-saver',    name: 'Saver',     price: 160.00, currency: 'AUD', frequency: 'monthly', features: ['No exit fees', 'Online billing'] },
      { planId: 'agl-everyday', name: 'Everyday',  price: 180.00, currency: 'AUD', frequency: 'monthly', features: ['Fixed rate', 'Flexible billing'] },
    ],
  },
  {
    providerId: 'origin-energy', name: 'Origin Energy', logoUrl: null, category: 'Utilities', region: 'AU',
    affiliateUrl: 'https://www.originenergy.com.au/electricity-gas/plans/',
    cancelUrl: 'https://www.originenergy.com.au/myaccount',
    portalUrl: 'https://www.originenergy.com.au/myaccount',
    invoiceType: 'RECURRING_INVOICE',
    benchmarkPrice: 175.00, benchmarkFrequency: 'monthly',
    tags: ['electricity', 'gas', 'energy'],
    plans: [
      { planId: 'origin-basic-online',  name: 'Basic Online', price: 165.00, currency: 'AUD', frequency: 'monthly', features: ['Online billing', 'No exit fees'] },
      { planId: 'origin-go-direct',     name: 'Go Direct',    price: 175.00, currency: 'AUD', frequency: 'monthly', features: ['Direct debit discount'] },
    ],
  },
];

// ── Affiliate URL helper ──────────────────────────────────────────────────────
// Appends BeboCard tracking params to affiliate URLs.
// Replace with real affiliate network IDs when partnerships are finalised.
// Format: ?ref=bebocard&utm_source=bebocard&utm_medium=affiliate&utm_campaign=marketplace

function withAffiliateParams(url: string): string {
  const separator = url.includes('?') ? '&' : '?';
  return `${url}${separator}ref=bebocard&utm_source=bebocard&utm_medium=affiliate&utm_campaign=marketplace`;
}

// ── Entry point ───────────────────────────────────────────────────────────────

export const handler = async () => {
  let upserted = 0;
  let errors   = 0;

  for (const provider of PROVIDERS) {
    try {
      await upsertCatalog(provider);
      await upsertBenchmark(provider);
      upserted++;
    } catch (err) {
      console.error(`[catalog-subscription-sync] error for ${provider.providerId}`, err);
      errors++;
    }
  }

  // Deactivate catalog records no longer in the list
  await deactivateRemoved(PROVIDERS.map(p => `SUBSCRIPTION_CATALOG#${p.providerId}`));

  console.log(`[catalog-subscription-sync] done upserted=${upserted} errors=${errors}`);
  return { upserted, errors };
};

// ── DynamoDB writes ───────────────────────────────────────────────────────────

async function upsertCatalog(p: Provider) {
  await dynamo.send(new PutCommand({
    TableName: REF_TABLE,
    Item: {
      pK:         `SUBSCRIPTION_CATALOG#${p.providerId}`,
      sK:         'profile',
      primaryCat: 'subscription_catalog',
      status:     'ACTIVE',
      source:     'sync',  // top-level — used by deactivateRemoved FilterExpression
      desc: JSON.stringify({
        providerId:         p.providerId,
        name:               p.name,
        logoUrl:            p.logoUrl,
        category:           p.category,
        region:             p.region,
        plans:              p.plans,
        affiliateUrl:       withAffiliateParams(p.affiliateUrl),
        benchmarkPrice:     p.benchmarkPrice,
        benchmarkFrequency: p.benchmarkFrequency,
        tags:               p.tags,
        cancelUrl:          p.cancelUrl ?? null,
        portalUrl:          p.portalUrl ?? null,
        invoiceType:        p.invoiceType ?? 'SUBSCRIPTION',
        isAffiliate:        true,   // BeboCard-curated providers are affiliate-tracked
        isTenantLinked:     false,  // curated list — not tenant self-registered
        hasLinking:         false,  // tenant OAuth linking not yet established
        listingStatus:      'ACTIVE',
        source:             'sync',
      }),
      updatedAt: new Date().toISOString(),
    },
  }));
}

async function upsertBenchmark(p: Provider) {
  await dynamo.send(new PutCommand({
    TableName: REF_TABLE,
    Item: {
      pK:              `BENCHMARK#${p.providerId}`,
      sK:              'BENCHMARK',
      primaryCat:      'benchmark',
      status:          'ACTIVE',
      benchmarkAmount: p.benchmarkPrice,
      updatedAt:       new Date().toISOString(),
    },
  }));
}

async function deactivateRemoved(activePKs: string[]) {
  const activeSet = new Set(activePKs);
  let lastKey: Record<string, unknown> | undefined;
  do {
    // Only deactivate sync-managed records (top-level source = 'sync' or absent).
    // Tenant self-registered entries (source = 'tenant') are managed by brand-api-handler.
    const res = await dynamo.send(new ScanCommand({
      TableName:        REF_TABLE,
      FilterExpression: 'primaryCat = :cat AND #s = :active AND (attribute_not_exists(#src) OR #src = :sync)',
      ExpressionAttributeNames:  { '#s': 'status', '#src': 'source' },
      ExpressionAttributeValues: { ':cat': 'subscription_catalog', ':active': 'ACTIVE', ':sync': 'sync' },
      ExclusiveStartKey: lastKey,
    }));
    for (const item of res.Items ?? []) {
      if (!activeSet.has(item.pK as string)) {
        await dynamo.send(new UpdateCommand({
          TableName:                 REF_TABLE,
          Key:                       { pK: item.pK, sK: 'profile' },
          UpdateExpression:          'SET #s = :inactive',
          ExpressionAttributeNames:  { '#s': 'status' },
          ExpressionAttributeValues: { ':inactive': 'INACTIVE' },
        }));
      }
    }
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);
}
