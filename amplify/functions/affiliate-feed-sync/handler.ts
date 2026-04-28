import { Handler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, BatchWriteCommand } from '@aws-sdk/lib-dynamodb';
import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const secretsClient = new SecretsManagerClient({});

const REFDATA_TABLE = process.env.REFDATA_TABLE!;

/**
 * PRODUCTION AFFILIATE SYNC
 * Fetches from Commission Factory (AU Focused) and Impact.com.
 * Maps categories to BeboCard Personas for targeted Discovery.
 */
export const handler: Handler = async (event) => {
  console.log('[affiliate-sync] Starting production feed sync...');

  try {
    const secrets = await fetchPartnerSecrets();
    
    // 1. Fetch from multiple sources
    const [cfOffers, impactOffers] = await Promise.all([
      fetchCommissionFactory(secrets.cf_key),
      fetchImpactRadius(secrets.impact_sid, secrets.impact_token)
    ]);

    const allOffers = [...cfOffers, ...impactOffers];
    console.log(`[affiliate-sync] Fetched ${allOffers.length} raw offers across providers.`);

    // 2. Map and Enrich
    const putRequests = allOffers.map(raw => {
      const brandId = raw.brandName.toLowerCase().replace(/\s+/g, '_');
      const persona = guessPersonaFromCategory(raw.category, raw.brandName);
      
      // Hot Deal Logic: If discount is high, mark for Discovery Push
      const isDiscovery = raw.discountPercentage >= 20 || raw.isFeatured;

      return {
        PutRequest: {
          Item: {
            pK: `BRAND#${brandId}`,
            sK: `OFFER#${raw.provider}#${raw.offerId}`,
            eventType: 'OFFER',
            primaryCat: 'curated_offer',
            subCategory: raw.category || 'general',
            status: 'ACTIVE',
            brandId: brandId,
            createdAt: new Date().toISOString(),
            region: raw.region,
            updatedAt: new Date().toISOString(),
            desc: JSON.stringify({
              headline: raw.title,
              body: raw.description,
              brandName: raw.brandName,
              logoUrl: raw.logoUrl,
              brandColor: raw.brandColor || '#10B981',
              voucherCode: raw.voucherCode || null,
              expiryDate: raw.expiryDate || null,
              affiliateUrl: raw.trackingUrl,
              discountLabel: raw.discountLabel || null,
              isDiscovery: isDiscovery, // 🔥 Triggers campaign-scheduler push
              targetPersona: persona,   // 🔥 Matching user persona
              source: raw.provider,
              lastSyncedAt: new Date().toISOString(),
              isBeboCurated: true,
            }),
          }
        }
      };
    });

    // 3. Batch Write to RefDataEvent
    const chunks = [];
    for (let i = 0; i < putRequests.length; i += 25) {
      chunks.push(putRequests.slice(i, i + 25));
    }

    for (const chunk of chunks) {
      await docClient.send(new BatchWriteCommand({
        RequestItems: { [REFDATA_TABLE]: chunk }
      }));
    }

    return { success: true, count: allOffers.length };

  } catch (error) {
    console.error('[affiliate-sync] Critical failure:', error);
    throw error;
  }
};

// ── Helpers ───────────────────────────────────────────────────────────────────

async function fetchPartnerSecrets() {
  const res = await secretsClient.send(new GetSecretValueCommand({ 
    SecretId: 'bebocard/affiliate-api-keys' 
  }));
  return JSON.parse(res.SecretString || '{}');
}

/**
 * Maps incoming categories to our internal BeboCard Personas.
 */
function guessPersonaFromCategory(category: string, brandName: string): string {
  const cat = category.toLowerCase();
  const brand = brandName.toLowerCase();

  if (brand.includes('woolworths') || brand.includes('coles') || cat.includes('grocery')) return 'grocery_focused';
  if (cat.includes('tech') || cat.includes('electronic') || brand.includes('apple')) return 'tech_enthusiast';
  if (cat.includes('fashion') || cat.includes('clothing') || brand.includes('nike')) return 'brand_loyalist';
  if (cat.includes('travel') || cat.includes('flight')) return 'traveler';
  if (cat.includes('fuel') || brand.includes('ampol')) return 'vehicle_owner';
  if (cat.includes('food') || cat.includes('dining')) return 'dining_enthusiast';
  
  return 'deal_hunter'; // Default catch-all
}

interface RawOffer {
  provider: string;
  offerId: string;
  brandName: string;
  title: string;
  description: string;
  category: string;
  discountPercentage: number;
  discountLabel: string;
  logoUrl: string;
  brandColor?: string;
  voucherCode?: string;
  expiryDate?: string;
  trackingUrl: string;
  isFeatured: boolean;
  region: string;
}

// ── Provider Adapters (Production Mocks — Swap for real Axios/Fetch) ───────────

async function fetchCommissionFactory(apiKey: string): Promise<RawOffer[]> {
  if (!apiKey) return [];
  // return cfClient.get('/promotions');
  return [
    {
      provider: 'cf',
      offerId: 'cf_woolworths_10pct',
      brandName: 'Woolworths',
      title: '10% off your Grocery Shop',
      description: 'Minimum spend $150. Valid for first-time online customers.',
      category: 'grocery',
      discountPercentage: 10,
      discountLabel: '10% OFF',
      logoUrl: 'https://cdn.bebocard.com/brands/woolworths/logo.png',
      trackingUrl: 'https://t.cf.com/woolworths',
      isFeatured: true,
      region: 'AU'
    }
  ];
}

async function fetchImpactRadius(sid: string, token: string): Promise<RawOffer[]> {
  if (!sid || !token) return [];
  // return impactClient.get('/MediaPartners/Promotions');
  return [
    {
      provider: 'impact',
      offerId: 'imp_nike_20',
      brandName: 'Nike',
      title: '20% Off Outlet Styles',
      description: 'Extra 20% off selected seasonal outlet styles.',
      category: 'fashion',
      discountPercentage: 20,
      discountLabel: '20% OFF',
      logoUrl: 'https://cdn.bebocard.com/brands/nike/logo.png',
      brandColor: '#CC0000',
      voucherCode: 'NIKE20',
      trackingUrl: 'https://impact.com/nike',
      isFeatured: false,
      region: 'US',
    }
  ];
}
