import { Handler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, BatchWriteCommand } from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

const REFDATA_TABLE = process.env.REFDATA_TABLE!;

// Simulated affiliate offers for demonstration
// In  production, this would use fetch() with an API key from Secrets Manager
const MOCK_AFFILIATE_OFFERS = [
  {
    offerId: 'cf_woolworths_10pct',
    brandName: 'Woolworths',
    brandLogo: 'https://cdn.bebocard.com/brands/woolworths/logo.png',
    title: '10% Off Your First Online Shop',
    description: 'Save 10% on your first grocery delivery or pick up order over $120.',
    discount: '10%',
    trackingUrl: 'https://t.cf.com/woolworths?offer=10pct',
    category: 'grocery',
    expiryDate: '2026-12-31T23:59:59Z',
    campaignType: 'acquisition',
  },
  {
    offerId: 'cf_apple_5pct',
    brandName: 'Apple',
    brandLogo: 'https://cdn.bebocard.com/brands/apple/logo.png',
    title: '5% Students & Educators Discount',
    description: 'Exclusive pricing for higher education students and educators.',
    discount: '5%',
    trackingUrl: 'https://t.cf.com/apple?offer=edu',
    category: 'electronics',
    expiryDate: '2027-01-01T00:00:00Z',
    campaignType: 'loyalty_reward',
  },
  {
    offerId: 'cf_nike_20pct',
    brandName: 'Nike',
    brandLogo: 'https://cdn.bebocard.com/brands/nike/logo.png',
    title: '20% Off Select Styles',
    description: 'Member Exclusive: Extra 20% off for a limited time.',
    discount: '20%',
    trackingUrl: 'https://t.cf.com/nike?offer=20pct',
    category: 'fashion',
    expiryDate: '2026-05-15T23:59:59Z',
    campaignType: 'seasonal',
  },
  {
    offerId: 'cf_hellofresh_60pct',
    brandName: 'HelloFresh',
    brandLogo: 'https://cdn.bebocard.com/brands/hellofresh/logo.png',
    title: '60% Off Your First Box',
    description: 'Get 60% off your first box + 25% off for the next 2 months.',
    discount: '60%',
    trackingUrl: 'https://t.cf.com/hellofresh?offer=60',
    category: 'grocery',
    expiryDate: '2026-06-30T23:59:59Z',
    campaignType: 'acquisition',
  },
  {
    offerId: 'cf_qantas_5000pts',
    brandName: 'Qantas',
    brandLogo: 'https://cdn.bebocard.com/brands/qantas/logo.png',
    title: '5,000 Bonus Points',
    description: 'Earn 5,000 bonus points when you book your next international flight with BeboCard.',
    discount: '5,000 pts',
    trackingUrl: 'https://t.cf.com/qantas?promo=bebo5000',
    category: 'travel',
    expiryDate: '2026-08-15T23:59:59Z',
    campaignType: 'partnership',
  },
  {
    offerId: 'cf_amazon_20pct',
    brandName: 'Amazon',
    brandLogo: 'https://cdn.bebocard.com/brands/amazon/logo.png',
    title: '20% Off Electronics',
    description: 'BeboCard Member Exclusive: Use code BEBO20 for 20% off all Kindle and Echo devices.',
    discount: '20%',
    trackingUrl: 'https://t.cf.com/amazon?code=BEBO20',
    category: 'electronics',
    expiryDate: '2026-05-31T23:59:59Z',
    campaignType: 'curated',
  },
  {
    offerId: 'cf_event_cinemas_bogo',
    brandName: 'Event Cinemas',
    brandLogo: 'https://cdn.bebocard.com/brands/event/logo.png',
    title: 'BOGO Movie Tickets',
    description: 'Buy one Get one free for any V-Max or Gold Class session this weekend.',
    discount: 'BOGO',
    trackingUrl: 'https://t.cf.com/event?deal=bogo',
    category: 'entertainment',
    expiryDate: '2026-04-30T23:59:59Z',
    campaignType: 'seasonal',
  }
];

export const handler: Handler = async (event) => {
  console.log('Starting affiliate feed sync...');

  try {
    // 1. Fetch real feed data (Simulated here)
    const offers = MOCK_AFFILIATE_OFFERS;

    // 2. Map to RefDataEvent records
    const chunks = [];
    for (let i = 0; i < offers.length; i += 25) {
      chunks.push(offers.slice(i, i + 25));
    }

    for (const chunk of chunks) {
      const putRequests = chunk.map(offer => ({
        PutRequest: {
          Item: {
            pK: `BEBO_OFFER#${offer.offerId}`,
            sK: 'offer',
            eventType: 'AFILIATE_OFFER',
            primaryCat: 'curated_offer',
            subCategory: offer.category,
            status: 'ACTIVE',
            brandId: offer.brandName.toLowerCase(), // normalization for ranking
            desc: {
              ...offer,
              source: 'affiliate',
              isBeboCurated: true,
              lastSyncedAt: new Date().toISOString(),
            },
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          }
        }
      }));

      await docClient.send(new BatchWriteCommand({
        RequestItems: {
          [REFDATA_TABLE]: putRequests
        }
      }));
    }

    console.log(`Successfully synced ${offers.length} affiliate offers.`);
    return { statusCode: 200, body: JSON.stringify({ message: 'Sync complete', count: offers.length }) };
  } catch (error) {
    console.error('Error syncing affiliate feed:', error);
    throw error;
  }
};
