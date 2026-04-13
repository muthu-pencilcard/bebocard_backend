import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

const ddb = new DynamoDBClient({});
const REF_TABLE = process.env.REFDATA_TABLE;

async function seedGiftCards() {
  if (!REF_TABLE) {
    console.error('REFDATA_TABLE env var not set');
    return;
  }

  const products = [
    // Woolworths AU
    { pK: 'BRAND#woolworths', sK: 'GIFTCARD#ww_10', primaryCat: 'gift_card_catalog', brandId: 'woolworths', status: 'ACTIVE',
      desc: JSON.stringify({ brandName: 'Woolworths', denomination: 10, currency: 'AUD', category: 'Grocery', region: 'AU', distributorId: 'prezzee', distributorSku: 'WW-AU-10', logoUrl: 'https://seeklogo.com/images/W/woolworths-logo-9A96740608-seeklogo.com.png' }) },
    { pK: 'BRAND#woolworths', sK: 'GIFTCARD#ww_50', primaryCat: 'gift_card_catalog', brandId: 'woolworths', status: 'ACTIVE',
      desc: JSON.stringify({ brandName: 'Woolworths', denomination: 50, currency: 'AUD', category: 'Grocery', region: 'AU', distributorId: 'prezzee', distributorSku: 'WW-AU-50', logoUrl: 'https://seeklogo.com/images/W/woolworths-logo-9A96740608-seeklogo.com.png' }) },
    
    // Amazon AU
    { pK: 'BRAND#amazon', sK: 'GIFTCARD#amzn_25', primaryCat: 'gift_card_catalog', brandId: 'amazon', status: 'ACTIVE',
      desc: JSON.stringify({ brandName: 'Amazon', denomination: 25, currency: 'AUD', category: 'Retail', region: 'AU', distributorId: 'tango', distributorSku: 'AMZN-AU-25', logoUrl: 'https://upload.wikimedia.org/wikipedia/commons/a/a9/Amazon_logo.svg' }) },
    { pK: 'BRAND#amazon', sK: 'GIFTCARD#amzn_100', primaryCat: 'gift_card_catalog', brandId: 'amazon', status: 'ACTIVE',
      desc: JSON.stringify({ brandName: 'Amazon', denomination: 100, currency: 'AUD', category: 'Retail', region: 'AU', distributorId: 'tango', distributorSku: 'AMZN-AU-100', logoUrl: 'https://upload.wikimedia.org/wikipedia/commons/a/a9/Amazon_logo.svg' }) },

    // Starbucks
    { pK: 'BRAND#starbucks', sK: 'GIFTCARD#sbux_15', primaryCat: 'gift_card_catalog', brandId: 'starbucks', status: 'ACTIVE',
      desc: JSON.stringify({ brandName: 'Starbucks', denomination: 15, currency: 'AUD', category: 'Dining', region: 'AU', distributorId: 'reloadly', distributorSku: 'SBUX-AU-15', logoUrl: 'https://upload.wikimedia.org/wikipedia/en/d/d3/Starbucks_Corporation_Logo_2011.svg' }) },

    // JB Hi-Fi
    { pK: 'BRAND#jbhifi', sK: 'GIFTCARD#jb_20', primaryCat: 'gift_card_catalog', brandId: 'jbhifi', status: 'ACTIVE',
      desc: JSON.stringify({ brandName: 'JB Hi-Fi', denomination: 20, currency: 'AUD', category: 'Retail', region: 'AU', distributorId: 'prezzee', distributorSku: 'JB-AU-20', logoUrl: 'https://upload.wikimedia.org/wikipedia/commons/3/30/JB_Hi-Fi_logo.svg' }) },

    // Netflix
    { pK: 'BRAND#netflix', sK: 'GIFTCARD#nflx_30', primaryCat: 'gift_card_catalog', brandId: 'netflix', status: 'ACTIVE',
      desc: JSON.stringify({ brandName: 'Netflix', denomination: 30, currency: 'AUD', category: 'Entertainment', region: 'AU', distributorId: 'reloadly', distributorSku: 'NFLX-AU-30', logoUrl: 'https://upload.wikimedia.org/wikipedia/commons/0/08/Netflix_2015_logo.svg' }) },
  ];

  console.log(`Seeding ${products.length} gift card products...`);

  for (const product of products) {
    try {
      await ddb.send(new PutItemCommand({
        TableName: REF_TABLE,
        Item: marshall({
          ...product,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString()
        }),
      }));
      console.log(`✅ Seeded ${product.brandId} - ${product.sK}`);
    } catch (e) {
      console.error(`❌ Failed to seed ${product.sK}:`, e);
    }
  }
}

seedGiftCards();
