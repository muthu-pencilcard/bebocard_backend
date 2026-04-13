import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

const ddb = new DynamoDBClient({});
const REF_TABLE = process.env.REFDATA_TABLE;

async function seedOffers() {
  if (!REF_TABLE) {
    console.error('REFDATA_TABLE env var not set');
    return;
  }

  const offers = [
    {
      pK: 'BRAND#woolworths',
      sK: 'OFFER#weekly_special_1',
      primaryCat: 'offer',
      status: 'ACTIVE',
      desc: JSON.stringify({
        brandId: 'woolworths',
        brandName: 'Woolworths',
        headline: 'Double points on all fresh produce this week! 🍏',
        voucherCode: 'FRESH2X',
        expiresAt: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString(),
      }),
      createdAt: new Date().toISOString(),
    },
    {
      pK: 'BRAND#flybuys',
      sK: 'OFFER#coles_bonus_1',
      primaryCat: 'offer',
      status: 'ACTIVE',
      desc: JSON.stringify({
        brandId: 'flybuys',
        brandName: 'Flybuys',
        headline: 'Spend $50 or more and get 2000 bonus points! 🛒',
        voucherCode: 'COLES2000',
        expiresAt: new Date(Date.now() + 14 * 24 * 60 * 60 * 1000).toISOString(),
      }),
      createdAt: new Date().toISOString(),
    },
    {
      pK: 'BRAND#starbucks',
      sK: 'OFFER#morning_brew',
      primaryCat: 'offer',
      status: 'ACTIVE',
      desc: JSON.stringify({
        brandId: 'starbucks',
        brandName: 'Starbucks',
        headline: 'Buy one get one free on all lattes before 10 AM ☕️',
        voucherCode: 'MORNINGBOGO',
        expiresAt: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString(),
      }),
      createdAt: new Date().toISOString(),
    }
  ];

  const brandProfiles = [
    {
      pK: 'BRAND#woolworths',
      sK: 'PROFILE',
      primaryCat: 'profile',
      status: 'ACTIVE',
      desc: JSON.stringify({
        brandName: 'Woolworths',
        brandColor: '#17412d',
        logoUrl: 'https://seeklogo.com/images/W/woolworths-logo-9A96740608-seeklogo.com.png',
      }),
      createdAt: new Date().toISOString(),
    },
    {
      pK: 'BRAND#flybuys',
      sK: 'PROFILE',
      primaryCat: 'profile',
      status: 'ACTIVE',
      desc: JSON.stringify({
        brandName: 'Flybuys',
        brandColor: '#e31837',
        logoUrl: 'https://upload.wikimedia.org/wikipedia/commons/e/e0/Flybuys_logo.svg',
      }),
      createdAt: new Date().toISOString(),
    }
  ];

  console.log(`Seeding offers and brand profiles to ${REF_TABLE}...`);

  for (const item of [...offers, ...brandProfiles]) {
    try {
      await ddb.send(new PutItemCommand({
        TableName: REF_TABLE,
        Item: marshall(item),
      }));
      console.log(`✅ Seeded ${item.pK} / ${item.sK}`);
    } catch (e) {
      console.error(`❌ Failed to seed ${item.sK}:`, e);
    }
  }
}

seedOffers();
