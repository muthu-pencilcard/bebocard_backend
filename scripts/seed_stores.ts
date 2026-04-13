import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

const ddb = new DynamoDBClient({});
const REF_TABLE = process.env.REFDATA_TABLE;

async function seedStores() {
  if (!REF_TABLE) {
    console.error('REFDATA_TABLE env var not set');
    return;
  }

  const stores = [
    {
      pK: 'STORES',
      sK: 'STORE#woolworths#sydney_cbd_1',
      primaryCat: 'store',
      status: 'ACTIVE',
      desc: JSON.stringify({
        lat: -33.8688,
        lng: 151.2093,
        name: 'Woolworths Sydney QVB',
        brandId: 'woolworths',
        radiusMetres: 200,
      }),
      createdAt: new Date().toISOString(),
    },
    {
      pK: 'STORES',
      sK: 'STORE#woolworths#melbourne_central',
      primaryCat: 'store',
      status: 'ACTIVE',
      desc: JSON.stringify({
        lat: -37.8105,
        lng: 144.9631,
        name: 'Woolworths Melbourne Central',
        brandId: 'woolworths',
        radiusMetres: 300,
      }),
      createdAt: new Date().toISOString(),
    },
    {
      pK: 'STORES',
      sK: 'STORE#flybuys#coles_surry_hills',
      primaryCat: 'store',
      status: 'ACTIVE',
      desc: JSON.stringify({
        lat: -33.8845,
        lng: 151.2111,
        name: 'Coles Surry Hills',
        brandId: 'flybuys',
        radiusMetres: 250,
      }),
      createdAt: new Date().toISOString(),
    },
  ];

  console.log(`Seeding ${stores.length} stores to ${REF_TABLE}...`);

  for (const store of stores) {
    try {
      await ddb.send(new PutItemCommand({
        TableName: REF_TABLE,
        Item: marshall(store),
      }));
      console.log(`✅ Seeded ${store.sK}`);
    } catch (e) {
      console.error(`❌ Failed to seed ${store.sK}:`, e);
    }
  }
}

seedStores();
