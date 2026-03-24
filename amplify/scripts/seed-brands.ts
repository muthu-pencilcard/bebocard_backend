/**
 * Seed script: populates RefDataEvent with Australian loyalty card brands.
 * Run: npx ts-node amplify/scripts/seed-brands.ts
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'ap-southeast-2' }));

const TABLE = process.env.REFDATA_TABLE!;

const brands = [
  {
    id: 'woolworths',
    displayName: 'Woolworths Rewards',
    color: '#1B8332',
    logoKey: 'brands/woolworths.png',
    category: 'grocery',
    cardFormat: 'numeric_16',
    pointsName: 'Woolworths Dollars',
    supportsReceipts: true,
    barcodeType: 'EAN13',
  },
  {
    id: 'coles',
    displayName: 'Flybuys',
    color: '#E2231A',
    logoKey: 'brands/flybuys.png',
    category: 'grocery',
    cardFormat: 'numeric_16',
    pointsName: 'Flybuys Points',
    supportsReceipts: true,
    barcodeType: 'EAN13',
  },
  {
    id: 'qantas',
    displayName: 'Qantas Frequent Flyer',
    color: '#E4002B',
    logoKey: 'brands/qantas.png',
    category: 'travel',
    cardFormat: 'alphanumeric_8',
    pointsName: 'Qantas Points',
    supportsReceipts: false,
    barcodeType: 'QR',
  },
  {
    id: 'velocity',
    displayName: 'Virgin Velocity',
    color: '#CC0000',
    logoKey: 'brands/velocity.png',
    category: 'travel',
    cardFormat: 'numeric_10',
    pointsName: 'Velocity Points',
    supportsReceipts: false,
    barcodeType: 'QR',
  },
  {
    id: 'myer',
    displayName: 'MYER one',
    color: '#000000',
    logoKey: 'brands/myer.png',
    category: 'retail',
    cardFormat: 'numeric_16',
    pointsName: 'MYER one Credits',
    supportsReceipts: true,
    barcodeType: 'CODE128',
  },
  {
    id: 'iga',
    displayName: 'IGA Rewards',
    color: '#E4002B',
    logoKey: 'brands/iga.png',
    category: 'grocery',
    cardFormat: 'numeric_13',
    pointsName: 'IGA Rewards Points',
    supportsReceipts: true,
    barcodeType: 'EAN13',
  },
  {
    id: 'bp',
    displayName: 'bp Rewards',
    color: '#00A650',
    logoKey: 'brands/bp.png',
    category: 'fuel',
    cardFormat: 'numeric_13',
    pointsName: 'bp Rewards Points',
    supportsReceipts: false,
    barcodeType: 'EAN13',
  },
  {
    id: 'coffee_club',
    displayName: 'The Coffee Club',
    color: '#6B2D8B',
    logoKey: 'brands/coffee_club.png',
    category: 'dining',
    cardFormat: 'numeric_10',
    pointsName: 'Points',
    supportsReceipts: false,
    barcodeType: 'QR',
  },
  {
    id: 'priceline',
    displayName: 'Priceline Sister Club',
    color: '#E4007C',
    logoKey: 'brands/priceline.png',
    category: 'retail',
    cardFormat: 'numeric_16',
    pointsName: 'Sister Club Points',
    supportsReceipts: false,
    barcodeType: 'CODE128',
  },
  {
    id: 'eftpos',
    displayName: 'eftpos rewards',
    color: '#0055A5',
    logoKey: 'brands/eftpos.png',
    category: 'payments',
    cardFormat: 'numeric_16',
    pointsName: 'Rewards Points',
    supportsReceipts: false,
    barcodeType: 'EAN13',
  },
];

async function seed() {
  const now = new Date().toISOString();
  for (const brand of brands) {
    await dynamo.send(new PutCommand({
      TableName: TABLE,
      Item: {
        pK: `BRAND#${brand.id}`,
        sK: 'profile',
        eventType: 'BRAND_PROFILE',
        status: 'ACTIVE',
        primaryCat: 'brand',
        subCategory: brand.category,
        desc: JSON.stringify(brand),
        createdAt: now,
        updatedAt: now,
        version: 1,
      },
    }));
    console.log(`Seeded ${brand.displayName}`);
  }

  // Category index
  const categoryMap: Record<string, string[]> = {};
  for (const brand of brands) {
    if (!categoryMap[brand.category]) categoryMap[brand.category] = [];
    categoryMap[brand.category].push(brand.id);
  }
  for (const [cat, ids] of Object.entries(categoryMap)) {
    await dynamo.send(new PutCommand({
      TableName: TABLE,
      Item: {
        pK: `CATEGORY#${cat}`,
        sK: 'brands',
        eventType: 'CATEGORY',
        status: 'ACTIVE',
        primaryCat: 'category',
        subCategory: cat,
        desc: JSON.stringify({ brandIds: ids }),
        createdAt: now,
        updatedAt: now,
        version: 1,
      },
    }));
    console.log(`Category ${cat}: ${ids.join(', ')}`);
  }
}

seed().catch(console.error);
