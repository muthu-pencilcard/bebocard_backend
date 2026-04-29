import type { Handler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, BatchWriteCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { createHash } from 'crypto';

const client    = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const ssmClient = new SSMClient({});

let cachedTableName: string | undefined;
async function getTableName(): Promise<string> {
  if (cachedTableName) return cachedTableName;
  const result = await ssmClient.send(new GetParameterCommand({ Name: process.env.REFDATA_TABLE_PARAM! }));
  cachedTableName = result.Parameter!.Value!;
  return cachedTableName;
}

interface CFFeed {
  Id: number;
  Name: string;
  MerchantName: string;
  Description: string;
  TrackingUrl: string;
  MerchantLogoUrl: string;
  EndDate?: string;
  Category?: string;
}

function contentHash(title: string, desc: string, url: string): string {
  return createHash('sha256').update(JSON.stringify({ title, desc, url })).digest('hex');
}

function toItem(offer: CFFeed) {
  return {
    PutRequest: {
      Item: {
        pK: `BEBO_OFFER#CF_${offer.Id}`,
        sK: 'offer',
        eventType: 'AFFILIATE_OFFER',
        primaryCat: 'curated_offer',
        status: 'ACTIVE',
        updatedAt: new Date().toISOString(),
        desc: {
          contentHash: contentHash(offer.Name, offer.Description, offer.TrackingUrl),
          headline: offer.Name,
          body: offer.Description,
          brandName: offer.MerchantName,
          logoUrl: offer.MerchantLogoUrl,
          trackingUrl: offer.TrackingUrl,
          expiryDate: offer.EndDate ?? null,
          category: offer.Category ?? 'general',
        },
      },
    },
  };
}

// AU-focused mock offers used when AFFILIATE_API_KEY is not set (dev / canary env).
// Replace these stubs with real CF data once API credentials are live.
const MOCK_OFFERS: CFFeed[] = [
  { Id: 1001, Name: '10% off Grocery Shop',    MerchantName: 'Woolworths',   Description: 'Save 10% on your next shop. Min spend $80.',             TrackingUrl: 'https://t.commissionfactory.com/woolworths',   MerchantLogoUrl: 'https://cdn.bebocard.com/brands/woolworths/logo.png',   Category: 'grocery' },
  { Id: 1002, Name: '$20 off $150+ Spend',      MerchantName: 'Coles',        Description: '$20 off when you spend $150 or more online.',             TrackingUrl: 'https://t.commissionfactory.com/coles',        MerchantLogoUrl: 'https://cdn.bebocard.com/brands/coles/logo.png',        Category: 'grocery' },
  { Id: 1003, Name: '10% off Tech',             MerchantName: 'JB Hi-Fi',     Description: 'Extra 10% off selected tech and appliances.',             TrackingUrl: 'https://t.commissionfactory.com/jbhifi',       MerchantLogoUrl: 'https://cdn.bebocard.com/brands/jbhifi/logo.png',       Category: 'tech'    },
  { Id: 1004, Name: '30% off Clothing',         MerchantName: 'Cotton On',    Description: '30% off full-price styles site-wide.',                    TrackingUrl: 'https://t.commissionfactory.com/cottonon',     MerchantLogoUrl: 'https://cdn.bebocard.com/brands/cottonon/logo.png',     Category: 'fashion' },
  { Id: 1005, Name: '$10 off First Order',      MerchantName: 'Menulog',      Description: '$10 off your first food delivery order over $25.',        TrackingUrl: 'https://t.commissionfactory.com/menulog',      MerchantLogoUrl: 'https://cdn.bebocard.com/brands/menulog/logo.png',      Category: 'food'    },
  { Id: 1006, Name: '15% off Hotels',           MerchantName: 'Booking.com',  Description: '15% off selected properties when you sign in.',           TrackingUrl: 'https://t.commissionfactory.com/bookingcom',   MerchantLogoUrl: 'https://cdn.bebocard.com/brands/bookingcom/logo.png',   Category: 'travel'  },
  { Id: 1007, Name: '20% off Pet Supplies',     MerchantName: 'Petbarn',      Description: '20% off your first online order at Petbarn.',             TrackingUrl: 'https://t.commissionfactory.com/petbarn',      MerchantLogoUrl: 'https://cdn.bebocard.com/brands/petbarn/logo.png',      Category: 'pets'    },
];

export const handler: Handler = async (_event) => {
  console.log('[affiliate-sync] Starting affiliate feed sync...');

  try {
    const table  = await getTableName();
    const apiKey = process.env.AFFILIATE_API_KEY;

    if (!apiKey) {
      console.log('[affiliate-sync] No AFFILIATE_API_KEY — writing mock offers.');
      await batchWrite(table, MOCK_OFFERS.map(toItem));
      return { statusCode: 200, body: JSON.stringify({ count: MOCK_OFFERS.length }) };
    }

    const [liveOffers, existing] = await Promise.all([
      fetchCF(apiKey),
      scanExisting(table),
    ]);

    const liveIds   = new Set(liveOffers.map(o => `BEBO_OFFER#CF_${o.Id}`));
    const existingByPk = new Map(existing.map(item => [item.pK as string, item]));

    type WriteRequest = { PutRequest: { Item: Record<string, unknown> } };

    const upserts: WriteRequest[] = [];
    for (const offer of liveOffers) {
      const pk   = `BEBO_OFFER#CF_${offer.Id}`;
      const hash = contentHash(offer.Name, offer.Description, offer.TrackingUrl);
      const prev = existingByPk.get(pk);
      if ((prev?.desc as Record<string, unknown> | undefined)?.contentHash === hash) continue;
      upserts.push(toItem(offer));
    }

    const expirations: WriteRequest[] = [];
    for (const item of existing) {
      if (!liveIds.has(item.pK as string)) {
        expirations.push({
          PutRequest: {
            Item: { ...item, status: 'EXPIRED', updatedAt: new Date().toISOString() },
          },
        });
      }
    }

    await batchWrite(table, [...upserts, ...expirations]);
    console.log(`[affiliate-sync] Upserted ${upserts.length}, expired ${expirations.length}`);
    return { statusCode: 200, body: JSON.stringify({ count: upserts.length + expirations.length }) };

  } catch (error) {
    console.error('[affiliate-sync] Critical failure:', error);
    return { statusCode: 500, body: JSON.stringify({ error: (error as Error).message }) };
  }
};

async function fetchCF(apiKey: string): Promise<CFFeed[]> {
  const res = await fetch('https://api.commissionfactory.com/v2/promotions', {
    headers: { 'X-ApiKey': apiKey, 'Content-Type': 'application/json' },
  });
  if (!res.ok) throw new Error(`CF API ${res.status}: ${res.statusText}`);
  return res.json() as Promise<CFFeed[]>;
}

async function scanExisting(table: string): Promise<Record<string, unknown>[]> {
  const result = await docClient.send(new ScanCommand({
    TableName: table,
    FilterExpression: 'begins_with(pK, :prefix) AND #st = :active',
    ExpressionAttributeNames: { '#st': 'status' },
    ExpressionAttributeValues: { ':prefix': 'BEBO_OFFER#CF_', ':active': 'ACTIVE' },
    ProjectionExpression: 'pK, desc',
  }));
  return (result.Items ?? []) as Record<string, unknown>[];
}

async function batchWrite(table: string, requests: { PutRequest: { Item: Record<string, unknown> } }[]) {
  for (let i = 0; i < requests.length; i += 25) {
    const chunk = requests.slice(i, i + 25);
    if (chunk.length > 0) {
      await docClient.send(new BatchWriteCommand({ RequestItems: { [table]: chunk } }));
    }
  }
}
