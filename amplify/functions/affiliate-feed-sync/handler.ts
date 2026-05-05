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

export const handler: Handler = async (_event) => {
  console.log('[affiliate-sync] Starting affiliate feed sync...');

  try {
    const table  = await getTableName();
    const apiKey = process.env.AFFILIATE_API_KEY;

    if (!apiKey) {
      console.log('[affiliate-sync] No AFFILIATE_API_KEY — skipping sync.');
      return { statusCode: 200, body: JSON.stringify({ count: 0 }) };
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
