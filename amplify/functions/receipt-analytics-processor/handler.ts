import type { SQSHandler } from 'aws-lambda';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';
import { createHmac } from 'crypto';

const s3 = new S3Client({});
const secretsManager = new SecretsManagerClient({});

const ANALYTICS_BUCKET     = process.env.ANALYTICS_BUCKET!;
const GLOBAL_ANALYTICS_SALT = process.env.GLOBAL_ANALYTICS_SALT!;

// Cache per-tenant salts for the Lambda lifetime to avoid repeated Secrets Manager calls
const tenantSaltCache: Record<string, string> = {};

async function getTenantSalt(tenantId: string): Promise<string | null> {
  if (tenantSaltCache[tenantId]) return tenantSaltCache[tenantId];
  try {
    const result = await secretsManager.send(new GetSecretValueCommand({
      SecretId: `bebocard/tenant-analytics-salt/${tenantId}`,
    }));
    const salt = result.SecretString ?? '';
    if (salt) tenantSaltCache[tenantId] = salt;
    return salt || null;
  } catch {
    return null;
  }
}

export interface AnalyticsReceiptMessage {
  permULID: string | null;     // null for anonymous walk-ins
  brandId: string;
  tenantId?: string;
  merchant: string;
  amount: number;
  currency?: string;
  purchaseDate: string;
  category?: string;
  items?: Array<{ sku?: string; name?: string; qty?: number; unit_price?: number }>;
}

interface AnalyticsRow {
  tenant_id: string;
  brand_id: string;
  purchase_date: string;
  visitor_hash: string | null;
  visitor_hash_tenant: string | null;
  is_bebocard: boolean;
  amount: number;
  currency: string;
  category: string;
  merchant: string;
  items: AnalyticsReceiptMessage['items'];
  ingested_at: string;
}

export const handler: SQSHandler = async (event) => {
  const batchItemFailures: { itemIdentifier: string }[] = [];
  const rows: AnalyticsRow[] = [];
  const partitionFirstMessageId: Record<string, string> = {};
  const ingestedAt = new Date().toISOString();

  for (const record of event.Records) {
    try {
      const msg = JSON.parse(record.body) as AnalyticsReceiptMessage;
      const { permULID, brandId, tenantId, merchant, amount, currency, purchaseDate, category, items } = msg;
      const resolvedTenantId = tenantId ?? brandId;

      let visitorHash: string | null = null;
      let visitorHashTenant: string | null = null;

      if (permULID && !permULID.startsWith('ANON#')) {
        visitorHash = createHmac('sha256', GLOBAL_ANALYTICS_SALT).update(permULID).digest('hex');

        const tenantSalt = await getTenantSalt(resolvedTenantId);
        if (tenantSalt) {
          visitorHashTenant = createHmac('sha256', tenantSalt).update(permULID).digest('hex');
        }
      }

      const partitionKey = `${resolvedTenantId}/${brandId}/${purchaseDate.substring(0, 10)}`;
      partitionFirstMessageId[partitionKey] ??= record.messageId;

      rows.push({
        tenant_id: resolvedTenantId,
        brand_id: brandId,
        purchase_date: purchaseDate.substring(0, 10),
        visitor_hash: visitorHash,
        visitor_hash_tenant: visitorHashTenant,
        is_bebocard: visitorHash !== null,
        amount,
        currency: currency ?? 'AUD',
        category: category ?? 'uncategorised',
        merchant,
        items,
        ingested_at: ingestedAt,
      });
    } catch (err) {
      console.error('[receipt-analytics-processor] Failed to parse record:', record.messageId, err);
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  if (rows.length === 0) return { batchItemFailures };

  // Group rows by tenant + brand + date for partitioned S3 writes
  const partitions: Record<string, AnalyticsRow[]> = {};
  for (const row of rows) {
    const key = `${row.tenant_id}/${row.brand_id}/${row.purchase_date}`;
    partitions[key] ??= [];
    partitions[key].push(row);
  }

  await Promise.all(
    Object.entries(partitions).map(([partition, partRows]) => {
      // Use the first SQS messageId for this partition as the filename — guaranteed unique per SQS
      const s3Key = `receipts/raw/${partition}/${partitionFirstMessageId[partition]}.jsonl`;
      const body = partRows.map(r => JSON.stringify(r)).join('\n');
      return s3.send(new PutObjectCommand({
        Bucket: ANALYTICS_BUCKET,
        Key: s3Key,
        Body: body,
        ContentType: 'application/x-ndjson',
      }));
    })
  );

  console.info(`[receipt-analytics-processor] Wrote ${rows.length} rows across ${Object.keys(partitions).length} partitions`);
  return { batchItemFailures };
};
