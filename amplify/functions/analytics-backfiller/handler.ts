import { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand, QueryExecutionState } from '@aws-sdk/client-athena';
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, QueryCommand as DocQueryCommand } from '@aws-sdk/lib-dynamodb';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { createHmac } from 'crypto';

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const athena = new AthenaClient({});

const ANALYTICS_BUCKET = process.env.ANALYTICS_BUCKET!;
const ATHENA_WORKGROUP = process.env.ATHENA_WORKGROUP!;
const GLUE_DATABASE    = process.env.GLUE_DATABASE!;
const REFDATA_TABLE    = process.env.REFDATA_TABLE!;
const DATA_TABLE       = process.env.USER_TABLE!;
const USER_HASH_SALT_PATH = process.env.USER_HASH_SALT_PATH;

const ssm = new SSMClient({});
let cachedSalt: string | undefined;

async function getSalt(): Promise<string> {
  if (cachedSalt) return cachedSalt;
  if (!USER_HASH_SALT_PATH) return process.env.USER_HASH_SALT || '';

  try {
    const res = await ssm.send(new GetParameterCommand({
      Name: USER_HASH_SALT_PATH,
      WithDecryption: true
    }));
    cachedSalt = res.Parameter?.Value;
    return cachedSalt || '';
  } catch (err) {
    console.error('[analytics-backfiller] Failed to fetch salt from SSM', { path: USER_HASH_SALT_PATH, err });
    return process.env.USER_HASH_SALT || '';
  }
}

interface BackfillEvent {
  tenantId: string;
}

interface ReceiptRow {
  userHash:     string;
  brandId:      string;
  tenantId:     string;
  purchaseDate: string;
  amount:       number;
  currency:     string;
  category:     string;
  merchant:     string;
  ingestedAt:   string;
}

export const handler = async (event: BackfillEvent) => {
  const { tenantId } = event;
  console.info(`[analytics-backfiller] Starting backfill for tenant: ${tenantId}`);

  // 1. Get tenant metadata (salt, tier, bucket)
  const tenantMeta = await getTenantMetadata(tenantId);
  if (!tenantMeta || tenantMeta.tier === 'ENGAGEMENT') {
    throw new Error(`Tenant ${tenantId} not found or on ENGAGEMENT tier`);
  }

  // 2. Get list of brandIds for this tenant
  const brands = await getBrandsForTenant(tenantId);
  console.info(`[analytics-backfiller] Found ${brands.length} brands: ${brands.join(', ')}`);

  let totalBackfilled = 0;

  // 3. For each brand, scan for historical receipts
  for (const brandId of brands) {
    console.info(`[analytics-backfiller] Searching receipts for brand: ${brandId}`);
    
    let lastEvaluatedKey: any = undefined;
    
    do {
      const result = await ddb.send(new DocQueryCommand({
        TableName: DATA_TABLE,
        IndexName: 'userDataBySubCategory',
        KeyConditionExpression: 'subCategory = :b',
        FilterExpression: 'primaryCat = :r',
        ExpressionAttributeValues: {
          ':b': brandId,
          ':r': 'receipt'
        },
        ExclusiveStartKey: lastEvaluatedKey
      }));

      const items = result.Items ?? [];
      const rows: ReceiptRow[] = [];

      for (const item of items) {
        const desc = typeof item.desc === 'string' ? JSON.parse(item.desc) : item.desc || {};
        
        // Strategy: Only backfill POS receipts (source=brand_push or has secondaryULID)
        if (!desc.secondaryULID) continue;

        const permULID = (item.pK as string).replace('USER#', '');
        const userHash = createHmac('sha256', tenantMeta.salt).update(permULID).digest('hex');

        rows.push({
          userHash,
          brandId,
          tenantId,
          purchaseDate: sanitizeDate(desc.purchaseDate) || new Date().toISOString().substring(0, 10),
          amount: sanitizeAmount(desc.amount) || 0,
          currency: sanitizeCurrency(desc.currency),
          category: sanitizeIdentifier(desc.category),
          merchant: sanitizeText(desc.merchant),
          ingestedAt: new Date().toISOString().replace('T', ' ').replace('Z', '').substring(0, 19)
        });
      }

      if (rows.length > 0) {
        await insertBatch(tenantId, rows, tenantMeta.analyticsBucket);
        totalBackfilled += rows.length;
      }

      lastEvaluatedKey = result.LastEvaluatedKey;
    } while (lastEvaluatedKey);
  }

  console.info(`[analytics-backfiller] Backfill complete. Total receipts added: ${totalBackfilled}`);
  return { success: true, backfilledCount: totalBackfilled };
};

// ── Helpers ───────────────────────────────────────────────────────────────────

async function getTenantMetadata(tenantId: string) {
  const res = await ddb.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'profile' }
  }));

  if (!res.Item) return null;
  const desc = typeof res.Item.desc === 'string' ? JSON.parse(res.Item.desc) : res.Item.desc || {};
  
  let analyticsBucket: string | undefined;
  if (res.Item.tier === 'ENTERPRISE') {
    if (desc.analyticsConfig?.customBucketArn) {
      analyticsBucket = desc.analyticsConfig.customBucketArn.split(':').pop();
    } else if (desc.analyticsConfig?.bucketType === 'DEDICATED') {
      analyticsBucket = `bebocard-enterprise-${tenantId.toLowerCase()}`;
    }
  }

  return {
    salt: desc.salt as string || await getSalt(),
    tier: res.Item.tier as string || 'ENGAGEMENT',
    analyticsBucket
  };
}

async function getBrandsForTenant(tenantId: string): Promise<string[]> {
  const result = await ddb.send(new DocQueryCommand({
    TableName: REFDATA_TABLE,
    IndexName: 'refDataByTenant',
    KeyConditionExpression: 'tenantId = :t',
    FilterExpression: 'primaryCat = :brand',
    ExpressionAttributeValues: {
      ':t': tenantId,
      ':brand': 'brand'
    }
  }));

  return (result.Items ?? []).map(i => i.brandId as string).filter(Boolean);
}

async function insertBatch(tenantId: string, rows: ReceiptRow[], customBucket?: string) {
  const tableName = `receipts_${tenantId.toLowerCase().replace(/[^a-z0-9_]/g, '_')}`;
  
  const valuesList = rows.map(r =>
    `('${r.userHash}', '${escapeSql(r.brandId)}', DATE '${r.purchaseDate}', ` +
    `${r.amount}, '${escapeSql(r.currency)}', '${escapeSql(r.category)}', '${escapeSql(r.merchant)}', TIMESTAMP '${r.ingestedAt}')`,
  ).join(',\n      ');

  const sql = `INSERT INTO \`${GLUE_DATABASE}\`.\`${tableName}\`
    (user_hash, brand_id, purchase_date, amount, currency, category, merchant, ingested_at)
  VALUES
    ${valuesList}`;

  const outputLocation = customBucket 
    ? `s3://${customBucket}/athena-results/backfill/` 
    : `s3://${ANALYTICS_BUCKET}/athena-results/backfill/`;

  const res = await athena.send(new StartQueryExecutionCommand({
    QueryString: sql,
    WorkGroup: ATHENA_WORKGROUP,
    ResultConfiguration: { OutputLocation: outputLocation },
  }));

  const queryId = res.QueryExecutionId!;
  await waitForQuery(queryId);
}

async function waitForQuery(queryId: string, maxWaitMs = 60_000) {
  const deadline = Date.now() + maxWaitMs;
  while (Date.now() < deadline) {
    const res = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));
    const state = res.QueryExecution?.Status?.State;
    if (state === QueryExecutionState.SUCCEEDED) return;
    if (state === QueryExecutionState.FAILED || state === QueryExecutionState.CANCELLED) {
      throw new Error(`Backfill Athena query ${queryId} ${state}: ${res.QueryExecution?.Status?.StateChangeReason}`);
    }
    await new Promise(r => setTimeout(r, 2000));
  }
  throw new Error(`Backfill Athena query ${queryId} timed out`);
}

// ── SQL sanitizers (duplicated from writer for independent backfiller) ───────

function escapeSql(value: string): string {
  return value.replace(/'/g, "''").replace(/\\/g, '\\\\');
}

function sanitizeDate(value: any): string | null {
  const s = String(value ?? '');
  const trimmed = s.substring(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(trimmed) ? trimmed : null;
}

function sanitizeAmount(value: any): number | null {
  const n = typeof value === 'number' ? value : parseFloat(String(value ?? ''));
  return Number.isFinite(n) && n >= 0 ? Math.round(n * 100) / 100 : null;
}

function sanitizeCurrency(value: any): string {
  const s = String(value ?? 'AUD');
  return /^[A-Z]{3}$/.test(s) ? s : 'AUD';
}

function sanitizeIdentifier(value: any): string {
  const s = String(value ?? 'other').trim().substring(0, 64);
  return /^[\w\s-]+$/.test(s) ? s : 'other';
}

function sanitizeText(value: any): string {
  return String(value ?? '')
    .replace(/[\x00-\x1F\x7F]/g, '')
    .trim()
    .substring(0, 200);
}
