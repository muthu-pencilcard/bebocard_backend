import { GlueClient, GetTablesCommand } from '@aws-sdk/client-glue';
import { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand, QueryExecutionState } from '@aws-sdk/client-athena';

const glue = new GlueClient({});
const athena = new AthenaClient({});

const GLUE_DATABASE = process.env.GLUE_DATABASE!;
const ATHENA_WORKGROUP = process.env.ATHENA_WORKGROUP!;
const ANALYTICS_BUCKET = process.env.ANALYTICS_BUCKET!;

export const handler = async () => {
  console.info(`[analytics-compactor] Starting nightly data lake optimization for database: ${GLUE_DATABASE}`);

  // 1. List all tenant receipt tables
  const tables = await listReceiptTables();
  console.info(`[analytics-compactor] Found ${tables.length} tables for optimization.`);

  // 2. Process each table in series
  for (const table of tables) {
    try {
      console.info(`[analytics-compactor] Optimizing table: ${table.name}...`);
      
      const sql = `OPTIMIZE \`${GLUE_DATABASE}\`.\`${table.name}\` REWRITE DATA`;
      
      const queryId = await startAthenaQuery(sql, table.bucket);
      if (queryId) {
        await waitForQuery(queryId);
        console.info(`[analytics-compactor] Successfully optimized ${table.name}`);
      }
    } catch (err) {
      console.error(`[analytics-compactor] Failed to optimize ${table.name}:`, err);
    }
  }

  console.info('[analytics-compactor] Nightly compaction complete.');
  return { success: true, processedCount: tables.length };
};

async function listReceiptTables(): Promise<{ name: string, bucket: string }[]> {
  const result = await glue.send(new GetTablesCommand({
    DatabaseName: GLUE_DATABASE,
  }));

  return (result.TableList ?? [])
    .filter(t => t.Name!.startsWith('receipts_'))
    .map(t => ({
      name: t.Name!,
      bucket: t.Parameters?.['bebocard:bucket_name'] || ANALYTICS_BUCKET
    }));
}

async function startAthenaQuery(sql: string, customBucket?: string): Promise<string | undefined> {
  const outputLocation = customBucket 
    ? `s3://${customBucket}/athena-results/compaction/` 
    : `s3://${ANALYTICS_BUCKET}/athena-results/compaction/`;

  const res = await athena.send(new StartQueryExecutionCommand({
    QueryString: sql,
    WorkGroup: ATHENA_WORKGROUP,
    ResultConfiguration: { OutputLocation: outputLocation }
  }));
  return res.QueryExecutionId;
}

async function waitForQuery(queryId: string, maxWaitMs = 600_000): Promise<void> {
  const deadline = Date.now() + maxWaitMs;
  while (Date.now() < deadline) {
    const res = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));
    const state = res.QueryExecution?.Status?.State;
    
    if (state === QueryExecutionState.SUCCEEDED) return;
    if (state === QueryExecutionState.FAILED || state === QueryExecutionState.CANCELLED) {
      throw new Error(`Query ${queryId} ${state}: ${res.QueryExecution?.Status?.StateChangeReason}`);
    }
    
    await new Promise(r => setTimeout(r, 5000));
  }
  throw new Error(`Query ${queryId} timed out`);
}
