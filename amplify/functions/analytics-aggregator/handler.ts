import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const ADMIN_TABLE = process.env.ADMIN_TABLE!;
const REPORT_TABLE = process.env.REPORT_TABLE!;

interface AggregatorEvent {
  brandId?: string;
  startDate?: string; // YYYY-MM-DD
  endDate?: string;   // YYYY-MM-DD
}

/**
 * Nightly Aggregator for Analytics
 *
 * Scans all segment records and computes brand-level daily summaries.
 * These are stored in ReportDataEvent for fast trend-line rendering
 * in the brand portal.
 */
export const handler = async (event?: AggregatorEvent) => {
  console.info('[analytics-aggregator] Starting aggregation');
  
  const targetBrands = event?.brandId ? [event.brandId] : await getActiveBrands();
  console.info(`[analytics-aggregator] Processing ${targetBrands.length} brands`);

  const startDate = event?.startDate ?? new Date().toISOString().split('T')[0];
  const endDate = event?.endDate ?? startDate;
  
  // Build list of dates to process
  const dates: string[] = [];
  let curr = new Date(startDate);
  const finish = new Date(endDate);
  while (curr <= finish) {
    dates.push(curr.toISOString().split('T')[0]);
    curr.setDate(curr.getDate() + 1);
  }

  for (const date of dates) {
    for (const brandId of targetBrands) {
      try {
        console.info(`[analytics-aggregator] Aggregating ${brandId} for ${date}...`);
        
        const stats = await aggregateSegmentsForBrand(brandId, date);
        
        await dynamo.send(new PutCommand({
          TableName: REPORT_TABLE,
          Item: {
            pK: `REPORT#${brandId}`,
            sK: `DAILY#${date}`,
            eventType: 'SEGMENT_DAILY',
            status: 'ACTIVE',
            desc: JSON.stringify(stats),
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          }
        }));

        console.info(`[analytics-aggregator] Success: ${brandId} @ ${date}`);
      } catch (err) {
        console.error(`[analytics-aggregator] Failed for ${brandId} @ ${date}:`, err);
      }
    }
  }

  console.info('[analytics-aggregator] Aggregation complete');
};

async function getActiveBrands(): Promise<string[]> {
  const result: string[] = [];
  let lastKey: Record<string, unknown> | undefined;

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: REFDATA_TABLE,
      IndexName: 'refDataByStatus',
      KeyConditionExpression: '#status = :active AND primaryCat = :brand',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':active': 'ACTIVE', ':brand': 'brand' },
      ExclusiveStartKey: lastKey,
    }));

    for (const item of res.Items ?? []) {
      if (item.pK.startsWith('BRAND#')) {
        result.push(item.pK.replace('BRAND#', ''));
      }
    }
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  return result;
}

async function aggregateSegmentsForBrand(brandId: string, today: string) {
  const segmentSK = `SEGMENT#${brandId}`;
  const subscriptionSK = `SUBSCRIPTION#${brandId}`;
  const auditPK = `AUDIT#${brandId}`;
  
  const todayPrefix = today; // YYYY-MM-DD
  
  let segmentSubscriberCount = 0;
  const spendCounts: Record<string, number> = { '<100': 0, '100-200': 0, '200-500': 0, '500+': 0 };
  const visitCounts: Record<string, number> = { new: 0, occasional: 0, frequent: 0, lapsed: 0 };
  
  let totalSpend = 0;
  let totalVisits = 0;

  // 1. Roll up segments (subscriber profile snapshot)
  let lastKey: Record<string, unknown> | undefined;
  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      IndexName: 'sK-pK-index',
      KeyConditionExpression: 'sK = :sk',
      ExpressionAttributeValues: { ':sk': segmentSK },
      ExclusiveStartKey: lastKey,
      Limit: 1000,
    }));

    for (const item of res.Items ?? []) {
      let seg: any;
      try { seg = JSON.parse(item.desc ?? '{}'); } catch { continue; }
      if (!seg.subscribed) continue;

      segmentSubscriberCount++;
      if (seg.spendBucket && seg.spendBucket in spendCounts) spendCounts[seg.spendBucket]++;
      if (seg.visitFrequency && seg.visitFrequency in visitCounts) visitCounts[seg.visitFrequency]++;
      totalSpend += seg.totalSpend ?? 0;
      totalVisits += seg.visitCount ?? 0;
    }
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  // 2. Roll up total subscriptions (cumulative growth)
  let totalActiveSubscribers = 0;
  let newJoinsToday = 0;
  let churnedToday = 0;

  lastKey = undefined;
  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      IndexName: 'sK-pK-index',
      KeyConditionExpression: 'sK = :sk',
      ExpressionAttributeValues: { ':sk': subscriptionSK },
      ExclusiveStartKey: lastKey,
      Limit: 1000,
    }));

    for (const item of res.Items ?? []) {
        const createdAt = String(item.createdAt ?? '');
        const status    = String(item.status ?? 'ACTIVE');
        // Simple string check for today's ISO date prefix
        if (status === 'ACTIVE') {
            totalActiveSubscribers++;
            if (createdAt.startsWith(todayPrefix)) newJoinsToday++;
        } else if (status === 'REVOKED' || status === 'INACTIVE') {
            if (String(item.updatedAt ?? '').startsWith(todayPrefix)) churnedToday++;
        }
    }
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  // 3. Roll up activity metrics (Scans, Receipts)
  // Scans: Count audit logs for today starting with LOG#today
  let scansToday = 0;
  lastKey = undefined;
  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: ADMIN_TABLE,
      KeyConditionExpression: 'pK = :pk AND begins_with(sK, :sk)',
      FilterExpression: 'contains(desc, :scan_action)',
      ExpressionAttributeValues: {
        ':pk': auditPK,
        ':sk': `LOG#${todayPrefix}`,
        ':scan_action': '/scan' 
      },
      ExclusiveStartKey: lastKey,
    }));
    scansToday += res.Count ?? (res.Items?.length ?? 0);
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  // Receipts: Query subCategory index for brandId and filter by date + primaryCat
  let receiptsToday = 0;
  let revenueToday = 0;
  const categoryCounts: Record<string, { total_amount: number, tx_count: number }> = {};

  lastKey = undefined;
  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      IndexName: 'subCategory-createdAt', // Physical index name in Amplify
      KeyConditionExpression: 'subCategory = :brand AND begins_with(createdAt, :date)',
      FilterExpression: 'primaryCat = :cat',
      ExpressionAttributeValues: {
        ':brand': brandId,
        ':date': todayPrefix,
        ':cat': 'receipt'
      },
      ExclusiveStartKey: lastKey,
    }));
    for (const item of res.Items ?? []) {
      receiptsToday++;
      let desc: any;
      try { desc = JSON.parse(item.desc ?? '{}'); } catch { continue; }
      
      const amt = desc.amount ?? 0;
      revenueToday += amt;

      const cat = desc.category ?? 'Uncategorized';
      if (!categoryCounts[cat]) categoryCounts[cat] = { total_amount: 0, tx_count: 0 };
      categoryCounts[cat].total_amount += amt;
      categoryCounts[cat].tx_count += 1;
    }
    lastKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (lastKey);

  // Convert categoryCounts to sorted array for the mix
  const categoryMix = Object.entries(categoryCounts)
    .map(([category, stats]) => ({
      category,
      total_amount: Math.round(stats.total_amount * 100) / 100,
      tx_count: stats.tx_count,
      atv: stats.tx_count > 0 ? Math.round((stats.total_amount / stats.tx_count) * 100) / 100 : 0
    }))
    .sort((a, b) => b.total_amount - a.total_amount);

  return {
    subscriberCount: totalActiveSubscribers,
    segmentSubscriberCount,
    newJoins: newJoinsToday,
    churned: churnedToday,
    scans: scansToday,
    receipts: receiptsToday,
    revenue: Math.round(revenueToday * 100) / 100,
    categoryMix,
    totalSpend: Math.round(totalSpend * 100) / 100,
    totalVisits,
    avgSpendPerUser: segmentSubscriberCount > 0 ? Math.round((totalSpend / segmentSubscriberCount) * 100) / 100 : 0,
    avgVisitsPerUser: segmentSubscriberCount > 0 ? Math.round((totalVisits / segmentSubscriberCount) * 100) / 100 : 0,
    spendDistribution: normalise(spendCounts, segmentSubscriberCount),
    visitFrequency: normalise(visitCounts, segmentSubscriberCount),
    computedAt: new Date().toISOString(),
  };
}

function normalise(counts: Record<string, number>, total: number): Record<string, number> {
  if (total === 0) return counts;
  return Object.fromEntries(
    Object.entries(counts).map(([k, v]) => [k, Math.round((v / total) * 100) / 100]),
  );
}
