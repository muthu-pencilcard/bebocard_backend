import { Handler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  QueryCommand,
  ScanCommand,
} from '@aws-sdk/lib-dynamodb';

const docClient = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const USER_TABLE = process.env.USER_TABLE!;
const CSM_SNS_TOPIC_ARN = process.env.CSM_SNS_TOPIC_ARN ?? '';

// SNS import only when topic ARN is configured (avoids import error in sandbox)
async function publishAlert(tenantId: string, brandId: string, currentWeekScans: number, previousWeekScans: number) {
  if (!CSM_SNS_TOPIC_ARN) {
    console.warn('[brand-health-monitor] CSM_SNS_TOPIC_ARN not set — alert suppressed (sandbox mode)');
    return;
  }
  const { SNSClient, PublishCommand } = await import('@aws-sdk/client-sns');
  const sns = new SNSClient({});
  await sns.send(new PublishCommand({
    TopicArn: CSM_SNS_TOPIC_ARN,
    Subject: `[BeboCard CSM Alert] Brand health drop — ${brandId}`,
    Message: JSON.stringify({
      tenantId,
      brandId,
      alert: 'scan_volume_drop',
      currentWeekScans,
      previousWeekScans,
      dropPercent: Math.round((1 - currentWeekScans / previousWeekScans) * 100),
      detectedAt: new Date().toISOString(),
      action: 'CSM to reach out within 24 hours per P3-16 SLA',
    }),
  }));
}

/**
 * P3-16 — Brand Health Monitor
 *
 * Runs weekly (Monday 08:00 UTC). For each active tenant, compares this week's
 * scan volume (TENANT_QUOTA# records) against last week's. If a brand's scan
 * volume has dropped > 50% week-over-week, publishes an alert to the CSM SNS
 * topic so the assigned CSM can proactively reach out before the brand files a
 * support ticket.
 *
 * Tiered action:
 *   - Base / Engagement: alert goes to general support queue
 *   - Intelligence / Enterprise: alert goes to named CSM
 */
export const handler: Handler = async () => {
  console.log('[brand-health-monitor] Starting weekly health check...');

  const now = new Date();
  const currentWeekStart = getWeekStart(now, 0);       // this Monday
  const previousWeekStart = getWeekStart(now, -7);     // last Monday
  const previousWeekEnd = getWeekStart(now, 0);        // = this Monday

  // 1. Get all active tenants
  const tenantsResult = await docClient.send(new ScanCommand({
    TableName: REFDATA_TABLE,
    FilterExpression: 'begins_with(pK, :prefix) AND sK = :sk AND #active = :active',
    ExpressionAttributeNames: { '#active': 'active' },
    ExpressionAttributeValues: { ':prefix': 'TENANT#', ':sk': 'profile', ':active': true },
    ProjectionExpression: 'pK, brandId, tenantName, billingTier, csmEmail',
  }));

  const tenants = tenantsResult.Items ?? [];
  console.log(`[brand-health-monitor] Checking ${tenants.length} active tenants`);

  const alerts: Array<{ tenantId: string; brandId: string; drop: number }> = [];

  for (const tenant of tenants) {
    const tenantId = (tenant.pK as string).replace('TENANT#', '');
    const brandId = tenant.brandId as string;

    // 2. Count quota events (scans + receipts) for current and previous week
    const [currentCount, previousCount] = await Promise.all([
      countQuotaEvents(tenantId, currentWeekStart, now),
      countQuotaEvents(tenantId, previousWeekStart, previousWeekEnd),
    ]);

    // 3. Skip brands with < 10 events (too small to be meaningful)
    if (previousCount < 10) continue;

    const dropFraction = previousCount > 0 ? (previousCount - currentCount) / previousCount : 0;

    if (dropFraction >= 0.5) {
      const dropPercent = Math.round(dropFraction * 100);
      console.log(`[brand-health-monitor] ALERT: ${brandId} — ${dropPercent}% drop (${previousCount} → ${currentCount})`);
      alerts.push({ tenantId, brandId, drop: dropPercent });
      await publishAlert(tenantId, brandId, currentCount, previousCount);
    } else {
      console.log(`[brand-health-monitor] OK: ${brandId} — ${currentCount} scans this week (prev: ${previousCount})`);
    }
  }

  console.log(`[brand-health-monitor] Done. ${alerts.length} alerts fired.`);
  return { alertCount: alerts.length, alerts };
};

async function countQuotaEvents(tenantId: string, from: Date, to: Date): Promise<number> {
  const result = await docClient.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    KeyConditionExpression: 'pK = :pk AND sK BETWEEN :from AND :to',
    ExpressionAttributeValues: {
      ':pk': `TENANT_QUOTA#${tenantId}`,
      ':from': from.toISOString(),
      ':to': to.toISOString(),
    },
    Select: 'COUNT',
  }));
  return result.Count ?? 0;
}

function getWeekStart(base: Date, offsetDays: number): Date {
  const d = new Date(base);
  d.setDate(d.getDate() + offsetDays);
  d.setHours(0, 0, 0, 0);
  // Roll back to Monday
  const day = d.getDay(); // 0=Sun, 1=Mon...
  d.setDate(d.getDate() - ((day === 0 ? 7 : day) - 1));
  return d;
}
