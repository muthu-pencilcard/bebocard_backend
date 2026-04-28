import type { Handler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, ScanCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { SESClient, SendEmailCommand } from '@aws-sdk/client-ses';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import {
  ALL_USAGE_TYPES,
  getUsageMonthKey,
  getTenantUsageCounter,
  getCategoryOverageRate,
  normalizeTenantTier,
  parseTenantIncludedEvents,
  type TenantTier,
  type UsageType,
} from '../../shared/tenant-billing';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ses = new SESClient({});
const ssm = new SSMClient({});

const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const USER_TABLE = process.env.USER_TABLE!;
const FROM_EMAIL = process.env.FROM_EMAIL ?? 'billing@bebocard.com.au';

// ── Stripe helpers (inline — avoid importing the portal's Stripe module) ──────

let cachedStripeKey: string | null = null;
async function getStripeKey(): Promise<string> {
  if (cachedStripeKey) return cachedStripeKey;
  try {
    const res = await ssm.send(new GetParameterCommand({
      Name: '/amplify/shared/STRIPE_SECRET_KEY',
      WithDecryption: true,
    }));
    cachedStripeKey = res.Parameter?.Value ?? '';
    return cachedStripeKey;
  } catch (err) {
    console.warn('[billing-run] Failed to fetch key from SSM, falling back to env');
    return process.env.STRIPE_SECRET_KEY ?? '';
  }
}

async function createStripeInvoiceItem(input: {
  customerId: string;
  amountAud: number;
  description: string;
  tenantId: string;
  month: string;
}): Promise<{ id: string; invoice?: string | null }> {
  const stripeKey = await getStripeKey();
  if (!stripeKey) throw new Error('STRIPE_SECRET_KEY not configured');

  const params = new URLSearchParams({
    customer: input.customerId,
    currency: 'aud',
    amount: String(Math.round(input.amountAud * 100)),
    description: input.description,
    'metadata[tenantId]': input.tenantId,
    'metadata[month]': input.month,
    'metadata[type]': 'overage',
  });

  const response = await fetch('https://api.stripe.com/v1/invoiceitems', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${stripeKey}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: params.toString(),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Stripe API error: ${response.status} ${text}`);
  }

  return response.json() as Promise<{ id: string; invoice?: string | null }>;
}

async function createStripeTransfer(input: {
  amountAud: number;
  destinationAccountId: string;
  transferGroup: string;
  metadata: Record<string, string>;
}): Promise<{ id: string }> {
  const stripeKey = await getStripeKey();
  if (!stripeKey) throw new Error('STRIPE_SECRET_KEY not configured');

  const params = new URLSearchParams({
    amount: String(Math.round(input.amountAud * 100)),
    currency: 'aud',
    destination: input.destinationAccountId,
    transfer_group: input.transferGroup,
  });
  
  for (const [k, v] of Object.entries(input.metadata)) {
    params.append(`metadata[${k}]`, v);
  }

  const response = await fetch('https://api.stripe.com/v1/transfers', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${stripeKey}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: params.toString(),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Stripe Transfer error: ${response.status} ${text}`);
  }

  return response.json() as Promise<{ id: string }>;
}

// ── Tier billable type filter ─────────────────────────────────────────────────

function isBillableTypeForTier(tier: TenantTier, type: string): boolean {
  if (tier === 'base') return ['offers', 'newsletters', 'catalogues'].includes(type);
  if (tier === 'engagement') return ['offers', 'newsletters', 'catalogues', 'invoices', 'geolocation'].includes(type);
  return ['offers', 'newsletters', 'catalogues', 'invoices', 'geolocation', 'payments', 'consent'].includes(type);
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

// ── Tenant/billing data helpers ───────────────────────────────────────────────

interface TenantProfile {
  tenantId: string;
  tenantName: string;
  tier: TenantTier;
  billingEmail: string | null;
  contactEmail: string | null;
  stripeCustomerId: string | null;
  stripeSubscriptionId: string | null;
  includedEventsPerMonth: number | null;
  billingStatus: string | null;
  status: string;
  createdAt?: string;
  scheduledTier?: TenantTier;
  scheduledTierEffectiveMonth?: string;
}

function parseRecord(value: unknown): Record<string, unknown> {
  if (typeof value === 'object' && value !== null) return value as Record<string, unknown>;
  if (typeof value !== 'string' || value.length === 0) return {};
  try { return JSON.parse(value) as Record<string, unknown>; } catch { return {}; }
}

async function listActiveTenants(): Promise<TenantProfile[]> {
  const items: any[] = [];
  let lastEvaluatedKey: Record<string, any> | undefined = undefined;

  do {
    const scanResponse = (await dynamo.send(new ScanCommand({
      TableName: REFDATA_TABLE,
      FilterExpression: 'primaryCat = :cat AND sK = :sk AND #status = :active',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':cat': 'tenant', ':sk': 'PROFILE', ':active': 'ACTIVE' },
      ExclusiveStartKey: lastEvaluatedKey,
    }))) as any;
    
    if (scanResponse.Items) {
      items.push(...scanResponse.Items);
    }
    lastEvaluatedKey = scanResponse.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  return items.map((item) => {
    const desc = parseRecord(item.desc);
    const tier = normalizeTenantTier(desc.tier);
    return {
      tenantId: typeof desc.tenantId === 'string' ? desc.tenantId : String(item.pK).replace('TENANT#', ''),
      tenantName: typeof desc.tenantName === 'string' ? desc.tenantName : 'Unknown',
      tier,
      billingEmail: typeof desc.billingEmail === 'string' ? desc.billingEmail : null,
      contactEmail: typeof desc.contactEmail === 'string' ? desc.contactEmail : null,
      stripeCustomerId: typeof desc.stripeCustomerId === 'string' ? desc.stripeCustomerId : null,
      stripeSubscriptionId: typeof desc.stripeSubscriptionId === 'string' ? desc.stripeSubscriptionId : null,
      includedEventsPerMonth: parseTenantIncludedEvents(desc.includedEventsPerMonth, tier),
      billingStatus: typeof desc.billingStatus === 'string' ? desc.billingStatus : 'ACTIVE',
      status: String(item.status ?? 'ACTIVE'),
      createdAt: item.createdAt,
      scheduledTier: desc.scheduledTier as TenantTier | undefined,
      scheduledTierEffectiveMonth: desc.scheduledTierEffectiveMonth as string | undefined,
    };
  });
}

async function getBillingRun(tenantId: string, month: string) {
  const res = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: `BILLING_RUN#${month}` },
  }));
  if (!res.Item) return null;
  const desc = parseRecord(res.Item.desc);
  return { status: desc.status as string | undefined };
}

async function saveBillingRun(tenantId: string, month: string, data: {
  overageCount: number;
  overageAud: number;
  stripeInvoiceId: string | null;
  stripeInvoiceItemId: string | null;
  status: 'PENDING' | 'INVOICED' | 'SKIPPED';
}) {
  const now = new Date().toISOString();
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK: `TENANT#${tenantId}`,
      sK: `BILLING_RUN#${month}`,
      eventType: 'TENANT_BILLING_RUN',
      status: data.status,
      primaryCat: 'tenant_billing_run',
      desc: JSON.stringify({ tenantId, month, ...data, createdAt: now, updatedAt: now }),
      createdAt: now,
      updatedAt: now,
    },
  }));
}

// ── SES email helpers ─────────────────────────────────────────────────────────

async function sendTrialWarningEmail(tenant: TenantProfile, expiryStr: string) {
  const email = tenant.billingEmail ?? tenant.contactEmail;
  if (!email) return;

  const body = [
    `Hi ${tenant.tenantName},`,
    '',
    `Your BeboCard trial is ending soon.`,
    '',
    `Your trial is scheduled to expire on ${expiryStr} (AEDT).`,
    `To avoid interruption to your brand engagement features, please link a payment method in the BeboCard Business Portal.`,
    '',
    'Best regards,',
    'BeboCard Billing',
  ].join('\n');

  await ses.send(new SendEmailCommand({
    Source: FROM_EMAIL,
    Destination: { ToAddresses: [email] },
    Message: {
      Subject: { Data: 'Action Required: Your BeboCard trial is ending soon' },
      Body: { Text: { Data: body } },
    },
  }));
}

async function sendBillingSummaryEmail(tenant: TenantProfile, month: string, summary: {
  overageCount: number;
  overageAud: number;
  status: string;
  categoryBreakdown: Array<{ type: string; count: number; overageShare: number; aud: number }>;
}) {
  const email = tenant.billingEmail ?? tenant.contactEmail;
  if (!email) return;

  const formattedMonth = new Date(`${month}-01`).toLocaleDateString('en-AU', { month: 'long', year: 'numeric' });
  const included = tenant.includedEventsPerMonth ?? 0;

  let body: string;
  if (summary.status === 'SKIPPED') {
    body = [
      `Hi ${tenant.tenantName},`,
      '',
      `Your BeboCard billing summary for ${formattedMonth}:`,
      '',
      `  Tier: ${capitalize(tenant.tier)}`,
      `  Included events: ${included.toLocaleString()}`,
      `  Your usage stayed within the included quota — no overage charges this month.`,
      '',
      'Best regards,',
      'BeboCard Billing',
    ].join('\n');
  } else {
    const categoryLines = summary.categoryBreakdown
      .filter((c) => c.overageShare > 0)
      .map((c) => `  ${capitalize(c.type)}: ${c.overageShare} overage deliveries → AUD $${c.aud.toFixed(2)}`);

    body = [
      `Hi ${tenant.tenantName},`,
      '',
      `Your BeboCard billing summary for ${formattedMonth}:`,
      '',
      `  Tier: ${capitalize(tenant.tier)}`,
      `  Included events: ${included.toLocaleString()}`,
      `  Total overage: ${summary.overageCount.toLocaleString()} events`,
      `  Overage charge: AUD $${summary.overageAud.toFixed(2)}`,
      '',
      'Category breakdown:',
      ...categoryLines,
      '',
      'This overage will appear on your next Stripe invoice.',
      '',
      'Best regards,',
      'BeboCard Billing',
    ].join('\n');
  }

  try {
    await ses.send(new SendEmailCommand({
      Source: FROM_EMAIL,
      Destination: { ToAddresses: [email] },
      Message: {
        Subject: { Data: `BeboCard Billing Summary — ${formattedMonth}` },
        Body: { Text: { Data: body } },
      },
    }));
    console.info(`[billing-run] Email sent to ${email} for ${tenant.tenantId}`);
  } catch (err) {
    console.error(`[billing-run] Failed to send email to ${email}:`, err);
  }
}

// ── Logic Helpers ─────────────────────────────────────────────────────────────

async function monitorQuota(tenant: TenantProfile, month: string) {
  const included = tenant.includedEventsPerMonth;
  if (included == null || included <= 0) return;

  const usageRecords = await Promise.all(
    ALL_USAGE_TYPES.map(async (type) => {
      const usage = await getTenantUsageCounter(dynamo, REFDATA_TABLE, tenant.tenantId, type, month);
      return { type, count: usage.usageCount };
    }),
  );

  const billableTotal = usageRecords
    .filter(r => isBillableTypeForTier(tenant.tier, r.type))
    .reduce((sum, r) => sum + r.count, 0);

  const ratio = billableTotal / included;
  const email = tenant.billingEmail ?? tenant.contactEmail;
  if (!email) return;

  if (ratio >= 1.0) {
     console.warn(`[billing-run] Quota EXCEEDED for ${tenant.tenantId} (${billableTotal}/${included})`);
  } else if (ratio >= 0.8) {
     console.info(`[billing-run] Quota warning (80%+) for ${tenant.tenantId} (${billableTotal}/${included})`);
  }
}

async function processOverage(tenant: TenantProfile, month: string) {
  const existingRun = await getBillingRun(tenant.tenantId, month);
  if (existingRun?.status === 'INVOICED') {
    console.info(`[billing-run] skip ${tenant.tenantId}: already invoiced`);
    return;
  }

  const usageRecords = await Promise.all(
    ALL_USAGE_TYPES.map(async (type) => {
      const usage = await getTenantUsageCounter(dynamo, REFDATA_TABLE, tenant.tenantId, type, month);
      return { type, count: usage.usageCount };
    }),
  );

  const billableRecords = usageRecords.filter((r) => isBillableTypeForTier(tenant.tier, r.type));
  const totalBillable = billableRecords.reduce((s, r) => s + r.count, 0);
  const included = tenant.includedEventsPerMonth ?? 0;
  const overageCount = Math.max(0, totalBillable - included);

  if (overageCount <= 0 || !tenant.stripeCustomerId) {
    await saveBillingRun(tenant.tenantId, month, {
      overageCount: 0,
      overageAud: 0,
      stripeInvoiceId: null,
      stripeInvoiceItemId: null,
      status: 'SKIPPED',
    });

    await sendBillingSummaryEmail(tenant, month, {
      overageCount: 0,
      overageAud: 0,
      status: 'SKIPPED',
      categoryBreakdown: [],
    });
    return;
  }

  let overageAudTotal = 0;
  let lastStripeItemId: string | null = null;
  let lastStripeInvoiceId: string | null = null;
  const categoryBreakdown: Array<{ type: string; count: number; overageShare: number; aud: number }> = [];

  for (const cat of billableRecords) {
    if (cat.count <= 0) continue;
    const catOverage = Math.round((cat.count / totalBillable) * overageCount);
    if (catOverage <= 0) continue;

    const rate = getCategoryOverageRate(tenant.tier, cat.type as UsageType);
    const catAud = Number((catOverage * rate).toFixed(2));
    categoryBreakdown.push({ type: cat.type, count: cat.count, overageShare: catOverage, aud: catAud });

    if (catAud <= 0) continue;

    overageAudTotal += catAud;
    const stripeKey = await getStripeKey();
    if (stripeKey) {
      const invoiceItem = await createStripeInvoiceItem({
        customerId: tenant.stripeCustomerId,
        amountAud: catAud,
        description: `${capitalize(cat.type)} overage: ${catOverage} deliveries @ $${rate.toFixed(2)}/ea (${month})`,
        tenantId: tenant.tenantId,
        month,
      });
      lastStripeItemId = invoiceItem.id;
      lastStripeInvoiceId = invoiceItem.invoice ?? lastStripeInvoiceId;
    }
  }

  await saveBillingRun(tenant.tenantId, month, {
    overageCount,
    overageAud: Number(overageAudTotal.toFixed(2)),
    stripeInvoiceId: lastStripeInvoiceId,
    stripeInvoiceItemId: lastStripeItemId,
    status: overageAudTotal > 0 ? 'INVOICED' : 'SKIPPED',
  });

  await sendBillingSummaryEmail(tenant, month, {
    overageCount,
    overageAud: overageAudTotal,
    status: overageAudTotal > 0 ? 'INVOICED' : 'SKIPPED',
    categoryBreakdown,
  });
}

async function applyTierChange(tenantId: string, newTier: TenantTier) {
  const now = new Date().toISOString();
  
  const getRes = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'PROFILE' },
  }));
  if (!getRes.Item) return;

  const desc = parseRecord(getRes.Item.desc);
  const updatedDesc = {
    ...desc,
    tier: newTier,
    tierStartDate: now,
    scheduledTier: null,
    scheduledTierEffectiveMonth: null,
    updatedAt: now,
  };

  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      ...getRes.Item,
      desc: JSON.stringify(updatedDesc),
      updatedAt: now,
    },
  }));
}

// ── Marketplace Payouts (P4-2) ────────────────────────────────────────────────

async function processMarketplacePayouts() {
  console.info('[billing-run] Sweeping PENDING marketplace withdrawals...');
  
  let lastEvaluatedKey: Record<string, any> | undefined = undefined;
  let successCount = 0;
  let failCount = 0;

  do {
    const scanResponse = await dynamo.send(new ScanCommand({
      TableName: USER_TABLE,
      FilterExpression: 'primaryCat = :cat AND #status = :pending',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':cat': 'withdrawal', ':pending': 'PENDING' },
      ExclusiveStartKey: lastEvaluatedKey,
    }));

    if (scanResponse.Items) {
      for (const item of scanResponse.Items) {
        try {
          const desc = parseRecord(item.desc);
          const permULID = String(item.pK).replace('USER#', '');
          const amount = Number(desc.amount ?? 0);
          
          // 1. Resolve Identity for Stripe Account ID
          const idRes = await dynamo.send(new GetCommand({
            TableName: USER_TABLE,
            Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
          }));
          
          if (!idRes.Item) throw new Error(`Identity not found for ${permULID}`);
          const idDesc = parseRecord(idRes.Item.desc);
          const stripeAccountId = idDesc.stripeAccountId as string | undefined;
          
          if (!stripeAccountId) {
            console.warn(`[payout] User ${permULID} has no stripeAccountId. Skipping.`);
            continue;
          }

          // 2. Trigger Stripe Transfer
          console.info(`[payout] Transferring $${amount} to ${stripeAccountId} for ${permULID}`);
          const transfer = await createStripeTransfer({
            amountAud: amount,
            destinationAccountId: stripeAccountId,
            transferGroup: `WITHDRAWAL_${item.sK}`,
            metadata: {
              permULID,
              withdrawalSK: item.sK,
              source: 'bebocard_marketplace'
            }
          });

          // 3. Mark as COMPLETED
          await dynamo.send(new PutCommand({
            TableName: USER_TABLE,
            Item: {
              ...item,
              status: 'COMPLETED',
              updatedAt: new Date().toISOString(),
              desc: JSON.stringify({
                ...desc,
                stripeTransferId: transfer.id,
                completedAt: new Date().toISOString(),
              }),
            },
          }));
          
          successCount++;
        } catch (err) {
          console.error(`[payout] Failed processing withdrawal ${item.sK}:`, err);
          failCount++;
        }
      }
    }
    lastEvaluatedKey = scanResponse.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  console.info(`[billing-run] Payouts direct: success=${successCount}, fail=${failCount}`);
}

// ── Main handler ──────────────────────────────────────────────────────────────

export const handler: Handler = async () => {
  const now = new Date();
  const today = now.toISOString().split('T')[0];
  const isFirstOfMonth = now.getDate() === 1;
  const currentMonth = now.toISOString().slice(0, 7);

  const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const prevMonth = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, '0')}`;

  console.info(`[billing-run] Day: ${today}, Month: ${currentMonth}, FirstOfMonth: ${isFirstOfMonth}`);

  const tenants = await listActiveTenants();
  console.info(`[billing-run] ${tenants.length} active tenants`);

  let processedCount = 0;

  for (const tenant of tenants) {
    try {
      // ── 1. Trial Expiry Check (Daily) ──
      const hasStripeSetup = !!tenant.stripeCustomerId && !!tenant.stripeSubscriptionId;
      if (!hasStripeSetup && tenant.createdAt) {
        const trialLengthDays = 30;
        const warningLeadDays = 7;
        const start = new Date(tenant.createdAt);
        const expiry = new Date(start.getTime() + trialLengthDays * 24 * 3600 * 1000);
        const warningDate = new Date(expiry.getTime() - warningLeadDays * 24 * 3600 * 1000);
        
        const warningStr = warningDate.toISOString().split('T')[0];
        if (today === warningStr) {
          console.info(`[billing-run] Sending trial warning to ${tenant.tenantId} (Expires: ${expiry.toISOString().split('T')[0]})`);
          await sendTrialWarningEmail(tenant, expiry.toISOString().split('T')[0]);
        }
      }

      // ── 2. Scheduled Tier Changes (1st of month) ──
      if (isFirstOfMonth && tenant.scheduledTier && tenant.scheduledTierEffectiveMonth === currentMonth) {
        console.info(`[billing-run] Applying scheduled tier change for ${tenant.tenantId}: ${tenant.tier} -> ${tenant.scheduledTier}`);
        await applyTierChange(tenant.tenantId, tenant.scheduledTier);
      }

      // ── 3. Quota Monitoring (Daily) ──
      await monitorQuota(tenant, currentMonth);

      // ── 4. Overage Invoicing (1st of month for previous month) ──
      if (isFirstOfMonth) {
        await processOverage(tenant, prevMonth);
      }

      processedCount++;
    } catch (err) {
      console.error(`[billing-run] Error processing ${tenant.tenantId}:`, err);
    }
  }

  // ── 5. Marketplace Payouts (P4-2) ──
  await processMarketplacePayouts();

  console.info(`[billing-run] Complete: processedCount=${processedCount} total=${tenants.length}`);
  return { today, processedCount, total: tenants.length };
};
