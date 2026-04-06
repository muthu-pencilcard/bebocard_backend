import type { Handler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, ScanCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { SESClient, SendEmailCommand } from '@aws-sdk/client-ses';
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

const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const FROM_EMAIL = process.env.FROM_EMAIL ?? 'billing@bebocard.com.au';

// ── Stripe helpers (inline — avoid importing the portal's Stripe module) ──────

const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY ?? '';

async function createStripeInvoiceItem(input: {
  customerId: string;
  amountAud: number;
  description: string;
  tenantId: string;
  month: string;
}): Promise<{ id: string; invoice?: string | null }> {
  if (!STRIPE_SECRET_KEY) throw new Error('STRIPE_SECRET_KEY not configured');

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
      Authorization: `Bearer ${STRIPE_SECRET_KEY}`,
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

// ── SES email helper ──────────────────────────────────────────────────────────

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

// ── Main handler ──────────────────────────────────────────────────────────────

export const handler: Handler = async () => {
  // Run for the *previous* month (the cron fires on the 1st of the new month)
  const now = new Date();
  const prevMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const month = `${prevMonth.getFullYear()}-${String(prevMonth.getMonth() + 1).padStart(2, '0')}`;

  console.info(`[billing-run] Starting billing run for month: ${month}`);

  const tenants = await listActiveTenants();
  console.info(`[billing-run] ${tenants.length} active tenants`);

  let invoiced = 0;
  let skipped = 0;

  for (const tenant of tenants) {
    try {
      // Skip if already invoiced for this month
      const existingRun = await getBillingRun(tenant.tenantId, month);
      if (existingRun?.status === 'INVOICED') {
        console.info(`[billing-run] skip ${tenant.tenantId}: already invoiced`);
        skipped++;
        continue;
      }

      // Collect usage
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

        skipped++;
        continue;
      }

      // Per-category overage line items
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
        if (STRIPE_SECRET_KEY) {
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

      if (overageAudTotal > 0) invoiced++;
      else skipped++;

      console.info(`[billing-run] ${tenant.tenantId}: overage=${overageCount} aud=${overageAudTotal.toFixed(2)}`);
    } catch (err) {
      console.error(`[billing-run] Error processing ${tenant.tenantId}:`, err);
    }
  }

  console.info(`[billing-run] Complete: invoiced=${invoiced} skipped=${skipped}`);
  return { month, invoiced, skipped, total: tenants.length };
};
