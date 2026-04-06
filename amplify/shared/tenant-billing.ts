import { GetCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

export const ALL_USAGE_TYPES = [
  // Core brand-initiated events (all tiers)
  'offers',
  'newsletters',
  'catalogues',
  'invoices',
  'geolocation',
  'payments',
  'consent',
  // Engagement events — Engagement tier and above
  'newsletter_reads',
  'offer_engagements',
  'catalogue_views',
  'delivery_outcomes',
  // Intelligence events — Intelligence tier and above
  'consent_decisions',
  'payment_decisions',
  'enrollment_decisions',
  'subscription_changes',
] as const;

export type UsageType = typeof ALL_USAGE_TYPES[number];
export type TenantTier = 'base' | 'engagement' | 'intelligence' | 'enterprise';

const TIER_ORDER: Record<TenantTier, number> = { base: 0, engagement: 1, intelligence: 2, enterprise: 3 };

/** Minimum tier required to meter or access this usage type. Absent = all tiers. */
export const USAGE_TYPE_MIN_TIER: Partial<Record<UsageType, TenantTier>> = {
  newsletter_reads:     'engagement',
  offer_engagements:    'engagement',
  catalogue_views:      'engagement',
  delivery_outcomes:    'engagement',
  consent_decisions:    'intelligence',
  payment_decisions:    'intelligence',
  enrollment_decisions: 'intelligence',
  subscription_changes: 'intelligence',
};

/**
 * Returns true when the tenant tier is high enough to access a usage type.
 * Use this in API routes to gate engagement/intelligence analytics.
 */
export function canAccessUsageType(tier: TenantTier, type: UsageType): boolean {
  const minTier = USAGE_TYPE_MIN_TIER[type];
  if (!minTier) return true;
  return TIER_ORDER[tier] >= TIER_ORDER[minTier];
}

function parseRecord(value: unknown): Record<string, unknown> {
  if (typeof value === 'object' && value !== null) return value as Record<string, unknown>;
  if (typeof value !== 'string' || value.length === 0) return {};
  try {
    return JSON.parse(value) as Record<string, unknown>;
  } catch {
    return {};
  }
}

export function getUsageMonthKey(date = new Date()): string {
  return date.toISOString().slice(0, 7);
}

export function normalizeTenantTier(value: unknown): TenantTier {
  if (value === 'engagement' || value === 'growth') return 'engagement';
  if (value === 'enterprise') return 'enterprise';
  if (value === 'intelligence') return 'intelligence';
  return 'base';
}

export const TIER_INCLUDED_EVENTS: Record<TenantTier, number> = {
  base: 250,
  engagement: 2500,
  intelligence: 25000,
  enterprise: 999999999, // Essentially unlimited; managed by custom contracts
};

export const TIER_OVERAGE_RATES: Record<TenantTier, number> = {
  base: 0.45,
  engagement: 0.20,
  intelligence: 0.08,
  enterprise: 0.00, // Custom billing
};

/** Per-category overage rate — consent is higher value; all others use the tier default. */
export function getCategoryOverageRate(tier: TenantTier, _category: UsageType): number {
  if (_category === 'consent' && tier === 'intelligence') return 0.15;
  return TIER_OVERAGE_RATES[tier];
}

export function parseTenantIncludedEvents(value: unknown, tier: TenantTier): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  return TIER_INCLUDED_EVENTS[tier] ?? null;
}

export function isBillingGraceExpired(desc: Record<string, unknown>, item?: Record<string, unknown> | null): boolean {
  const hasStripeSetup = typeof desc.stripeCustomerId === 'string' && desc.stripeCustomerId.length > 0
    && typeof desc.stripeSubscriptionId === 'string' && desc.stripeSubscriptionId.length > 0;
  if (hasStripeSetup) return false;

  const anchor = typeof desc.tierStartDate === 'string'
    ? desc.tierStartDate
    : typeof item?.createdAt === 'string'
      ? item.createdAt
      : null;
  if (!anchor) return false;

  const deadline = new Date(anchor);
  deadline.setDate(deadline.getDate() + 14);
  return Date.now() > deadline.getTime();
}

export async function getTenantStateForBrand(
  dynamo: DynamoDBDocumentClient,
  refTable: string,
  brandId: string,
): Promise<{ tenantId: string | null; tier: TenantTier; active: boolean; includedEventsPerMonth: number | null }> {
  const brandRes = await dynamo.send(new GetCommand({
    TableName: refTable,
    Key: { pK: `BRAND#${brandId}`, sK: 'profile' },
  }));
  const brandDesc = parseRecord(brandRes.Item?.desc);
  const tenantId = typeof brandDesc.tenantId === 'string' && brandDesc.tenantId.length > 0
    ? brandDesc.tenantId
    : null;
  if (!tenantId) return { tenantId: null, tier: 'base', active: true, includedEventsPerMonth: null };

  const tenantRes = await dynamo.send(new GetCommand({
    TableName: refTable,
    Key: { pK: `TENANT#${tenantId}`, sK: 'PROFILE' },
  }));
  const tenantDesc = parseRecord(tenantRes.Item?.desc);
  const billingStatus = typeof tenantDesc.billingStatus === 'string' ? tenantDesc.billingStatus : 'ACTIVE';
  const status = String(tenantRes.Item?.status ?? 'ACTIVE');
  const tier = normalizeTenantTier(tenantDesc.tier);
  const graceExpired = isBillingGraceExpired(tenantDesc, tenantRes.Item);

  return {
    tenantId,
    tier,
    active: status === 'ACTIVE' && billingStatus !== 'SUSPENDED' && !graceExpired,
    includedEventsPerMonth: parseTenantIncludedEvents(tenantDesc.includedEventsPerMonth, tier),
  };
}

export async function getTenantUsageCounter(
  dynamo: DynamoDBDocumentClient,
  refTable: string,
  tenantId: string,
  type: UsageType,
  month = getUsageMonthKey(),
) {
  const res = await dynamo.send(new GetCommand({
    TableName: refTable,
    Key: { pK: `TENANT#${tenantId}`, sK: `USAGE#${month}#${type}` },
  }));
  const desc = parseRecord(res.Item?.desc);
  return {
    month,
    usageCount: Number(res.Item?.usageCount ?? 0),
    lastUpdatedAt: (desc.lastUpdatedAt as string | undefined) ?? null,
    lastBrandId: (desc.lastBrandId as string | undefined) ?? null,
  };
}

/**
 * Checks whether the tenant has exceeded their monthly quota.
 * - Base tier: hard block when quota exceeded
 * - Engagement / Intelligence: soft limit — events continue, overage invoiced
 *
 * Returns `usageRatio` so callers can determine threshold alerts (80%, 100%).
 */
export async function checkTenantQuota(
  dynamo: DynamoDBDocumentClient,
  refTable: string,
  tenantState: { tenantId: string | null; tier: TenantTier; includedEventsPerMonth: number | null },
  type: UsageType,
): Promise<{ allowed: boolean; message?: string; currentTotal?: number; usageRatio?: number }> {
  if (!tenantState.tenantId) return { allowed: true };

  const included = tenantState.includedEventsPerMonth;
  if (included == null) return { allowed: true };

  const usageRecords = await Promise.all(
    ALL_USAGE_TYPES.map((usageType) => getTenantUsageCounter(dynamo, refTable, tenantState.tenantId!, usageType)),
  );
  const currentTotal = usageRecords.reduce((sum, record) => sum + record.usageCount, 0);
  const usageRatio = included > 0 ? currentTotal / included : 0;

  if (tenantState.tier === 'base' && currentTotal + 1 > included) {
    return {
      allowed: false,
      message: `Base tier monthly quota exceeded for ${type}. Upgrade tenant billing to continue sending.`,
      currentTotal,
      usageRatio,
    };
  }

  return { allowed: true, currentTotal, usageRatio };
}

export async function incrementTenantUsageCounter(
  dynamo: DynamoDBDocumentClient,
  refTable: string,
  tenantId: string | null,
  brandId: string,
  type: UsageType,
): Promise<{ month: string; usageCount: number; lastUpdatedAt: string; lastBrandId: string }> {
  if (!tenantId) {
    return {
      month: getUsageMonthKey(),
      usageCount: 0,
      lastUpdatedAt: new Date().toISOString(),
      lastBrandId: brandId,
    };
  }

  const now = new Date().toISOString();
  const month = getUsageMonthKey();
  const desc = JSON.stringify({
    tenantId,
    type,
    month,
    lastBrandId: brandId,
    lastUpdatedAt: now,
  });

  await dynamo.send(new UpdateCommand({
    TableName: refTable,
    Key: { pK: `TENANT#${tenantId}`, sK: `USAGE#${month}#${type}` },
    UpdateExpression: 'SET eventType = if_not_exists(eventType, :eventType), #status = if_not_exists(#status, :status), primaryCat = if_not_exists(primaryCat, :primaryCat), desc = :desc, createdAt = if_not_exists(createdAt, :now), updatedAt = :now ADD usageCount :inc',
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: {
      ':eventType': 'TENANT_USAGE',
      ':status': 'ACTIVE',
      ':primaryCat': 'tenant_usage',
      ':desc': desc,
      ':now': now,
      ':inc': 1,
    },
  }));

  const usage = await getTenantUsageCounter(dynamo, refTable, tenantId, type, month);
  return {
    month,
    usageCount: usage.usageCount,
    lastUpdatedAt: usage.lastUpdatedAt ?? now,
    lastBrandId: usage.lastBrandId ?? brandId,
  };
}
