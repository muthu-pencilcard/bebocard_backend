import { GetCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

export const ALL_USAGE_TYPES = [
  'offers',
  'newsletters',
  'catalogues',
  'invoices',
  'geolocation',
  'payments',
] as const;

export type UsageType = typeof ALL_USAGE_TYPES[number];
export type TenantTier = 'base' | 'engagement' | 'intelligence';

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
  if (value === 'intelligence' || value === 'enterprise') return 'intelligence';
  return 'base';
}

export function parseTenantIncludedEvents(value: unknown, tier: TenantTier): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (tier === 'base') return 250;
  if (tier === 'engagement') return 2500;
  return null;
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
