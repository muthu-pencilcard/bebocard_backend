import type { SQSHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb';
import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';
import { createHmac } from 'crypto';
import { 
  getTenantStateForBrand, 
  checkTenantQuota, 
  incrementTenantUsageCounter,
  type UsageType 
} from '../../shared/tenant-billing';

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const secrets = new SecretsManagerClient({});
const REFDATA_TABLE = process.env.REFDATA_TABLE!;

// Per-invocation cache — avoids repeated Secrets Manager calls for the same brand
const signingSecretCache: Record<string, string> = {};

export const handler: SQSHandler = async (event) => {
  console.info(`[webhook-dispatcher] Processing batch of ${event.Records.length} webhooks`);

  for (const record of event.Records) {
    try {
      const payload = JSON.parse(record.body);
      const { brandId, type, data, deliveryId } = payload;

      if (!brandId || !type) {
        console.error('[webhook-dispatcher] Invalid payload - missing brandId or type', payload);
        continue;
      }

      // 1. Resolve brand webhook configuration
      const brandRes = await ddb.send(new GetCommand({
        TableName: REFDATA_TABLE,
        Key: { pK: `BRAND#${brandId}`, sK: 'profile' }
      }));

      const brandProfile = JSON.parse(brandRes.Item?.desc ?? '{}');
      const webhookUrl = brandProfile.webhookUrl as string | undefined;

      if (!webhookUrl) {
        console.info(`[webhook-dispatcher] No webhookUrl configured for brand ${brandId}. Skipping.`);
        continue;
      }
 
      // 1.5 Billing/Quota Check (P1-8)
      // Note: Webhook dispatcher is often triggered by backend fan-out (offers, receipts).
      // We check quota here to ensure 'base' tier tenants don't exceed their free limit.
      const tenantState = await getTenantStateForBrand(ddb, REFDATA_TABLE, brandId);
      if (!tenantState.active) {
        console.warn(`[webhook-dispatcher] Skipping delivery for ${brandId}: Tenant suspended/inactive`);
        continue;
      }
 
      // Map webhook types to usage counter types
      const usageType: UsageType = type.toLowerCase().includes('offer') ? 'offers' 
        : type.toLowerCase().includes('newsletter') ? 'newsletters'
        : type.toLowerCase().includes('catalogue') ? 'catalogues'
        : type.toLowerCase().includes('invoice') ? 'invoices'
        : (type.toLowerCase() as UsageType);

      const quota = await checkTenantQuota(ddb, REFDATA_TABLE, tenantState, usageType);
      if (!quota.allowed) {
        console.warn(`[webhook-dispatcher] Quota exceeded for brand ${brandId} (${usageType}). Delivery suppressed.`);
        continue;
      }

      // 2. Build the payload body
      const body = JSON.stringify({
        version: 'v1',
        type,
        timestamp: new Date().toISOString(),
        brandId,
        deliveryId: deliveryId ?? record.messageId,
        data,
      });

      // 3. Compute HMAC-SHA256 signature (P2-12 AC: X-Bebocard-Signature on every delivery)
      const sigHeader = await buildSignatureHeader(brandProfile.webhookSigningSecretArn as string | undefined, body);

      // 4. Deliver with exponential-backoff retry handled by SQS visibility timeout
      console.info(`[webhook-dispatcher] Delivering ${type} to ${webhookUrl} for brand ${brandId}`);

      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-BeboCard-Event': type,
          'X-BeboCard-Delivery-ID': deliveryId ?? record.messageId,
          ...(sigHeader ? { 'X-Bebocard-Signature': sigHeader } : {}),
        },
        body,
        // 10-second hard timeout per the spec
        signal: AbortSignal.timeout(10_000),
      });

      if (!response.ok) {
        throw new Error(`Webhook delivery failed with status ${response.status}: ${await response.text()}`);
      }

      console.info(`[webhook-dispatcher] ${type} delivered successfully to ${brandId}`);
 
      // 5. Increment usage counter on success
      await incrementTenantUsageCounter(ddb, REFDATA_TABLE, tenantState.tenantId, brandId, usageType);

    } catch (err) {
      console.error('[webhook-dispatcher] Delivery error', err);
      // Re-throw so SQS retries the message with backoff (5 attempts via DLQ config)
      throw err;
    }
  }
};

// ── HMAC Signature ────────────────────────────────────────────────────────────
// Format: sha256=<hex-digest>  (mirrors GitHub webhook signature convention)
// Secret stored in Secrets Manager at the ARN recorded on the brand profile.
// Falls back to unsigned delivery if ARN is absent (e.g. brands that predate the signing feature).

async function buildSignatureHeader(secretArn: string | undefined, body: string): Promise<string | null> {
  if (!secretArn) return null;

  try {
    if (!signingSecretCache[secretArn]) {
      const res = await secrets.send(new GetSecretValueCommand({ SecretId: secretArn }));
      signingSecretCache[secretArn] = res.SecretString ?? '';
    }

    const secret = signingSecretCache[secretArn];
    if (!secret) return null;

    const hmac = createHmac('sha256', secret).update(body).digest('hex');
    return `sha256=${hmac}`;
  } catch (err) {
    console.error('[webhook-dispatcher] Failed to fetch signing secret, sending unsigned:', err);
    return null;
  }
}
