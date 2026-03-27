import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';

const ulid = monotonicFactory();

export interface AuditEntry {
  actor: string;                           // permULID (user) | brandId (brand) | 'system'
  actorType: 'user' | 'brand' | 'system';
  action: string;                          // e.g. 'addLoyaltyCard', 'POST /scan'
  resource: string;                        // e.g. 'CARD#woolworths#1234'
  outcome: 'success' | 'failure';
  errorCode?: string;
  ip?: string;
  durationMs?: number;
  metadata?: Record<string, unknown>;
}

/**
 * Writes a structured audit log entry to:
 *  1. CloudWatch via console.log (queryable with Logs Insights)
 *  2. AdminDataEvent table as AUDIT#<actor> / LOG#<ts>#<ulid>
 *
 * Always fire-and-forget from handlers — never await this in the hot path.
 */
export async function writeAuditLog(
  ddb: DynamoDBDocumentClient,
  entry: AuditEntry,
): Promise<void> {
  const now = new Date().toISOString();
  const logId = ulid();

  // Structured CloudWatch log — queryable via Logs Insights:
  // fields @timestamp, audit.action, audit.actor, audit.outcome
  // | filter audit.outcome = "failure"
  console.log(JSON.stringify({ audit: true, logId, timestamp: now, ...entry }));

  const adminTable = process.env.ADMIN_TABLE;
  if (!adminTable) return; // Graceful no-op in unit tests without env

  await ddb.send(new PutCommand({
    TableName: adminTable,
    Item: {
      pK: `AUDIT#${entry.actor}`,
      sK: `LOG#${now}#${logId}`,
      eventType: 'AUDIT_LOG',
      status: entry.outcome,
      primaryCat: 'audit',
      desc: JSON.stringify(entry),
      createdAt: now,
      updatedAt: now,
    },
  }));
}

// ─── Middleware ────────────────────────────────────────────────────────────────

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyHandler = (...args: any[]) => Promise<any> | void;

interface AuditContext {
  permULID?: string;
  brandId?: string;
  ip?: string;
}

/**
 * Wraps any async Lambda handler with automatic audit logging.
 *
 * Usage in card-manager:
 *   export const handler = withAuditLog(ddbClient, rawHandler);
 *
 * Usage in scan-handler:
 *   const result = await withAuditLog(ddb, handleScan)(event);
 */
export function withAuditLog<T extends AnyHandler>(
  ddb: DynamoDBDocumentClient,
  fn: T,
): T {
  return (async (event: any, ...rest: any[]) => {
    const start = Date.now();
    const ctx = extractContext(event);
    const action = extractAction(event);
    const resource = extractResource(event, action);

    try {
      const result = await fn(event, ...rest);
      // Fire-and-forget — don't block response
      writeAuditLog(ddb, {
        actor: ctx.permULID ?? ctx.brandId ?? 'anonymous',
        actorType: ctx.permULID ? 'user' : ctx.brandId ? 'brand' : 'system',
        action,
        resource,
        outcome: 'success',
        ip: ctx.ip,
        durationMs: Date.now() - start,
      }).catch((e) => console.error('[audit-logger] write failed', e));
      return result;
    } catch (err: any) {
      writeAuditLog(ddb, {
        actor: ctx.permULID ?? ctx.brandId ?? 'anonymous',
        actorType: ctx.permULID ? 'user' : ctx.brandId ? 'brand' : 'system',
        action,
        resource,
        outcome: 'failure',
        errorCode: err?.message ?? String(err),
        ip: ctx.ip,
        durationMs: Date.now() - start,
      }).catch((e) => console.error('[audit-logger] write failed', e));
      throw err;
    }
  }) as T;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function extractContext(event: any): AuditContext {
  // AppSync resolver event
  const claims = event?.identity?.claims as Record<string, string> | undefined;
  if (claims?.['custom:permULID']) {
    return { permULID: claims['custom:permULID'] };
  }
  // API Gateway proxy event (brand API)
  const headers = event?.headers as Record<string, string> | undefined;
  const ip = event?.requestContext?.identity?.sourceIp;
  // Prefer explicit x-brand-id header. Fall back to request body fields so that
  // scan-handler audit entries (which authenticate via API key, not header) are
  // attributed to the correct brand rather than written as actor='anonymous'.
  const body = (() => { try { return JSON.parse(event?.body ?? '{}'); } catch { return {} as Record<string, string>; } })();
  const brandId = headers?.['x-brand-id'] ?? body.storeBrandLoyaltyName ?? body.brandId;
  return { brandId, ip };
}

function extractAction(event: any): string {
  // AppSync: use fieldName
  if (event?.info?.fieldName) return event.info.fieldName;
  // API Gateway: use METHOD /path
  if (event?.httpMethod && event?.path) return `${event.httpMethod} ${event.path}`;
  return 'unknown';
}

function extractResource(event: any, action: string): string {
  const args = event?.arguments ?? {};
  const body = (() => { try { return JSON.parse(event?.body ?? '{}'); } catch { return {}; } })();
  const src = { ...args, ...body };
  return (
    src.cardSK ??
    src.invoiceSK ??
    src.brandId ??
    src.secondaryULID ??
    src.cardNumber ??
    action
  );
}
