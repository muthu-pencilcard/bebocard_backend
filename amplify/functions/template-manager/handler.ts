/**
 * template-manager — BeboCard loyalty card template CRUD
 *
 * Called by the business portal (super_admin only) via direct Lambda invoke.
 * Defence-in-depth: every call must carry HMAC-SHA256 signed payload.
 *
 * DynamoDB key patterns (RefDataEvent table):
 *   pK: TEMPLATE#<templateId>   sK: PROFILE   — template master record
 *   pK: DISCOVERY#TEMPLATES     sK: TEMPLATE#<templateId>  — discovery index (approved only)
 *
 * Routes (mapped from portal API proxy):
 *   POST   /templates                — create template
 *   GET    /templates                — list all templates
 *   GET    /templates/:id            — get single template
 *   PUT    /templates/:id            — update template
 *   DELETE /templates/:id            — soft-delete template (status → ARCHIVED)
 *   POST   /templates/:id/approve    — approve template (status DRAFT → APPROVED)
 *   POST   /templates/:id/withdraw   — withdraw template (status APPROVED → WITHDRAWN)
 */

import type { APIGatewayProxyHandler, APIGatewayProxyEvent } from 'aws-lambda';
import { createHmac, timingSafeEqual, randomBytes } from 'crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  PutCommand,
  GetCommand,
  UpdateCommand,
  QueryCommand,
  DeleteCommand,
  TransactWriteCommand,
} from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const REFDATA_TABLE = process.env.REFDATA_TABLE!;

const CORS_HEADERS = {
  'Content-Type': 'application/json',
  'X-Content-Type-Options': 'nosniff',
  'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
};

const PORTAL_ORIGIN = process.env.PORTAL_ORIGIN ?? 'https://business.bebocard.com.au';

function resolveOrigin(event: APIGatewayProxyEvent): string {
  const origin = event.headers.origin || event.headers.Origin || PORTAL_ORIGIN;
  return origin === PORTAL_ORIGIN ? origin : PORTAL_ORIGIN;
}

function ok(event: APIGatewayProxyEvent, body: unknown) {
  return {
    statusCode: 200,
    headers: { ...CORS_HEADERS, 'Access-Control-Allow-Origin': resolveOrigin(event) },
    body: JSON.stringify(body),
  };
}

function created(event: APIGatewayProxyEvent, body: unknown) {
  return {
    statusCode: 201,
    headers: { ...CORS_HEADERS, 'Access-Control-Allow-Origin': resolveOrigin(event) },
    body: JSON.stringify(body),
  };
}

function err(event: APIGatewayProxyEvent, status: number, message: string) {
  return {
    statusCode: status,
    headers: { ...CORS_HEADERS, 'Access-Control-Allow-Origin': resolveOrigin(event) },
    body: JSON.stringify({ error: message }),
  };
}

// ─── Internal auth (HMAC-signed portal calls) ─────────────────────────────────

const INTERNAL_SIGNING_SECRET = process.env.INTERNAL_SIGNING_SECRET ?? '';
const INTERNAL_TIMESTAMP_WINDOW_MS = 5 * 60 * 1000;

function verifyInternalSignature(actorEmail: string, timestamp: string, sig: string): boolean {
  if (!INTERNAL_SIGNING_SECRET) return false;
  const now = Date.now();
  const ts = parseInt(timestamp, 10);
  if (isNaN(ts) || Math.abs(now - ts) > INTERNAL_TIMESTAMP_WINDOW_MS) return false;
  const expected = createHmac('sha256', INTERNAL_SIGNING_SECRET)
    .update(`${actorEmail}:${timestamp}`)
    .digest('hex');
  const sigBuf = Buffer.from(sig);
  const expBuf = Buffer.from(expected);
  if (sigBuf.length !== expBuf.length) return false;
  return timingSafeEqual(sigBuf, expBuf);
}

function requireInternalAuth(event: APIGatewayProxyEvent): string | null {
  const eventAny = event as unknown as Record<string, string>;
  const actorEmail = eventAny._internalActorEmail ?? '';
  const timestamp = eventAny._internalTimestamp ?? '';
  const sig = eventAny._internalSig ?? '';
  if (!actorEmail || !verifyInternalSignature(actorEmail, timestamp, sig)) return null;
  return actorEmail;
}

// ─── ID generation ────────────────────────────────────────────────────────────

function generateTemplateId(): string {
  return randomBytes(8).toString('hex'); // 16-char hex, URL-safe
}

// ─── Types ────────────────────────────────────────────────────────────────────

type TemplateStatus = 'DRAFT' | 'APPROVED' | 'WITHDRAWN' | 'ARCHIVED';

interface LoyaltyCardTemplate {
  templateId: string;
  name: string;
  description: string;
  barcodeFormat: string;        // e.g. 'QR_CODE', 'CODE_128', 'EAN_13'
  primaryColor: string;         // hex
  accentColor: string;          // hex
  logoUrl?: string;
  backgroundImageUrl?: string;
  fieldLabels: {                 // display labels shown on the card face
    loyaltyIdLabel?: string;     // e.g. "Member ID"
    pointsLabel?: string;        // e.g. "Points"
    tierLabel?: string;          // e.g. "Tier"
  };
  requiredScopes: string[];      // API key scopes a brand must have to use this template
  status: TemplateStatus;
  createdAt: string;
  updatedAt: string;
  createdBy: string;
  approvedAt?: string;
  approvedBy?: string;
  withdrawnAt?: string;
  tenantCount?: number;          // read-only, populated from card-config query
}

// ─── Handlers ────────────────────────────────────────────────────────────────

async function createTemplate(
  event: APIGatewayProxyEvent,
  actorEmail: string,
): Promise<ReturnType<typeof ok>> {
  const body = JSON.parse(event.body ?? '{}') as Partial<LoyaltyCardTemplate>;

  const name = (body.name ?? '').trim();
  const description = (body.description ?? '').trim();
  const barcodeFormat = (body.barcodeFormat ?? 'QR_CODE').trim().toUpperCase();
  const primaryColor = (body.primaryColor ?? '#1A1A2E').trim();
  const accentColor = (body.accentColor ?? '#16213E').trim();
  const fieldLabels = body.fieldLabels ?? {};
  const requiredScopes: string[] = Array.isArray(body.requiredScopes) ? body.requiredScopes : [];

  if (!name) return err(event, 400, 'name is required');
  if (name.length > 100) return err(event, 400, 'name must be ≤ 100 characters');
  if (!/^#[0-9a-fA-F]{6}$/.test(primaryColor)) return err(event, 400, 'primaryColor must be a valid hex colour (#rrggbb)');
  if (!/^#[0-9a-fA-F]{6}$/.test(accentColor)) return err(event, 400, 'accentColor must be a valid hex colour (#rrggbb)');

  const validFormats = ['QR_CODE', 'CODE_128', 'EAN_13', 'CODE_39', 'PDF_417', 'DATA_MATRIX'];
  if (!validFormats.includes(barcodeFormat)) {
    return err(event, 400, `barcodeFormat must be one of: ${validFormats.join(', ')}`);
  }

  const templateId = generateTemplateId();
  const now = new Date().toISOString();

  const template: LoyaltyCardTemplate = {
    templateId,
    name,
    description,
    barcodeFormat,
    primaryColor,
    accentColor,
    ...(body.logoUrl ? { logoUrl: body.logoUrl } : {}),
    ...(body.backgroundImageUrl ? { backgroundImageUrl: body.backgroundImageUrl } : {}),
    fieldLabels,
    requiredScopes,
    status: 'DRAFT',
    createdAt: now,
    updatedAt: now,
    createdBy: actorEmail,
  };

  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK: `TEMPLATE#${templateId}`,
      sK: 'PROFILE',
      ...template,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  console.info('[template-manager] created template', { templateId, name, actor: actorEmail });
  return created(event, template);
}

async function listTemplates(event: APIGatewayProxyEvent): Promise<ReturnType<typeof ok>> {
  // Scan all TEMPLATE# records. At BeboCard scale (tens, not thousands),
  // a Scan over these is fine. A GSI can be added if needed later.
  const result = await dynamo.send(new QueryCommand({
    TableName: REFDATA_TABLE,
    IndexName: 'sK-pK-index', // RefDataEvent has sK as a GSI hash key
    KeyConditionExpression: 'sK = :sk AND begins_with(pK, :prefix)',
    ExpressionAttributeValues: {
      ':sk': 'PROFILE',
      ':prefix': 'TEMPLATE#',
    },
  }));

  const templates = (result.Items ?? []).map(item => {
    const { pK, sK, ...rest } = item;
    return rest as LoyaltyCardTemplate;
  });

  // Sort by createdAt descending
  templates.sort((a, b) => b.createdAt.localeCompare(a.createdAt));

  return ok(event, { templates, count: templates.length });
}

async function getTemplate(
  event: APIGatewayProxyEvent,
  templateId: string,
): Promise<ReturnType<typeof ok>> {
  const result = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TEMPLATE#${templateId}`, sK: 'PROFILE' },
  }));

  if (!result.Item) return err(event, 404, 'Template not found');

  const { pK, sK, ...template } = result.Item;
  return ok(event, template);
}

async function updateTemplate(
  event: APIGatewayProxyEvent,
  templateId: string,
  actorEmail: string,
): Promise<ReturnType<typeof ok>> {
  // Fetch existing
  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TEMPLATE#${templateId}`, sK: 'PROFILE' },
  }));

  if (!existing.Item) return err(event, 404, 'Template not found');

  const current = existing.Item as LoyaltyCardTemplate & { pK: string; sK: string };

  if (current.status === 'ARCHIVED') {
    return err(event, 409, 'Archived templates cannot be modified');
  }

  const body = JSON.parse(event.body ?? '{}') as Partial<LoyaltyCardTemplate>;
  const now = new Date().toISOString();

  const updatable: Partial<LoyaltyCardTemplate> = {};

  if (body.name !== undefined) {
    const name = body.name.trim();
    if (!name || name.length > 100) return err(event, 400, 'name must be 1–100 characters');
    updatable.name = name;
  }
  if (body.description !== undefined) updatable.description = body.description.trim();
  if (body.barcodeFormat !== undefined) {
    const validFormats = ['QR_CODE', 'CODE_128', 'EAN_13', 'CODE_39', 'PDF_417', 'DATA_MATRIX'];
    const fmt = body.barcodeFormat.trim().toUpperCase();
    if (!validFormats.includes(fmt)) return err(event, 400, `barcodeFormat must be one of: ${validFormats.join(', ')}`);
    updatable.barcodeFormat = fmt;
  }
  if (body.primaryColor !== undefined) {
    if (!/^#[0-9a-fA-F]{6}$/.test(body.primaryColor)) return err(event, 400, 'primaryColor must be a valid hex colour');
    updatable.primaryColor = body.primaryColor;
  }
  if (body.accentColor !== undefined) {
    if (!/^#[0-9a-fA-F]{6}$/.test(body.accentColor)) return err(event, 400, 'accentColor must be a valid hex colour');
    updatable.accentColor = body.accentColor;
  }
  if (body.logoUrl !== undefined) updatable.logoUrl = body.logoUrl;
  if (body.backgroundImageUrl !== undefined) updatable.backgroundImageUrl = body.backgroundImageUrl;
  if (body.fieldLabels !== undefined) updatable.fieldLabels = body.fieldLabels;
  if (body.requiredScopes !== undefined) {
    if (!Array.isArray(body.requiredScopes)) return err(event, 400, 'requiredScopes must be an array');
    updatable.requiredScopes = body.requiredScopes;
  }

  if (Object.keys(updatable).length === 0) return err(event, 400, 'No updatable fields provided');

  const updated = { ...current, ...updatable, updatedAt: now };
  delete (updated as Record<string, unknown>).pK;
  delete (updated as Record<string, unknown>).sK;

  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: { ...(updated as Record<string, unknown>), pK: `TEMPLATE#${templateId}`, sK: 'PROFILE' },
    ConditionExpression: 'attribute_exists(pK)',
  }));

  // If APPROVED, keep discovery index in sync
  if (current.status === 'APPROVED') {
    await dynamo.send(new UpdateCommand({
      TableName: REFDATA_TABLE,
      Key: { pK: 'DISCOVERY#TEMPLATES', sK: `TEMPLATE#${templateId}` },
      UpdateExpression: 'SET #name = :name, updatedAt = :now',
      ExpressionAttributeNames: { '#name': 'name' },
      ExpressionAttributeValues: {
        ':name': updated.name ?? current.name,
        ':now': now,
      },
      ConditionExpression: 'attribute_exists(pK)',
    })).catch(() => {
      // Discovery index entry may not exist yet — non-fatal
    });
  }

  console.info('[template-manager] updated template', { templateId, actor: actorEmail });
  return ok(event, updated);
}

async function approveTemplate(
  event: APIGatewayProxyEvent,
  templateId: string,
  actorEmail: string,
): Promise<ReturnType<typeof ok>> {
  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TEMPLATE#${templateId}`, sK: 'PROFILE' },
  }));

  if (!existing.Item) return err(event, 404, 'Template not found');

  const current = existing.Item as LoyaltyCardTemplate & { pK: string; sK: string };

  if (current.status === 'APPROVED') return err(event, 409, 'Template is already approved');
  if (current.status === 'ARCHIVED') return err(event, 409, 'Archived templates cannot be approved');

  const now = new Date().toISOString();

  // Transactionally: update template status + write discovery index entry
  await dynamo.send(new TransactWriteCommand({
    TransactItems: [
      {
        Update: {
          TableName: REFDATA_TABLE,
          Key: { pK: `TEMPLATE#${templateId}`, sK: 'PROFILE' },
          UpdateExpression: 'SET #status = :status, approvedAt = :now, approvedBy = :actor, updatedAt = :now',
          ExpressionAttributeNames: { '#status': 'status' },
          ExpressionAttributeValues: {
            ':status': 'APPROVED',
            ':now': now,
            ':actor': actorEmail,
          },
          ConditionExpression: 'attribute_exists(pK)',
        },
      },
      {
        Put: {
          TableName: REFDATA_TABLE,
          Item: {
            pK: 'DISCOVERY#TEMPLATES',
            sK: `TEMPLATE#${templateId}`,
            templateId,
            name: current.name,
            description: current.description,
            barcodeFormat: current.barcodeFormat,
            primaryColor: current.primaryColor,
            accentColor: current.accentColor,
            ...(current.logoUrl ? { logoUrl: current.logoUrl } : {}),
            requiredScopes: current.requiredScopes,
            status: 'APPROVED',
            approvedAt: now,
            updatedAt: now,
          },
        },
      },
    ],
  }));

  console.info('[template-manager] approved template', { templateId, actor: actorEmail });
  return ok(event, { templateId, status: 'APPROVED', approvedAt: now });
}

async function withdrawTemplate(
  event: APIGatewayProxyEvent,
  templateId: string,
  actorEmail: string,
): Promise<ReturnType<typeof ok>> {
  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TEMPLATE#${templateId}`, sK: 'PROFILE' },
  }));

  if (!existing.Item) return err(event, 404, 'Template not found');

  const current = existing.Item as LoyaltyCardTemplate & { pK: string; sK: string };

  if (current.status !== 'APPROVED') return err(event, 409, 'Only APPROVED templates can be withdrawn');

  const now = new Date().toISOString();

  await dynamo.send(new TransactWriteCommand({
    TransactItems: [
      {
        Update: {
          TableName: REFDATA_TABLE,
          Key: { pK: `TEMPLATE#${templateId}`, sK: 'PROFILE' },
          UpdateExpression: 'SET #status = :status, withdrawnAt = :now, updatedAt = :now',
          ExpressionAttributeNames: { '#status': 'status' },
          ExpressionAttributeValues: {
            ':status': 'WITHDRAWN',
            ':now': now,
          },
          ConditionExpression: 'attribute_exists(pK)',
        },
      },
      {
        Delete: {
          TableName: REFDATA_TABLE,
          Key: { pK: 'DISCOVERY#TEMPLATES', sK: `TEMPLATE#${templateId}` },
        },
      },
    ],
  }));

  console.info('[template-manager] withdrew template', { templateId, actor: actorEmail });
  return ok(event, { templateId, status: 'WITHDRAWN', withdrawnAt: now });
}

async function deleteTemplate(
  event: APIGatewayProxyEvent,
  templateId: string,
  actorEmail: string,
): Promise<ReturnType<typeof ok>> {
  const existing = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TEMPLATE#${templateId}`, sK: 'PROFILE' },
  }));

  if (!existing.Item) return err(event, 404, 'Template not found');

  const current = existing.Item as LoyaltyCardTemplate & { pK: string; sK: string };
  const now = new Date().toISOString();

  // Soft-delete: set status to ARCHIVED and remove from discovery index
  const transactItems: NonNullable<ConstructorParameters<typeof TransactWriteCommand>[0]['TransactItems']> = [
    {
      Update: {
        TableName: REFDATA_TABLE,
        Key: { pK: `TEMPLATE#${templateId}`, sK: 'PROFILE' },
        UpdateExpression: 'SET #status = :status, updatedAt = :now',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':status': 'ARCHIVED', ':now': now },
        ConditionExpression: 'attribute_exists(pK)',
      },
    },
  ];

  // Only delete from discovery index if it was APPROVED
  if (current.status === 'APPROVED') {
    transactItems.push({
      Delete: {
        TableName: REFDATA_TABLE,
        Key: { pK: 'DISCOVERY#TEMPLATES', sK: `TEMPLATE#${templateId}` },
      },
    });
  }

  await dynamo.send(new TransactWriteCommand({ TransactItems: transactItems }));

  console.info('[template-manager] archived template', { templateId, actor: actorEmail });
  return ok(event, { templateId, status: 'ARCHIVED', archivedAt: now });
}

// ─── Router ───────────────────────────────────────────────────────────────────

export const handler: APIGatewayProxyHandler = async (event) => {
  const method = event.httpMethod;
  const path = event.path ?? '';

  // CORS preflight
  if (method === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        ...CORS_HEADERS,
        'Access-Control-Allow-Origin': resolveOrigin(event),
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Correlation-Id',
      },
      body: '',
    };
  }

  // All routes require internal auth (super_admin portal only)
  const actorEmail = requireInternalAuth(event);
  if (!actorEmail) {
    return err(event, 401, 'Unauthorized');
  }

  // POST /templates
  if (method === 'POST' && path === '/templates') {
    return createTemplate(event, actorEmail);
  }

  // GET /templates
  if (method === 'GET' && path === '/templates') {
    return listTemplates(event);
  }

  // GET /templates/:id
  const singleMatch = path.match(/^\/templates\/([^/]+)$/);
  if (method === 'GET' && singleMatch) {
    return getTemplate(event, singleMatch[1]);
  }

  // PUT /templates/:id
  if (method === 'PUT' && singleMatch) {
    return updateTemplate(event, singleMatch[1], actorEmail);
  }

  // DELETE /templates/:id
  if (method === 'DELETE' && singleMatch) {
    return deleteTemplate(event, singleMatch[1], actorEmail);
  }

  // POST /templates/:id/approve
  const approveMatch = path.match(/^\/templates\/([^/]+)\/approve$/);
  if (method === 'POST' && approveMatch) {
    return approveTemplate(event, approveMatch[1], actorEmail);
  }

  // POST /templates/:id/withdraw
  const withdrawMatch = path.match(/^\/templates\/([^/]+)\/withdraw$/);
  if (method === 'POST' && withdrawMatch) {
    return withdrawTemplate(event, withdrawMatch[1], actorEmail);
  }

  return err(event, 404, 'Not found');
};
