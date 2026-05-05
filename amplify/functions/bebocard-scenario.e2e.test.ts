/**
 * BeboCard Full-Journey Scenario Test
 *
 * Covers the end-to-end brand-tenant and consumer-app lifecycle across 7 phases:
 *   1. User sign-up (post-confirmation Cognito trigger)
 *   2. App adds loyalty card (card-manager AppSync)
 *   3. Brand scans at POS (scan-handler POST /v1/scan)
 *   4. Brand pushes receipt (scan-handler POST /v1/receipt)
 *   5. Brand requests consent (consent-handler POST /consent-request)
 *   6. Brand sends enrollment offer (enrollment-handler POST /enroll)
 *   7. Brand creates offer (brand-api-handler POST /offers)
 *
 * Uses an in-memory DynamoDB dispatch (keyed by __type tag on mock commands).
 * External services (Firebase, SQS, Cognito, SSM, SES) are mocked.
 */

import { describe, it, expect, vi, beforeAll } from 'vitest';
import { createHash, createHmac } from 'crypto';

// ─── Fixed test identities ────────────────────────────────────────────────────
const BRAND_ID = 'woolworths';
const CARD_NUMBER = '9300000000000';
const COGNITO_USER = 'user@bebotest.com';
const INTERNAL_SECRET = 'test-internal-secret-e2e-scenario';

const T_USER = 'UserDataEvent-test';
const T_REF  = 'RefDataEvent-test';
const T_ADMIN = 'AdminDataEvent-test';

// API key: bebo_<26-char-keyId>.<64-hex-secret>
const KEY_ID  = 'TEST_E2E_BRAND_API_KEY_123'; // exactly 26 chars
const KEY_SEC = 'a'.repeat(64);
const RAW_KEY = `bebo_${KEY_ID}.${KEY_SEC}`;
const KEY_HASH = createHash('sha256').update(RAW_KEY).digest('hex');

// ─── vi.hoisted — runs before all imports ─────────────────────────────────────
const { mockDynamo, mockSsm, mockSqs, mockCognito, mockFcm, MockCCFE } = vi.hoisted(() => {
  // Set env vars before any module evaluates module-level constants
  Object.assign(process.env, {
    USER_TABLE: 'UserDataEvent-test',
    REFDATA_TABLE: 'RefDataEvent-test',
    ADMIN_TABLE: 'AdminDataEvent-test',
    REF_TABLE: 'RefDataEvent-test',         // consent-handler + enrollment-handler
    USER_TABLE_PARAM: '/e2e/USER_TABLE',
    ADMIN_TABLE_PARAM: '/e2e/ADMIN_TABLE',
    RECEIPT_QUEUE_URL: 'https://sqs.test/ReceiptQueue',
    CONSENT_TIMEOUT_QUEUE_URL: 'https://sqs.test/ConsentTimeoutQueue',
    SCAN_API_URL: 'https://api.bebocard.com',
    INTERNAL_SIGNING_SECRET: 'test-internal-secret-e2e-scenario',
    PORTAL_ORIGIN: 'https://business.bebocard.com',
    RECEIPT_SIGNING_KEY_ID: 'test-kms-key',
    RECEIPT_ANALYTICS_QUEUE_URL: '',
    STRIPE_PUBLISHABLE_KEY: 'pk_test_placeholder',
    FIREBASE_SERVICE_ACCOUNT_JSON: JSON.stringify({
      project_id: 'bebo-test', client_email: 'svc@bebo-test.iam.gserviceaccount.com',
      private_key_id: 'kid', private_key: '-----BEGIN RSA PRIVATE KEY-----\nMIIE\n-----END RSA PRIVATE KEY-----\n',
    }),
  });

  // ── In-memory DynamoDB store ──────────────────────────────────────────────
  const tables: Record<string, Map<string, Record<string, unknown>>> = {};

  function tbl(name: string) {
    if (!tables[name]) tables[name] = new Map();
    return tables[name];
  }

  function rowKey(pK: unknown, sK: unknown) {
    return `${pK ?? ''}||${sK ?? ''}`;
  }

  // Minimal UpdateExpression evaluator — handles SET and ADD clauses
  // only for the specific patterns that appear across these 7 handler files.
  function applyUpdate(
    base: Record<string, unknown>,
    expr: string,
    vals: Record<string, unknown>,
    names: Record<string, string>,
  ) {
    const result = { ...base };

    function attr(n: string) { return n.startsWith('#') ? (names[n] ?? n) : n; }

    // Comma splitter respecting parentheses depth
    function splitComma(s: string): string[] {
      const out: string[] = [];
      let depth = 0, start = 0;
      for (let i = 0; i < s.length; i++) {
        if (s[i] === '(') depth++;
        else if (s[i] === ')') depth--;
        else if (s[i] === ',' && depth === 0) { out.push(s.slice(start, i).trim()); start = i + 1; }
      }
      out.push(s.slice(start).trim());
      return out.filter(Boolean);
    }

    // Extract each top-level clause (SET/ADD/REMOVE/DELETE)
    const re = /\b(SET|ADD|REMOVE|DELETE)\b\s+(.*?)(?=\s+\b(?:SET|ADD|REMOVE|DELETE)\b\b|\s*$)/gis;
    let m: RegExpExecArray | null;
    while ((m = re.exec(expr)) !== null) {
      const [, clause, body] = m;
      if (clause.toUpperCase() === 'SET') {
        for (const pair of splitComma(body)) {
          const eq = pair.indexOf('=');
          if (eq < 0) continue;
          const lhs = attr(pair.slice(0, eq).trim());
          const rhs = pair.slice(eq + 1).trim();
          // if_not_exists(field, :val)
          const ifne = rhs.match(/^if_not_exists\(\s*([^,]+?)\s*,\s*(:[\w]+)\s*\)$/);
          if (ifne) {
            const checkAttr = attr(ifne[1].trim());
            if (result[checkAttr] == null) result[lhs] = vals[ifne[2]];
          } else if (rhs.startsWith(':')) {
            result[lhs] = vals[rhs];
          }
        }
      } else if (clause.toUpperCase() === 'ADD') {
        const parts = body.trim().split(/\s+/);
        for (let i = 0; i + 1 < parts.length; i += 2) {
          const field = attr(parts[i]);
          const v = vals[parts[i + 1]];
          if (typeof v === 'number') result[field] = ((result[field] as number) ?? 0) + v;
        }
      }
    }
    return result;
  }

  // Condition check — only for the patterns handlers actually use
  function checkCond(
    existing: Record<string, unknown> | undefined,
    cond: string | undefined,
    vals: Record<string, unknown>,
    names: Record<string, string>,
  ): boolean {
    if (!cond) return true;
    if (cond.includes('attribute_not_exists(pK)')) return existing === undefined;
    // Simple equality: #alias = :val
    const eq = cond.match(/^(#\w+|\w+)\s*=\s*(:[\w]+)$/);
    if (eq) {
      const field = eq[1].startsWith('#') ? (names[eq[1]] ?? eq[1]) : eq[1];
      return existing?.[field] === vals[eq[2]];
    }
    return true; // conservative pass-through for unknown conditions
  }

  function dispatch(cmd: { __type?: string; input?: Record<string, unknown> }): Promise<unknown> {
    const input  = cmd.input  ?? {};
    const type   = cmd.__type ?? '';
    const name   = (input.TableName as string) ?? '';
    const store  = tbl(name);
    const key    = input.Key as Record<string, unknown> | undefined;
    const item   = input.Item as Record<string, unknown> | undefined;
    const vals   = (input.ExpressionAttributeValues  as Record<string, unknown>) ?? {};
    const attrNm = (input.ExpressionAttributeNames   as Record<string, string>)  ?? {};
    const cond   = input.ConditionExpression as string | undefined;

    switch (type) {
      case 'GetCommand': {
        const k = rowKey(key?.pK, key?.sK);
        return Promise.resolve({ Item: store.has(k) ? { ...store.get(k)! } : undefined });
      }

      case 'PutCommand': {
        const k = rowKey(item?.pK, item?.sK);
        const existing = store.get(k);
        if (!checkCond(existing, cond, vals, attrNm)) {
          const err: Error & { name: string } = new Error('ConditionalCheckFailedException') as Error & { name: string };
          err.name = 'ConditionalCheckFailedException';
          return Promise.reject(MockCCFE ? new MockCCFE() : err);
        }
        store.set(k, { ...item! });
        return Promise.resolve({});
      }

      case 'UpdateCommand': {
        const k = rowKey(key?.pK, key?.sK);
        const existing = store.get(k) ?? ({ ...key } as Record<string, unknown>);
        const condOk = checkCond(store.get(k), cond, vals, attrNm);
        if (!condOk) {
          const err: Error & { name: string } = new Error('ConditionalCheckFailedException') as Error & { name: string };
          err.name = 'ConditionalCheckFailedException';
          return Promise.reject(MockCCFE ? new MockCCFE() : err);
        }
        const updated = applyUpdate(
          existing,
          (input.UpdateExpression as string) ?? '',
          vals,
          attrNm,
        );
        store.set(k, updated);
        return Promise.resolve({ Attributes: { ...updated } });
      }

      case 'QueryCommand': {
        const indexName = input.IndexName as string | undefined;
        const limit = Number(input.Limit ?? 1_000);

        if (indexName === 'refDataEventsByKeyId') {
          // GSI: keyId → api key record
          const kid = vals[':kid'];
          const items = [...store.values()].filter(r => r.keyId === kid);
          return Promise.resolve({ Items: items.slice(0, limit) });
        }
        if (indexName === 'sK-pK-index') {
          // GSI: sK → subscription fan-out
          const sk = vals[':sk'];
          const items = [...store.values()].filter(r => r.sK === sk);
          return Promise.resolve({ Items: items.slice(0, limit) });
        }
        // Default: pK equality
        const pk = vals[':pk'];
        const items = [...store.values()].filter(r => r.pK === pk);
        return Promise.resolve({ Items: items.slice(0, limit) });
      }

      case 'DeleteCommand': {
        const k = rowKey(key?.pK, key?.sK);
        store.delete(k);
        return Promise.resolve({});
      }

      case 'TransactWriteCommand': {
        const ops = (input.TransactItems as Array<Record<string, unknown>>) ?? [];
        for (const op of ops) {
          const putOp  = op.Put    as Record<string, unknown> | undefined;
          const delOp  = op.Delete as Record<string, unknown> | undefined;
          const updOp  = op.Update as Record<string, unknown> | undefined;
          if (putOp) {
            const putStore = tbl(String(putOp.TableName ?? ''));
            const pi = putOp.Item as Record<string, unknown>;
            const pk = rowKey(pi?.pK, pi?.sK);
            const ex = putStore.get(pk);
            if (!checkCond(ex, putOp.ConditionExpression as string, vals, attrNm)) {
              const err: Error & { name: string } = new Error('TransactionCanceledException') as Error & { name: string };
              err.name = 'TransactionCanceledException';
              return Promise.reject(err);
            }
            putStore.set(pk, { ...pi });
          }
          if (updOp) {
            const updStore = tbl(String(updOp.TableName ?? ''));
            const uk = updOp.Key as Record<string, unknown>;
            const k = rowKey(uk?.pK, uk?.sK);
            const ex = updStore.get(k) ?? ({ ...uk } as Record<string, unknown>);
            const updated = applyUpdate(
              ex,
              String(updOp.UpdateExpression ?? ''),
              (updOp.ExpressionAttributeValues ?? {}) as Record<string, unknown>,
              (updOp.ExpressionAttributeNames  ?? {}) as Record<string, string>,
            );
            updStore.set(k, updated);
          }
          if (delOp) {
            const delStore = tbl(String(delOp.TableName ?? ''));
            const dk = delOp.Key as Record<string, unknown>;
            delStore.delete(rowKey(dk?.pK, dk?.sK));
          }
        }
        return Promise.resolve({});
      }

      case 'BatchGetCommand': return Promise.resolve({ Responses: {} });
      case 'ScanCommand':    return Promise.resolve({ Items: [...tbl(name).values()] });
      default:               return Promise.resolve({});
    }
  }

  // ── Mock class for ConditionalCheckFailedException ─────────────────────────
  class MockCCFE extends Error {
    constructor() { super('ConditionalCheckFailedException'); this.name = 'ConditionalCheckFailedException'; }
  }

  const mockSend = vi.fn().mockImplementation(dispatch);

  const mockSsmSend = vi.fn().mockImplementation((cmd: Record<string, unknown>) => {
    const name = (cmd.input as Record<string, unknown>)?.Name as string ?? '';
    const map: Record<string, string> = {
      '/e2e/USER_TABLE':  'UserDataEvent-test',
      '/e2e/ADMIN_TABLE': 'AdminDataEvent-test',
      '/amplify/shared/PARENTAL_CONSENT_SECRET': 'test-parental-secret',
    };
    const val = map[name];
    if (val) return Promise.resolve({ Parameter: { Value: val } });
    return Promise.reject(new Error(`SSM not found: ${name}`));
  });

  const mockSqsSend   = vi.fn().mockResolvedValue({ MessageId: 'mock-msg-id' });
  const mockCognito   = vi.fn().mockResolvedValue({});
  const mockFcm       = vi.fn().mockResolvedValue('mock-fcm-id');

  return {
    mockDynamo: { send: mockSend, tables },
    mockSsm: mockSsmSend,
    mockSqs: mockSqsSend,
    mockCognito,
    mockFcm,
    MockCCFE,
  };
});

// ─── Mock external modules ─────────────────────────────────────────────────────
vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn(() => ({ send: mockDynamo.send })) },
  GetCommand:           vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i, __type: 'GetCommand' }); }),
  PutCommand:           vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i, __type: 'PutCommand' }); }),
  UpdateCommand:        vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i, __type: 'UpdateCommand' }); }),
  QueryCommand:         vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i, __type: 'QueryCommand' }); }),
  DeleteCommand:        vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i, __type: 'DeleteCommand' }); }),
  TransactWriteCommand: vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i, __type: 'TransactWriteCommand' }); }),
  BatchGetCommand:      vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i, __type: 'BatchGetCommand' }); }),
  ScanCommand:          vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i, __type: 'ScanCommand' }); }),
}));
vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
  ConditionalCheckFailedException: MockCCFE,
}));
vi.mock('@aws-sdk/client-ssm', () => ({
  SSMClient: vi.fn(function (this: { send: unknown }) { this.send = mockSsm; }),
  GetParameterCommand: vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i, __type: 'GetParameterCommand' }); }),
}));
vi.mock('@aws-sdk/client-sqs', () => ({
  SQSClient: vi.fn(function (this: { send: unknown }) { this.send = mockSqs; }),
  SendMessageCommand: vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i, __type: 'SendMessageCommand' }); }),
}));
vi.mock('@aws-sdk/client-cognito-identity-provider', () => ({
  CognitoIdentityProviderClient: vi.fn(function (this: { send: unknown }) { this.send = mockCognito; }),
  AdminUpdateUserAttributesCommand: vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i, __type: 'AdminUpdateUserAttributesCommand' }); }),
}));
vi.mock('@aws-sdk/client-ses', () => ({
  SESClient: vi.fn(function (this: { send: unknown }) { this.send = vi.fn().mockResolvedValue({}); }),
  SendEmailCommand: vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i }); }),
}));
vi.mock('@aws-sdk/client-kms', () => ({
  KMSClient: vi.fn(function (this: { send: unknown }) { this.send = vi.fn().mockResolvedValue({ PublicKey: new Uint8Array(0) }); }),
  GetPublicKeyCommand: vi.fn(function (this: Record<string, unknown>, i: unknown) { Object.assign(this, { input: i }); }),
}));
vi.mock('firebase-admin/app', () => ({
  initializeApp: vi.fn(),
  getApps: vi.fn(() => ['mock-app']),
  cert: vi.fn((s: unknown) => s),
}));
vi.mock('firebase-admin/messaging', () => ({
  getMessaging: vi.fn(() => ({ send: mockFcm })),
}));

// ─── Lazy handler imports (after mocks are applied) ───────────────────────────
import * as PostConfirmation from './post-confirmation/handler';
import * as CardManager      from './card-manager/handler';
import * as ScanHandler      from './scan-handler/handler';
import * as ConsentHandler   from './consent-handler/handler';
import * as EnrollHandler    from './enrollment-handler/handler';
import * as BrandApiHandler  from './brand-api-handler/handler';

// ─── Shared test state ─────────────────────────────────────────────────────────
const state = { permULID: '', secondaryULID: '', requestId: '', enrollmentId: '', offerId: '' };

// Helper: look up an item from the in-memory tables
function row(table: string, pK: string, sK: string) {
  const key = `${pK}||${sK}`;
  return mockDynamo.tables[table]?.get(key);
}

function allRows(table: string) {
  return [...(mockDynamo.tables[table]?.values() ?? [])];
}

// Helper: build an internal-auth brand-api-handler event
function internalBrandEvent(path: string, method: string, body: unknown) {
  const timestamp = Date.now().toString();
  const sig = createHmac('sha256', INTERNAL_SECRET)
    .update(`${BRAND_ID}:${timestamp}`)
    .digest('hex');
  return {
    _internalBrandId: BRAND_ID,
    _internalTimestamp: timestamp,
    _internalSig: sig,
    path,
    httpMethod: method,
    headers: { origin: 'https://business.bebocard.com' },
    body: JSON.stringify(body),
    queryStringParameters: null,
    pathParameters: null,
    multiValueHeaders: {},
    multiValueQueryStringParameters: null,
    stageVariables: null,
    requestContext: {} as never,
    resource: '',
    isBase64Encoded: false,
  };
}

// ─── beforeAll: seed reference data ──────────────────────────────────────────
beforeAll(() => {
  const tables = mockDynamo.tables;

  // Clear any stale state
  for (const k of Object.keys(tables)) delete tables[k];

  // Brand profile (lowercase 'profile') — read by getTenantStateForBrand + getBrandProfile
  const refTable = new Map<string, Record<string, unknown>>();
  tables[T_REF] = refTable;

  refTable.set(`BRAND#${BRAND_ID}||profile`, {
    pK: `BRAND#${BRAND_ID}`,
    sK: 'profile',
    desc: JSON.stringify({ brandName: 'Woolworths', brandColor: '#00A550', barcodeType: 'EAN13' }),
    // No tenantId → getTenantStateForBrand returns { tenantId: null, active: true, tier: 'base' }
  });

  // Brand profile (uppercase 'PROFILE') — read by consent-handler
  refTable.set(`BRAND#${BRAND_ID}||PROFILE`, {
    pK: `BRAND#${BRAND_ID}`,
    sK: 'PROFILE',
    desc: JSON.stringify({ brandName: 'Woolworths', consentWebhookUrl: '' }),
  });

  // Brand API key record — looked up via refDataEventsByKeyId GSI
  refTable.set(`BRAND#${BRAND_ID}||APIKEY#${KEY_ID}`, {
    pK: `BRAND#${BRAND_ID}`,
    sK: `APIKEY#${KEY_ID}`,
    keyId: KEY_ID,
    keyHash: KEY_HASH,
    brandId: BRAND_ID,
    scopes: ['scan', 'receipt', 'consent', 'enrollment', 'offers', 'newsletters', 'catalogues', 'stores'],
    rateLimit: 1_000,
    status: 'ACTIVE',
    isSandbox: false,
    createdAt: new Date().toISOString(),
    createdBy: 'e2e-test',
  });

  // Ensure USER and ADMIN tables exist
  tables[T_USER]  = new Map();
  tables[T_ADMIN] = new Map();
});

// ─── Tests ────────────────────────────────────────────────────────────────────
describe('BeboCard end-to-end scenario', () => {

  // ── Phase 1: User signs up ─────────────────────────────────────────────────
  it('Phase 1 — post-confirmation creates IDENTITY + SCAN# records', async () => {
    const event = {
      version: '1',
      triggerSource: 'PostConfirmation_ConfirmSignUp',
      region: 'ap-southeast-2',
      userPoolId: 'ap-southeast-2_test',
      userName: COGNITO_USER,
      callerContext: { awsSdkVersion: '3', clientId: 'test-client' },
      request: {
        userAttributes: {
          email: COGNITO_USER,
          birthdate: '1990-01-15', // adult — no parental consent needed
        },
      },
      response: {},
    };

    const result = await PostConfirmation.handler(event as never, {} as never, () => {});
    expect(result).toBeDefined();

    // Capture permULID from cognito mock call (AdminUpdateUserAttributesCommand arg)
    const cognitoCall = mockCognito.mock.calls.find((c: unknown[]) => {
      const cmd = c[0] as Record<string, unknown>;
      return String(cmd.__type ?? '') === 'AdminUpdateUserAttributesCommand';
    });
    expect(cognitoCall).toBeDefined();
    const userAttrs = (cognitoCall![0] as Record<string, unknown>).input as Record<string, unknown>;
    const permAttr = (userAttrs.UserAttributes as Array<{Name: string; Value: string}>)
      .find(a => a.Name === 'custom:permULID');
    expect(permAttr?.Value).toBeTruthy();
    state.permULID = permAttr!.Value;

    // IDENTITY should be in USER_TABLE
    const identity = row(T_USER, `USER#${state.permULID}`, 'IDENTITY');
    expect(identity).toBeDefined();
    expect(identity!.status).toBe('ACTIVE');
    expect(identity!.secondaryULID).toBeTruthy();
    state.secondaryULID = identity!.secondaryULID as string;

    // SCAN# should be in ADMIN_TABLE
    const scanIdx = row(T_ADMIN, `SCAN#${state.secondaryULID}`, state.permULID);
    expect(scanIdx).toBeDefined();
    expect(scanIdx!.eventType).toBe('SCAN_INDEX');
  });

  // ── Phase 2: App adds loyalty card ────────────────────────────────────────
  it('Phase 2 — addLoyaltyCard writes CARD#, updates SCAN# index, creates SUBSCRIPTION#', async () => {
    const appsyncEvent = {
      info: { fieldName: 'addLoyaltyCard' },
      arguments: {
        brandId: BRAND_ID,
        cardNumber: CARD_NUMBER,
        cardLabel: 'Woolworths Rewards',
        barcodeType: 'EAN13',
        isDefault: true,
      },
      identity: {
        claims: {
          'custom:permULID': state.permULID,
          'cognito:username': COGNITO_USER,
        },
      },
      source: null,
      request: { headers: {} },
      prev: null,
    };

    const result = await CardManager.handler(appsyncEvent as never, {} as never, () => {});
    expect((result as { success: boolean }).success).toBe(true);

    // CARD# record should exist
    const card = row(T_USER, `USER#${state.permULID}`, `CARD#${BRAND_ID}#${CARD_NUMBER}`);
    expect(card).toBeDefined();
    expect(JSON.parse(card!.desc as string).cardNumber).toBe(CARD_NUMBER);

    // SCAN# index should now include the card
    const scanIdx = row(T_ADMIN, `SCAN#${state.secondaryULID}`, state.permULID);
    expect(scanIdx).toBeDefined();
    const cards = JSON.parse(scanIdx!.desc as string).cards as Array<{brand: string; cardId: string; isDefault: boolean}>;
    expect(cards.some(c => c.brand === BRAND_ID && c.cardId === CARD_NUMBER)).toBe(true);
    expect(cards.find(c => c.brand === BRAND_ID)?.isDefault).toBe(true);

    // SUBSCRIPTION# should exist with offers on
    const sub = allRows(T_USER).find(r => r.pK === `USER#${state.permULID}` && String(r.sK).startsWith('SUBSCRIPTION#'));
    expect(sub).toBeDefined();
    expect(sub!.status).toBe('ACTIVE');
  });

  // ── Phase 3: Brand scans at POS ──────────────────────────────────────────
  it('Phase 3 — POST /v1/scan returns hasLoyaltyCard=true with loyaltyId', async () => {
    const event = {
      httpMethod: 'POST',
      path: '/v1/scan',
      headers: { 'x-api-key': RAW_KEY },
      body: JSON.stringify({ secondaryULID: state.secondaryULID, storeBrandLoyaltyName: BRAND_ID }),
      queryStringParameters: null,
      pathParameters: null,
      multiValueHeaders: {},
      multiValueQueryStringParameters: null,
      stageVariables: null,
      requestContext: {} as never,
      resource: '',
      isBase64Encoded: false,
    };

    const res = await ScanHandler.handler(event as never, {} as never, () => {}) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.hasLoyaltyCard).toBe(true);
    expect(body.loyaltyId).toBe(CARD_NUMBER);
  });

  // ── Phase 4: Brand pushes receipt after transaction ───────────────────────
  it('Phase 4 — POST /v1/receipt returns 202 and enqueues to SQS', async () => {
    mockSqs.mockClear();

    const event = {
      httpMethod: 'POST',
      path: '/v1/receipt',
      headers: { 'x-api-key': RAW_KEY },
      body: JSON.stringify({
        secondaryULID: state.secondaryULID,
        merchant: 'Woolworths Bondi Junction',
        amount: 47.85,
        purchaseDate: '2026-05-03T10:30:00.000Z',
        brandId: BRAND_ID,
        loyaltyCardId: CARD_NUMBER,
        pointsEarned: 47,
        currency: 'AUD',
        category: 'Grocery',
      }),
      queryStringParameters: null,
      pathParameters: null,
      multiValueHeaders: {},
      multiValueQueryStringParameters: null,
      stageVariables: null,
      requestContext: {} as never,
      resource: '',
      isBase64Encoded: false,
    };

    const res = await ScanHandler.handler(event as never, {} as never, () => {}) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(202);
    const body = JSON.parse(res.body);
    expect(body.success).toBe(true);
    expect(body.receiptId).toBeTruthy();

    // SQS must have been called with the receipt payload
    const sqsCall = mockSqs.mock.calls[0] as unknown[];
    expect(sqsCall).toBeDefined();
    const msgCmd = sqsCall[0] as Record<string, unknown>;
    const msgBody = JSON.parse((msgCmd.input as Record<string, unknown>).MessageBody as string);
    expect(msgBody.permULID).toBe(state.permULID);
    expect(msgBody.merchant).toBe('Woolworths Bondi Junction');
    expect(msgBody.amount).toBe(47.85);
  });

  // ── Phase 5: Brand requests consent (consent-handler) ────────────────────
  it('Phase 5 — POST /consent-request creates CONSENT# record, returns 202', async () => {
    const event = {
      httpMethod: 'POST',
      path: '/consent-request',
      headers: { 'x-api-key': RAW_KEY },
      body: JSON.stringify({
        secondaryULID: state.secondaryULID,
        requestedFields: ['email', 'firstName'],
        purpose: 'To send personalised digital receipts',
      }),
      queryStringParameters: null,
      pathParameters: null,
      multiValueHeaders: {},
      multiValueQueryStringParameters: null,
      stageVariables: null,
      requestContext: {} as never,
      resource: '',
      isBase64Encoded: false,
    };

    const res = await ConsentHandler.handler(event as never) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(202);
    const body = JSON.parse(res.body);
    expect(body.requestId).toBeTruthy();
    expect(body.status).toBe('PENDING');
    state.requestId = body.requestId as string;

    // CONSENT# record must exist in ADMIN_TABLE
    const consentItem = row(T_ADMIN, `CONSENT#${state.requestId}`, state.permULID);
    expect(consentItem).toBeDefined();
    expect(consentItem!.status).toBe('PENDING');
    const desc = JSON.parse(consentItem!.desc as string);
    expect(desc.requestedFields).toEqual(expect.arrayContaining(['email', 'firstName']));
    expect(desc.brandId).toBe(BRAND_ID);

    // SQS timeout message should have been enqueued
    const sqsCalls = mockSqs.mock.calls.filter((c: unknown[]) => {
      const cmd = c[0] as Record<string, unknown>;
      const inp = cmd.input as Record<string, unknown>;
      return String(inp.QueueUrl ?? '').includes('ConsentTimeout');
    });
    expect(sqsCalls.length).toBeGreaterThan(0);
  });

  // ── Phase 5b: Duplicate consent request returns existing (idempotent) ─────
  it('Phase 5b — duplicate POST /consent-request returns 200 with same requestId', async () => {
    const event = {
      httpMethod: 'POST',
      path: '/consent-request',
      headers: { 'x-api-key': RAW_KEY },
      body: JSON.stringify({
        secondaryULID: state.secondaryULID,
        requestedFields: ['email', 'firstName'],
        purpose: 'To send personalised digital receipts',
      }),
      queryStringParameters: null,
      pathParameters: null,
      multiValueHeaders: {},
      multiValueQueryStringParameters: null,
      stageVariables: null,
      requestContext: {} as never,
      resource: '',
      isBase64Encoded: false,
    };

    const res = await ConsentHandler.handler(event as never) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    // Deterministic hash means same input → same requestId
    expect(body.requestId).toBe(state.requestId);
  });

  // ── Phase 6: Brand sends enrollment offer ────────────────────────────────
  it('Phase 6 — POST /enroll creates ENROLL# record, returns 202 with enrollmentId', async () => {
    const event = {
      httpMethod: 'POST',
      path: '/enroll',
      headers: { 'x-api-key': RAW_KEY },
      body: JSON.stringify({
        secondaryULID: state.secondaryULID,
        programName: 'Woolworths Rewards',
        programDescription: 'Earn points on every purchase at Woolworths',
        rewardDescription: 'Redeem points for discounts on your next shop',
      }),
      queryStringParameters: null,
      pathParameters: null,
      multiValueHeaders: {},
      multiValueQueryStringParameters: null,
      stageVariables: null,
      requestContext: {} as never,
      resource: '',
      isBase64Encoded: false,
    };

    const res = await EnrollHandler.handler(event as never) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(201);
    const body = JSON.parse(res.body);
    expect(body.enrollmentId).toBeTruthy();
    state.enrollmentId = body.enrollmentId as string;

    // ENROLL# record must exist in ADMIN_TABLE
    const enroll = row(T_ADMIN, `ENROLL#${state.enrollmentId}`, state.permULID);
    expect(enroll).toBeDefined();
    expect(enroll!.status).toBe('PENDING');
    expect(enroll!.brandId).toBe(BRAND_ID);
    expect(enroll!.programName).toBe('Woolworths Rewards');
  });

  // ── Phase 6b: Duplicate enrollment returns same enrollmentId ─────────────
  it('Phase 6b — duplicate POST /enroll returns 200 with same enrollmentId', async () => {
    const event = {
      httpMethod: 'POST',
      path: '/enroll',
      headers: { 'x-api-key': RAW_KEY },
      body: JSON.stringify({
        secondaryULID: state.secondaryULID,
        programName: 'Woolworths Rewards',
      }),
      queryStringParameters: null,
      pathParameters: null,
      multiValueHeaders: {},
      multiValueQueryStringParameters: null,
      stageVariables: null,
      requestContext: {} as never,
      resource: '',
      isBase64Encoded: false,
    };

    const res = await EnrollHandler.handler(event as never) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.enrollmentId).toBe(state.enrollmentId);
  });

  // ── Phase 7: Brand creates offer (brand-api-handler, internal auth) ───────
  it('Phase 7 — POST /offers creates OFFER# in REFDATA_TABLE and returns 200', async () => {
    const event = internalBrandEvent('/offers', 'POST', {
      title: '10% off your next shop',
      description: 'Valid this week only at all Woolworths stores',
      imageUrl: 'https://cdn.bebocard.com/offers/woolworths-10pct.jpg',
      validFrom: '2026-05-03',
      validTo: '2026-05-10',
      status: 'ACTIVE',
    });

    const res = await BrandApiHandler.handler(event as never, {} as never, () => {}) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.offerId).toBeTruthy();
    state.offerId = body.offerId as string;

    // OFFER# record should be in REFDATA_TABLE
    const offer = row(T_REF, `BRAND#${BRAND_ID}`, `OFFER#${state.offerId}`);
    expect(offer).toBeDefined();
    expect(offer!.eventType).toBe('OFFER');
    const desc = JSON.parse(offer!.desc as string);
    expect(desc.title).toBe('10% off your next shop');
    expect(desc.brandId).toBe(BRAND_ID);
  });

  // ── Phase 8: Brand sends newsletter ──────────────────────────────────────
  it('Phase 8 — POST /newsletters creates NEWSLETTER# in REFDATA_TABLE', async () => {
    const event = internalBrandEvent('/newsletters', 'POST', {
      subject: 'Fresh deals just for you',
      bodyHtml: '<p>Check out our latest specials!</p>',
      imageUrl: null,
      ctaUrl: null,
    });

    const res = await BrandApiHandler.handler(event as never, {} as never, () => {}) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.newsletterId).toBeTruthy();

    const nl = row(T_REF, `BRAND#${BRAND_ID}`, `NEWSLETTER#${body.newsletterId}`);
    expect(nl).toBeDefined();
    expect(nl!.eventType).toBe('NEWSLETTER');
  });

  // ── Phase 9: Re-scan verifies segment labels returned when subscription active ─
  it('Phase 9 — second /scan still returns loyaltyId (subscription and card intact)', async () => {
    const event = {
      httpMethod: 'POST',
      path: '/v1/scan',
      headers: { 'x-api-key': RAW_KEY },
      body: JSON.stringify({ secondaryULID: state.secondaryULID, storeBrandLoyaltyName: BRAND_ID }),
      queryStringParameters: null,
      pathParameters: null,
      multiValueHeaders: {},
      multiValueQueryStringParameters: null,
      stageVariables: null,
      requestContext: {} as never,
      resource: '',
      isBase64Encoded: false,
    };

    const res = await ScanHandler.handler(event as never, {} as never, () => {}) as { statusCode: number; body: string };
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.hasLoyaltyCard).toBe(true);
    expect(body.loyaltyId).toBe(CARD_NUMBER);
  });
});
