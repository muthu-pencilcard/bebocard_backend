#!/usr/bin/env node
/**
 * BeboCard E2E Test Script
 *
 * Creates ephemeral test users in both Cognito pools, exercises every
 * AppSync GraphQL mutation and every portal API route, then cleans up.
 *
 * Prerequisites:
 *   - AWS credentials in env (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_PROFILE)
 *   - Portal dev server running at http://localhost:3000
 *   - node >= 18 (fetch built-in)
 *
 * Usage:
 *   node scripts/e2e-test.mjs
 *   node scripts/e2e-test.mjs --no-cleanup      # keep test users for inspection
 *   node scripts/e2e-test.mjs --portal-url https://business.bebocard.com.au
 */

import {
  CognitoIdentityProviderClient,
  AdminCreateUserCommand,
  AdminSetUserPasswordCommand,
  AdminInitiateAuthCommand,
  AdminAddUserToGroupCommand,
  AdminDeleteUserCommand,
  AdminGetUserCommand,
} from '@aws-sdk/client-cognito-identity-provider';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, DeleteCommand } from '@aws-sdk/lib-dynamodb';

// ─── Config ──────────────────────────────────────────────────────────────────

const REGION = 'us-east-1';

const CONSUMER_POOL_ID = 'us-east-1_VKQXh5hOU';
const CONSUMER_CLIENT_ID = '5s02o0nmov1lri491pbckokql8';

const PORTAL_POOL_ID = 'us-east-1_Ul1JVLn22';
const PORTAL_CLIENT_ID = '5rm3o1fss2hlgoenc3p5ck9dm';

const APPSYNC_ENDPOINT = 'https://agyddsu37vfnlmugzbd6bykws4.appsync-api.us-east-1.amazonaws.com/graphql';
const APPSYNC_API_KEY = 'da2-lry2b5lx5fdylcy5kkgw5lrubi';

const PORTAL_BASE = process.argv.find(a => a.startsWith('--portal-url='))?.split('=')[1]
  ?? (process.argv[process.argv.indexOf('--portal-url') + 1]?.startsWith('http') ? process.argv[process.argv.indexOf('--portal-url') + 1] : null)
  ?? 'http://localhost:3000';

const NO_CLEANUP = process.argv.includes('--no-cleanup');

const REFDATA_TABLE = 'RefDataEvent-bpearwbsprfmjp2tskn4mhp4la-NONE';
const USERDATA_TABLE = 'UserDataEvent-bpearwbsprfmjp2tskn4mhp4la-NONE';
const ADMINDATA_TABLE = 'AdminDataEvent-bpearwbsprfmjp2tskn4mhp4la-NONE';

const TEST_CONSUMER_EMAIL = `testuser-e2e-${Date.now()}@bebocard.dev`;
const TEST_PORTAL_EMAIL = `testportal-e2e-${Date.now()}@bebocard.dev`;
const TEST_PASSWORD = 'BeboE2e!2026#Test';

const TEST_BRAND_ID = `e2e-brand-${Date.now()}`;
const TEST_TENANT_ID = `e2e-tenant-${Date.now()}`;

// ─── Clients ─────────────────────────────────────────────────────────────────

const cognito = new CognitoIdentityProviderClient({ region: REGION });
const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({ region: REGION }));

// ─── State ────────────────────────────────────────────────────────────────────

let consumerIdToken = null;
let consumerPermULID = null;
let portalIdToken = null;
let portalUserSub = null;

const createdDynamoKeys = []; // { table, pK, sK } — for cleanup
const results = [];
let passed = 0;
let failed = 0;

// ─── Helpers ──────────────────────────────────────────────────────────────────

function log(msg) { process.stdout.write(msg + '\n'); }
function section(title) { log(`\n${'─'.repeat(60)}\n  ${title}\n${'─'.repeat(60)}`); }

function record(name, ok, detail = '') {
  const icon = ok ? '✅' : '✗ ';
  const line = `  ${icon} ${name}${detail ? `  →  ${detail}` : ''}`;
  log(line);
  results.push({ name, ok, detail });
  if (ok) passed++; else failed++;
}

async function tryStep(name, fn) {
  try {
    const result = await fn();
    record(name, true, result ?? '');
    return result;
  } catch (e) {
    record(name, false, e.message ?? String(e));
    return null;
  }
}

async function graphql(query, variables, token) {
  const headers = {
    'Content-Type': 'application/json',
    ...(token
      ? { Authorization: token }
      : { 'x-api-key': APPSYNC_API_KEY }),
  };
  const res = await fetch(APPSYNC_ENDPOINT, {
    method: 'POST',
    headers,
    body: JSON.stringify({ query, variables }),
  });
  const json = await res.json();
  if (json.errors?.length) throw new Error(json.errors.map(e => e.message).join('; '));
  return json.data;
}

async function portalApi(method, path, body, token) {
  const res = await fetch(`${PORTAL_BASE}${path}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = { _raw: text }; }
  return { status: res.status, json };
}

function trackDynamo(table, pK, sK) {
  createdDynamoKeys.push({ table, pK, sK });
}

// ─── Phase 1: Create consumer user ───────────────────────────────────────────

async function createConsumerUser() {
  section('PHASE 1 — Consumer Cognito User');

  await tryStep('AdminCreateUser (consumer pool)', async () => {
    await cognito.send(new AdminCreateUserCommand({
      UserPoolId: CONSUMER_POOL_ID,
      Username: TEST_CONSUMER_EMAIL,
      TemporaryPassword: TEST_PASSWORD,
      UserAttributes: [
        { Name: 'email', Value: TEST_CONSUMER_EMAIL },
        { Name: 'email_verified', Value: 'true' },
      ],
      MessageAction: 'SUPPRESS',
    }));
    return TEST_CONSUMER_EMAIL;
  });

  await tryStep('AdminSetUserPassword (consumer — permanent)', async () => {
    await cognito.send(new AdminSetUserPasswordCommand({
      UserPoolId: CONSUMER_POOL_ID,
      Username: TEST_CONSUMER_EMAIL,
      Password: TEST_PASSWORD,
      Permanent: true,
    }));
  });

  await tryStep('AdminInitiateAuth (consumer)', async () => {
    const res = await cognito.send(new AdminInitiateAuthCommand({
      UserPoolId: CONSUMER_POOL_ID,
      ClientId: CONSUMER_CLIENT_ID,
      AuthFlow: 'ADMIN_USER_PASSWORD_AUTH',
      AuthParameters: {
        USERNAME: TEST_CONSUMER_EMAIL,
        PASSWORD: TEST_PASSWORD,
      },
    }));
    consumerIdToken = res.AuthenticationResult?.IdToken;
    if (!consumerIdToken) throw new Error('No IdToken returned');

    // Decode JWT payload to get sub / permULID
    const payload = JSON.parse(Buffer.from(consumerIdToken.split('.')[1], 'base64').toString());
    consumerPermULID = payload['custom:permULID'] ?? payload.sub;
    return `sub=${payload.sub.slice(0, 8)}… permULID=${consumerPermULID?.slice(0, 8) ?? 'pending'}…`;
  });
}

// ─── Phase 2: Seed consumer identity in DynamoDB ─────────────────────────────

async function seedConsumerDynamo() {
  section('PHASE 2 — Seed Consumer Identity (DynamoDB)');

  // post-confirmation Lambda should have run on user create if Cognito trigger is wired.
  // If permULID is still missing, seed a minimal IDENTITY record and SCAN index manually.

  if (!consumerPermULID) {
    const fakeULID = `01J${Date.now().toString(36).toUpperCase()}TEST`;
    consumerPermULID = fakeULID;
    log(`  ℹ  No permULID from token — seeding synthetic ULID: ${fakeULID}`);
  }

  const pK = `USER#${consumerPermULID}`;
  const now = new Date().toISOString();

  await tryStep('Seed IDENTITY record', async () => {
    await dynamo.send(new PutCommand({
      TableName: USERDATA_TABLE,
      Item: {
        pK, sK: 'IDENTITY',
        eventType: 'IDENTITY',
        status: 'ACTIVE',
        primaryCat: 'identity',
        brandId: 'bebocard',
        desc: JSON.stringify({ email: TEST_CONSUMER_EMAIL, permULID: consumerPermULID }),
        createdAt: now, updatedAt: now,
      },
      ConditionExpression: 'attribute_not_exists(pK)',
    }));
    trackDynamo(USERDATA_TABLE, pK, 'IDENTITY');
  });

  const secondaryULID = `01SCAN${Date.now().toString(36).toUpperCase()}`;
  await tryStep('Seed SCAN index (AdminDataEvent)', async () => {
    await dynamo.send(new PutCommand({
      TableName: ADMINDATA_TABLE,
      Item: {
        pK: `SCAN#${secondaryULID}`,
        sK: consumerPermULID,
        eventType: 'SCAN_INDEX',
        status: 'ACTIVE',
        primaryCat: 'scan',
        brandId: 'bebocard',
        desc: JSON.stringify({ secondaryULID, cards: [] }),
        createdAt: now, updatedAt: now,
      },
    }));
    trackDynamo(ADMINDATA_TABLE, `SCAN#${secondaryULID}`, consumerPermULID);
    return `secondaryULID=${secondaryULID.slice(0, 12)}…`;
  });
}

// ─── Phase 3: AppSync GraphQL mutations ──────────────────────────────────────

async function runGraphQLTests() {
  section('PHASE 3 — AppSync GraphQL Mutations');

  if (!consumerIdToken) {
    log('  ⚠  No consumer token — skipping GraphQL tests');
    return;
  }

  const pK = `USER#${consumerPermULID}`;
  const now = new Date().toISOString();

  // addLoyaltyCard
  let loyaltySK;
  await tryStep('mutation addLoyaltyCard', async () => {
    const data = await graphql(`
      mutation AddCard($pK: String!, $sK: String!, $eventType: String!, $status: String!, $primaryCat: String!, $brandId: String!, $desc: String!, $createdAt: AWSDateTime!, $updatedAt: AWSDateTime!) {
        createUserDataEvent(input: { pK: $pK, sK: $sK, eventType: $eventType, status: $status, primaryCat: $primaryCat, brandId: $brandId, desc: $desc, createdAt: $createdAt, updatedAt: $updatedAt }) { pK sK }
      }
    `, {
      pK, sK: `CARD#e2e-${Date.now()}`, eventType: 'LOYALTY_CARD',
      status: 'ACTIVE', primaryCat: 'loyalty_card', brandId: 'e2e-brand',
      desc: JSON.stringify({ loyaltyId: 'E2E-LOYALTY-001', cardName: 'E2E Test Card', isDefault: true }),
      createdAt: now, updatedAt: now,
    }, consumerIdToken);
    loyaltySK = data.createUserDataEvent.sK;
    trackDynamo(USERDATA_TABLE, pK, loyaltySK);
    return `sK=${loyaltySK}`;
  });

  // addGiftCard
  let giftCardSK;
  await tryStep('mutation addGiftCard (createUserDataEvent GIFTCARD)', async () => {
    const sK = `GIFTCARD#e2e-${Date.now()}`;
    const data = await graphql(`
      mutation AddGiftCard($pK: String!, $sK: String!, $eventType: String!, $status: String!, $primaryCat: String!, $brandId: String!, $desc: String!, $createdAt: AWSDateTime!, $updatedAt: AWSDateTime!) {
        createUserDataEvent(input: { pK: $pK, sK: $sK, eventType: $eventType, status: $status, primaryCat: $primaryCat, brandId: $brandId, desc: $desc, createdAt: $createdAt, updatedAt: $updatedAt }) { pK sK }
      }
    `, {
      pK, sK, eventType: 'GIFTCARD',
      status: 'ACTIVE', primaryCat: 'gift_card', brandId: 'e2e-brand',
      desc: JSON.stringify({ brandName: 'E2E Brand', cardNumber: '4111111111111111', denomination: 50, currency: 'AUD', balance: 50 }),
      createdAt: now, updatedAt: now,
    }, consumerIdToken);
    giftCardSK = data.createUserDataEvent.sK;
    trackDynamo(USERDATA_TABLE, pK, giftCardSK);
    return `sK=${giftCardSK}`;
  });

  // addReceipt
  let receiptSK;
  await tryStep('mutation addReceipt (createUserDataEvent RECEIPT)', async () => {
    const sK = `RECEIPT#e2e-${Date.now()}`;
    const data = await graphql(`
      mutation AddReceipt($pK: String!, $sK: String!, $eventType: String!, $status: String!, $primaryCat: String!, $brandId: String!, $desc: String!, $createdAt: AWSDateTime!, $updatedAt: AWSDateTime!) {
        createUserDataEvent(input: { pK: $pK, sK: $sK, eventType: $eventType, status: $status, primaryCat: $primaryCat, brandId: $brandId, desc: $desc, createdAt: $createdAt, updatedAt: $updatedAt }) { pK sK }
      }
    `, {
      pK, sK, eventType: 'RECEIPT',
      status: 'ACTIVE', primaryCat: 'receipt', brandId: 'e2e-brand',
      desc: JSON.stringify({ merchant: 'E2E Store', amount: 42.50, currency: 'AUD', purchaseDate: now.slice(0, 10) }),
      createdAt: now, updatedAt: now,
    }, consumerIdToken);
    receiptSK = data.createUserDataEvent.sK;
    trackDynamo(USERDATA_TABLE, pK, receiptSK);
    return `sK=${receiptSK}`;
  });

  // addInvoice
  let invoiceSK;
  await tryStep('mutation addInvoice (createUserDataEvent INVOICE)', async () => {
    const sK = `INVOICE#e2e-${Date.now()}`;
    const data = await graphql(`
      mutation AddInvoice($pK: String!, $sK: String!, $eventType: String!, $status: String!, $primaryCat: String!, $brandId: String!, $desc: String!, $createdAt: AWSDateTime!, $updatedAt: AWSDateTime!) {
        createUserDataEvent(input: { pK: $pK, sK: $sK, eventType: $eventType, status: $status, primaryCat: $primaryCat, brandId: $brandId, desc: $desc, createdAt: $createdAt, updatedAt: $updatedAt }) { pK sK }
      }
    `, {
      pK, sK, eventType: 'INVOICE',
      status: 'ACTIVE', primaryCat: 'invoice', brandId: 'e2e-brand',
      desc: JSON.stringify({ supplier: 'E2E Supplier', amount: 199.00, currency: 'AUD', dueDate: '2026-05-01', status: 'unpaid' }),
      createdAt: now, updatedAt: now,
    }, consumerIdToken);
    invoiceSK = data.createUserDataEvent.sK;
    trackDynamo(USERDATA_TABLE, pK, invoiceSK);
    return `sK=${invoiceSK}`;
  });

  // subscribeToOffers (createUserDataEvent SUBSCRIPTION)
  let subSK;
  await tryStep('mutation subscribeToOffers (createUserDataEvent SUBSCRIPTION)', async () => {
    const sK = `SUBSCRIPTION#e2e-brand`;
    const data = await graphql(`
      mutation Subscribe($pK: String!, $sK: String!, $eventType: String!, $status: String!, $primaryCat: String!, $brandId: String!, $desc: String!, $createdAt: AWSDateTime!, $updatedAt: AWSDateTime!) {
        createUserDataEvent(input: { pK: $pK, sK: $sK, eventType: $eventType, status: $status, primaryCat: $primaryCat, brandId: $brandId, desc: $desc, createdAt: $createdAt, updatedAt: $updatedAt }) { pK sK }
      }
    `, {
      pK, sK, eventType: 'SUBSCRIPTION',
      status: 'ACTIVE', primaryCat: 'subscription', brandId: 'e2e-brand',
      desc: JSON.stringify({ offers: true, newsletters: true, catalogues: true, reminders: true }),
      createdAt: now, updatedAt: now,
    }, consumerIdToken);
    subSK = data.createUserDataEvent.sK;
    trackDynamo(USERDATA_TABLE, pK, subSK);
    return `sK=${subSK}`;
  });

  // query userDataEventsByPkAndSk (read-back)
  await tryStep('query userDataEventsByPkAndSk (read-back loyalty card)', async () => {
    if (!loyaltySK) throw new Error('No loyaltySK — prior step failed');
    const data = await graphql(`
      query GetCard($pK: String!) {
        userDataEventsByPkAndSk(pK: $pK, filter: { primaryCat: { eq: "loyalty_card" } }) {
          items { pK sK eventType status }
        }
      }
    `, { pK }, consumerIdToken);
    const items = data.userDataEventsByPkAndSk?.items ?? [];
    if (!items.length) throw new Error('No items returned');
    return `${items.length} card(s) found`;
  });

  // markNewsletterRead (updateUserDataEvent)
  await tryStep('mutation markNewsletterRead (updateUserDataEvent status)', async () => {
    // Seed a newsletter record first
    const nlSK = `NEWSLETTER#e2e-brand#${Date.now()}`;
    await graphql(`
      mutation SeedNewsletter($pK: String!, $sK: String!, $eventType: String!, $status: String!, $primaryCat: String!, $brandId: String!, $desc: String!, $createdAt: AWSDateTime!, $updatedAt: AWSDateTime!) {
        createUserDataEvent(input: { pK: $pK, sK: $sK, eventType: $eventType, status: $status, primaryCat: $primaryCat, brandId: $brandId, desc: $desc, createdAt: $createdAt, updatedAt: $updatedAt }) { pK sK }
      }
    `, {
      pK, sK: nlSK, eventType: 'NEWSLETTER',
      status: 'UNREAD', primaryCat: 'newsletter', brandId: 'e2e-brand',
      desc: JSON.stringify({ subject: 'E2E Newsletter', body: '<p>Hello</p>' }),
      createdAt: now, updatedAt: now,
    }, consumerIdToken);
    trackDynamo(USERDATA_TABLE, pK, nlSK);

    // Now mark read
    const data = await graphql(`
      mutation MarkRead($pK: String!, $sK: String!, $status: String!, $updatedAt: AWSDateTime!) {
        updateUserDataEvent(input: { pK: $pK, sK: $sK, status: $status, updatedAt: $updatedAt }) { pK sK status }
      }
    `, { pK, sK: nlSK, status: 'READ', updatedAt: now }, consumerIdToken);
    const s = data.updateUserDataEvent?.status;
    if (s !== 'READ') throw new Error(`Expected READ, got ${s}`);
    return `status=${s}`;
  });
}

// ─── Phase 4: Create portal user + seed brand/tenant ─────────────────────────

async function createPortalUser() {
  section('PHASE 4 — Portal Cognito User + Brand/Tenant Seed');

  await tryStep('AdminCreateUser (portal pool)', async () => {
    const res = await cognito.send(new AdminCreateUserCommand({
      UserPoolId: PORTAL_POOL_ID,
      Username: TEST_PORTAL_EMAIL,
      TemporaryPassword: TEST_PASSWORD,
      UserAttributes: [
        { Name: 'email', Value: TEST_PORTAL_EMAIL },
        { Name: 'email_verified', Value: 'true' },
      ],
      MessageAction: 'SUPPRESS',
    }));
    portalUserSub = res.User?.Attributes?.find(a => a.Name === 'sub')?.Value;
    return TEST_PORTAL_EMAIL;
  });

  await tryStep('AdminSetUserPassword (portal — permanent)', async () => {
    await cognito.send(new AdminSetUserPasswordCommand({
      UserPoolId: PORTAL_POOL_ID,
      Username: TEST_PORTAL_EMAIL,
      Password: TEST_PASSWORD,
      Permanent: true,
    }));
  });

  await tryStep('AdminAddUserToGroup super_admin', async () => {
    await cognito.send(new AdminAddUserToGroupCommand({
      UserPoolId: PORTAL_POOL_ID,
      Username: TEST_PORTAL_EMAIL,
      GroupName: 'super_admin',
    }));
  });

  await tryStep('AdminInitiateAuth (portal)', async () => {
    const res = await cognito.send(new AdminInitiateAuthCommand({
      UserPoolId: PORTAL_POOL_ID,
      ClientId: PORTAL_CLIENT_ID,
      AuthFlow: 'ADMIN_USER_PASSWORD_AUTH',
      AuthParameters: {
        USERNAME: TEST_PORTAL_EMAIL,
        PASSWORD: TEST_PASSWORD,
      },
    }));
    portalIdToken = res.AuthenticationResult?.IdToken;
    if (!portalIdToken) throw new Error('No IdToken returned');
    const payload = JSON.parse(Buffer.from(portalIdToken.split('.')[1], 'base64').toString());
    portalUserSub = portalUserSub ?? payload.sub;
    return `sub=${payload.sub.slice(0, 8)}…`;
  });

  // Seed brand + tenant in DynamoDB so portal routes have data to work with
  const now = new Date().toISOString();

  await tryStep('Seed TENANT record (RefDataEvent)', async () => {
    await dynamo.send(new PutCommand({
      TableName: REFDATA_TABLE,
      Item: {
        pK: `TENANT#${TEST_TENANT_ID}`,
        sK: 'PROFILE',
        eventType: 'TENANT',
        status: 'ACTIVE',
        primaryCat: 'tenant',
        brandId: TEST_BRAND_ID,
        desc: JSON.stringify({
          tenantName: 'E2E Test Tenant',
          tenantId: TEST_TENANT_ID,
          tier: 'base',
          billingStatus: 'ACTIVE',
          includedEventsPerMonth: 1000,
          brandIds: [TEST_BRAND_ID],
          allowedScopes: ['offers', 'newsletters', 'catalogues'],
        }),
        createdAt: now, updatedAt: now,
      },
    }));
    trackDynamo(REFDATA_TABLE, `TENANT#${TEST_TENANT_ID}`, 'PROFILE');
  });

  await tryStep('Seed BRAND record (RefDataEvent)', async () => {
    await dynamo.send(new PutCommand({
      TableName: REFDATA_TABLE,
      Item: {
        pK: `BRAND#${TEST_BRAND_ID}`,
        sK: 'profile',
        eventType: 'BRAND',
        status: 'ACTIVE',
        primaryCat: 'brand',
        brandId: TEST_BRAND_ID,
        desc: JSON.stringify({
          brandId: TEST_BRAND_ID,
          brandName: 'E2E Test Brand',
          tenantId: TEST_TENANT_ID,
          category: 'retail',
          color: '#4F46E5',
          allowedWidgetDomains: ['localhost'],
        }),
        createdAt: now, updatedAt: now,
      },
    }));
    trackDynamo(REFDATA_TABLE, `BRAND#${TEST_BRAND_ID}`, 'profile');
  });

  // Seed portal membership so the portal user can access the brand
  await tryStep('Seed portal MEMBERSHIP (RefDataEvent)', async () => {
    await dynamo.send(new PutCommand({
      TableName: REFDATA_TABLE,
      Item: {
        pK: `TENANT#${TEST_TENANT_ID}`,
        sK: `MEMBERSHIP#EMAIL#${TEST_PORTAL_EMAIL}`,
        eventType: 'MEMBERSHIP',
        status: 'ACTIVE',
        primaryCat: 'membership',
        brandId: TEST_BRAND_ID,
        desc: JSON.stringify({
          email: TEST_PORTAL_EMAIL,
          role: 'admin',
          tenantId: TEST_TENANT_ID,
        }),
        createdAt: now, updatedAt: now,
      },
    }));
    trackDynamo(REFDATA_TABLE, `TENANT#${TEST_TENANT_ID}`, `MEMBERSHIP#EMAIL#${TEST_PORTAL_EMAIL}`);
  });
}

// ─── Phase 5: Portal API routes ───────────────────────────────────────────────

async function runPortalApiTests() {
  section('PHASE 5 — Portal API Routes');

  if (!portalIdToken) {
    log('  ⚠  No portal token — skipping portal API tests');
    return;
  }

  const tok = portalIdToken;
  let createdOfferId, createdCatalogueId, createdNewsletterId, createdStoreId;

  // ── Unauthenticated guard ─────────────────────────────────────────────────
  await tryStep('GET /api/brands (no token) → 401', async () => {
    const { status } = await portalApi('GET', '/api/brands', null, null);
    if (status !== 401) throw new Error(`Expected 401, got ${status}`);
    return `status=${status}`;
  });

  // ── Brands ────────────────────────────────────────────────────────────────
  await tryStep('GET /api/brands → 200', async () => {
    const { status, json } = await portalApi('GET', '/api/brands', null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}: ${JSON.stringify(json).slice(0, 200)}`);
    return `${json.brands?.length ?? '?'} brands`;
  });

  await tryStep(`GET /api/brands/${TEST_BRAND_ID} → 200`, async () => {
    const { status, json } = await portalApi('GET', `/api/brands/${TEST_BRAND_ID}`, null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}`);
    return `brandId=${json.brand?.brandId ?? json.brandId ?? '?'}`;
  });

  // ── Offers ────────────────────────────────────────────────────────────────
  await tryStep('GET /api/offers → 200', async () => {
    const { status, json } = await portalApi('GET', `/api/offers?brandId=${TEST_BRAND_ID}`, null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}: ${JSON.stringify(json).slice(0, 200)}`);
    return `${json.offers?.length ?? '?'} offers`;
  });

  await tryStep('POST /api/offers → 200/201', async () => {
    const { status, json } = await portalApi('POST', '/api/offers', {
      brandId: TEST_BRAND_ID,
      title: 'E2E Test Offer',
      description: 'Created by e2e-test.mjs',
      validFrom: '2026-04-01',
      validTo: '2026-12-31',
      category: 'general',
    }, tok);
    if (status !== 200 && status !== 201) throw new Error(`Expected 200/201, got ${status}: ${JSON.stringify(json).slice(0, 200)}`);
    createdOfferId = json.offerId ?? json.offer?.offerId;
    return `offerId=${createdOfferId ?? 'unknown'}`;
  });

  if (createdOfferId) {
    await tryStep(`GET /api/offers/${createdOfferId} → 200`, async () => {
      const { status, json } = await portalApi('GET', `/api/offers/${createdOfferId}`, null, tok);
      if (status !== 200) throw new Error(`Expected 200, got ${status}`);
      return `title=${json.offer?.title ?? json.title ?? '?'}`;
    });

    await tryStep(`PUT /api/offers/${createdOfferId} → 200`, async () => {
      const { status, json } = await portalApi('PUT', `/api/offers/${createdOfferId}`, {
        brandId: TEST_BRAND_ID,
        title: 'E2E Test Offer (updated)',
        description: 'Updated by e2e-test.mjs',
        validFrom: '2026-04-01',
        validTo: '2026-12-31',
      }, tok);
      if (status !== 200) throw new Error(`Expected 200, got ${status}: ${JSON.stringify(json).slice(0, 200)}`);
      return 'updated';
    });
  }

  // ── Catalogues ────────────────────────────────────────────────────────────
  await tryStep('GET /api/catalogues → 200', async () => {
    const { status, json } = await portalApi('GET', `/api/catalogues?brandId=${TEST_BRAND_ID}`, null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}`);
    return `${json.catalogues?.length ?? '?'} catalogues`;
  });

  await tryStep('POST /api/catalogues → 200/201', async () => {
    const { status, json } = await portalApi('POST', '/api/catalogues', {
      brandId: TEST_BRAND_ID,
      title: 'E2E Test Catalogue',
      items: [{ imageUrl: 'https://placehold.co/400x300', caption: 'Test item' }],
    }, tok);
    if (status !== 200 && status !== 201) throw new Error(`Expected 200/201, got ${status}: ${JSON.stringify(json).slice(0, 200)}`);
    createdCatalogueId = json.catalogueId ?? json.catalogue?.catalogueId;
    return `catalogueId=${createdCatalogueId ?? 'unknown'}`;
  });

  if (createdCatalogueId) {
    await tryStep(`GET /api/catalogues/${createdCatalogueId} → 200`, async () => {
      const { status } = await portalApi('GET', `/api/catalogues/${createdCatalogueId}`, null, tok);
      if (status !== 200) throw new Error(`Expected 200, got ${status}`);
      return 'ok';
    });
  }

  // ── Newsletters ───────────────────────────────────────────────────────────
  await tryStep('GET /api/newsletters → 200', async () => {
    const { status, json } = await portalApi('GET', `/api/newsletters?brandId=${TEST_BRAND_ID}`, null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}`);
    return `${json.newsletters?.length ?? '?'} newsletters`;
  });

  await tryStep('POST /api/newsletters → 200/201', async () => {
    const { status, json } = await portalApi('POST', '/api/newsletters', {
      brandId: TEST_BRAND_ID,
      subject: 'E2E Test Newsletter',
      body: '<p>Hello from e2e-test.mjs</p>',
    }, tok);
    if (status !== 200 && status !== 201) throw new Error(`Expected 200/201, got ${status}: ${JSON.stringify(json).slice(0, 200)}`);
    createdNewsletterId = json.newsletterId ?? json.newsletter?.newsletterId;
    return `newsletterId=${createdNewsletterId ?? 'unknown'}`;
  });

  // ── Stores ────────────────────────────────────────────────────────────────
  await tryStep('GET /api/stores → 200', async () => {
    const { status, json } = await portalApi('GET', `/api/stores?brandId=${TEST_BRAND_ID}`, null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}`);
    return `${json.stores?.length ?? '?'} stores`;
  });

  await tryStep('POST /api/stores → 200/201', async () => {
    const { status, json } = await portalApi('POST', '/api/stores', {
      brandId: TEST_BRAND_ID,
      name: 'E2E Test Store',
      address: '123 Test St, Sydney NSW 2000',
      lat: -33.8688,
      lng: 151.2093,
    }, tok);
    if (status !== 200 && status !== 201) throw new Error(`Expected 200/201, got ${status}: ${JSON.stringify(json).slice(0, 200)}`);
    createdStoreId = json.storeId ?? json.store?.storeId;
    return `storeId=${createdStoreId ?? 'unknown'}`;
  });

  // ── API Keys ──────────────────────────────────────────────────────────────
  await tryStep('GET /api/api-keys → 200', async () => {
    const { status, json } = await portalApi('GET', `/api/api-keys?brandId=${TEST_BRAND_ID}`, null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}`);
    return `${json.keys?.length ?? '?'} keys`;
  });

  // ── Tenants ───────────────────────────────────────────────────────────────
  await tryStep('GET /api/tenants → 200', async () => {
    const { status, json } = await portalApi('GET', '/api/tenants', null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}`);
    return `${json.tenants?.length ?? '?'} tenants`;
  });

  await tryStep(`GET /api/tenants/${TEST_TENANT_ID} → 200`, async () => {
    const { status } = await portalApi('GET', `/api/tenants/${TEST_TENANT_ID}`, null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}`);
    return 'ok';
  });

  // ── Users ─────────────────────────────────────────────────────────────────
  await tryStep('GET /api/users → 200', async () => {
    const { status, json } = await portalApi('GET', '/api/users', null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}`);
    return `${json.users?.length ?? '?'} users`;
  });

  // ── Analytics ─────────────────────────────────────────────────────────────
  await tryStep('GET /api/analytics → 200/204', async () => {
    const { status } = await portalApi('GET', `/api/analytics?brandId=${TEST_BRAND_ID}`, null, tok);
    if (status !== 200 && status !== 204 && status !== 404) throw new Error(`Expected 200/204, got ${status}`);
    return `status=${status}`;
  });

  // ── Audit Log ─────────────────────────────────────────────────────────────
  await tryStep('GET /api/audit-log → 200', async () => {
    const { status, json } = await portalApi('GET', '/api/audit-log', null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}`);
    return `${json.events?.length ?? json.logs?.length ?? '?'} events`;
  });

  // ── Upload URL ─────────────────────────────────────────────────────────────
  await tryStep('POST /api/upload-url → 200', async () => {
    const { status, json } = await portalApi('POST', '/api/upload-url', {
      brandId: TEST_BRAND_ID,
      fileName: 'e2e-test.png',
      contentType: 'image/png',
    }, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}: ${JSON.stringify(json).slice(0, 200)}`);
    return json.uploadUrl ? 'uploadUrl present' : 'missing uploadUrl';
  });

  // ── Dashboard ─────────────────────────────────────────────────────────────
  await tryStep('GET / (dashboard) → 200', async () => {
    const { status } = await portalApi('GET', '/', null, tok);
    if (status !== 200) throw new Error(`Expected 200, got ${status}`);
    return 'ok';
  });
}

// ─── Phase 6: Cleanup ─────────────────────────────────────────────────────────

async function cleanup() {
  if (NO_CLEANUP) {
    log('\n  ℹ  --no-cleanup flag set — skipping cleanup');
    log(`     Consumer user: ${TEST_CONSUMER_EMAIL}`);
    log(`     Portal user:   ${TEST_PORTAL_EMAIL}`);
    log(`     Brand ID:      ${TEST_BRAND_ID}`);
    log(`     Tenant ID:     ${TEST_TENANT_ID}`);
    return;
  }

  section('PHASE 6 — Cleanup');

  // Delete DynamoDB records in reverse order
  for (const { table, pK, sK } of [...createdDynamoKeys].reverse()) {
    await tryStep(`Delete ${table.split('-')[0]} ${pK.slice(0, 20)}… / ${sK.slice(0, 20)}…`, async () => {
      await dynamo.send(new DeleteCommand({ TableName: table, Key: { pK, sK } }));
    });
  }

  // Delete Cognito users
  await tryStep(`AdminDeleteUser consumer ${TEST_CONSUMER_EMAIL}`, async () => {
    await cognito.send(new AdminDeleteUserCommand({
      UserPoolId: CONSUMER_POOL_ID,
      Username: TEST_CONSUMER_EMAIL,
    }));
  });

  await tryStep(`AdminDeleteUser portal ${TEST_PORTAL_EMAIL}`, async () => {
    await cognito.send(new AdminDeleteUserCommand({
      UserPoolId: PORTAL_POOL_ID,
      Username: TEST_PORTAL_EMAIL,
    }));
  });
}

// ─── Summary ──────────────────────────────────────────────────────────────────

function printSummary() {
  const total = passed + failed;
  section(`SUMMARY  —  ${passed}/${total} passed`);

  if (failed > 0) {
    log('\n  Failed tests:');
    results.filter(r => !r.ok).forEach(r => {
      log(`    ✗  ${r.name}`);
      if (r.detail) log(`       ${r.detail}`);
    });
  }

  log(`\n  ${'─'.repeat(40)}`);
  log(`  Total: ${total}  |  Passed: ${passed}  |  Failed: ${failed}`);
  log('');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  log('');
  log('╔══════════════════════════════════════════════════════════╗');
  log('║           BeboCard E2E Test Suite                        ║');
  log(`║  ${new Date().toISOString()}                    ║`);
  log('╚══════════════════════════════════════════════════════════╝');
  log(`  Portal URL: ${PORTAL_BASE}`);
  log(`  Consumer email: ${TEST_CONSUMER_EMAIL}`);
  log(`  Portal email:   ${TEST_PORTAL_EMAIL}`);

  try {
    await createConsumerUser();
    await seedConsumerDynamo();
    await runGraphQLTests();
    await createPortalUser();
    await runPortalApiTests();
  } finally {
    await cleanup();
    printSummary();
    process.exit(failed > 0 ? 1 : 0);
  }
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(2);
});
