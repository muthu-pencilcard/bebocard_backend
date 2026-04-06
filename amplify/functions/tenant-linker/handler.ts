/**
 * tenant-linker — REST Lambda (HTTP API)
 *
 * Handles the two phases of loyalty program OAuth:
 *
 * GET  /auth/link/{brandId}      — Initiate: redirect user to brand's OAuth page
 * GET  /auth/callback/{brandId}  — Callback: exchange code → fetch card → store → redirect to success
 *
 * Supported brands (phase 1):
 *  - woolworths  (Everyday Rewards)
 *  - flybuys     (Coles Group)
 *  - velocity    (Virgin Australia)
 *  - qantas      (Qantas Frequent Flyer)
 *
 * Card number is stored in UserDataEvent via the same CARD# key pattern as card-manager.
 * The SCAN index is updated so POS Lambdas can look up the card at checkout.
 */

import type { APIGatewayProxyHandlerV2 } from 'aws-lambda';
import { CognitoIdentityProviderClient, GetUserCommand } from '@aws-sdk/client-cognito-identity-provider';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  DeleteCommand,
  GetCommand,
  PutCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';
import { randomBytes, createHash } from 'crypto';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const cognito = new CognitoIdentityProviderClient({});
const ulid = monotonicFactory();

const USER_TABLE = process.env.USER_TABLE!;
const ADMIN_TABLE = process.env.ADMIN_TABLE!;
const APP_SUCCESS_URL = process.env.APP_SUCCESS_URL ?? 'https://bebocard.app/link-success';
const APP_FAILURE_URL = process.env.APP_FAILURE_URL ?? 'https://bebocard.app/link-failed';
const BASE_URL = process.env.API_BASE_URL!; // e.g. https://api.bebocard.app
const OAUTH_STATE_TTL_SECONDS = Number(process.env.OAUTH_STATE_TTL_SECONDS ?? '300');

// ---------------------------------------------------------------------------
// Brand OAuth config — add real client IDs / scopes from env per brand
// ---------------------------------------------------------------------------

interface BrandOAuthConfig {
  displayName: string;
  color: string;
  authUrl: string;       // brand's OAuth authorization endpoint
  tokenUrl: string;      // brand's OAuth token endpoint
  cardApiUrl: string;    // brand's API endpoint to fetch card number post-auth
  clientIdEnv: string;   // env var name holding this brand's client_id
  scopes: string;
}

const BRAND_CONFIG: Record<string, BrandOAuthConfig> = {
  woolworths: {
    displayName: 'Woolworths Everyday Rewards',
    color: '#00AA46',
    authUrl: 'https://api.woolworthsrewards.com.au/wx/v1/oauth2/authorize',
    tokenUrl: 'https://api.woolworthsrewards.com.au/wx/v1/oauth2/token',
    cardApiUrl: 'https://api.woolworthsrewards.com.au/wx/v1/rewards/member/card',
    clientIdEnv: 'WOOLWORTHS_CLIENT_ID',
    scopes: 'openid profile card',
  },
  flybuys: {
    displayName: 'Flybuys',
    color: '#C8102E',
    authUrl: 'https://secure.flybuys.com.au/oauth2/authorize',
    tokenUrl: 'https://secure.flybuys.com.au/oauth2/token',
    cardApiUrl: 'https://api.flybuys.com.au/v1/member/card',
    clientIdEnv: 'FLYBUYS_CLIENT_ID',
    scopes: 'openid member:read',
  },
  velocity: {
    displayName: 'Velocity Frequent Flyer',
    color: '#E2231A',
    authUrl: 'https://identity.velocityfrequentflyer.com/oauth2/authorize',
    tokenUrl: 'https://identity.velocityfrequentflyer.com/oauth2/token',
    cardApiUrl: 'https://api.velocityfrequentflyer.com/v1/member/profile',
    clientIdEnv: 'VELOCITY_CLIENT_ID',
    scopes: 'openid profile:read',
  },
  qantas: {
    displayName: 'Qantas Frequent Flyer',
    color: '#E40000',
    authUrl: 'https://api.qantasfreqflyer.com/oauth2/authorize',
    tokenUrl: 'https://api.qantasfreqflyer.com/oauth2/token',
    cardApiUrl: 'https://api.qantasfreqflyer.com/v1/member',
    clientIdEnv: 'QANTAS_CLIENT_ID',
    scopes: 'openid member:read',
  },
};

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  const path = event.rawPath ?? '';
  const params = event.queryStringParameters ?? {};

  try {
    const headers = event.headers ?? {};
    const userAgent = headers['user-agent'] ?? headers['User-Agent'] ?? '';

    // GET /auth/link/{brandId}?permULID=<permULID>&authToken=<accessToken>[&scope=subscriptions]
    const linkMatch = path.match(/\/auth\/link\/([a-z-]+)$/);
    if (linkMatch) {
      // Subscription consent path — no external OAuth needed, just verify + write consent
      if (params.scope === 'subscriptions') {
        return linkSubscriptions(linkMatch[1], params.permULID ?? '', params.authToken ?? '');
      }
      return initiateOAuth(linkMatch[1], params.permULID ?? '', params.authToken ?? '', userAgent);
    }

    // GET /auth/callback/{brandId}?code=...&state=<nonce>
    const callbackMatch = path.match(/\/auth\/callback\/([a-z-]+)$/);
    if (callbackMatch) {
      return handleCallback(callbackMatch[1], params.code ?? '', params.state ?? '', userAgent);
    }

    return { statusCode: 404, body: 'Not found' };
  } catch (err) {
    console.error('tenant-linker error', err);
    return redirect(`${APP_FAILURE_URL}?reason=server_error`);
  }
};

// ---------------------------------------------------------------------------
// Subscription consent linking
// No external OAuth needed — user identity verified via BeboCard authToken.
// The brand website redirects here after the user taps "Link BeboCard".
// On success: writes SUBSCRIPTION# consent record, then redirects to success URL.
// Brand can then call POST /recurring/register + /invoice using their API key.
// ---------------------------------------------------------------------------

const REF_TABLE = process.env.REFDATA_TABLE ?? process.env.REF_TABLE!;

async function linkSubscriptions(brandId: string, permULID: string, authToken: string) {
  if (!permULID || !authToken) return { statusCode: 400, body: 'Missing auth context' };

  // Must be a known brand in BRAND_CONFIG or registered in RefDataEvent
  const verifiedPermULID = await verifyAuthToken(authToken, permULID);
  if (!verifiedPermULID) return redirect(`${APP_FAILURE_URL}?reason=invalid_auth`);

  // Resolve brand display name from RefDataEvent (prefer tenant record, fall back to BRAND_CONFIG)
  let brandName = BRAND_CONFIG[brandId]?.displayName ?? brandId;
  try {
    const brandRef = await dynamo.send(new GetCommand({
      TableName: REF_TABLE,
      Key: { pK: `BRAND#${brandId}`, sK: 'PROFILE' },
    }));
    if (brandRef.Item) {
      const d = JSON.parse(brandRef.Item.desc ?? '{}');
      brandName = (d.brandName as string) ?? brandName;
    }
  } catch { /* non-fatal */ }

  const now = new Date().toISOString();

  // Write SUBSCRIPTION# consent record — this is the same record that gates
  // segment data return in scan-handler and fan-out in fanOutToSubscribers.
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${verifiedPermULID}`,
      sK: `SUBSCRIPTION#${brandId}`,
      eventType: 'SUBSCRIPTION',
      status: 'ACTIVE',
      primaryCat: 'subscription_consent',
      subCategory: brandId,
      desc: JSON.stringify({
        brandId,
        brandName,
        scope: 'recurring,invoices',
        source: 'tenant_linked',   // distinguishes from user-initiated opt-ins
        linkedAt: now,
        offers: true,
        newsletters: true,
        reminders: true,
        catalogues: true,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  console.log(`[tenant-linker] Subscription consent granted for ${brandId} → USER#${verifiedPermULID}`);
  return redirect(`${APP_SUCCESS_URL}?brand=${brandId}&scope=subscriptions&linked=true`);
}

// ---------------------------------------------------------------------------
// Phase 1 — Initiate OAuth
// ---------------------------------------------------------------------------

async function initiateOAuth(brandId: string, permULID: string, authToken: string, userAgent: string) {
  const cfg = BRAND_CONFIG[brandId];
  if (!cfg) return { statusCode: 400, body: `Unknown brand: ${brandId}` };
  if (!permULID || !authToken) return { statusCode: 400, body: 'Missing auth context' };

  const clientId = process.env[cfg.clientIdEnv];
  if (!clientId) return { statusCode: 503, body: 'Brand integration not configured' };
  const verifiedPermULID = await verifyAuthToken(authToken, permULID);
  if (!verifiedPermULID) return redirect(`${APP_FAILURE_URL}?reason=invalid_state`);

  const { codeVerifier, codeChallenge } = generatePKCE();
  const stateToken = await createOauthState(brandId, verifiedPermULID, codeVerifier, userAgent);

  const redirectUri = encodeURIComponent(`${BASE_URL}/auth/callback/${brandId}`);
  const scope = encodeURIComponent(cfg.scopes);
  const authUrl = `${cfg.authUrl}?response_type=code&client_id=${clientId}&redirect_uri=${redirectUri}&scope=${scope}&state=${stateToken}&code_challenge=${codeChallenge}&code_challenge_method=S256`;

  return redirect(authUrl);
}

// ---------------------------------------------------------------------------
// Phase 2 — OAuth Callback
// ---------------------------------------------------------------------------

async function handleCallback(brandId: string, code: string, stateToken: string, userAgent: string) {
  const cfg = BRAND_CONFIG[brandId];
  if (!cfg || !code || !stateToken) {
    return redirect(`${APP_FAILURE_URL}?reason=invalid_params`);
  }

  const state = await consumeOauthState(stateToken, userAgent);
  if (!state || state.brandId != brandId) {
    return redirect(`${APP_FAILURE_URL}?reason=invalid_state`);
  }
  const { permULID, codeVerifier } = state;

  const clientId = process.env[cfg.clientIdEnv];
  const clientSecret = process.env[`${cfg.clientIdEnv.replace('_CLIENT_ID', '')}_CLIENT_SECRET`];
  if (!clientId || !clientSecret) {
    return redirect(`${APP_FAILURE_URL}?reason=not_configured`);
  }

  // Exchange code for access token (include PKCE verifier so brand server can verify the challenge)
  const redirectUri = `${BASE_URL}/auth/callback/${brandId}`;
  const tokenRes = await fetch(cfg.tokenUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Authorization': `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
    },
    body: new URLSearchParams({
      grant_type: 'authorization_code',
      code,
      redirect_uri: redirectUri,
      code_verifier: codeVerifier,
    }).toString(),
  });

  if (!tokenRes.ok) {
    console.error('Token exchange failed', await tokenRes.text());
    return redirect(`${APP_FAILURE_URL}?reason=token_exchange_failed`);
  }

  const tokenData = await tokenRes.json() as { access_token: string };

  // Fetch card number from brand's member API
  const cardRes = await fetch(cfg.cardApiUrl, {
    headers: { Authorization: `Bearer ${tokenData.access_token}` },
  });

  if (!cardRes.ok) {
    console.error('Card fetch failed', await cardRes.text());
    return redirect(`${APP_FAILURE_URL}?reason=card_fetch_failed`);
  }

  const cardData = await cardRes.json() as Record<string, unknown>;
  const cardNumber = extractCardNumber(brandId, cardData);

  if (!cardNumber) {
    return redirect(`${APP_FAILURE_URL}?reason=card_not_found`);
  }

  // Store in UserDataEvent
  await storeLinkedCard(permULID, brandId, cardNumber, cfg.displayName, cfg.color);

  // Redirect to success deep-link that Flutter WebView intercepts
  return redirect(`${APP_SUCCESS_URL}?brand=${brandId}&linked=true`);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Extract card number from each brand's API response shape */
function extractCardNumber(brandId: string, data: Record<string, unknown>): string | null {
  switch (brandId) {
    case 'woolworths':
      return (data.cardNumber ?? data.card_number ?? data.rewardsCardNumber) as string ?? null;
    case 'flybuys':
      return (data.cardNumber ?? data.flybuysCardNumber ?? data.memberNumber) as string ?? null;
    case 'velocity':
      return (data.velocityNumber ?? data.memberNumber ?? data.frequentFlyerNumber) as string ?? null;
    case 'qantas':
      return (data.frequentFlyerNumber ?? data.memberNumber ?? data.qffNumber) as string ?? null;
    default:
      return null;
  }
}

async function storeLinkedCard(
  permULID: string,
  brandId: string,
  cardNumber: string,
  brandName: string,
  brandColor: string,
) {
  const now = new Date().toISOString();
  const cardSK = `CARD#${brandId}#${cardNumber}`;

  try {
    await dynamo.send(new PutCommand({
      TableName: USER_TABLE,
      Item: {
        pK: `USER#${permULID}`,
        sK: cardSK,
        eventType: 'CARD',
        status: 'ACTIVE',
        primaryCat: 'loyalty_card',
        subCategory: brandId,
        desc: JSON.stringify({
          brandId,
          brandName,
          brandColor,
          cardNumber,
          cardLabel: brandName,
          isCustom: false,
          isLinked: true,          // marks this card as OAuth-linked (not manually added)
          linkedAt: now,
          pointsBalance: 0,
        }),
        createdAt: now,
        updatedAt: now,
      },
      ConditionExpression: 'attribute_not_exists(sK)',
    }));
  } catch {
    // Card already exists — update the linkedAt timestamp instead
    await dynamo.send(new UpdateCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: cardSK },
      UpdateExpression: 'SET #s = :active, updatedAt = :now',
      ExpressionAttributeNames: { '#s': 'status' },
      ExpressionAttributeValues: { ':active': 'ACTIVE', ':now': now },
    }));
  }

  // Update SCAN index
  await appendToScanIndex(permULID, brandId, cardNumber);
}

async function appendToScanIndex(permULID: string, brandId: string, cardId: string) {
  const identity = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
  }));
  // secondaryULID is a top-level attribute on the IDENTITY record, not inside desc
  const secondaryULID = identity.Item?.secondaryULID as string | undefined;
  if (!secondaryULID) return;

  // sK of the SCAN index record is permULID, not the constant 'INDEX'
  const indexRecord = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `SCAN#${secondaryULID}`, sK: permULID },
  }));
  const indexDesc = JSON.parse(indexRecord.Item?.desc ?? '{}') as { cards?: unknown[] };
  const existing = (indexDesc.cards ?? []) as Array<{ brand: string; cardId: string; isDefault: boolean }>;

  // Keep other brands; replace any existing card for this brand with the new one
  const otherCards = existing.filter(c => c.brand !== brandId);
  const isFirst = existing.filter(c => c.brand === brandId).length === 0;
  const updatedCards = [...otherCards, { brand: brandId, cardId, isDefault: isFirst }];

  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `SCAN#${secondaryULID}`, sK: permULID },
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
    ExpressionAttributeValues: {
      ':desc': JSON.stringify({ ...indexDesc, cards: updatedCards }),
      ':now': new Date().toISOString(),
    },
  }));
}

function redirect(url: string) {
  return {
    statusCode: 302,
    headers: { Location: url },
    body: '',
  };
}

/** Generates a PKCE code_verifier (random 64-byte hex) and its SHA-256 code_challenge (base64url). */
function generatePKCE(): { codeVerifier: string; codeChallenge: string } {
  const codeVerifier = randomBytes(64).toString('hex'); // 128 chars, well above 43-char PKCE minimum
  const codeChallenge = createHash('sha256')
    .update(codeVerifier)
    .digest('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');
  return { codeVerifier, codeChallenge };
}

/** Returns a SHA-256 hex digest of the user-agent string for device binding. */
function hashUserAgent(ua: string): string {
  return createHash('sha256').update(ua).digest('hex');
}

async function verifyAuthToken(authToken: string, expectedPermULID: string): Promise<string | null> {
  try {
    const res = await cognito.send(new GetUserCommand({ AccessToken: authToken }));
    const permAttr = res.UserAttributes?.find((attribute) => attribute.Name === 'custom:permULID')?.Value ?? null;
    return permAttr === expectedPermULID ? permAttr : null;
  } catch (err) {
    console.error('Failed to verify auth token for tenant-linker', err);
    return null;
  }
}

async function createOauthState(
  brandId: string,
  permULID: string,
  codeVerifier: string,
  userAgent: string,
): Promise<string> {
  const token = ulid();
  const now = Date.now();
  const expiresAt = new Date(now + (OAUTH_STATE_TTL_SECONDS * 1000)).toISOString();

  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `OAUTHSTATE#${token}`,
      sK: 'STATE',
      eventType: 'OAUTH_STATE',
      status: 'PENDING',
      desc: JSON.stringify({ brandId, permULID, expiresAt, codeVerifier, uaHash: hashUserAgent(userAgent) }),
      createdAt: new Date(now).toISOString(),
      updatedAt: new Date(now).toISOString(),
    },
  }));

  return token;
}

async function consumeOauthState(
  token: string,
  userAgent: string,
): Promise<{ brandId: string; permULID: string; codeVerifier: string } | null> {
  const result = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `OAUTHSTATE#${token}`, sK: 'STATE' },
  }));

  const item = result.Item;
  if (!item) return null;

  const desc = JSON.parse(String(item.desc ?? '{}')) as {
    brandId?: string;
    permULID?: string;
    expiresAt?: string;
    codeVerifier?: string;
    uaHash?: string;
  };

  // Always delete the record to prevent replay regardless of validation outcome
  await dynamo.send(new DeleteCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `OAUTHSTATE#${token}`, sK: 'STATE' },
  }));

  if (!desc.brandId || !desc.permULID || !desc.expiresAt || !desc.codeVerifier) return null;
  if (desc.expiresAt < new Date().toISOString()) return null;
  // Verify the callback comes from the same client that initiated the flow
  if (desc.uaHash && desc.uaHash !== hashUserAgent(userAgent)) {
    console.warn('tenant-linker: user-agent mismatch on callback', { brandId: desc.brandId });
    return null;
  }

  return { brandId: desc.brandId, permULID: desc.permULID, codeVerifier: desc.codeVerifier };
}
