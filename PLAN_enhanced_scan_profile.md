# Implementation Plan — Enhanced Scan + Loyalty Profile API

## Overview

Add a richer brand-facing endpoint that, given a `secondaryULID`, returns the user's full
loyalty context: loyalty card details, associated gift cards, active subscriptions, and linked
invoices. The existing `POST /scan` remains unchanged (POS hot path). The new endpoint is
designed for brand CRM portals, kiosk enrichment, and post-checkout flows where latency is
less critical than depth.

---

## New Endpoint

```
POST /profile
Authorization: X-Api-Key <brand-api-key>  (scope: scan)

Body:
{
  "secondaryULID": "<barcode-value>",
  "includeGiftCards":    true,   // optional, default false
  "includeSubscriptions": true,  // optional, default false
  "includeInvoices":     true    // optional, default false — last 6 invoices linked to subs
}

Response 200:
{
  "permULIDHash": "<HMAC-SHA256(permULID, BRAND_SALT)>",  // stable pseudonymous ID — NOT permULID
  "loyalty": {
    "hasLoyaltyCard":  true,
    "loyaltyId":       "<cardId>",
    "cardLabel":       "Everyday Rewards",
    "tier":            "gold",        // from card desc
    "isDefault":       true
  },
  "segments": {                       // only if SUBSCRIPTION#<brandId> active
    "spendBucket":      "100-200",
    "visitFrequency":   "frequent",
    "lastVisit":        "2026-04-01",
    "totalSpend":       180.50,
    "visitCount":       8,
    "persona":          ["high-value", "repeat"]
  },
  "giftCards": [                      // only if includeGiftCards=true + SUBSCRIPTION# active
    {
      "giftCardSK":    "GIFTCARD#...",
      "brandName":     "Woolworths",
      "denomination":  50,
      "balance":       30,
      "currency":      "AUD",
      "expiryDate":    "2027-01-01",
      "status":        "ACTIVE"
    }
  ],
  "subscriptions": [                  // only if includeSubscriptions=true + SUBSCRIPTION# active
    {
      "subId":          "sub-abc",
      "brandId":        "woolworths",
      "amount":         15.99,
      "frequency":      "monthly",
      "status":         "ACTIVE",
      "nextBillingDate":"2026-05-01",
      "providerId":     "woolworths",  // if catalog-linked
      "invoiceType":    "SUBSCRIPTION"
    }
  ],
  "recentInvoices": [                 // only if includeInvoices=true — last 6, linked to subs
    {
      "invoiceSK":             "INVOICE#2026-04-01#...",
      "supplier":              "Woolworths",
      "amount":                15.99,
      "dueDate":               "2026-04-01",
      "status":                "PAID",
      "invoiceType":           "SUBSCRIPTION_BILLING",
      "linkedSubscriptionSk":  "RECURRING#woolworths#sub-abc"
    }
  ]
}

Response 404: { "error": "User not found" }
Response 403: { "error": "Subscription consent required" }
```

---

## Implementation Steps

### 1. Route — scan-handler/handler.ts

Add new route in the router:

```typescript
if (path.endsWith('/profile') && method === 'POST')
  return handleLoyaltyProfile(event, headers);
```

### 2. Handler — handleLoyaltyProfile()

```
scan-handler/handler.ts
```

**Auth:** Existing `validateApiKey(dynamo, rawKey, 'scan')` — same API key scope as `/scan`.

**Flow:**

```
1. Parse body: secondaryULID, includeGiftCards, includeSubscriptions, includeInvoices
2. Validate: secondaryULID required
3. Resolve: QueryCommand on AdminDataEvent SCAN#<secondaryULID> → permULID
   └─ 404 if not found
4. Parallel fetch:
   a. GetCommand: default loyalty card (pK: USER#<permULID>, starts_with CARD#<brandId>)
      — use QueryCommand with FilterExpression isDefault=true OR first card for brandId
   b. GetCommand: SUBSCRIPTION#<brandId> — consent + push prefs
   c. GetCommand: SEGMENT#<brandId> — pre-computed segments (only if sub active)
5. Build loyalty block (always returned if card found)
6. If SUBSCRIPTION#<brandId> active:
   a. Add segments to response
   b. If includeGiftCards: QueryCommand USER# with begins_with(sK, GIFTCARD#<brandId>), Limit 10
   c. If includeSubscriptions: QueryCommand USER# with begins_with(sK, RECURRING#<brandId>), Limit 5
   d. If includeInvoices: QueryCommand USER# with begins_with(sK, INVOICE#), filter by
      linkedSubscriptionSk contains brandId, Limit 6, ScanIndexForward: false
7. Generate permULIDHash = HMAC-SHA256(permULID, BRAND_SALT_<brandId>) — brand's stable
   pseudonymous customer ID (never expose raw permULID)
8. Return assembled profile
```

**BRAND_SALT generation:**
- One secret per brand, generated at API key creation, stored in AWS Secrets Manager as
  `BRAND_SALT_<brandId>`.
- Fetched at handler startup with `secretsmanager:GetSecretValue` (Lambda execution role).
- Rotate on key rotation; old salt kept 24h for grace period (same as old API key).

### 3. Consent gating

- `giftCards`, `subscriptions`, `recentInvoices` blocks returned ONLY if
  `SUBSCRIPTION#<brandId>` record exists and `status === 'ACTIVE'`.
- Segments returned under same condition (already the case for `/scan`).
- If no active subscription: `loyalty` block is still returned; all other fields are `null`.
- Response shape is consistent; fields are `null` not missing (simpler client parsing).

### 4. Validation schema — validation-schemas.ts

```typescript
export const ProfileRequestSchema = z.object({
  secondaryULID:         z.string().min(1).max(64),
  includeGiftCards:      z.boolean().default(false),
  includeSubscriptions:  z.boolean().default(false),
  includeInvoices:       z.boolean().default(false),
});
```

### 5. Lambda IAM additions

The scan-handler Lambda execution role already has:
- `dynamodb:GetItem` / `Query` on UserDataEvent, AdminDataEvent
- No new permissions needed for DynamoDB

New permission:
- `secretsmanager:GetSecretValue` scoped to `arn:aws:secretsmanager:*:*:secret:BRAND_SALT_*`
  (one secret per brand, fetched lazily, cached in-process for Lambda lifetime)

### 6. BRAND_SALT provisioning

In `brand-api-handler/handler.ts` — `handleRotateKey()`:
- On each key rotation, generate a new `BRAND_SALT_<brandId>` 256-bit random value.
- Write to Secrets Manager (create-or-update).
- Old salt retained with a `grace_` prefix for 24h then deleted by a TTL-based cleanup job
  (or simply ignore old salt; hash rotation means old `permULIDHash` values issued to the
  brand are invalidated, which is acceptable since the brand's own DB rows hold them).

Alternatively (simpler Phase 1): use a single global `BRAND_PROFILE_HASH_SALT` env secret,
same for all brands. Less isolation but no Secrets Manager plumbing needed initially.
Can upgrade to per-brand salts when tenant data isolation becomes a compliance requirement.

### 7. Tests — scan-handler/handler.test.ts

New test group `POST /profile`:

| Test | Asserts |
|---|---|
| returns loyalty block with loyaltyId for known secondaryULID | 200, loyalty.loyaltyId present |
| returns 404 for unknown secondaryULID | 404 |
| omits gift cards when includeGiftCards=false | giftCards === null |
| omits subscriptions when includeSubscriptions=false | subscriptions === null |
| omits segments when no SUBSCRIPTION# record | segments === null |
| returns gift cards + subscriptions + invoices when all flags true + SUBSCRIPTION# active | all blocks populated |
| never exposes raw permULID in response | JSON.stringify(body) does not contain permULID |
| includeInvoices only returns invoices linked to brandId subscriptions | each invoice has linkedSubscriptionSk containing brandId |

---

## Business Portal (bebocard_business) — Tenant Onboarding UI

The subscription catalog onboarding backend is complete (`POST/PUT/GET /subscription-catalog`
via `brand-api-handler`, `recurring` scope). What the business portal needs:

### Pages to add in bebocard_business

| Route | Purpose |
|---|---|
| `/dashboard/catalog` | View current subscription catalog entry (if any) |
| `/dashboard/catalog/new` | Register new subscription product / recurring invoice issuer |
| `/dashboard/catalog/edit` | Update plans, cancelUrl, portalUrl, description |

### API calls from portal to brand-api-handler

```typescript
// GET — fetch existing entry
GET /subscription-catalog
X-Api-Key: <brand-api-key>

// POST — register new entry
POST /subscription-catalog
X-Api-Key: <brand-api-key>
Body: { providerName, category, invoiceType, plans[], cancelUrl, portalUrl, ... }

// PUT — update
PUT /subscription-catalog
X-Api-Key: <brand-api-key>
Body: { providerName?, plans?, cancelUrl?, portalUrl?, hasLinking? }
```

### Subscription linking flow (tenant website → BeboCard app)

When a user is logged into the brand's website and clicks "Link to BeboCard":

```
1. Brand redirects to:
   https://api.bebocard.com.au/auth/link/<brandId>?scope=subscriptions&permULID=<X>&authToken=<Y>

2. tenant-linker Lambda (scope=subscriptions):
   - verifyAuthToken(authToken, permULID) via Cognito GetUser
   - Writes SUBSCRIPTION#<brandId> consent record
   - Redirects to bebocard://link-success?brand=<brandId>&scope=subscriptions&linked=true

3. BeboCard app deep-links into Finance tab, shows linked subscription
```

The `permULID` and `authToken` are obtained from the BeboCard mobile SDK (Phase 6):
- `BeboCardSDK.getLinkToken()` → returns `{ permULIDHash, authToken }` (short-lived Cognito token)
- Brand embeds these in the redirect URL after user authenticates on brand site

---

## Data Flow Diagram

```
Brand POS / Kiosk
      │
      │  POST /profile { secondaryULID }
      ▼
scan-handler
      │
      ├─ AdminDataEvent: SCAN#<secondaryULID> → permULID
      │
      ├─ UserDataEvent: CARD#<brandId>#*   → loyaltyCard (isDefault)
      ├─ UserDataEvent: SUBSCRIPTION#<brandId> → consent + prefs
      ├─ UserDataEvent: SEGMENT#<brandId>  → pre-computed labels (if consented)
      ├─ UserDataEvent: GIFTCARD#<brandId>#* → gift cards (if consented + flag)
      ├─ UserDataEvent: RECURRING#<brandId>#* → subscriptions (if consented + flag)
      └─ UserDataEvent: INVOICE#*          → invoices filtered by linkedSub (if flag)
            │
            └─ Response: { loyalty, segments, giftCards, subscriptions, recentInvoices }
                         (all fields null when not consented or not requested)
```

---

## Files to Change

| File | Change |
|---|---|
| `amplify/functions/scan-handler/handler.ts` | Add `handleLoyaltyProfile()` + route |
| `amplify/shared/validation-schemas.ts` | Add `ProfileRequestSchema` |
| `amplify/functions/scan-handler/handler.test.ts` | Add ~8 tests for `/profile` |
| `amplify/backend.ts` (optional) | IAM: `secretsmanager:GetSecretValue` for scan-handler if using per-brand salts |
| `bebocard_business/src/app/dashboard/catalog/` | New UI pages for subscription catalog management |

---

## Deferred (Phase 2+)

- Per-brand `BRAND_SALT` isolation (Secrets Manager per tenant)
- Receipt and invoice history in `/profile` (currently scoped to linked-subscription invoices only)
- Real-time balance from distributor in gift card block (currently uses `lastBalanceSync` from DDB)
- `POST /profile/batch` — multiple `secondaryULID` lookups in one call (bulk checkout flows)
