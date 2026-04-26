# BeboCard Backend

Amplify Gen 2 + AppSync + DynamoDB + API Gateway. All functions live under `amplify/functions/`.

## Lambda Functions

| Function | Trigger | Purpose |
|---|---|---|
| `post-confirmation` | Cognito trigger | Creates `IDENTITY` + `SCAN#` index on sign-up |
| `card-manager` | AppSync GraphQL | Add/remove cards, rotate barcode, set default, subscribe to offers |
| `scan-handler` | Public REST (`/scan`, `/receipt`) | Loyalty lookup + receipt push for brand backends |
| `tenant-linker` | REST OAuth flow | PKCE OAuth — Flybuys, Woolworths, Qantas, Velocity linking |
| `geofence-handler` | AppSync GraphQL | Store visit logging, personalised FCM push |
| `segment-processor` | DynamoDB Streams (`UserDataEvent`) | Recomputes `SEGMENT#<brandId>` + `SEGMENT#global` on every `RECEIPT#` write. **DLQ:** `SegmentProcessorDLQ` (14-day, 3 retries) |
| `receipt-iceberg-writer` | DynamoDB Streams (`UserDataEvent`) | Writes anonymised receipt rows to S3/Athena on every `RECEIPT#` write. **DLQ:** `ReceiptIcebergDLQ` (14-day, 3 retries) |
| `content-validator` | S3 trigger (`bebocard-tenant-uploads/`) | MIME + size check + Rekognition moderation; promotes to app-reference bucket or rejects |
| `reminder-handler` | EventBridge cron (daily 21:00 UTC) | Scans upcoming due dates, FCM reminders, deduplicates via AdminDataEvent sent-log |
| `brand-api-handler` | REST (brand portal API) | Brand-facing CRUD — offers, stores, catalogues, newsletters; API key auth via `api-key-auth.ts` |
| `tenant-analytics` | REST (`/analytics/*`) | Tenant-authenticated aggregate analytics pull; never returns individual records |
| `gift-card-handler` | AppSync GraphQL + REST (`/gift/:token`) | Purchase (self + gifting), claim, balance sync; Stripe + SES |
| `widget-action-handler` | REST (`WidgetActionApi`) | Embeddable widget auth, invoice creation, gift card redemption for brand iframes. WAF-protected. |
| `catalog-sync` | EventBridge cron | Syncs gift card catalogs from all distributors (Prezzee, Tango, Runa, YOUGotaGift, Reloadly); deactivates removed SKUs |
| `gift-card-refund` | EventBridge cron | Scans for expired unclaimed gifts, KMS-decrypts, returns card to sender wallet, sends FCM |
| `subscription-negotiator` | EventBridge cron | Compares user subscription costs against benchmark; creates `SAVING_OPPORTUNITY` record + FCM when >15% above |
| `subscription-proxy` | REST | Register/deregister recurring charges; amount-change detection |
| `gift-card-router` | REST | Federated gift card delivery via brand scan channel using opaque delegation token |
| `template-manager` | Direct Lambda invoke (IAM, portal only) | CRUD for loyalty card templates; super_admin only via HMAC internal auth (`actorEmail:timestamp`). Routes: POST/GET `/templates`, GET/PUT/DELETE `/templates/:id`, POST `/templates/:id/approve`, POST `/templates/:id/withdraw`. Approve: TransactWrite (status → APPROVED + write `DISCOVERY#TEMPLATES` entry). Withdraw: TransactWrite (status → WITHDRAWN + delete `DISCOVERY#TEMPLATES`). **No DLQ** — synchronous portal invoke. |

### Shared utilities (`amplify/functions/shared/`)
- `api-key-auth.ts` — hash validation, scope enforcement, brand binding
- `tenant-billing.ts` — tier check (`base`/`intelligence`), quota enforcement, usage counters
- `circuit-breaker.ts` — `CircuitBreaker` + factory (createFastAPI / createSlowAPI / createCritical)
- `audit-logger.ts` — structured CloudWatch + AdminDataEvent logging with correlation IDs
- `fanOutToSubscribers` — GSI scan of `SUBSCRIPTION#<brandId>`, status/preference/snooze gates, FCM push

---

## Public REST API (scan-handler)

Called by **brand backends**, not users.

```
POST /scan     { secondaryULID, storeBrandLoyaltyName }
               → { hasLoyaltyCard: false }
               → { hasLoyaltyCard: true, loyaltyId, tier?, spendBucket? }
               tier + spendBucket only returned if SUBSCRIPTION#<brandId> exists

POST /receipt  { secondaryULID, merchant, amount, purchaseDate, brandId?,
                 loyaltyCardId?, pointsEarned?, currency?, items?, category? }
               → writes RECEIPT# to UserDataEvent + FCM push "Receipt from <merchant>"
               Idempotency: SHA-256 key (permULID|brandId|date|merchant|amount) via RECEIPT_IDEM# sentinel
               secondaryULID is currently required — 400 if absent (anonymous path not yet supported)
```

---

## Tenant Analytics API (`tenant-analytics`)

```
GET /analytics/segments?brandId=woolworths&period=2026-03
    Authorization: ApiKey <tenant-api-key>
→ { brandId, period, cohortSize, spendDistribution, visitFrequency, subscriberCount }
  — suppressed entirely if cohortSize < minCohortThreshold (default 50)
  — only users with SUBSCRIPTION#<brandId> are included
```

**Auth flow:** API key → bcrypt hash lookup in `TENANT#*` → extract `brandIds` + `allowedScopes` → validate requested brandId + scope → aggregate `SEGMENT#` records → suppress if < threshold → return (no PII).

**`TENANT#` record shape (`RefDataEvent`):**
```json
{ "tenantName": "Woolworths Group", "apiKeyHash": "...", "brandIds": ["woolworths","bigw","everyday-rewards"],
  "rateLimitPerHour": 1000, "quotaPerDay": 5000, "allowedScopes": ["segments","receipts_aggregate","subscriber_count"],
  "minCohortThreshold": 50, "active": true }
```

---

## Brand/Tenant Workflows

### 1. Loyalty Check (`POST /scan`)
`scan-handler` → resolve `secondaryULID` → `AdminDataEvent` → find default card → parallel fetch `SUBSCRIPTION#` + `SEGMENT#` → return labels. If no card: `maybeSendCardSuggestion()` FCM.

### 2. Receipt Push (`POST /receipt`) — BeboCard users
`scan-handler` → API key auth (scope: `receipt`) → resolve `secondaryULID` → `permULID` → idempotency check → write `RECEIPT#` (includes `loyaltyCardId`, `pointsEarned` as metadata) → FCM push ("Receipt from X · Y pts earned") → DynamoDB Streams: `segment-processor` recomputes segments; `receipt-iceberg-writer` writes anonymised row to S3/Athena Iceberg.

**Note on loyalty points:** `pointsEarned` is stored in the receipt record and surfaced in the FCM notification. BeboCard does not maintain a points balance ledger — the brand's own loyalty system manages that. BeboCard surfaces the value the brand reports.

### 3. Offer Fan-out
`brand-api-handler` → write `OFFER#<ulid>` → `fanOutToSubscribers`: scan `SUBSCRIPTION#<brandId>` via GSI → status gate → preference gate (`offers`) → snooze gate (`offersSnoozeUntil`, `offersGlobalSnoozeUntil`) → FCM `type: NEW_OFFER`.

### 4. Catalogue Fan-out
Write `CATALOGUE#<ulid>` → `fanOutToSubscribers` with `matchesTargetSegments()` (checks `spendBuckets[]` + `visitFrequencies[]`) + `perSubscriberFn` (writes `CATALOGUE#<brandId>#<id>` to user's UserDataEvent, status: `UNREAD`).

### 5. Newsletter Fan-out
Write `NEWSLETTER#<ulid>` → `perSubscriberFn` writes `NEWSLETTER#<brandId>#<id>` to user's UserDataEvent (status: `UNREAD`) + FCM `deepLink: bebocard://newsletter/<id>`.

### 6. Widget Invoice
`POST /widget/auth` → Cognito token verify → `getWidgetBrandConfig()` (brand → tenant → `allowedWidgetDomains` + `widgetActions.invoice`) → HMAC-signed single-use widget token (5-min TTL, stored in AdminDataEvent) → `POST /widget/invoice` → `authorizeWidgetRequest()` → write `INVOICE#` → mark token used.

### 7. Widget Gift Card
Auth same as invoice → `GET /widget/giftcards` → user's `GIFTCARD#` filtered to `brandId` → masked cards + balances → `POST /widget/giftcard/select` → verify ownership → return full card details → mark token used.

### 8. Analytics Pull
API key → tenant record → validate brandId in allowed list → query `SEGMENT#` + `SUBSCRIPTION#` intersection → aggregate → k-anonymity suppression → return (no PII).

---

## Backlog — Next Phase

### Receipt Analytics Pipeline (all receipts → tenant S3 analytics bucket)

**Problem:** `POST /receipt` currently requires `secondaryULID` and returns 400 if absent. For non-BeboCard walk-in customers the brand has no secondaryULID — no data reaches S3. Additionally, S3 analytics should never contain raw `secondaryULID` values — only a hashed column — so brand tenants can count repeat visitors pseudonymously without being able to reconstruct or correlate actual BeboCard identifiers.

**Full pipeline design:**

```
POST /receipt (scan-handler — stays thin, fire-and-forget)
      │
      ├─ if secondaryULID resolves → permULID:
      │     Write RECEIPT# to UserDataEvent (existing)
      │     FCM push to BeboCard app (existing)
      │     DynamoDB Stream → receipt-iceberg-writer (existing ✅, no change)
      │
      └─ always (both BeboCard and anonymous):
            SQS → receipt-analytics-queue
                  (raw payload: merchant, amount, date, brandId, tenantId,
                   secondaryULID if present, category, items)

receipt-analytics-processor Lambda  (SQS trigger, batch size 10)
      │
      ├─ hash secondaryULID if present:
      │     HMAC-SHA256(secondaryULID, ANALYTICS_HASH_SALT) → visitorHash
      │     raw secondaryULID is dropped — never written to S3
      │
      ├─ permULID never available here (not in the SQS message)
      │
      ├─ build analytics row:
      │     { tenantId, brandId, purchaseDate, amount, currency,
      │       category, merchant, items[], visitorHash (nullable),
      │       isBeboCardUser: visitorHash != null, ingestedAt }
      │
      └─ write to S3 via Kinesis Firehose (Parquet, buffered 5 min / 128 MB)
             s3://bebocard-analytics/<tenantId>/<brandId>/raw/<date>/

EventBridge cron (daily 02:00 UTC)
receipt-aggregator Lambda
      │
      ├─ Athena query over raw/ partition for yesterday
      │     GROUP BY brandId, category, merchant, purchaseDate
      │     → top products by volume + revenue
      │     → AOV (average order value)
      │     → basket size distribution
      │     → repeat visitor rate (COUNT(DISTINCT visitorHash WHERE visitorHash IS NOT NULL))
      │     → BeboCard vs anonymous split
      │
      └─ write pre-computed aggregates:
             DynamoDB: METRIC#<brandId>#<date> (fast portal reads)
             or S3: .../aggregated/<date>/metrics.parquet (for Athena ad-hoc)
```

**Why SQS not direct Firehose from scan-handler:**
- `scan-handler` is on the POS hot path — SQS `SendMessage` is ~1ms; Firehose `PutRecord` adds latency + retry logic to a latency-sensitive Lambda
- SQS DLQ (14-day, 3 retries) gives the same durability as the existing stream DLQs
- The processor Lambda can evolve (new columns, enrichment, schema changes) without touching scan-handler
- Consistent pattern with how `segment-processor` and `receipt-iceberg-writer` are decoupled via DynamoDB Streams

**`visitorHash` design — stable, cross-tenant, rotation-proof:**

`secondaryULID` rotates on a time interval by design. Hashing it directly would produce a different `visitorHash` each rotation — breaking cross-time and cross-tenant joins. Instead:

```
BeboCard user (secondaryULID resolves → permULID):
  visitorHash = HMAC-SHA256(permULID, GLOBAL_ANALYTICS_SALT)
  — permULID is permanent → visitorHash is stable across all tenants and all time

Anonymous walk-in (no secondaryULID):
  visitorHash = null
```

`scan-handler` already resolves `permULID` before enqueuing (needed to write `RECEIPT#`), so the SQS message carries `permULID` for BeboCard users. The processor hashes it — raw `permULID` and raw `secondaryULID` are both dropped before any S3 write.

**`GLOBAL_ANALYTICS_SALT` is NOT tenant-specific — by design:**

The salt must be a single global BeboCard platform secret. If it were per-tenant, the same `permULID` would hash differently for each tenant, which destroys cross-tenant join capability entirely.

The salt is safe to keep global because tenants never see it:
- Stored in AWS Secrets Manager, injected only into `receipt-analytics-processor` Lambda
- Never returned in any API response or egress payload
- Never accessible to portal users or API key holders

```
Tenant A gets visitorHash = "abc123" for a user in their portal.
Tenant B gets visitorHash = "abc123" for the same user in their portal.
Neither tenant knows the other has this value — they see only their own partition.
Neither tenant can exploit it — they have no salt to rehash, and no access to the other's data.
BeboCard can JOIN across partitions ON visitor_hash for platform-level insights.
```

**Cross-tenant join capability (BeboCard-internal only):**
- Same BeboCard user at Tenant A (Woolworths) and Tenant B (BigW) → same `visitorHash` in both partitions
- BeboCard runs `JOIN ON visitor_hash` across tenants for aggregate insights (cross-brand spend patterns, multi-brand wallet users, deduplication)
- Tenants cannot join across each other — they only see their own S3 partition and have no knowledge of the salt

**Data egress rule:**

| Column | In-platform analytics | Data egress (export) |
|---|---|---|
| `visitor_hash` | ✅ available (cross-tenant joins, BeboCard internal) | ❌ stripped — enforced at `tenant-analytics` Lambda |
| `visitor_hash_tenant` | ✅ available | ✅ included — tenant's own stable pseudonymous ID |

`visitor_hash` is stripped from egress at the `tenant-analytics` Lambda before serialisation. `visitor_hash_tenant` passes through — tenants can load it into their own BI tools, CRM, or data warehouse as a stable anonymous customer key with no cross-tenant correlation risk.

**Dual visitor hash design:**

```
visitor_hash        = HMAC-SHA256(permULID, GLOBAL_ANALYTICS_SALT)
                      — single global BeboCard secret
                      — same value for the same user across ALL tenants
                      — enables cross-tenant joins at the BeboCard platform level
                      — NEVER included in data egress

visitor_hash_tenant = HMAC-SHA256(permULID, TENANT_ANALYTICS_SALT_<tenantId>)
                      — per-tenant secret, generated at tenant onboarding, stored in Secrets Manager
                      — different value for the same user at each tenant (Tenant A ≠ Tenant B)
                      — tenant's own stable pseudonymous customer identifier
                      — SAFE to include in data egress (CSV export, S3 transfer, BI tools, CRM)
```

Both are null for anonymous walk-ins (no `permULID` to hash).

The `TENANT_ANALYTICS_SALT_<tenantId>` is provisioned when a tenant is onboarded and stored in Secrets Manager. The `receipt-analytics-processor` Lambda fetches it at runtime keyed by `tenantId`.

**S3 schema (Parquet, both paths unified):**
```
tenant_id           STRING   (partition key)
brand_id            STRING   (partition key)
purchase_date       DATE     (partition key)
visitor_hash        STRING   NULLABLE  — HMAC-SHA256(permULID, GLOBAL_ANALYTICS_SALT)
                                         null = anonymous; excluded from all egress
visitor_hash_tenant STRING   NULLABLE  — HMAC-SHA256(permULID, TENANT_ANALYTICS_SALT_<tenantId>)
                                         null = anonymous; safe to include in egress
is_bebocard         BOOLEAN  — true when visitor_hash is not null
amount              DECIMAL(10,2)
currency            STRING
category            STRING
merchant            STRING
items               ARRAY<STRUCT<sku,name,qty,unit_price>>  NULLABLE
ingested_at         TIMESTAMP
```

**Pre-aggregated DynamoDB metrics (for portal `/analytics/products` tab):**
```
METRIC#<brandId>#<date>  →  {
  topProducts: [{ sku, name, units, revenue }],  // top 20
  aov: number,
  basketSizeP50: number,
  repeatVisitorRate: number,  // % with visitorHash seen >1 in window
  beboCardShare: number,      // % of transactions from BeboCard users
  totalTransactions: number,
  totalRevenue: number
}
```

**Changes required:**
1. `scan-handler` `handleReceipt`: make `secondaryULID` optional; always enqueue to SQS after identity fork
2. New Lambda: `receipt-analytics-processor` (SQS trigger, batch 10, DLQ 14-day) — fetches both `GLOBAL_ANALYTICS_SALT` and `TENANT_ANALYTICS_SALT_<tenantId>` from Secrets Manager; computes both hashes
3. New Lambda: `receipt-aggregator` (EventBridge cron, daily 02:00 UTC)
4. New SQS queue: `receipt-analytics-queue` in `amplify/backend.ts`
5. New Kinesis Firehose stream → S3 (from processor, not scan-handler)
6. Tenant onboarding: generate + store `TENANT_ANALYTICS_SALT_<tenantId>` in Secrets Manager when tenant is provisioned
7. IAM: scan-handler gets `sqs:SendMessage`; processor gets `firehose:PutRecord` + `secretsmanager:GetSecretValue` (scoped to `GLOBAL_ANALYTICS_SALT` + `TENANT_ANALYTICS_SALT_*`); aggregator gets `athena:StartQueryExecution` + `dynamodb:PutItem`
8. `tenant-analytics` Lambda: strip `visitor_hash` from all egress responses; pass through `visitor_hash_tenant`
9. Glue catalog: register Parquet schema including both hash columns
10. Tests: anonymous path + SQS enqueue in `scan-handler/handler.test.ts`; processor hash correctness + egress strip in `tenant-analytics/handler.test.ts`; full aggregator unit tests

---

## Cross-Cutting Concerns

| Concern | Where |
|---|---|
| API key auth (`api-key-auth.ts`) | `scan-handler`, `brand-api-handler` |
| Tenant billing + quota (`tenant-billing.ts`) | `brand-api-handler` (offers, newsletters, catalogues) |
| Zod validation (`OfferInputSchema`, `NewsletterInputSchema`, `CatalogueInputSchema`) | `brand-api-handler` |
| Subscription gating (fan-out only to ACTIVE `SUBSCRIPTION#`) | All fan-out workflows |
| Per-channel preferences (`offers`, `newsletters`, `reminders`, `catalogues`) | `fanOutToSubscribers` |
| Offer snooze (`offersSnoozeUntil`, `offersGlobalSnoozeUntil`) | `fanOutToSubscribers` |
| Widget origin validation (`allowedWidgetDomains`) | `widget-action-handler` |
| Audit logging (`withAuditLog()`) | `scan-handler`, `brand-api-handler` |
| WAF (managed rules + 1000 req/5 min IP rate limit) | All public REST APIs |
| Idempotency (SHA-256 `RECEIPT_IDEM#` sentinel) | `scan-handler` |

---

## Secrets (`amplify secret set`)

```bash
FIREBASE_SERVICE_ACCOUNT_JSON   # FCM push (scan-handler, geofence-handler)
WOOLWORTHS_CLIENT_ID / _SECRET  # tenant-linker OAuth
FLYBUYS_CLIENT_ID / _SECRET
VELOCITY_CLIENT_ID / _SECRET
QANTAS_CLIENT_ID / _SECRET
STRIPE_SECRET_KEY               # gift-card-handler
PREZZEE_API_KEY                 # AU distributor
TANGO_API_KEY                   # US distributor
RUNA_API_KEY                    # UK distributor
RELOADLY_CLIENT_ID / _SECRET    # global fallback
YOUGOTAGIFT_API_KEY             # UAE/GCC distributor
```

---

## Tests

592/592 passing across 37 test files (includes `template-manager`). All functions have `handler.test.ts` using Vitest + `vi.hoisted()` for mocks.
Key pattern: env vars read at module level (e.g. `USER_HASH_SALT`) must be set inside `vi.hoisted()`, not `beforeEach`.

---

## Patent — Potential New Claims (attorney review pending)

| Claim | Description | Strength |
|---|---|---|
| **A** | Pre-computed per-user behavioral segment records stored as first-class DynamoDB items | High conflict risk — prior art: Twilio Segment, Adobe CDP |
| **B** | DynamoDB Streams-triggered async recomputation (O(1) scan-time read) | Weak standalone — claim as dependent under A or E |
| **C** | Real-time scan enrichment with pre-computed labels during POS transaction | Moderate — proxy-mediated pseudonymous enrichment |
| **D** | Consent-gated label release — suppressed from `/scan` unless `SUBSCRIPTION#<brandId>` exists | **Low conflict risk — strongest standalone claim** |
| **E** | Dual-scope segments: write-time scope enforcement (per-brand vs global) | Moderate — vs CDPs that do read-time filtering |

Strongest: D and C. Bundle B as dependent under A or E.
