import { type ClientSchema, a, defineData } from '@aws-amplify/backend';
import { cardManagerFn } from '../functions/card-manager/resource';
import { geofenceHandlerFn } from '../functions/geofence-handler/resource';
import { subscriptionProxyFn } from '../functions/subscription-proxy/resource';
import { exporterFn } from '../functions/user-data-exporter/resource';
import { giftCardHandlerFn } from '../functions/gift-card-handler/resource';
import { remoteConfigHandlerFn } from '../functions/remote-config-handler/resource';
import { clickTrackingHandlerFn } from '../functions/click-tracking-handler/resource';
import { consentHandlerFn } from '../functions/consent-handler/resource';

const schema = a.schema({
  // ── User data: loyalty cards, receipts, invoices, gift cards, segments ───
  UserDataEvent: a.model({
    pK: a.string().required(),   // USER#<permULID>
    sK: a.string().required(),   // IDENTITY | CARD#... | RECEIPT#... | INVOICE#... | GIFTCARD#... | SEGMENT#... | SUBSCRIPTION#...
    eventType: a.string(),       // IDENTITY | CARD | RECEIPT | INVOICE | GIFTCARD | SEGMENT | SUBSCRIPTION | ...
    status: a.string(),          // ACTIVE | REVOKED | ARCHIVED
    primaryCat: a.string(),      // loyalty_card | receipt | invoice | gift_card | segment | subscription | ...
    subCategory: a.string(),     // usually brand id, or another feature-specific subtype
    desc: a.json(),              // all entity-specific fields
    secondaryULID: a.string(),   // IDENTITY only
    rotatesAt: a.string(),       // IDENTITY only
    expiryDate: a.string(),      // Top-level for GSI lookups (invoices, gift cards, points)
    persona: a.string(),         // SEGMENT only — top-level for GSI lookup targeting
    createdAt: a.datetime(),
    updatedAt: a.datetime(),
  })
    .identifier(['pK', 'sK'])
    .secondaryIndexes(index => [
      index('primaryCat').sortKeys(['createdAt']).queryField('userDataByCategory'),
      index('subCategory').sortKeys(['createdAt']).queryField('userDataBySubCategory'),
      index('expiryDate').queryField('userDataByExpiry'),
      index('persona').queryField('userDataByPersona'),
    ])
    .authorization(allow => [
      allow.owner().identityClaim('sub'),
    ]),

  // ── Analytics reporting: daily snapshots, trend metrics (P2-17) ────────────
  ReportDataEvent: a.model({
    pK: a.string().required(),   // REPORT#<brandId> | ANALYTICS#GLOBAL
    sK: a.string().required(),   // DAILY#<date> | WEEKLY#<date> | MONTHLY#<date>
    eventType: a.string(),       // SEGMENT_DAILY | REVENUE_DAILY | ...
    status: a.string(),          // ACTIVE | SUPPRESSED
    desc: a.json(),              // metrics: segment distribution, spend totals, visit frequency
    createdAt: a.datetime(),
    updatedAt: a.datetime(),
  })
    .identifier(['pK', 'sK'])
    .authorization(allow => [
      // Only readable by brand-scoped roles through the portal API, 
      // never directly from the client.
      allow.group('admin'),
    ]),

  // ── Reference data: tenants, brands, portal memberships, categories, API keys ─
  RefDataEvent: a.model({
    pK: a.string().required(),   // BRAND#<id> | TENANT#<id> | CATEGORY#<id>
    sK: a.string().required(),   // PROFILE | BRAND#<brandId> | MEMBERSHIP#EMAIL#<email> | OFFER#<ulid> | APIKEY#<keyId> | STORE#<storeId>
    eventType: a.string(),
    status: a.string(),          // ACTIVE | INACTIVE | REVOKED | GRACE
    primaryCat: a.string(),      // brand | tenant | tenant_brand | portal_membership | category
    subCategory: a.string(),     // business-specific subtype such as grocery | travel | fuel
    desc: a.json(),              // entity-specific fields
    createdAt: a.datetime(),
    updatedAt: a.datetime(),
    version: a.integer(),
    tenantId: a.string(),        // tenant profile / tenant brand / portal membership
    brandId: a.string(),         // brand profile / tenant brand / portal membership / API key
    roleKey: a.string(),         // portal membership only — tenant_admin | brand_admin | editor | reader
    subjectEmail: a.string(),    // portal membership lookup by invited user's email
    // API key records only — top-level for GSI lookup by keyId
    keyId: a.string(),           // APIKEY records: ULID key identifier
    logoUrl: a.string(),         // brand profile: CDN URL after content validation
    bannerUrl: a.string(),       // brand profile: CDN URL after content validation
    offerImageUrl: a.string(),   // brand profile: CDN URL after content validation
  })
    .identifier(['pK', 'sK'])
    .secondaryIndexes(index => [
      index('primaryCat').sortKeys(['subCategory']).queryField('refDataByCategory'),
      index('status').sortKeys(['primaryCat']).queryField('refDataByStatus'),
      index('tenantId').sortKeys(['sK']).queryField('refDataByTenant'),
      index('subjectEmail').sortKeys(['tenantId']).queryField('refDataBySubjectEmail'),
      index('keyId').queryField('refDataByKeyId'),   // used by api-key-auth.ts to look up API keys
      index('brandId').queryField('refDataByBrand'), // used by business portal for profile lookups
    ])

    .authorization(allow => [
      allow.authenticated().to(['read']),
      allow.group('admin'),
    ]),

  // ── Lambda-managed operational records: scan index, audit logs, reminders ─
  AdminDataEvent: a.model({
    pK: a.string().required(),   // SCAN#<secondaryULID> | AUDIT#<actor> | NEWSLETTER#... | REMINDER#<permULID>
    sK: a.string().required(),   // <permULID> | LOG#<iso>#<ulid> | SENT#... 
    eventType: a.string(),
    status: a.string(),
    desc: a.json(),              // SCAN: card index payload; AUDIT: structured log; reminder sent logs omit desc
    createdAt: a.datetime(),
    updatedAt: a.datetime(),
  })
    .identifier(['pK', 'sK'])
    .authorization(allow => [
      // AdminDataEvent is never accessed through AppSync by client apps.
      // All Lambda access is via direct DynamoDB IAM grants in backend.ts.
      // Restricting AppSync access to the admin group only (no regular users).
      allow.group('admin'),
    ]),

  // ── GraphQL mutations (Lambda-backed) ─────────────────────────────────────

  // Loyalty cards
  addLoyaltyCard: a.mutation()
    .arguments({
      brandId: a.string().required(),
      cardNumber: a.string().required(),
      cardLabel: a.string(),
      isCustom: a.boolean(),
      customBrandName: a.string(),
      customBrandColor: a.string(),
      isDefault: a.boolean(),   // defaults to true if first card for brand, else false
      barcodeType: a.string(),
      storeId: a.string(),
      attributionBrandId: a.string(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  removeLoyaltyCard: a.mutation()
    .arguments({ cardSK: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  setDefaultCard: a.mutation()
    .arguments({
      cardSK: a.string().required(),   // CARD#<brandId>#<cardNumber>
      brandId: a.string().required(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // Brand communication opt-in — separate from holding a loyalty card.
  // Controls whether the brand can push offers, newsletters, catalogues, notifications.
  subscribeToOffers: a.mutation()
    .arguments({ brandId: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  unsubscribeFromOffers: a.mutation()
    .arguments({ brandId: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  updateIdentity: a.mutation()
    .arguments({
      globalSnoozeStart: a.string(),
      globalSnoozeEnd: a.string(),
      lastActiveHour: a.integer(),
      displayName: a.string(),
      email: a.string(),
      phone: a.string(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  rotateQR: a.mutation()
    .arguments({})
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // Idempotent token refresh: returns current secondaryULID if still valid,
  // rotates if expired, or creates fresh if no IDENTITY record exists.
  // Always returns serverTime so the client can correct clock skew.
  getOrRefreshIdentity: a.mutation()
    .arguments({})
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // Gift cards (PIN stored on device only — never sent here)
  addGiftCard: a.mutation()
    .arguments({
      brandName: a.string().required(),
      brandColor: a.string(),
      cardNumber: a.string().required(),
      cardLabel: a.string(),
      balance: a.float(),
      currency: a.string(),
      expiryDate: a.string(),        // ISO 8601
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  removeGiftCard: a.mutation()
    .arguments({ cardSK: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  updateGiftCardBalance: a.mutation()
    .arguments({ cardSK: a.string().required(), balance: a.float().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // Invoices
  addInvoice: a.mutation()
    .arguments({
      supplier: a.string().required(),
      amount: a.float().required(),
      dueDate: a.string().required(),   // ISO 8601
      invoiceNumber: a.string(),
      category: a.string(),
      notes: a.string(),
      currency: a.string(),
      brandId: a.string(),
      linkedSubscriptionSk: a.string(),
      providerId: a.string(),
      billingPeriod: a.string(),
      invoiceType: a.string(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  updateInvoiceStatus: a.mutation()
    .arguments({
      invoiceSK: a.string().required(),
      status: a.string().required(),    // paid | cancelled
      paidDate: a.string(),             // ISO 8601 — required when status = paid
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  removeInvoice: a.mutation()
    .arguments({ invoiceSK: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // Receipts
  addReceipt: a.mutation()
    .arguments({
      merchant: a.string().required(),
      amount: a.float().required(),
      purchaseDate: a.string().required(),
      category: a.string(),
      notes: a.string(),
      warrantyExpiry: a.string(),
      items: a.json(),                  // [{name, price, quantity}]
      photoKey: a.string(),             // local device file path
      loyaltyCardSK: a.string(),
      currency: a.string(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  removeReceipt: a.mutation()
    .arguments({ receiptSK: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  markNewsletterRead: a.mutation()
    .arguments({ newsletterSK: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // ── Granular subscription management ─────────────────────────────────────
  // Extends the coarse subscribe/unsubscribe mutations with per-channel controls.
  updateSubscription: a.mutation()
    .arguments({
      brandId: a.string().required(),
      offers: a.boolean(),
      newsletters: a.boolean(),
      reminders: a.boolean(),
      catalogues: a.boolean(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  snoozeOffers: a.mutation()
    .arguments({
      brandId: a.string(),
      until: a.string(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // User-level notification preferences (reminder toggles)
  updatePreferences: a.mutation()
    .arguments({ reminders: a.json().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // ── Payment routing (Patent Claims 19–22) ─────────────────────────────────
  // Called from the app when the user approves or declines a brand payment request.
  // BeboCard relays the result (+ payment token on approval) to the brand's webhook.
  // The actual payment transaction executes exclusively between the user's device
  // (Apple Pay / Google Pay) and the brand's payment processor — BeboCard never
  // touches payment credentials or settlement.
  respondToCheckout: a.mutation()
    .arguments({
      orderId: a.string().required(),
      approved: a.boolean().required(),
      paymentToken: a.string(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // ── Consent-Gated Identity Release (Patent Claims 25–26) ─────────────────
  // Called from the app after biometric approval. The user specifies exactly
  // which requested fields they approve — partial approval is allowed.
  // BeboCard reads identity values from UserDataEvent and relays only the
  // approved fields to the brand's consent webhook.
  // ── Subscription Revocation Proxy (Patent Claims 27–29) ──────────────────
  // Called from the app when the user cancels a recurring charge.
  // BeboCard relays the cancellation to the brand's webhook.
  cancelRecurring: a.mutation()
    .arguments({
      subId: a.string().required(),
      brandId: a.string().required(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  addManualSubscription: a.mutation()
    .arguments({
      brandName: a.string().required(),
      productName: a.string().required(),
      amount: a.float().required(),
      currency: a.string().required(),
      frequency: a.string().required(),
      nextBillingDate: a.string().required(),
      category: a.string().required(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  respondToConsent: a.mutation()
    .arguments({
      requestId: a.string().required(),
      approvedFields: a.string().array().required(),   // [] = full denial
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // ── Tracking Correlation Severance (Patent Claims 75–86) ─────────────────
  // User controls how frequently their secondaryULID (barcode-facing ID) rotates.
  // Rotation severs the link between past and future brand scans — brands cannot
  // correlate visits across a rotation boundary.
  // Frequencies: 'every_scan' | 'every_24h' | 'every_7d' | 'manual'
  setRotationFrequency: a.mutation()
    .arguments({ frequency: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // ── Enrollment Marketplace (Patent Claims 65–72) ──────────────────────────
  // User responds to a brand's enrollment offer. On acceptance, BeboCard
  // generates a pseudonymous email alias and delivers it to the brand's webhook.
  respondToEnrollment: a.mutation()
    .arguments({
      enrollmentId: a.string().required(),
      accepted: a.boolean().required(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // User-initiated enrollment — taps "Join program" from app's brand profile page.
  // Brand receives enrollment offer FCM push and alias on acceptance.
  initiateEnrollment: a.mutation()
    .arguments({ brandId: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // ── Gift Card Marketplace (Patent Claims 72–74) ───────────────────────────────
  // User purchases a gift card from a brand's catalog.
  // Creates a pending order record; brand delivers via POST /gift-card/deliver.
  purchaseGiftCard: a.mutation()
    .arguments({
      brandId: a.string().required(),
      catalogItemId: a.string().required(),
      denomination: a.float().required(),
      currency: a.string().required(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // User requests a balance refresh for an existing gift card.
  // card-manager calls the brand's balance webhook and updates UserDataEvent.
  syncGiftCardBalance: a.mutation()
    .arguments({
      cardSK: a.string().required(),   // sK of the GIFTCARD# record in UserDataEvent
      brandId: a.string().required(),
    })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // Gift Card Marketplace v2 (Stripe-integrated)
  purchaseForSelf: a.mutation()
    .arguments({
      brandId: a.string().required(),
      skuId: a.string().required(),
      denomination: a.float().required(),
      currency: a.string(),
    })
    .returns(a.json())
    .handler(a.handler.function(giftCardHandlerFn))
    .authorization(allow => [allow.authenticated()]),

  purchaseAsGift: a.mutation()
    .arguments({
      brandId: a.string().required(),
      skuId: a.string().required(),
      denomination: a.float().required(),
      currency: a.string(),
      recipientEmail: a.string().required(),
      senderDisplayName: a.string(),
      message: a.string(),
    })
    .returns(a.json())
    .handler(a.handler.function(giftCardHandlerFn))
    .authorization(allow => [allow.authenticated()]),

  listYourGiftCardForSale: a.mutation()
    .arguments({
      cardSK: a.string().required(),
      askingPrice: a.float().required(),
      currency: a.string(),
      sellerNote: a.string(),
    })
    .returns(a.json())
    .handler(a.handler.function(giftCardHandlerFn))
    .authorization(allow => [allow.authenticated()]),

  purchaseResoldCard: a.mutation()
    .arguments({ resaleId: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(giftCardHandlerFn))
    .authorization(allow => [allow.authenticated()]),

  withdrawBalance: a.mutation()
    .arguments({ amount: a.float().required(), currency: a.string() })
    .returns(a.json())
    .handler(a.handler.function(giftCardHandlerFn))
    .authorization(allow => [allow.authenticated()]),

  linkStripeAccount: a.mutation()
    .arguments({ stripeAccountId: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // ── SMB Loyalty-as-a-Service stamp cards (Phase 11) ──────────────────────
  // User-facing read access to their stamp card state. Mutations are performed
  // by brand backends via the SMB REST API (smb-handler Lambda).

  getStampCard: a.query()
    .arguments({ brandId: a.string().required() })
    .returns(a.json())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  listStampCards: a.query()
    .arguments({})
    .returns(a.json().array())
    .handler(a.handler.function(cardManagerFn))
    .authorization(allow => [allow.authenticated()]),

  // ── Geofencing & push notifications ───────────────────────────────────────

  // App reports a geofence entry event → backend applies personalisation rules
  // and fires FCM/APNs push notification to this device.
  // Device sends only geofenceId + secondaryULID — server resolves identity and
  // brand/store from its own records. No explicit location data from the device.
  reportGeofenceEntry: a.mutation()
    .arguments({
      secondaryULID: a.string().required(),  // server resolves → permULID via AdminDataEvent
      geofenceId: a.string().required(),     // server parses → brandId + storeId
      entryTime: a.string().required(),      // ISO 8601
    })
    .returns(a.string())
    .handler(a.handler.function(geofenceHandlerFn))
    .authorization(allow => [allow.authenticated()]),

  // Device token registration — enables server-side push
  registerDeviceToken: a.mutation()
    .arguments({
      token: a.string().required(),
      platform: a.string().required(),   // 'prod' | 'dev'
      permULID: a.string(),
    })
    .returns(a.string())
    .handler(a.handler.function(geofenceHandlerFn))
    .authorization(allow => [allow.authenticated()]),

  unregisterDeviceToken: a.mutation()
    .arguments({ token: a.string().required(), permULID: a.string().required() })
    .returns(a.string())
    .handler(a.handler.function(geofenceHandlerFn))
    .authorization(allow => [allow.authenticated()]),

  // Query nearest store locations for a brand (used when registering geofences)
  getNearbyStores: a.query()
    .arguments({
      brandId: a.string().required(),
      lat: a.float().required(),
      lng: a.float().required(),
      radiusKm: a.float().required(),
      limit: a.integer().required(),
    })
    .returns(a.json().array())
    .handler(a.handler.function(geofenceHandlerFn))
    .authorization(allow => [allow.authenticated()]),

  // GDPR Data Portability & Erasure (P2-5)
  recordBipaConsent: a.mutation()
    .arguments({
      textVersion: a.string().required(),
    })
    .returns(a.boolean())
    .handler(a.handler.function(consentHandlerFn))
    .authorization(allow => [allow.authenticated()]),

  startDataExport: a.mutation()
    .arguments({})
    .returns(a.string())
    .handler(a.handler.function(exporterFn))
    .authorization(allow => [allow.authenticated()]),

  deleteUserAccount: a.mutation()
    .arguments({})
    .returns(a.string())
    .handler(a.handler.function(exporterFn))
    .authorization(allow => [allow.authenticated()]),

  trackEngagement: a.mutation()
    .arguments({
      eventType: a.string().required(),
      targetId: a.string().required(),
      source: a.string(),
      metadata: a.json(),
      permULID: a.string().required(),
    })
    .returns(a.json())
    .handler(a.handler.function(clickTrackingHandlerFn))
    .authorization(allow => [allow.authenticated()]),
});

export type Schema = ClientSchema<typeof schema>;

export const data = defineData({
  schema,
  authorizationModes: {
    defaultAuthorizationMode: 'userPool',
    apiKeyAuthorizationMode: { expiresInDays: 30 },
  },
});
