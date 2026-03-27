import { type ClientSchema, a, defineData } from '@aws-amplify/backend';
import { cardManagerFn } from '../functions/card-manager/resource';
import { geofenceHandlerFn } from '../functions/geofence-handler/resource';

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
    secondaryULID: a.string(),   // IDENTITY only — QR-facing rotating ID (top-level for fast reads)
    rotatesAt: a.string(),       // IDENTITY only — ISO 8601, when secondaryULID should next be rotated
    createdAt: a.datetime(),
    updatedAt: a.datetime(),
  })
    .identifier(['pK', 'sK'])
    .secondaryIndexes(index => [
      index('primaryCat').sortKeys(['createdAt']).queryField('userDataByCategory'),
    ])
    .authorization(allow => [
      allow.owner(),
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

  rotateQR: a.mutation()
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
      brandId:     a.string().required(),
      offers:      a.boolean(),
      newsletters: a.boolean(),
      reminders:   a.boolean(),
      catalogues:  a.boolean(),
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
});

export type Schema = ClientSchema<typeof schema>;

export const data = defineData({
  schema,
  authorizationModes: {
    defaultAuthorizationMode: 'userPool',
    apiKeyAuthorizationMode: { expiresInDays: 30 },
  },
});
