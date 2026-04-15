import { z } from 'zod';

// ── Common field types ────────────────────────────────────────────────────────

const isoDate = z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'Expected YYYY-MM-DD');

const httpsUrl = z
  .string()
  .url()
  .max(512)
  .refine(u => u.startsWith('https://'), 'URL must use HTTPS')
  .optional();

const safeText = (max: number) =>
  z.string().trim().min(1).max(max).refine(
    v => !/[<>]/.test(v),
    'HTML tags not allowed in this field',
  );

const segmentBucket = z.enum(['<100', '100-200', '200-500', '500+']);
const segmentFrequency = z.enum(['new', 'occasional', 'frequent', 'lapsed']);

// ── Brand-API schemas ─────────────────────────────────────────────────────────

export const OfferInputSchema = z.object({
  title:          safeText(100),
  description:    safeText(1000).optional().nullable(),
  imageUrl:       httpsUrl.nullable(),
  validFrom:      isoDate,
  validTo:        isoDate,
  targetStoreIds: z.array(z.string().max(64)).max(100).optional().nullable(),
  minVisitCount:  z.number().int().min(0).max(9999).optional().nullable(),
  category:       z.string().max(64).optional().nullable(),
  status:         z.enum(['DRAFT', 'SCHEDULED', 'ACTIVE', 'PAUSED', 'EXPIRED', 'ARCHIVED']).default('DRAFT'),
  scheduledFor:   z.string().datetime().optional().nullable(),
  campaignType:   z.enum(['untargeted', 'acquisition', 'loyalty_reward', 're_engagement', 'seasonal', 'clearance']).default('untargeted'),
}).refine((d: { validFrom: string; validTo: string }) => d.validTo >= d.validFrom, { message: 'validTo must be on or after validFrom' });

export const NewsletterInputSchema = z.object({
  subject:  safeText(200),
  bodyHtml: z.string().trim().min(1).max(50_000),
  imageUrl: httpsUrl.nullable(),
  ctaUrl:   httpsUrl.nullable(),
  ctaLabel: safeText(60).optional().nullable(),
});

export const CatalogueInputSchema = z.object({
  title:           safeText(100),
  description:     safeText(1000).optional().nullable(),
  imageUrl:        httpsUrl.nullable(),
  headerImageUrl:  httpsUrl.nullable(),
  validFrom:       isoDate.optional().nullable(),
  validTo:         isoDate.optional().nullable(),
  ctaLabel:        safeText(60).optional().nullable(),
  ctaUrl:          httpsUrl.nullable(),
  cataloguePdfKey: z.string().max(512).optional().nullable(),
  targetSegments:  z.object({
    spendBuckets: z.array(segmentBucket).max(4).optional().nullable(),
    visitFrequencies: z.array(segmentFrequency).max(4).optional().nullable(),
  }).optional().nullable(),
  items: z.array(z.object({
    name: safeText(120),
    price: z.number().min(0).max(1_000_000).optional().nullable(),
    imageUrl: httpsUrl.nullable(),
    ctaUrl: httpsUrl.nullable(),
  })).max(500).optional().nullable(),
}).refine(
  d => !d.validFrom || !d.validTo || d.validTo >= d.validFrom,
  { message: 'validTo must be on or after validFrom' },
);

export const StoreInputSchema = z.object({
  name:       safeText(100),
  address:    safeText(200),
  suburb:     safeText(60),
  state:      z.string().max(10),
  postcode:   z.string().regex(/^\d{4}$/, 'Expected 4-digit postcode'),
  country:    z.string().length(2).default('AU'),
  latitude:   z.number().min(-90).max(90),
  longitude:  z.number().min(-180).max(180),
  radiusKm:   z.number().min(0.05).max(5).default(0.2),
  phone:      z.string().max(20).optional().nullable(),
  hours:      z.record(z.string().max(50)).optional().nullable(),
});

export const BrandProfileSchema = z.object({
  brandName:        safeText(100),
  brandColor:       z.string().regex(/^#[0-9a-fA-F]{6}$/, 'Expected #RRGGBB'),
  logoUrl:          httpsUrl.nullable(),
  description:      safeText(1000).optional().nullable(),
  barcodeType:      z.string().max(20).default('EAN13'),
  isWidgetEnabled:  z.boolean().default(false),
  widgetConfig: z.object({
    iframeUrl:        httpsUrl.nullable(),
    supportedActions: z.array(z.enum(['invoice', 'giftcard', 'subscription'])).optional().nullable(),
  }).optional().nullable(),
  loyaltyDefaults: z.object({
    supportsMultipleCards: z.boolean().default(false),
    pointsName:            safeText(30).optional().nullable(),
  }).optional().nullable(),
});

export const SubscriptionCatalogInputSchema = z.object({
  providerName: safeText(100),
  category:     z.enum(['streaming', 'music', 'productivity', 'telecom', 'utilities', 'insurance', 'gaming', 'health', 'other']),
  invoiceType:  z.enum(['SUBSCRIPTION', 'RECURRING_INVOICE', 'BOTH']).default('SUBSCRIPTION'),
  plans: z.array(z.object({
    planName:  safeText(80),
    amount:    z.number().min(0).max(10_000_000),
    frequency: z.enum(['weekly', 'fortnightly', 'monthly', 'quarterly', 'annually']),
    currency:  z.string().length(3).default('AUD'),
    features:  z.array(safeText(120)).max(20).optional().nullable(),
  })).min(1).max(20),
  websiteUrl:  httpsUrl.nullable(),
  logoUrl:     httpsUrl.nullable(),
  cancelUrl:   httpsUrl.nullable(),
  portalUrl:   httpsUrl.nullable(),
  description: safeText(500).optional().nullable(),
  region:      z.string().max(10).default('AU'),
  hasLinking:  z.boolean().default(false),
});

export const AddLoyaltyCardSchema = z.object({
  brandId:          z.string().max(64).optional().nullable(),
  cardNumber:       z.string().trim().min(1).max(50),
  cardLabel:        safeText(60).optional().nullable(),
  isCustom:         z.boolean().optional().nullable(),
  customBrandName:  safeText(80).optional().nullable(),
  customBrandColor: z.string().regex(/^#[0-9a-fA-F]{6}$/, 'Expected #RRGGBB').optional().nullable(),
  isDefault:        z.boolean().optional().nullable(),
  barcodeType:      z.string().max(20).optional().nullable(),
  storeId:          z.string().max(64).optional().nullable(),
  attributionBrandId: z.string().max(64).optional().nullable(),
}).refine(
  d => d.isCustom ? !!d.customBrandName : !!d.brandId,
  { message: 'brandId required for standard cards; customBrandName required for custom cards' },
);

export const AddGiftCardSchema = z.object({
  brandName:   safeText(80),
  brandColor:  z.string().regex(/^#[0-9a-fA-F]{6}$/).optional().nullable(),
  balance:     z.number().min(0).max(100_000),
  currency:    z.string().length(3).default('AUD'),
  expiryDate:  isoDate.optional().nullable(),
  cardNumber:  z.string().max(50).optional().nullable(),
  pinCode:     z.string().max(20).optional().nullable(),
});

export const AddInvoiceSchema = z.object({
  supplier:              safeText(100),
  amount:                z.number().min(0).max(10_000_000),
  dueDate:               isoDate.optional().nullable(),
  invoiceNumber:         safeText(50).optional().nullable(),
  category:              safeText(60).optional().nullable(),
  notes:                 safeText(500).optional().nullable(),
  linkedSubscriptionSk:  z.string().max(200).optional().nullable(),
  providerId:            safeText(80).optional().nullable(),
  billingPeriod:         z.string().max(25).optional().nullable(),
  invoiceType:           z.enum(['ONE_TIME', 'SUBSCRIPTION_BILLING']).default('ONE_TIME'),
});

export const AddReceiptSchema = z.object({
  merchant:       safeText(100),
  amount:         z.number().min(0).max(10_000_000),
  purchaseDate:   isoDate,
  currency:       z.string().length(3).default('AUD'),
  category:       safeText(60).optional().nullable(),
  notes:          safeText(500).optional().nullable(),
  warrantyExpiry: isoDate.optional().nullable(),
  items:          z.array(z.unknown()).max(200).optional().nullable(),
  photoKey:       z.string().max(512).optional().nullable(),
});
