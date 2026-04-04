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
  description:    safeText(1000).optional(),
  imageUrl:       httpsUrl,
  validFrom:      isoDate,
  validTo:        isoDate,
  targetStoreIds: z.array(z.string().max(64)).max(100).optional(),
  minVisitCount:  z.number().int().min(0).max(9999).optional(),
  category:       z.string().max(64).optional(),
}).refine((d: { validFrom: string; validTo: string }) => d.validTo >= d.validFrom, { message: 'validTo must be on or after validFrom' });

export const NewsletterInputSchema = z.object({
  subject:  safeText(200),
  bodyHtml: z.string().trim().min(1).max(50_000),  // HTML allowed in body only
  imageUrl: httpsUrl,
  ctaUrl:   httpsUrl,
  ctaLabel: safeText(60).optional(),
});

export const CatalogueInputSchema = z.object({
  title:           safeText(100),
  description:     safeText(1000).optional(),
  imageUrl:        httpsUrl,
  headerImageUrl:  httpsUrl,
  validFrom:       isoDate.optional(),
  validTo:         isoDate.optional(),
  ctaLabel:        safeText(60).optional(),
  ctaUrl:          httpsUrl,
  cataloguePdfKey: z.string().max(512).optional(),
  targetSegments:  z.object({
    spendBuckets: z.array(segmentBucket).max(4).optional(),
    visitFrequencies: z.array(segmentFrequency).max(4).optional(),
  }).optional(),
  items: z.array(z.object({
    name: safeText(120),
    price: z.number().min(0).max(1_000_000).optional(),
    imageUrl: httpsUrl,
    ctaUrl: httpsUrl,
  })).max(500).optional(),
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
  phone:      z.string().max(20).optional(),
  hours:      z.record(z.string().max(50)).optional(),
});

// ── Card-manager schemas ──────────────────────────────────────────────────────

export const AddLoyaltyCardSchema = z.object({
  brandId:          z.string().max(64).optional(),
  cardNumber:       z.string().trim().min(1).max(50),
  cardLabel:        safeText(60).optional(),
  isCustom:         z.boolean().optional(),
  customBrandName:  safeText(80).optional(),
  customBrandColor: z.string().regex(/^#[0-9a-fA-F]{6}$/, 'Expected #RRGGBB').optional(),
  isDefault:        z.boolean().optional(),
}).refine(
  d => d.isCustom ? !!d.customBrandName : !!d.brandId,
  { message: 'brandId required for standard cards; customBrandName required for custom cards' },
);

export const AddGiftCardSchema = z.object({
  brandName:   safeText(80),
  brandColor:  z.string().regex(/^#[0-9a-fA-F]{6}$/).optional(),
  balance:     z.number().min(0).max(100_000),
  currency:    z.string().length(3).default('AUD'),
  expiryDate:  isoDate.optional(),
  cardNumber:  z.string().max(50).optional(),
  pinCode:     z.string().max(20).optional(),
});

export const AddInvoiceSchema = z.object({
  supplier:       safeText(100),
  amount:         z.number().min(0).max(10_000_000),
  dueDate:        isoDate.optional(),
  invoiceNumber:  safeText(50).optional(),
  category:       safeText(60).optional(),
  notes:          safeText(500).optional(),
});

export const AddReceiptSchema = z.object({
  merchant:       safeText(100),
  amount:         z.number().min(0).max(10_000_000),
  purchaseDate:   isoDate,
  currency:       z.string().length(3).default('AUD'),
  category:       safeText(60).optional(),
  notes:          safeText(500).optional(),
  warrantyExpiry: isoDate.optional(),
  items:          z.array(z.unknown()).max(200).optional(),
  photoKey:       z.string().max(512).optional(),
});
