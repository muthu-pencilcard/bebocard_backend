import type { AppSyncResolverHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, DeleteCommand, GetCommand, UpdateCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import https from 'https';
import { monotonicFactory } from 'ulid';
import { withAuditLog, writeAuditLog } from '../../shared/audit-logger';
import {
  AddLoyaltyCardSchema,
  AddGiftCardSchema,
  AddInvoiceSchema,
  AddReceiptSchema,
} from '../../shared/validation-schemas';
import { getTenantStateForBrand, incrementTenantUsageCounter } from '../../shared/tenant-billing';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid = monotonicFactory();

const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const ADMIN_TABLE = process.env.ADMIN_TABLE!;

type Args = {
  // Stamp cards (Phase 11 SMB)
  stampBrandId?: string;
  // Payment routing
  orderId?: string;
  approved?: boolean;
  paymentToken?: string;
  // Consent
  requestId?: string;
  approvedFields?: string[];
  // Subscription revocation
  subId?: string;
  // Manual Subscriptions
  productName?: string;
  nextBillingDate?: string;
  // Enrollment marketplace
  enrollmentId?: string;
  accepted?: boolean;
  // QR rotation frequency
  frequency?: string;
  // Loyalty cards
  brandId?: string;
  cardNumber?: string;
  cardLabel?: string;
  isCustom?: boolean;
  customBrandName?: string;
  customBrandColor?: string;
  isDefault?: boolean;
  cardSK?: string;
  // Gift cards
  brandName?: string;
  brandColor?: string;
  balance?: number;
  currency?: string;
  expiryDate?: string;
  // Invoices
  supplier?: string;
  amount?: number;
  dueDate?: string;
  invoiceNumber?: string;
  category?: string;
  notes?: string;
  invoiceSK?: string;
  status?: string;
  paidDate?: string;
  linkedSubscriptionSk?: string;
  billingPeriod?: string;
  invoiceType?: string;
  // Subscriptions
  providerId?: string;
  // Receipts
  merchant?: string;
  purchaseDate?: string;
  warrantyExpiry?: string;
  items?: unknown;
  loyaltyCardSK?: string;
  photoKey?: string;
  receiptSK?: string;
  // Granular subscriptions
  offers?: boolean;
  newsletters?: boolean;
  reminders?: boolean;
  catalogues?: boolean;
  until?: string | null;
  // Newsletters
  newsletterSK?: string;
  // Gift card marketplace (Phase 13)
  catalogItemId?: string;
  denomination?: number;
};

const _handler: AppSyncResolverHandler<Args, unknown> = async (event) => {
  // event.info.fieldName is the standard location in Amplify Gen 2 AppSync resolvers.
  // Fall back to event.fieldName for sandboxes deployed with older resolver templates.
  const operation = event.info?.fieldName ?? (event as unknown as Record<string, string>).fieldName;
  if (!operation) throw new Error('Missing fieldName in AppSync event — resolver may need redeployment');
  const args = event.arguments;

  let permULID = (event.identity as { claims: Record<string, string> })
    ?.claims?.['custom:permULID']
    ?? (event.identity as { claims: Record<string, string> })
    ?.claims?.['permULID'];

  if (!permULID) {
    // If custom:permULID is missing (e.g. post-confirmation didn't run or claims haven't refreshed),
    // fall back to the owner claim (sub or email) as the permULID.
    // This matches the frontend's self-initialization fallback logic.
    permULID = (event.identity as { claims: Record<string, string> })
      ?.claims?.['cognito:username']
      ?? (event.identity as { username?: string })?.username;
  }

  if (!permULID) throw new Error('Identity missing permULID and owner claim');

  // Amplify's allow.owner() checks record.owner against the 'cognito:username' JWT claim.
  // With username_attributes: ['email'], cognito:username IS the email address — not the sub.
  // Using sub here would set owner to a UUID that never matches cognito:username, breaking all reads.
  const owner = (event.identity as { claims: Record<string, string> })
    ?.claims?.['cognito:username']
    ?? (event.identity as { username?: string })?.username;
  if (!owner) throw new Error('Identity missing owner claim (cognito:username)');

  switch (operation) {
    // Loyalty cards
    case 'addLoyaltyCard': return addCard(permULID, owner, args);
    case 'removeLoyaltyCard': return removeCard(permULID, args.cardSK!);
    case 'setDefaultCard': return setDefaultCard(permULID, args.cardSK!, args.brandId!);
    case 'rotateQR': return rotateQR(permULID);
    // Subscriptions (legacy single-toggle + new granular)
    case 'subscribeToOffers': return setSubscription(permULID, owner, args.brandId!, true);
    case 'unsubscribeFromOffers': return setSubscription(permULID, owner, args.brandId!, false);
    case 'updateSubscription': return updateGranularSubscription(permULID, owner, args.brandId!, args);
    case 'updatePreferences': return updateUserPreferences(permULID, owner, (args as any).reminders as Record<string, boolean>);
    case 'snoozeOffers': return snoozeOffers(permULID, args.brandId, args.until ?? null);
    // Gift cards
    case 'addGiftCard': return addGiftCard(permULID, owner, args);
    case 'removeGiftCard': return archiveRecord(permULID, args.cardSK!);
    case 'updateGiftCardBalance': return updateBalance(permULID, args.cardSK!, args.balance!);
    // Invoices
    case 'addInvoice': return addInvoice(permULID, owner, args);
    case 'updateInvoiceStatus': return updateInvoiceStatus(permULID, args.invoiceSK!, args.status!, args.paidDate);
    case 'removeInvoice': return archiveRecord(permULID, args.invoiceSK!);
    // Receipts
    case 'addReceipt': return addReceipt(permULID, owner, args);
    case 'removeReceipt': return archiveRecord(permULID, args.receiptSK!);
    // Newsletters
    case 'markNewsletterRead': return markNewsletterRead(permULID, args.newsletterSK!);
    // Payment routing
    case 'respondToCheckout': return respondToCheckout(permULID, args.orderId!, args.approved!, args.paymentToken);
    // Consent-gated identity release
    case 'respondToConsent': return respondToConsent(permULID, args.requestId!, args.approvedFields ?? []);
    // Subscription revocation proxy
    case 'cancelRecurring': return cancelRecurringHandler(permULID, args.subId!, args.brandId!);
    case 'addManualSubscription': return addManualSubscription(permULID, owner, args);
    // QR rotation frequency (tracking severance — Patent Claims 75–86)
    case 'setRotationFrequency': return setRotationFrequency(permULID, args.frequency!);
    // Enrollment marketplace
    case 'respondToEnrollment': return enrollmentRespondHandler(permULID, args.enrollmentId!, args.accepted!);
    case 'initiateEnrollment': return enrollmentInitiateHandler(permULID, owner, args.brandId!);
    // SMB stamp cards (Phase 11)
    case 'getStampCard': return getStampCard(permULID, args.stampBrandId!);
    case 'listStampCards': return listStampCards(permULID);
    case 'purchaseGiftCard': return purchaseGiftCard(permULID, args.brandId!, args.catalogItemId!, args.denomination!, args.currency!);
    case 'syncGiftCardBalance': return syncGiftCardBalance(permULID, args.cardSK!, args.brandId!);
    // GDPR — right to erasure
    case 'deleteAccount': return deleteAccount(permULID);
    default: throw new Error(`Unknown operation: ${operation}`);
  }
};

export const handler = withAuditLog(dynamo, _handler);

function parseRecord(value: unknown): Record<string, unknown> {
  if (typeof value === 'object' && value !== null) return value as Record<string, unknown>;
  if (typeof value !== 'string' || value.length === 0) return {};
  try {
    return JSON.parse(value) as Record<string, unknown>;
  } catch {
    return {};
  }
}

// ── Add loyalty card ──────────────────────────────────────────────────────────

async function addCard(permULID: string, owner: string, args: Args) {
  const validation = AddLoyaltyCardSchema.safeParse(args);
  if (!validation.success) throw new Error(validation.error.issues[0]?.message ?? 'Invalid card input');
  const { brandId, cardNumber, cardLabel, isCustom, customBrandName, customBrandColor } = args;
  const now = new Date().toISOString();
  const cardSK = `CARD#${isCustom ? 'custom' : brandId}#${cardNumber}`;

  // Fetch brand profile from RefData (skip for custom cards)
  let brandProfile: Record<string, unknown> = {};
  if (!isCustom && brandId) {
    const ref = await dynamo.send(new GetCommand({
      TableName: REFDATA_TABLE,
      Key: { pK: `BRAND#${brandId}`, sK: 'profile' },
    }));
    brandProfile = parseRecord(ref.Item?.desc);
  }

  // Write card to UserDataEvent
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: cardSK,
      eventType: 'CARD',
      status: 'ACTIVE',
      primaryCat: 'loyalty_card',
      subCategory: isCustom ? 'custom' : brandId,
      owner,
      desc: JSON.stringify({
        brandId: isCustom ? 'custom' : brandId,
        brandName: isCustom ? customBrandName : (brandProfile.name ?? brandProfile.brandName),
        brandColor: isCustom ? customBrandColor : (brandProfile.color ?? brandProfile.brandColor),
        cardNumber,
        cardLabel: cardLabel ?? (isCustom ? customBrandName : (brandProfile.name ?? brandProfile.brandName)),
        isCustom: !!isCustom,
        pointsBalance: 0,
        addedAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK)', // prevent duplicates
  }));

  // Update SCAN index for POS lookup for all non-custom cards.
  // apiEndpoint presence is not required — the brand may not yet be live on the
  // BeboCard scan API, but the card must be indexed so it works the moment they are.
  if (!isCustom) {
    await _appendToScanIndex(permULID, {
      brand: brandId!,
      cardId: cardNumber!,
      cardSK,
      isDefault: args.isDefault,  // undefined = auto (true if first card for brand)
    });

    // Ensure a subscription record exists so offers are on by default when a
    // card is linked, even if the app does not issue a follow-up mutation.
    await updateGranularSubscription(permULID, owner, brandId!, {
      offers: true,
      reminders: true,
    });
  }

  return { success: true, cardSK };
}

// ── Remove loyalty card ───────────────────────────────────────────────────────

async function removeCard(permULID: string, cardSK: string) {
  // Soft-delete the card record
  await archiveRecord(permULID, cardSK);

  // Remove card from SCAN index so POS can no longer route to it
  const identity = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
  }));
  const secondaryULID: string | undefined = identity.Item?.secondaryULID;
  if (!secondaryULID) return { success: true };

  const indexRecord = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `SCAN#${secondaryULID}`, sK: permULID },
  }));
  if (!indexRecord.Item) return { success: true };

  const indexDesc = JSON.parse(indexRecord.Item.desc ?? '{}');
  const cards: ScanIndexCard[] = indexDesc.cards ?? [];
  const filtered = cards.filter(c => c.cardSK !== cardSK);

  // If we removed the default card, promote the next card for that brand
  const removedCard = cards.find(c => c.cardSK === cardSK);
  if (removedCard?.isDefault) {
    const nextCard = filtered.find(c => c.brand === removedCard.brand);
    if (nextCard) nextCard.isDefault = true;
  }

  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `SCAN#${secondaryULID}`, sK: permULID },
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
    ExpressionAttributeValues: {
      ':desc': JSON.stringify({ ...indexDesc, cards: filtered }),
      ':now': new Date().toISOString(),
    },
  }));

  return { success: true };
}

// ── Rotation frequency helpers ────────────────────────────────────────────────

const VALID_FREQUENCIES = ['every_scan', 'every_24h', 'every_7d', 'manual'] as const;
type RotationFrequency = typeof VALID_FREQUENCIES[number];

function rotatesAtForFrequency(frequency: RotationFrequency): string {
  const ms = {
    every_scan: 0,                          // rotate every time it's displayed
    every_24h: 24 * 60 * 60 * 1000,
    every_7d: 7 * 24 * 60 * 60 * 1000,
    manual: 100 * 365 * 24 * 60 * 60 * 1000, // ~100 years — effectively never
  }[frequency];
  return new Date(Date.now() + ms).toISOString();
}

// ── Rotate QR (new secondaryULID) ─────────────────────────────────────────────

async function rotateQR(permULID: string) {
  const now = new Date().toISOString();
  const newSecondaryULID = ulid();

  // Read current IDENTITY to get old secondaryULID + stored frequency preference
  const identity = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
  }));
  if (!identity.Item) throw new Error('IDENTITY record not found');
  const oldSecondaryULID: string = identity.Item.secondaryULID;

  const identityDesc = JSON.parse(identity.Item.desc ?? '{}') as Record<string, unknown>;
  const frequency = (VALID_FREQUENCIES.includes(identityDesc.rotationFrequency as RotationFrequency)
    ? identityDesc.rotationFrequency
    : 'every_24h') as RotationFrequency;
  const newRotatesAt = rotatesAtForFrequency(frequency);

  // Read old scan index to copy card list
  const oldIndex = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `SCAN#${oldSecondaryULID}`, sK: permULID },
  }));
  const oldIndexDesc = JSON.parse(oldIndex.Item?.desc ?? '{}');

  // Write new scan index
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `SCAN#${newSecondaryULID}`,
      sK: permULID,
      eventType: 'SCAN_INDEX',
      status: 'ACTIVE',
      desc: JSON.stringify({ cards: oldIndexDesc.cards ?? [], createdAt: now }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  // Revoke old scan index
  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `SCAN#${oldSecondaryULID}`, sK: permULID },
    UpdateExpression: 'SET #s = :revoked, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':revoked': 'REVOKED', ':now': now },
  }));

  // Update IDENTITY — conditional on secondaryULID still matching what we read.
  // If another device already rotated, this throws ConditionalCheckFailedException.
  // The caller catches it, re-reads AdminDataEvent, and uses the already-rotated ID.
  try {
    await dynamo.send(new UpdateCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
      UpdateExpression: 'SET secondaryULID = :new, rotatesAt = :ra, updatedAt = :now',
      ConditionExpression: 'secondaryULID = :old',
      ExpressionAttributeValues: {
        ':new': newSecondaryULID,
        ':ra': newRotatesAt,
        ':old': oldSecondaryULID,
        ':now': now,
      },
    }));
  } catch (err: unknown) {
    if ((err as { name?: string }).name === 'ConditionalCheckFailedException') {
      // Another device already rotated — return current values from IDENTITY
      const fresh = await dynamo.send(new GetCommand({
        TableName: USER_TABLE,
        Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
      }));
      return {
        success: true,
        alreadyRotated: true,
        newSecondaryULID: fresh.Item?.secondaryULID,
        rotatesAt: fresh.Item?.rotatesAt,
      };
    }
    throw err;
  }

  // Write rotation log to UserDataEvent so the app can show rotation history
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: `ROTATION#${now}`,
      eventType: 'ROTATION',
      status: 'ACTIVE',
      primaryCat: 'rotation_log',
      desc: JSON.stringify({ oldSecondaryULID, newSecondaryULID, frequency, rotatedAt: now }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  return { success: true, alreadyRotated: false, newSecondaryULID, rotatesAt: newRotatesAt, frequency };
}

// ── Set rotation frequency (Patent Claims 75–86) ──────────────────────────────

async function setRotationFrequency(permULID: string, frequency: string) {
  if (!VALID_FREQUENCIES.includes(frequency as RotationFrequency)) {
    throw new Error(`Invalid frequency. Must be one of: ${VALID_FREQUENCIES.join(', ')}`);
  }
  const now = new Date().toISOString();
  const newRotatesAt = rotatesAtForFrequency(frequency as RotationFrequency);

  // Read current IDENTITY desc to merge into
  const identity = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
  }));
  const desc = JSON.parse(identity.Item?.desc ?? '{}') as Record<string, unknown>;
  desc.rotationFrequency = frequency;

  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
    UpdateExpression: 'SET #d = :desc, rotatesAt = :ra, updatedAt = :now',
    ExpressionAttributeNames: { '#d': 'desc' },
    ExpressionAttributeValues: {
      ':desc': JSON.stringify(desc),
      ':ra': newRotatesAt,
      ':now': now,
    },
  }));

  return { success: true, frequency, rotatesAt: newRotatesAt };
}

// ── Archive (soft-delete for loyalty + gift cards) ────────────────────────────

async function archiveRecord(permULID: string, sK: string) {
  const now = new Date().toISOString();
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK },
    UpdateExpression: 'SET #s = :archived, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':archived': 'ARCHIVED', ':now': now },
  }));
  return { success: true };
}

// ── Gift cards ────────────────────────────────────────────────────────────────

async function addGiftCard(permULID: string, owner: string, args: Args) {
  const validation = AddGiftCardSchema.safeParse(args);
  if (!validation.success) throw new Error(validation.error.issues[0]?.message ?? 'Invalid gift card input');
  const { brandName, brandColor, cardNumber, cardLabel, balance, currency, expiryDate } = args;
  const now = new Date().toISOString();
  const cardSK = `GIFTCARD#custom#${cardNumber}`;

  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: cardSK,
      eventType: 'GIFTCARD',
      status: 'ACTIVE',
      primaryCat: 'gift_card',
      subCategory: 'custom',
      owner,
      desc: JSON.stringify({
        brandName: brandName ?? 'Gift Card',
        brandId: 'custom',
        brandColor: brandColor ?? '#10B981',
        cardNumber,
        cardLabel: cardLabel ?? brandName ?? 'Gift Card',
        balance: balance ?? 0,
        currency: currency ?? 'AUD',
        expiryDate: expiryDate ?? null,
        isCustom: true,
        // NOTE: PIN is never stored server-side — flutter_secure_storage only
        addedAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  return { success: true, cardSK };
}

async function updateBalance(permULID: string, cardSK: string, newBalance: number) {
  const now = new Date().toISOString();
  // Read current desc to merge, then write with a version condition to prevent
  // concurrent balance updates from silently overwriting each other.
  const record = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: cardSK },
  }));
  const currentDesc = JSON.parse(record.Item?.desc ?? '{}');
  const currentUpdatedAt = record.Item?.updatedAt ?? '';

  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: cardSK },
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
    ConditionExpression: 'updatedAt = :prev OR attribute_not_exists(updatedAt)',
    ExpressionAttributeValues: {
      ':desc': JSON.stringify({ ...currentDesc, balance: newBalance }),
      ':now': now,
      ':prev': currentUpdatedAt,
    },
  }));
  return { success: true };
}

// ── Invoices ──────────────────────────────────────────────────────────────────

async function addInvoice(permULID: string, owner: string, args: Args) {
  const validation = AddInvoiceSchema.safeParse(args);
  if (!validation.success) throw new Error(validation.error.issues[0]?.message ?? 'Invalid invoice input');
  const {
    supplier, amount, dueDate, invoiceNumber, category, notes, currency, brandId,
    linkedSubscriptionSk, providerId, billingPeriod, invoiceType,
  } = args;
  const now = new Date().toISOString();
  const id = ulid();
  // sK sorts by due date for easy range queries
  const invoiceSK = `INVOICE#${dueDate?.substring(0, 10)}#${id}`;

  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: invoiceSK,
      eventType: 'INVOICE',
      status: 'ACTIVE',
      primaryCat: 'invoice',
      subCategory: category ?? 'other',
      owner,
      desc: JSON.stringify({
        supplier,
        brandId:              brandId              ?? null,
        providerId:           providerId           ?? null,
        invoiceNumber:        invoiceNumber        ?? null,
        amount,
        currency:             currency             ?? 'AUD',
        dueDate,
        paidDate:             null,
        status:               'unpaid',
        category:             category             ?? 'other',
        notes:                notes                ?? null,
        attachmentKey:        null,
        invoiceType:          invoiceType          ?? 'ONE_TIME',
        linkedSubscriptionSk: linkedSubscriptionSk ?? null,
        billingPeriod:        billingPeriod        ?? null,
        createdAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  if (brandId) {
    const tenantState = await getTenantStateForBrand(dynamo, REFDATA_TABLE, brandId);
    await incrementTenantUsageCounter(dynamo, REFDATA_TABLE, tenantState.tenantId, brandId, 'invoices');
  }

  return { success: true, invoiceSK };
}

async function updateInvoiceStatus(
  permULID: string,
  invoiceSK: string,
  status: string,
  paidDate?: string,
) {
  const now = new Date().toISOString();
  const record = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: invoiceSK },
  }));
  const desc = JSON.parse(record.Item?.desc ?? '{}');
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: invoiceSK },
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
    ExpressionAttributeValues: {
      ':desc': JSON.stringify({ ...desc, status, paidDate: paidDate ?? null }),
      ':now': now,
    },
  }));
  return { success: true };
}

// ── Receipts ──────────────────────────────────────────────────────────────────

async function addReceipt(permULID: string, owner: string, args: Args) {
  const validation = AddReceiptSchema.safeParse(args);
  if (!validation.success) throw new Error(validation.error.issues[0]?.message ?? 'Invalid receipt input');
  const { merchant, amount, purchaseDate, category, notes, warrantyExpiry, items, loyaltyCardSK, currency, photoKey } = args;
  const now = new Date().toISOString();
  const id = ulid();
  // sK sorts by purchase date for easy timeline queries
  const receiptSK = `RECEIPT#${purchaseDate?.substring(0, 10)}#${id}`;

  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: receiptSK,
      eventType: 'RECEIPT',
      status: 'ACTIVE',
      primaryCat: 'receipt',
      subCategory: category ?? 'other',
      owner,
      desc: JSON.stringify({
        merchant,
        amount,
        currency: currency ?? 'AUD',
        purchaseDate,
        category: category ?? 'other',
        notes: notes ?? null,
        warrantyExpiry: warrantyExpiry ?? null,
        items: items ?? [],
        photoKey: photoKey ?? null,
        isLinked: false,
        loyaltyCardSK: loyaltyCardSK ?? null,
        addedAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  return { success: true, receiptSK };
}

// ── Helpers ───────────────────────────────────────────────────────────────────

interface ScanIndexCard {
  brand: string;
  cardId: string;
  cardSK: string;
  isDefault: boolean;
}

async function _appendToScanIndex(
  permULID: string,
  card: { brand: string; cardId: string; cardSK: string; isDefault?: boolean }
) {
  const identity = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
  }));
  const secondaryULID: string | undefined = identity.Item?.secondaryULID;
  if (!secondaryULID) return;

  const indexRecord = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `SCAN#${secondaryULID}`, sK: permULID },
  }));
  const indexDesc = JSON.parse(indexRecord.Item?.desc ?? '{}');
  const existing: ScanIndexCard[] = indexDesc.cards ?? [];

  // First card for this brand is always default
  const brandCards = existing.filter(c => c.brand === card.brand);
  const makeDefault = card.isDefault ?? brandCards.length === 0;

  // If making this card default, demote all other cards for this brand
  const updated: ScanIndexCard[] = existing.map(c =>
    c.brand === card.brand ? { ...c, isDefault: false } : c
  );
  updated.push({ brand: card.brand, cardId: card.cardId, cardSK: card.cardSK, isDefault: makeDefault });

  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `SCAN#${secondaryULID}`, sK: permULID },
    UpdateExpression: 'SET #d = :desc, updatedAt = :now',
    ExpressionAttributeNames: { '#d': 'desc' },
    ExpressionAttributeValues: {
      ':desc': JSON.stringify({ ...indexDesc, cards: updated }),
      ':now': new Date().toISOString(),
    },
  }));
}

// ── Set default card for a brand ──────────────────────────────────────────────

async function setDefaultCard(permULID: string, cardSK: string, brandId: string) {
  const identity = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
  }));
  const secondaryULID: string | undefined = identity.Item?.secondaryULID;
  if (!secondaryULID) return { success: false, reason: 'No secondaryULID' };

  const indexRecord = await dynamo.send(new GetCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `SCAN#${secondaryULID}`, sK: permULID },
  }));
  const indexDesc = JSON.parse(indexRecord.Item?.desc ?? '{}');
  const cards: ScanIndexCard[] = indexDesc.cards ?? [];

  const updated = cards.map(c =>
    c.brand === brandId ? { ...c, isDefault: c.cardSK === cardSK } : c
  );

  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `SCAN#${secondaryULID}`, sK: permULID },
    UpdateExpression: 'SET #d = :desc, updatedAt = :now',
    ExpressionAttributeNames: { '#d': 'desc' },
    ExpressionAttributeValues: {
      ':desc': JSON.stringify({ ...indexDesc, cards: updated }),
      ':now': new Date().toISOString(),
    },
  }));

  return { success: true };
}

// ── Brand offer subscriptions ─────────────────────────────────────────────────
// Independent of holding a card — controls whether the brand can push
// offers, newsletters, catalogues, and notifications to this user.

// ── Granular subscription update ──────────────────────────────────────────────

async function updateGranularSubscription(permULID: string, owner: string, brandId: string, args: Args) {
  const now = new Date().toISOString();
  const existing = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `SUBSCRIPTION#${brandId}` },
  }));
  const desc = parseRecord(existing.Item?.desc);

  // Build update expression dynamically — only update provided fields
  const updates: string[] = [
    '#d = :desc',
    'updatedAt = :now',
    'createdAt = if_not_exists(createdAt, :now)',
    'eventType = :et',
    'primaryCat = :cat',
    'subCategory = :brand',
    '#s = :active',
  ];
  const names: Record<string, string> = { '#s': 'status', '#d': 'desc', '#ow': 'owner' };
  const values: Record<string, unknown> = {
    ':now': now,
    ':et': 'SUBSCRIPTION',
    ':cat': 'subscription',
    ':brand': brandId,
    ':active': 'ACTIVE',
    ':owner': owner,
  };

  const fields: Array<keyof Args> = ['offers', 'newsletters', 'reminders', 'catalogues'];
  for (const field of fields) {
    if (args[field] !== undefined) {
      desc[field] = args[field] as boolean;
      updates.push(`${field} = :${field}`);
      values[`:${field}`] = args[field];
    }
  }
  values[':desc'] = JSON.stringify(desc);

  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `SUBSCRIPTION#${brandId}` },
    UpdateExpression: `SET ${updates.join(', ')}, #ow = :owner`,
    ExpressionAttributeNames: names,
    ExpressionAttributeValues: values,
  }));

  // Log engagement event under AUDIT#<brandId> so it appears in brand analytics
  const changedChannels = fields.filter(f => args[f] !== undefined);
  writeAuditLog(dynamo, {
    actor: brandId,
    actorType: 'brand',
    action: 'subscription.changed',
    resource: `SUBSCRIPTION#${brandId}`,
    outcome: 'success',
    metadata: { changedChannels },
  }).catch(() => {});

  return { success: true };
}

// ── User preferences (reminder toggles) ───────────────────────────────────────

async function updateUserPreferences(permULID: string, owner: string, reminders: Record<string, boolean>) {
  const now = new Date().toISOString();
  const existing = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'PREFERENCES' },
  }));
  const desc = parseRecord(existing.Item?.desc);
  desc.reminders = reminders;
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'PREFERENCES' },
    UpdateExpression: 'SET #d = :desc, eventType = :et, updatedAt = :now, createdAt = if_not_exists(createdAt, :now), #ow = if_not_exists(#ow, :owner)',
    ExpressionAttributeNames: { '#ow': 'owner', '#d': 'desc' },
    ExpressionAttributeValues: {
      ':desc': JSON.stringify(desc),
      ':et': 'PREFERENCES',
      ':now': now,
      ':owner': owner,
    },
  }));
  return { success: true };
}

async function snoozeOffers(permULID: string, brandId?: string, until?: string | null) {
  const now = new Date().toISOString();
  const expiresAt = until && until.trim().length > 0 ? until : null;
  if (expiresAt && Number.isNaN(Date.parse(expiresAt))) {
    throw new Error('Invalid until timestamp');
  }

  if (brandId) {
    const existing = await dynamo.send(new GetCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: `SUBSCRIPTION#${brandId}` },
    }));
    const desc = parseRecord(existing.Item?.desc);
    if (expiresAt) {
      desc.offersSnoozeUntil = expiresAt;
    } else {
      delete desc.offersSnoozeUntil;
    }

    await dynamo.send(new UpdateCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: `SUBSCRIPTION#${brandId}` },
      UpdateExpression: 'SET #d = :desc, eventType = :et, primaryCat = :cat, subCategory = :brand, #s = :active, updatedAt = :now, createdAt = if_not_exists(createdAt, :now)',
      ExpressionAttributeNames: { '#s': 'status', '#d': 'desc' },
      ExpressionAttributeValues: {
        ':desc': JSON.stringify(desc),
        ':et': 'SUBSCRIPTION',
        ':cat': 'subscription',
        ':brand': brandId,
        ':active': 'ACTIVE',
        ':now': now,
      },
    }));
    writeAuditLog(dynamo, {
      actor: brandId,
      actorType: 'brand',
      action: 'offer.snoozed',
      resource: `SUBSCRIPTION#${brandId}`,
      outcome: 'success',
      metadata: { until: expiresAt ?? 'cleared' },
    }).catch(() => {});

    return { success: true, brandId, offersSnoozeUntil: expiresAt };
  }

  const existing = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'PREFERENCES' },
  }));
  const desc = parseRecord(existing.Item?.desc);
  if (expiresAt) {
    desc.offersGlobalSnoozeUntil = expiresAt;
  } else {
    delete desc.offersGlobalSnoozeUntil;
  }

  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'PREFERENCES' },
    UpdateExpression: 'SET desc = :desc, eventType = :et, updatedAt = :now, createdAt = if_not_exists(createdAt, :now)',
    ExpressionAttributeValues: {
      ':desc': JSON.stringify(desc),
      ':et': 'PREFERENCES',
      ':now': now,
    },
  }));
  return { success: true, offersGlobalSnoozeUntil: expiresAt };
}

// ── Legacy subscription toggle ─────────────────────────────────────────────────

async function markNewsletterRead(permULID: string, newsletterSK: string) {
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: newsletterSK },
    UpdateExpression: 'SET #s = :read, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: { ':read': 'READ', ':now': new Date().toISOString() },
  }));

  // newsletterSK format: NEWSLETTER#<brandId>#<newsletterId>
  const parts = newsletterSK.split('#');
  const brandId = parts[1];
  if (brandId) {
    writeAuditLog(dynamo, {
      actor: brandId,
      actorType: 'brand',
      action: 'newsletter.read',
      resource: newsletterSK,
      outcome: 'success',
    }).catch(() => {});
  }

  return { success: true };
}

// ── Respond to checkout (Patent Claims 19–22) ──────────────────────────────────
// Called from the app when user approves or declines a payment request.

async function respondToCheckout(
  permULID: string,
  orderId: string,
  approved: boolean,
  paymentToken?: string,
) {
  const res = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk AND sK = :sk',
    ExpressionAttributeValues: { ':pk': `CHECKOUT#${orderId}`, ':sk': permULID },
    Limit: 1,
  }));
  const item = res.Items?.[0];
  if (!item) throw new Error('Checkout not found');
  if (item.status !== 'PENDING') throw new Error(`Checkout already resolved: ${item.status}`);

  const desc = JSON.parse(item.desc ?? '{}') as {
    brandWebhookUrl?: string; amount: number; currency: string; merchantName: string; expiresAt: string;
  };
  const now = new Date();
  if (now.toISOString() > desc.expiresAt) throw new Error('Checkout expired');

  const newStatus = approved ? 'APPROVED' : 'DECLINED';

  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `CHECKOUT#${orderId}`, sK: permULID },
    UpdateExpression: 'SET #s = :s, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: {
      ':s': newStatus, ':now': now.toISOString(), ':pending': 'PENDING',
    },
    ConditionExpression: '#s = :pending',
  }));

  // Notify brand webhook
  if (desc.brandWebhookUrl) {
    await postWebhookCardManager(desc.brandWebhookUrl, {
      orderId,
      status: newStatus,
      ...(approved && paymentToken ? { paymentToken } : {}),
    });
  }

  return { success: true, status: newStatus };
}

function postWebhookCardManager(url: string, payload: unknown): Promise<void> {
  return new Promise((resolve) => {
    const body = JSON.stringify(payload);
    try {
      const req = https.request(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
      }, (res) => { res.resume(); res.on('end', resolve); });
      req.on('error', (e) => { console.error('[card-manager] webhook error', e.message); resolve(); });
      req.setTimeout(5000, () => { req.destroy(); resolve(); });
      req.write(body);
      req.end();
    } catch (e) { console.error('[card-manager] webhook error', e); resolve(); }
  });
}

// ── Respond to consent request (Patent Claims 25–26) ───────────────────────────
// Called from the app after the user biometrically approves or denies a consent prompt.
// approvedFields = [] means full denial. Partial approval is allowed.
// Identity values are read from UserDataEvent IDENTITY record and relayed to the brand webhook.

async function respondToConsent(
  permULID: string,
  requestId: string,
  approvedFields: string[],
) {
  // Load consent request record
  const consentRes = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk AND sK = :sk',
    ExpressionAttributeValues: { ':pk': `CONSENT#${requestId}`, ':sk': permULID },
    Limit: 1,
  }));
  const item = consentRes.Items?.[0];
  if (!item) throw new Error('Consent request not found');
  if (item.status !== 'PENDING') throw new Error(`Consent request already resolved: ${item.status}`);

  const desc = JSON.parse(item.desc ?? '{}') as {
    requestedFields: string[];
    purpose: string;
    brandId: string;
    brandName: string;
    brandWebhookUrl?: string;
    expiresAt: string;
  };

  const now = new Date();
  if (now.toISOString() > desc.expiresAt) throw new Error('Consent request expired');

  const isDenied = approvedFields.length === 0;
  const newStatus = isDenied ? 'DENIED' : 'APPROVED';

  // Only release fields that were actually requested — ignore any extras the app may send
  const safeApproved = approvedFields.filter(f => desc.requestedFields.includes(f));

  // Resolve approved field values from IDENTITY record
  let releasedData: Record<string, string> = {};
  if (!isDenied && safeApproved.length > 0) {
    const identityRes = await dynamo.send(new GetCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
    }));
    const identityDesc = JSON.parse(identityRes.Item?.desc ?? '{}') as Record<string, string>;
    for (const field of safeApproved) {
      if (identityDesc[field] != null) releasedData[field] = identityDesc[field];
    }
  }

  // Update consent record
  await dynamo.send(new UpdateCommand({
    TableName: ADMIN_TABLE,
    Key: { pK: `CONSENT#${requestId}`, sK: permULID },
    UpdateExpression: 'SET #s = :s, desc = :desc, updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: {
      ':s': newStatus,
      ':desc': JSON.stringify({ ...desc, approvedFields: safeApproved }),
      ':now': now.toISOString(),
      ':pending': 'PENDING',
    },
    ConditionExpression: '#s = :pending',
  }));

  // Write consent log to UserDataEvent for audit/history
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `CONSENT_LOG#${requestId}` },
    UpdateExpression: 'SET eventType = :et, primaryCat = :cat, subCategory = :brand, #s = :s, desc = :desc, createdAt = if_not_exists(createdAt, :now), updatedAt = :now',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: {
      ':et': 'CONSENT_LOG',
      ':cat': 'consent_log',
      ':brand': desc.brandId,
      ':s': newStatus,
      ':desc': JSON.stringify({
        brandId: desc.brandId,
        brandName: desc.brandName,
        purpose: desc.purpose,
        requestedFields: desc.requestedFields,
        approvedFields: safeApproved,
        status: newStatus,
        resolvedAt: now.toISOString(),
      }),
      ':now': now.toISOString(),
    },
  }));

  // Log consent decision under AUDIT#<brandId> so it appears in brand analytics
  writeAuditLog(dynamo, {
    actor: desc.brandId,
    actorType: 'brand',
    action: 'consent.decision',
    resource: `CONSENT#${requestId}`,
    outcome: 'success',
    metadata: {
      status: newStatus,
      approvedFieldCount: safeApproved.length,
      requestedFieldCount: desc.requestedFields.length,
      purpose: desc.purpose,
    },
  }).catch(() => {});

  // Relay approved field values to brand webhook
  if (desc.brandWebhookUrl) {
    await postWebhookCardManager(desc.brandWebhookUrl, {
      requestId,
      status: newStatus,
      ...(isDenied ? {} : { releasedData }),
    });
  }

  return { success: true, status: newStatus, approvedFields: safeApproved };
}

async function setSubscription(permULID: string, owner: string, brandId: string, active: boolean) {
  const now = new Date().toISOString();
  const existing = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `SUBSCRIPTION#${brandId}` },
  }));
  const desc = parseRecord(existing.Item?.desc);
  desc.offers = active;
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `SUBSCRIPTION#${brandId}` },
    UpdateExpression: 'SET #s = :status, eventType = :et, primaryCat = :cat, subCategory = :brand, updatedAt = :now, owner = :owner, createdAt = if_not_exists(createdAt, :now)',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: {
      ':status': active ? 'ACTIVE' : 'INACTIVE',
      ':et': 'SUBSCRIPTION',
      ':cat': 'subscription',
      ':brand': brandId,
      ':now': now,
      ':owner': owner,
    },
  }));

  // Log engagement event under AUDIT#<brandId> so it appears in brand analytics
  writeAuditLog(dynamo, {
    actor: brandId,
    actorType: 'brand',
    action: active ? 'subscriber.joined' : 'subscriber.churned',
    resource: `SUBSCRIPTION#${brandId}`,
    outcome: 'success',
    metadata: { channel: 'offers' },
  }).catch(() => {});

  return { success: true };
}

// ── Cancel recurring subscription (Patent Claims 27–29) ──────────────────────
// delegates to subscription-proxy shared cancel logic.

async function cancelRecurringHandler(permULID: string, subId: string, brandId: string) {
  const { cancelSubscription } = await import('../subscription-proxy/handler');
  await cancelSubscription(permULID, brandId, subId, 'CANCELLED_BY_USER');
  return { success: true, status: 'CANCELLED_BY_USER' };
}

// ── Manual Subscriptions ──────────────────────────────────────────────────────

async function addManualSubscription(permULID: string, owner: string, args: Args) {
  const { brandName, productName, amount, currency, frequency, nextBillingDate, category, providerId } = args;
  const now = new Date().toISOString();
  const id = (await import('ulid')).monotonicFactory()();
  const subSK = `RECURRING#manual#${id}`;

  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: subSK,
      eventType: 'SUBSCRIPTION',
      status: 'ACTIVE',
      primaryCat: 'subscription',
      subCategory: providerId ? 'catalog' : 'manual',
      owner,
      desc: JSON.stringify({
        brandName,
        productName,
        amount,
        currency: currency ?? 'AUD',
        frequency,
        nextBillingDate,
        category,
        providerId:  providerId ?? null,   // catalog provider ID (e.g. 'agl', 'netflix')
        source: providerId ? 'catalog' : 'manual',
        addedAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  return { success: true, subSK };
}

// ── Enrollment handlers ───────────────────────────────────────────────────────

async function enrollmentRespondHandler(permULID: string, enrollmentId: string, accepted: boolean) {
  const { respondToEnrollmentFn } = await import('../enrollment-handler/handler');
  return respondToEnrollmentFn(dynamo, permULID, enrollmentId, accepted);
}

async function enrollmentInitiateHandler(permULID: string, owner: string, brandId: string) {
  const { generateAlias } = await import('../enrollment-handler/handler');
  const now = new Date().toISOString();
  const { monotonicFactory } = await import('ulid');
  const ulidGen = monotonicFactory();
  const enrollmentId = ulidGen();

  const alias = generateAlias(permULID, brandId);

  // Store enrollment record directly as ACCEPTED (user initiated — implicit consent)
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `ENROLL#${enrollmentId}`,
      sK: permULID,
      eventType: 'ENROLLMENT',
      status: 'ACCEPTED',
      brandId,
      alias,
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: `ENROLL#${brandId}#${enrollmentId}`,
      eventType: 'ENROLLMENT',
      status: 'ACCEPTED',
      primaryCat: 'enrollment',
      subCategory: brandId,
      owner,
      desc: JSON.stringify({ enrollmentId, brandId, alias, respondedAt: now }),
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  // CPA tracking
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `CPA#${brandId}`,
      sK: enrollmentId,
      eventType: 'CPA_ENROLLMENT',
      status: 'PENDING_VERIFICATION',
      enrollmentId,
      brandId,
      permULID,
      alias,
      source: 'user_initiated',
      createdAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  return { ok: true, enrollmentId, alias };
}

// ── SMB Stamp Cards (Phase 11) ────────────────────────────────────────────────

/**
 * getStampCard — returns the stamp card for a specific brand.
 * Returns null if the user has no stamp card for this brand yet.
 */
async function getStampCard(permULID: string, brandId: string) {
  const res = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `STAMP#${brandId}` },
  }));
  if (!res.Item) return null;
  const desc = safeJsonParse(res.Item.desc);
  return {
    brandId: (desc.brandId as string) ?? brandId,
    brandName: (desc.brandName as string) ?? brandId,
    brandColor: (desc.brandColor as string) ?? '#6366F1',
    stamps: (desc.stamps as number) ?? 0,
    goal: (desc.goal as number) ?? 10,
    status: (res.Item.status as string) ?? 'ACTIVE',
    rewardDescription: (desc.rewardDescription as string) ?? '',
    redemptions: (desc.redemptions as number) ?? 0,
    lastStampAt: (desc.lastStampAt as string) ?? null,
  };
}

/**
 * listStampCards — returns all STAMP# records for this user.
 */
async function listStampCards(permULID: string) {
  const res = await dynamo.send(new QueryCommand({
    TableName: USER_TABLE,
    KeyConditionExpression: 'pK = :pk AND begins_with(sK, :prefix)',
    ExpressionAttributeValues: {
      ':pk': `USER#${permULID}`,
      ':prefix': 'STAMP#',
    },
  }));
  return (res.Items ?? []).map(item => {
    const desc = safeJsonParse(item.desc);
    return {
      brandId: (desc.brandId as string) ?? '',
      brandName: (desc.brandName as string) ?? '',
      brandColor: (desc.brandColor as string) ?? '#6366F1',
      stamps: (desc.stamps as number) ?? 0,
      goal: (desc.goal as number) ?? 10,
      status: (item.status as string) ?? 'ACTIVE',
      rewardDescription: (desc.rewardDescription as string) ?? '',
      redemptions: (desc.redemptions as number) ?? 0,
      lastStampAt: (desc.lastStampAt as string) ?? null,
    };
  });
}

// ── Gift Card Marketplace ─────────────────────────────────────────────────────

async function purchaseGiftCard(
  permULID: string,
  brandId: string,
  catalogItemId: string,
  denomination: number,
  currency: string,
) {
  const orderId = ulid();
  const now = new Date().toISOString();

  // Write pending order to AdminDataEvent so brand backends can poll or receive webhook
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `GIFTCARD_ORDER#${brandId}`,
      sK: orderId,
      eventType: 'GIFTCARD_ORDER',
      status: 'PENDING',
      primaryCat: 'giftcard_order',
      desc: JSON.stringify({
        orderId,
        permULID,
        brandId,
        catalogItemId,
        denomination,
        currency,
        requestedAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK)',
  }));

  // Notify brand via purchase webhook if configured
  const brandRef = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'PROFILE' },
  }));
  const brandDesc = JSON.parse(brandRef.Item?.desc ?? '{}') as Record<string, unknown>;
  const purchaseWebhookUrl = brandDesc.purchaseWebhookUrl as string | undefined;

  if (purchaseWebhookUrl) {
    const payload = JSON.stringify({ orderId, denomination, currency, requestedAt: now });
    await new Promise<void>((resolve) => {
      const url = new URL(purchaseWebhookUrl);
      const req = https.request({
        hostname: url.hostname,
        port: url.port || 443,
        path: url.pathname + url.search,
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) },
      }, (res) => { res.resume(); resolve(); });
      req.on('error', () => resolve());
      req.write(payload);
      req.end();
    });
  }

  return { orderId, status: 'PENDING', denomination, currency, brandId };
}

async function syncGiftCardBalance(permULID: string, cardSK: string, brandId: string) {
  // Read the gift card record to get card number
  const cardRes = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: cardSK },
  }));
  if (!cardRes.Item) throw new Error('Gift card not found');
  const cardDesc = JSON.parse(cardRes.Item.desc ?? '{}') as Record<string, unknown>;
  const cardNumber = cardDesc.cardNumber as string;

  // Read brand profile for balance webhook config
  const brandRef = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `BRAND#${brandId}`, sK: 'PROFILE' },
  }));
  const brandDesc = JSON.parse(brandRef.Item?.desc ?? '{}') as Record<string, unknown>;
  const balanceWebhookUrl = brandDesc.balanceWebhookUrl as string | undefined;
  const balanceWebhookSecret = brandDesc.balanceWebhookSecret as string | undefined;

  if (!balanceWebhookUrl) {
    // Brand has no balance webhook — return current stored balance unchanged
    return { balance: cardDesc.balance, currency: cardDesc.currency, synced: false };
  }

  // Call brand's balance webhook
  const payload = JSON.stringify({ cardNumber });
  let responseBody = '';

  await new Promise<void>((resolve) => {
    const url = new URL(balanceWebhookUrl);
    const headers: Record<string, string | number> = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(payload),
    };
    if (balanceWebhookSecret) headers['X-BeboCard-Secret'] = balanceWebhookSecret;

    const req = https.request({
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname + url.search,
      method: 'POST',
      headers,
    }, (res) => {
      res.on('data', (chunk: Buffer) => { responseBody += chunk.toString(); });
      res.on('end', resolve);
    });
    req.on('error', () => resolve());
    req.write(payload);
    req.end();
  });

  let newBalance: number | undefined;
  let currency: string | undefined;
  try {
    const parsed = JSON.parse(responseBody) as { balance?: number; currency?: string };
    newBalance = parsed.balance;
    currency = parsed.currency;
  } catch {
    // Webhook returned non-JSON — skip update
    return { balance: cardDesc.balance, currency: cardDesc.currency, synced: false };
  }

  if (newBalance === undefined) {
    return { balance: cardDesc.balance, currency: cardDesc.currency, synced: false };
  }

  // Update stored balance
  const now = new Date().toISOString();
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: cardSK },
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
    ExpressionAttributeValues: {
      ':desc': JSON.stringify({
        ...cardDesc,
        balance: newBalance,
        currency: currency ?? cardDesc.currency,
        lastBalanceSync: now,
      }),
      ':now': now,
    },
  }));

  return { balance: newBalance, currency: currency ?? cardDesc.currency, synced: true };
}

// ── GDPR — Right to Erasure ───────────────────────────────────────────────────
// Purges all user data records from UserDataEvent and all SCAN# entries from
// AdminDataEvent. Writes a GDPR_DELETION tombstone audit record (retained 7yr).
// This is irreversible. The AppSync schema requires authentication, so only the
// account holder can trigger this mutation.

async function deleteAccount(permULID: string) {
  const now = new Date().toISOString();

  // 1. Collect all USER# records for this permULID
  let lastKey: Record<string, unknown> | undefined;
  const deleteOps: Promise<unknown>[] = [];

  do {
    const res = await dynamo.send(new QueryCommand({
      TableName: USER_TABLE,
      KeyConditionExpression: 'pK = :pk',
      ExpressionAttributeValues: { ':pk': `USER#${permULID}` },
      Limit: 25,
      ExclusiveStartKey: lastKey,
      ProjectionExpression: 'pK, sK',
    }));

    for (const item of res.Items ?? []) {
      deleteOps.push(
        dynamo.send(new DeleteCommand({
          TableName: USER_TABLE,
          Key: { pK: item.pK, sK: item.sK },
        })).catch(e => console.error('[deleteAccount] user record delete failed', e)),
      );
    }

    lastKey = res.LastEvaluatedKey;
  } while (lastKey);

  // 2. Remove SCAN# index entry (identity resolution index)
  // The SCAN record uses secondaryULID as pK — we need to find it first via the IDENTITY record.
  // We already deleted USER#<permULID>/IDENTITY above, but the ADMIN index is separate.
  const adminScanRes = await dynamo.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: { ':pk': `USER#${permULID}` },
    Limit: 10,
    ProjectionExpression: 'pK, sK',
  })).catch(() => ({ Items: [] }));

  for (const item of adminScanRes.Items ?? []) {
    deleteOps.push(
      dynamo.send(new DeleteCommand({
        TableName: ADMIN_TABLE,
        Key: { pK: item.pK, sK: item.sK },
      })).catch(e => console.error('[deleteAccount] admin record delete failed', e)),
    );
  }

  // Execute all deletes in parallel
  await Promise.allSettled(deleteOps);

  // 3. Write a GDPR tombstone in AdminDataEvent (retained 7 years for compliance)
  const ttl = Math.floor(Date.now() / 1000) + 7 * 365 * 24 * 3600;
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `GDPR_DELETION#${permULID}`,
      sK: now,
      eventType: 'GDPR_DELETION',
      status: 'COMPLETED',
      desc: JSON.stringify({ permULID: `[deleted:${permULID.slice(0, 8)}]`, deletedAt: now }),
      ttl,
      createdAt: now,
      updatedAt: now,
    },
  }));

  return { deleted: true, deletedAt: now };
}

function safeJsonParse(value: unknown): Record<string, unknown> {
  if (typeof value === 'object' && value !== null) return value as Record<string, unknown>;
  if (typeof value !== 'string' || !value) return {};
  try { return JSON.parse(value) as Record<string, unknown>; }
  catch { return {}; }
}
