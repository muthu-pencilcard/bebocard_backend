import type { AppSyncResolverHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, DeleteCommand, GetCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';
import { withAuditLog } from '../../shared/audit-logger';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid = monotonicFactory();

const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const ADMIN_TABLE = process.env.ADMIN_TABLE!;

type Args = {
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
  // Receipts
  merchant?: string;
  purchaseDate?: string;
  warrantyExpiry?: string;
  items?: unknown;
  loyaltyCardSK?: string;
  photoKey?: string;
  // Granular subscriptions
  offers?: boolean;
  newsletters?: boolean;
  reminders?: boolean;
  catalogues?: boolean;
};

const _handler: AppSyncResolverHandler<Args, unknown> = async (event) => {
  const operation = event.info.fieldName;
  const args = event.arguments;

  const permULID = (event.identity as { claims: Record<string, string> })
    ?.claims?.['custom:permULID'];
  if (!permULID) throw new Error('Identity missing permULID');

  switch (operation) {
    // Loyalty cards
    case 'addLoyaltyCard': return addCard(permULID, args);
    case 'removeLoyaltyCard': return removeCard(permULID, args.cardSK!);
    case 'setDefaultCard': return setDefaultCard(permULID, args.cardSK!, args.brandId!);
    case 'rotateQR': return rotateQR(permULID);
    // Subscriptions (legacy single-toggle + new granular)
    case 'subscribeToOffers':   return setSubscription(permULID, args.brandId!, true);
    case 'unsubscribeFromOffers': return setSubscription(permULID, args.brandId!, false);
    case 'updateSubscription':  return updateGranularSubscription(permULID, args.brandId!, args);
    case 'updatePreferences':   return updateUserPreferences(permULID, (args as any).reminders as Record<string, boolean>);
    // Gift cards
    case 'addGiftCard': return addGiftCard(permULID, args);
    case 'removeGiftCard': return archiveRecord(permULID, args.cardSK!);
    case 'updateGiftCardBalance': return updateBalance(permULID, args.cardSK!, args.balance!);
    // Invoices
    case 'addInvoice': return addInvoice(permULID, args);
    case 'updateInvoiceStatus': return updateInvoiceStatus(permULID, args.invoiceSK!, args.status!, args.paidDate);
    // Receipts
    case 'addReceipt': return addReceipt(permULID, args);
    default: throw new Error(`Unknown operation: ${operation}`);
  }
};

export const handler = withAuditLog(dynamo, _handler);

// ── Add loyalty card ──────────────────────────────────────────────────────────

async function addCard(permULID: string, args: Args) {
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
    brandProfile = (ref.Item?.desc as Record<string, unknown>) ?? {};
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
      desc: JSON.stringify({
        brandId: isCustom ? 'custom' : brandId,
        brandName: isCustom ? customBrandName : brandProfile.name,
        brandColor: isCustom ? customBrandColor : brandProfile.color,
        cardNumber,
        cardLabel: cardLabel ?? (isCustom ? customBrandName : brandProfile.name),
        isCustom: !!isCustom,
        pointsBalance: 0,
        addedAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(pK)', // prevent duplicates
  }));

  // Update SCAN index for POS lookup (only for non-custom cards with known API)
  if (!isCustom && brandProfile.apiEndpoint) {
    await _appendToScanIndex(permULID, {
      brand: brandId!,
      cardId: cardNumber!,
      cardSK,
      isDefault: args.isDefault,  // undefined = auto (true if first card for brand)
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

// ── Rotate QR (new secondaryULID) ─────────────────────────────────────────────

async function rotateQR(permULID: string) {
  const now = new Date().toISOString();
  const newSecondaryULID = ulid();
  const newRotatesAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(); // 24h

  // Read current IDENTITY to get old secondaryULID
  const identity = await dynamo.send(new GetCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
  }));
  if (!identity.Item) throw new Error('IDENTITY record not found');
  const oldSecondaryULID: string = identity.Item.secondaryULID;

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

  return { success: true, alreadyRotated: false, newSecondaryULID, rotatesAt: newRotatesAt };
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

async function addGiftCard(permULID: string, args: Args) {
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

async function addInvoice(permULID: string, args: Args) {
  const { supplier, amount, dueDate, invoiceNumber, category, notes, currency } = args;
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
      desc: JSON.stringify({
        supplier,
        invoiceNumber: invoiceNumber ?? null,
        amount,
        currency: currency ?? 'AUD',
        dueDate,
        paidDate: null,
        status: 'unpaid',
        category: category ?? 'other',
        notes: notes ?? null,
        attachmentKey: null,
        createdAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));

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

async function addReceipt(permULID: string, args: Args) {
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
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
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
    UpdateExpression: 'SET desc = :desc, updatedAt = :now',
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

async function updateGranularSubscription(permULID: string, brandId: string, args: Args) {
  const now = new Date().toISOString();
  // Build update expression dynamically — only update provided fields
  const updates: string[] = [
    'updatedAt = :now',
    'createdAt = if_not_exists(createdAt, :now)',
    'eventType = :et',
    'primaryCat = :cat',
    'subCategory = :brand',
    '#s = :active',
  ];
  const names: Record<string, string> = { '#s': 'status' };
  const values: Record<string, unknown> = {
    ':now': now,
    ':et': 'SUBSCRIPTION',
    ':cat': 'subscription',
    ':brand': brandId,
    ':active': 'ACTIVE',
  };

  const fields: Array<keyof Args> = ['offers', 'newsletters', 'reminders', 'catalogues'];
  for (const field of fields) {
    if (args[field] !== undefined) {
      updates.push(`${field} = :${field}`);
      values[`:${field}`] = args[field];
    }
  }

  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `SUBSCRIPTION#${brandId}` },
    UpdateExpression: `SET ${updates.join(', ')}`,
    ExpressionAttributeNames: names,
    ExpressionAttributeValues: values,
  }));
  return { success: true };
}

// ── User preferences (reminder toggles) ───────────────────────────────────────

async function updateUserPreferences(permULID: string, reminders: Record<string, boolean>) {
  const now = new Date().toISOString();
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: 'PREFERENCES' },
    UpdateExpression: 'SET desc = :desc, eventType = :et, updatedAt = :now, createdAt = if_not_exists(createdAt, :now)',
    ExpressionAttributeValues: {
      ':desc': JSON.stringify({ reminders }),
      ':et': 'PREFERENCES',
      ':now': now,
    },
  }));
  return { success: true };
}

// ── Legacy subscription toggle ─────────────────────────────────────────────────

async function setSubscription(permULID: string, brandId: string, active: boolean) {
  const now = new Date().toISOString();
  await dynamo.send(new UpdateCommand({
    TableName: USER_TABLE,
    Key: { pK: `USER#${permULID}`, sK: `SUBSCRIPTION#${brandId}` },
    UpdateExpression: 'SET #s = :status, eventType = :et, primaryCat = :cat, subCategory = :brand, updatedAt = :now, createdAt = if_not_exists(createdAt, :now)',
    ExpressionAttributeNames: { '#s': 'status' },
    ExpressionAttributeValues: {
      ':status': active ? 'ACTIVE' : 'INACTIVE',
      ':et': 'SUBSCRIPTION',
      ':cat': 'subscription',
      ':brand': brandId,
      ':now': now,
    },
  }));
  return { success: true };
}
