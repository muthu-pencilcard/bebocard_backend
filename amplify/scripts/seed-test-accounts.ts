/**
 * Seed script: creates test consumer user + brand tenant accounts with rich sample data.
 *
 * Run:
 *   USER_TABLE=<name> ADMIN_TABLE=<name> REFDATA_TABLE=<name> \
 *     npx ts-node amplify/scripts/seed-test-accounts.ts
 *
 * Output: prints all generated IDs and credentials to stdout.
 * Save the raw API key — it is shown once and never stored.
 */
import { createHash, randomBytes } from 'crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({ region: process.env.AWS_REGION ?? 'ap-southeast-2' }));
const ulid = monotonicFactory();

const USER_TABLE   = process.env.USER_TABLE!;
const ADMIN_TABLE  = process.env.ADMIN_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;

if (!USER_TABLE || !ADMIN_TABLE || !REFDATA_TABLE) {
  console.error('Missing required env vars: USER_TABLE, ADMIN_TABLE, REFDATA_TABLE');
  process.exit(1);
}

// ── ID generation ──────────────────────────────────────────────────────────────

function generateApiKey(): { rawKey: string; keyId: string; keyHash: string } {
  const keyId  = ulid();
  const secret = randomBytes(32).toString('hex');
  const rawKey = `bebo_${keyId}.${secret}`;
  const keyHash = createHash('sha256').update(rawKey).digest('hex');
  return { rawKey, keyId, keyHash };
}

// ── Main ───────────────────────────────────────────────────────────────────────

async function seed() {
  const now = new Date().toISOString();
  const rotatesAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();

  // ── Consumer test user (Alex Chen) ──────────────────────────────────────────
  const permULID      = ulid();
  const secondaryULID = ulid();
  const cognitoUserId = `test-user-${permULID.toLowerCase()}`;

  // Card IDs (sK tokens)
  const woolworthsCardId = `CARD#woolworths#6006490100000000`;
  const qantasCardId     = `CARD#qantas#QF987654`;
  const myerCardId       = `CARD#myer#9400123456789012`;

  // Receipt, invoice, gift card IDs
  const receipt1Id  = ulid();
  const receipt2Id  = ulid();
  const receipt3Id  = ulid();
  const invoiceId   = ulid();
  const giftCardId  = ulid();

  // ── IDENTITY record (UserDataEvent) ─────────────────────────────────────────
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK:           `USER#${permULID}`,
      sK:           'IDENTITY',
      eventType:    'IDENTITY',
      status:       'ACTIVE',
      primaryCat:   'identity',
      subCategory:  'wallet',
      secondaryULID,
      rotatesAt,
      desc: JSON.stringify({
        permULID,
        cognitoUserId,
        displayName: 'Alex Chen',
        email: 'alex.chen@testuser.bebocard.com.au',
        createdAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log(`[user] IDENTITY written  USER#${permULID}`);

  // ── SCAN index (AdminDataEvent) ───────────────────────────────────────────
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK:        `SCAN#${secondaryULID}`,
      sK:        permULID,
      eventType: 'SCAN_INDEX',
      status:    'ACTIVE',
      desc: JSON.stringify({
        cards: [
          { brand: 'woolworths', cardId: woolworthsCardId, isDefault: true },
          { brand: 'qantas',     cardId: qantasCardId,     isDefault: true },
          { brand: 'myer',       cardId: myerCardId,       isDefault: true },
        ],
        createdAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log(`[user] SCAN_INDEX written  SCAN#${secondaryULID}`);

  // ── Loyalty cards (UserDataEvent) ──────────────────────────────────────────
  const cards = [
    {
      sK:        woolworthsCardId,
      brandId:   'woolworths',
      cardNumber: '6006490100000000',
      cardLabel: 'Woolworths Rewards',
      isDefault: true,
    },
    {
      sK:        qantasCardId,
      brandId:   'qantas',
      cardNumber: 'QF987654',
      cardLabel: 'Qantas Frequent Flyer',
      isDefault: true,
    },
    {
      sK:        myerCardId,
      brandId:   'myer',
      cardNumber: '9400123456789012',
      cardLabel: 'MYER one',
      isDefault: true,
    },
  ];

  for (const card of cards) {
    await dynamo.send(new PutCommand({
      TableName: USER_TABLE,
      Item: {
        pK:          `USER#${permULID}`,
        sK:          card.sK,
        eventType:   'CARD',
        status:      'ACTIVE',
        primaryCat:  'loyalty_card',
        subCategory: card.brandId,
        desc: JSON.stringify({
          brandId:    card.brandId,
          cardNumber: card.cardNumber,
          cardLabel:  card.cardLabel,
          isDefault:  card.isDefault,
        }),
        createdAt: now,
        updatedAt: now,
      },
    }));
  }
  console.log(`[user] 3 loyalty cards written`);

  // ── Receipts (UserDataEvent) ──────────────────────────────────────────────
  const receipts = [
    {
      id:           receipt1Id,
      merchant:     'Woolworths Bondi Junction',
      amount:       87.45,
      purchaseDate: '2026-03-28',
      brandId:      'woolworths',
      loyaltyCardId: woolworthsCardId,
      pointsEarned: 87,
      currency:     'AUD',
      category:     'grocery',
      items: [
        { name: 'Organic Milk 2L', price: 4.50, qty: 2 },
        { name: 'Sourdough Bread', price: 6.00, qty: 1 },
        { name: 'Chicken Breast 500g', price: 12.00, qty: 1 },
        { name: 'Mixed Salad Leaves', price: 4.50, qty: 1 },
      ],
    },
    {
      id:           receipt2Id,
      merchant:     'Woolworths Surry Hills',
      amount:       54.20,
      purchaseDate: '2026-04-01',
      brandId:      'woolworths',
      loyaltyCardId: woolworthsCardId,
      pointsEarned: 54,
      currency:     'AUD',
      category:     'grocery',
      items: [
        { name: 'Greek Yoghurt 1kg', price: 8.00, qty: 1 },
        { name: 'Orange Juice 1L',   price: 4.20, qty: 2 },
        { name: 'Pasta 500g',         price: 3.00, qty: 3 },
      ],
    },
    {
      id:           receipt3Id,
      merchant:     'Woolworths Metro George St',
      amount:       32.80,
      purchaseDate: '2026-04-03',
      brandId:      'woolworths',
      loyaltyCardId: woolworthsCardId,
      pointsEarned: 32,
      currency:     'AUD',
      category:     'grocery',
      items: [
        { name: 'Avocados 4-pack', price: 6.50, qty: 1 },
        { name: 'Cherry Tomatoes', price: 5.00, qty: 1 },
        { name: 'Sparkling Water 6-pack', price: 8.00, qty: 1 },
      ],
    },
  ];

  for (const r of receipts) {
    await dynamo.send(new PutCommand({
      TableName: USER_TABLE,
      Item: {
        pK:          `USER#${permULID}`,
        sK:          `RECEIPT#${r.id}`,
        eventType:   'RECEIPT',
        status:      'ACTIVE',
        primaryCat:  'receipt',
        subCategory: r.brandId,
        desc: JSON.stringify({
          merchant:      r.merchant,
          amount:        r.amount,
          purchaseDate:  r.purchaseDate,
          brandId:       r.brandId,
          loyaltyCardId: r.loyaltyCardId,
          pointsEarned:  r.pointsEarned,
          currency:      r.currency,
          category:      r.category,
          items:         r.items,
        }),
        createdAt: now,
        updatedAt: now,
      },
    }));
  }
  console.log(`[user] 3 receipts written`);

  // ── Invoice (UserDataEvent) ───────────────────────────────────────────────
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK:          `USER#${permULID}`,
      sK:          `INVOICE#${invoiceId}`,
      eventType:   'INVOICE',
      status:      'ACTIVE',
      primaryCat:  'invoice',
      subCategory: 'supplier',
      desc: JSON.stringify({
        supplier:    'ABC Supplies Pty Ltd',
        amount:      1250.00,
        currency:    'AUD',
        dueDate:     '2026-04-20',
        category:    'business',
        invoiceRef:  'INV-2026-0042',
        notes:       'Q1 stationery and office supplies',
        isPaid:      false,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log(`[user] 1 invoice written`);

  // ── Gift card (UserDataEvent) ─────────────────────────────────────────────
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK:          `USER#${permULID}`,
      sK:          `GIFTCARD#${giftCardId}`,
      eventType:   'GIFTCARD',
      status:      'ACTIVE',
      primaryCat:  'gift_card',
      subCategory: 'woolworths',
      desc: JSON.stringify({
        brandId:       'woolworths',
        brandName:     'Woolworths',
        cardNumber:    '6281234567890123',
        pin:           null,                // stored device-side after claim; null server-side
        balance:       50.00,
        currency:      'AUD',
        expiryDate:    '2027-04-01',
        distributorId: 'prezzee',
        purchaseId:    giftCardId,
        source:        'marketplace',       // 'marketplace' | 'gifted'
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log(`[user] 1 gift card written`);

  // ── Subscriptions (UserDataEvent) ─────────────────────────────────────────
  const subscriptions = [
    {
      brandId:     'woolworths',
      offers:      true,
      newsletters: true,
      reminders:   true,
      catalogues:  true,
    },
    {
      brandId:     'qantas',
      offers:      false,
      newsletters: true,
      reminders:   true,
      catalogues:  false,
    },
  ];

  for (const sub of subscriptions) {
    await dynamo.send(new PutCommand({
      TableName: USER_TABLE,
      Item: {
        pK:          `USER#${permULID}`,
        sK:          `SUBSCRIPTION#${sub.brandId}`,
        eventType:   'SUBSCRIPTION',
        status:      'ACTIVE',
        primaryCat:  'subscription',
        subCategory: sub.brandId,
        desc: JSON.stringify({
          offers:      sub.offers,
          newsletters: sub.newsletters,
          reminders:   sub.reminders,
          catalogues:  sub.catalogues,
        }),
        createdAt: now,
        updatedAt: now,
      },
    }));
  }
  console.log(`[user] 2 brand subscriptions written`);

  // ── Segment records (UserDataEvent) ──────────────────────────────────────
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK:          `USER#${permULID}`,
      sK:          'SEGMENT#woolworths',
      eventType:   'SEGMENT',
      status:      'ACTIVE',
      primaryCat:  'segment',
      subCategory: 'woolworths',
      desc: JSON.stringify({
        spendBucket:    '100-200',
        visitFrequency: 'frequent',
        lastVisit:       '2026-04-03',
        totalSpend:      174.45,
        visitCount:      3,
        persona:         ['grocery_focused', 'regular_shopper'],
        computedAt:      now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK:          `USER#${permULID}`,
      sK:          'SEGMENT#global',
      eventType:   'SEGMENT',
      status:      'ACTIVE',
      primaryCat:  'segment',
      subCategory: 'global',
      desc: JSON.stringify({
        spendBucket:    '100-200',
        visitFrequency: 'frequent',
        lastVisit:       '2026-04-03',
        totalSpend:      174.45,
        visitCount:      3,
        persona:         ['grocery_focused', 'regular_shopper'],
        computedAt:      now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log(`[user] 2 segment records written`);

  // ── Brand tenant account ──────────────────────────────────────────────────
  const tenantId = 'demo-tenant-001';
  const brandId  = 'demobrand';

  // ── Brand profile (RefDataEvent) ─────────────────────────────────────────
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK:          `BRAND#${brandId}`,
      sK:          'profile',
      eventType:   'BRAND_PROFILE',
      status:      'ACTIVE',
      primaryCat:  'brand',
      subCategory: 'dining',
      desc: JSON.stringify({
        id:              brandId,
        displayName:     'BeboCard Demo Café',
        color:           '#FF6B35',
        logoKey:         'brands/demobrand.png',
        category:        'dining',
        cardFormat:      'numeric_10',
        pointsName:      'Demo Points',
        supportsReceipts: true,
        barcodeType:     'QR',
        tenantId,
        website:         'https://democafe.bebocard.com.au',
        description:     'A demonstration brand account for BeboCard onboarding and testing.',
        allowedWidgetDomains: ['https://democafe.bebocard.com.au', 'http://localhost:3000'],
        widgetActions:   { invoice: true, giftCard: true },
      }),
      createdAt: now,
      updatedAt: now,
      version: 1,
    },
  }));
  console.log(`[tenant] brand profile written  BRAND#${brandId}`);

  // ── Tenant profile (RefDataEvent) ─────────────────────────────────────────
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK:          `TENANT#${tenantId}`,
      sK:          'PROFILE',
      eventType:   'TENANT',
      status:      'ACTIVE',
      primaryCat:  'tenant',
      desc: JSON.stringify({
        tenantId,
        tenantName:              'BeboCard Demo Co',
        tier:                    'engagement',        // 2500 events/month
        brandIds:                [brandId],
        status:                  'ACTIVE',
        billingStatus:           'ACTIVE',
        contactEmail:            'admin@democafe.bebocard.com.au',
        billingEmail:            'billing@democafe.bebocard.com.au',
        includedEventsPerMonth:  2500,
        monthlyFeeAud:           null,               // Stripe not connected in test
        stripeCustomerId:        null,
        stripeSubscriptionId:    null,
        tierStartDate:           now,
        scheduledTier:           null,
        scheduledTierEffectiveMonth: null,
        allowedWidgetDomains:    ['https://democafe.bebocard.com.au', 'http://localhost:3000'],
        widgetActions:           { invoice: true, giftCard: true },
        createdAt:               now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log(`[tenant] tenant profile written  TENANT#${tenantId}`);

  // ── Portal membership — tenant admin (RefDataEvent) ──────────────────────
  const adminEmail = 'demo-admin@democafe.bebocard.com.au';
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK:          `TENANT#${tenantId}`,
      sK:          `MEMBERSHIP#EMAIL#${adminEmail}`,
      eventType:   'PORTAL_MEMBERSHIP',
      status:      'ACTIVE',
      primaryCat:  'portal_membership',
      brandId:     null,
      subjectEmail: adminEmail,
      tenantId,
      desc: JSON.stringify({
        email:     adminEmail,
        tenantId,
        role:      'tenant_admin',
        brandId:   null,
        invitedBy: 'system',
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  // ── Portal membership — brand editor (RefDataEvent) ───────────────────────
  const editorEmail = 'demo-editor@democafe.bebocard.com.au';
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK:          `TENANT#${tenantId}`,
      sK:          `MEMBERSHIP#EMAIL#${editorEmail}`,
      eventType:   'PORTAL_MEMBERSHIP',
      status:      'ACTIVE',
      primaryCat:  'portal_membership',
      brandId,
      subjectEmail: editorEmail,
      tenantId,
      desc: JSON.stringify({
        email:     editorEmail,
        tenantId,
        role:      'brand_admin',
        brandId,
        invitedBy: adminEmail,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log(`[tenant] 2 portal memberships written`);

  // ── API key — all scopes (RefDataEvent) ──────────────────────────────────
  const { rawKey, keyId, keyHash } = generateApiKey();
  const allScopes = [
    'scan', 'receipt', 'offers', 'newsletters', 'catalogues',
    'analytics', 'stores', 'payment', 'consent', 'recurring',
    'gift_card', 'enrollment', 'smb',
  ];

  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK:          `BRAND#${brandId}`,
      sK:          `APIKEY#${keyId}`,
      eventType:   'API_KEY',
      status:      'ACTIVE',
      primaryCat:  'api_key',
      keyId,
      brandId,
      keyHash,
      scopes:      allScopes,
      rateLimit:   1000,
      createdBy:   adminEmail,
      desc: JSON.stringify({
        keyHash,
        scopes:    allScopes,
        rateLimit: 1000,
        createdBy: adminEmail,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log(`[tenant] API key written  BRAND#${brandId} APIKEY#${keyId}`);

  // ── Offers (RefDataEvent) ─────────────────────────────────────────────────
  const offers = [
    {
      id:        ulid(),
      title:     '20% off all hot drinks — weekdays only',
      body:      'Show your BeboCard QR code at the counter and enjoy 20% off any hot drink Monday to Friday.',
      validFrom: '2026-04-01',
      validTo:   '2026-04-30',
      imageKey:  'brands/demobrand/offers/hot-drinks.jpg',
      terms:     'One redemption per visit. Not valid with any other offer.',
    },
    {
      id:        ulid(),
      title:     'Free slice of cake with any coffee',
      body:      'Treat yourself — buy any size coffee and receive a free slice of our cake of the day.',
      validFrom: '2026-04-07',
      validTo:   '2026-04-14',
      imageKey:  'brands/demobrand/offers/cake.jpg',
      terms:     'Valid once per customer per day. In-store only.',
    },
  ];

  for (const offer of offers) {
    await dynamo.send(new PutCommand({
      TableName: REFDATA_TABLE,
      Item: {
        pK:          `BRAND#${brandId}`,
        sK:          `OFFER#${offer.id}`,
        eventType:   'OFFER',
        status:      'ACTIVE',
        primaryCat:  'offer',
        subCategory: brandId,
        desc: JSON.stringify({
          title:     offer.title,
          body:      offer.body,
          validFrom: offer.validFrom,
          validTo:   offer.validTo,
          imageKey:  offer.imageKey,
          terms:     offer.terms,
        }),
        createdAt: now,
        updatedAt: now,
      },
    }));
  }
  console.log(`[tenant] 2 offers written`);

  // ── Newsletter (RefDataEvent) ─────────────────────────────────────────────
  const newsletterId = ulid();
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK:          `BRAND#${brandId}`,
      sK:          `NEWSLETTER#${newsletterId}`,
      eventType:   'NEWSLETTER',
      status:      'ACTIVE',
      primaryCat:  'newsletter',
      subCategory: brandId,
      desc: JSON.stringify({
        subject: 'Welcome to BeboCard Demo Café — April Update',
        body: `
          <h2>Hello from BeboCard Demo Café!</h2>
          <p>Thanks for connecting your loyalty card through BeboCard.</p>
          <p>Here's what's happening this April:</p>
          <ul>
            <li><strong>New menu items</strong> — we've added seasonal specials including a mango iced latte and a mushroom bruschetta.</li>
            <li><strong>Weekday offer</strong> — 20% off all hot drinks Monday to Friday. Show your BeboCard at the counter.</li>
            <li><strong>Birthday treat</strong> — if your birthday is this month, enjoy a free slice of cake on us.</li>
          </ul>
          <p>See you soon!</p>
          <p><em>The Demo Café Team</em></p>
        `.trim(),
        sentAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log(`[tenant] 1 newsletter written`);

  // ── Catalogue (RefDataEvent) ──────────────────────────────────────────────
  const catalogueId = ulid();
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK:          `BRAND#${brandId}`,
      sK:          `CATALOGUE#${catalogueId}`,
      eventType:   'CATALOGUE',
      status:      'ACTIVE',
      primaryCat:  'catalogue',
      subCategory: brandId,
      desc: JSON.stringify({
        title:      'April Menu — BeboCard Demo Café',
        brandName:  'BeboCard Demo Café',
        brandColor: '#FF6B35',
        items: [
          {
            type:    'image',
            url:     'brands/demobrand/catalogues/april-menu-p1.jpg',
            caption: 'April menu cover',
          },
          {
            type:    'image',
            url:     'brands/demobrand/catalogues/april-menu-p2.jpg',
            caption: 'Hot drinks & specials',
          },
        ],
        targetSegments: {
          spendBuckets:     ['<100', '100-200'],
          visitFrequencies: ['frequent', 'occasional'],
        },
        sentAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log(`[tenant] 1 catalogue written`);

  // ── Store location (RefDataEvent) ─────────────────────────────────────────
  const storeId = ulid();
  await dynamo.send(new PutCommand({
    TableName: REFDATA_TABLE,
    Item: {
      pK:          `BRAND#${brandId}`,
      sK:          `STORE#${storeId}`,
      eventType:   'STORE',
      status:      'ACTIVE',
      primaryCat:  'store',
      subCategory: brandId,
      desc: JSON.stringify({
        storeId,
        name:      'BeboCard Demo Café — Sydney CBD',
        address:   '1 Demo Street, Sydney NSW 2000',
        lat:       -33.8688,
        lng:       151.2093,
        phone:     '+61 2 0000 0000',
        hours: {
          mon: '07:00-17:00',
          tue: '07:00-17:00',
          wed: '07:00-17:00',
          thu: '07:00-17:00',
          fri: '07:00-17:00',
          sat: '08:00-15:00',
          sun: 'closed',
        },
        geofenceRadiusMetres: 150,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log(`[tenant] 1 store written`);

  // ── Summary ────────────────────────────────────────────────────────────────
  console.log('\n════════════════════════════════════════════════════════════');
  console.log('  SEED COMPLETE — save these credentials');
  console.log('════════════════════════════════════════════════════════════');
  console.log('\n── Consumer test account ──────────────────────────────────');
  console.log(`  Name:           Alex Chen`);
  console.log(`  Email:          alex.chen@testuser.bebocard.com.au`);
  console.log(`  Cognito ID:     ${cognitoUserId}`);
  console.log(`  permULID:       ${permULID}`);
  console.log(`  secondaryULID:  ${secondaryULID}`);
  console.log(`  Cards:          Woolworths (default), Qantas, MYER one`);
  console.log(`  Receipts:       3 × Woolworths grocery`);
  console.log(`  Invoice:        ABC Supplies Pty Ltd — AUD 1,250.00 due 2026-04-20`);
  console.log(`  Gift card:      Woolworths $50 (GIFTCARD#${giftCardId})`);
  console.log(`  Subscriptions:  woolworths (all channels), qantas (newsletters + reminders)`);
  console.log('\n── Brand tenant account ───────────────────────────────────');
  console.log(`  Tenant:         BeboCard Demo Co`);
  console.log(`  Tenant ID:      ${tenantId}`);
  console.log(`  Tier:           engagement (2,500 events/month)`);
  console.log(`  Brand:          BeboCard Demo Café`);
  console.log(`  Brand ID:       ${brandId}`);
  console.log(`  Admin email:    ${adminEmail}  (tenant_admin)`);
  console.log(`  Editor email:   ${editorEmail}  (brand_admin)`);
  console.log(`  API key ID:     ${keyId}`);
  console.log(`  API key:        ${rawKey}`);
  console.log(`  Scopes:         all`);
  console.log(`  Offers:         2 active`);
  console.log(`  Newsletter:     1 sent`);
  console.log(`  Catalogue:      1 active`);
  console.log(`  Store:          Sydney CBD (STORE#${storeId})`);
  console.log('════════════════════════════════════════════════════════════\n');
}

seed().catch(console.error);
