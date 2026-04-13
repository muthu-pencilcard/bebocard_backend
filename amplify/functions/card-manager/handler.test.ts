import { describe, it, expect, vi, beforeEach } from 'vitest';

const { mockSend, mockCancelSubscription } = vi.hoisted(() => ({
  mockSend: vi.fn(),
  mockCancelSubscription: vi.fn().mockResolvedValue(undefined),
}));
const { mockHttpsRequest } = vi.hoisted(() => ({
  mockHttpsRequest: vi.fn(),
}));

import { handler } from './handler';

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));

vi.mock('https', () => ({
  default: {
    request: mockHttpsRequest,
  },
}));

vi.mock('@aws-sdk/lib-dynamodb', () => {
    return {
        DynamoDBDocumentClient: {
            from: () => ({ send: mockSend }),
        },
        PutCommand:    class PutCommand    { __type = 'PutCommand';    constructor(public input: any) { } },
        UpdateCommand: class UpdateCommand { __type = 'UpdateCommand'; constructor(public input: any) { } },
        GetCommand:    class GetCommand    { __type = 'GetCommand';    constructor(public input: any) { } },
        QueryCommand:  class QueryCommand  { __type = 'QueryCommand';  constructor(public input: any) { } },
        TransactWriteCommand: class TransactWriteCommand { __type = 'TransactWriteCommand'; constructor(public input: any) { } },
    };
});

// Mock dynamic imports used by card-manager
vi.mock('../subscription-proxy/handler', () => ({
  cancelSubscription: mockCancelSubscription,
}));

// Mock enrollment-handler dynamic imports (used by initiateEnrollment / respondToEnrollment)
vi.mock('../enrollment-handler/handler', () => ({
  respondToEnrollmentFn: vi.fn().mockResolvedValue({ success: true }),
  generateAlias: vi.fn((permULID: string, brandId: string) => `alias-${permULID}-${brandId}@bebocard.app`),
}));

describe('card-manager privacy tests', () => {
    beforeEach(() => {
        vi.resetAllMocks();
        mockSend.mockReset();
    });

    it('rejects unauthenticated requests instantly', async () => {
        const event = {
            info: { fieldName: 'rotateQR' },
            identity: null,
            arguments: {},
        } as any;

        await expect(handler(event, null as any, null as any)).rejects.toThrow('Identity missing permULID');
    });

    it('rotateQR safely generates new ULID and rotates atomicity', async () => {
        const event = {
            info: { fieldName: 'rotateQR' },
            identity: { claims: { 'custom:permULID': 'perm-ulid-123', 'cognito:username': 'test-user' } },
            arguments: {},
        } as any;

        mockSend.mockImplementation(async (cmd: any) => {
            if (cmd?.input?.Key?.pK) {
                return { Item: { secondaryULID: 'old-ulid', rotatesAt: '2020-01-01' } };
            }
            return {};
        });

        const res: any = await handler(event, null as any, null as any);
        expect(res).toBeDefined();

        const callCount = mockSend.mock.calls.length;
        expect(callCount).toBeGreaterThan(0);
    });
});

describe('gift card marketplace (Phase 13)', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSend.mockReset();
    mockHttpsRequest.mockReset();
  });

  it('purchaseGiftCard creates PENDING order record', async () => {
    mockSend.mockResolvedValue({ Item: { desc: '{"purchaseWebhookUrl": null}' } });
    const event = {
      info: { fieldName: 'purchaseGiftCard' },
      identity: { claims: { 'custom:permULID': 'test-perm', 'cognito:username': 'test-user' } },
      arguments: { brandId: 'woolworths', catalogItemId: 'cat-001', denomination: 50, currency: 'AUD' },
    } as any;
    const res: any = await handler(event, null as any, null as any);
    expect(res.status).toBe('PENDING');
    expect(res.orderId).toBeDefined();
    expect(res.denomination).toBe(50);
  });

  it('syncGiftCardBalance returns synced:false when no balance webhook configured', async () => {
    // First call: GetCommand for gift card record
    // Second call: GetCommand for brand profile (no balanceWebhookUrl)
    mockSend
      .mockResolvedValueOnce({ Item: { desc: '{"cardNumber":"1234","balance":50,"currency":"AUD"}' } })
      .mockResolvedValueOnce({ Item: { desc: '{}' } });

    const event = {
      info: { fieldName: 'syncGiftCardBalance' },
      identity: { claims: { 'custom:permULID': 'test-perm', 'cognito:username': 'test-user' } },
      arguments: { cardSK: 'GIFTCARD#delivery-01', brandId: 'woolworths' },
    } as any;
    const res: any = await handler(event, null as any, null as any);
    expect(res.synced).toBe(false);
    expect(res.balance).toBe(50);
  });

  it('syncGiftCardBalance throws when gift card not found', async () => {
    mockSend.mockResolvedValueOnce({ Item: undefined });
    const event = {
      info: { fieldName: 'syncGiftCardBalance' },
      identity: { claims: { 'custom:permULID': 'test-perm', 'cognito:username': 'test-user' } },
      arguments: { cardSK: 'GIFTCARD#nonexistent', brandId: 'woolworths' },
    } as any;
    await expect(handler(event, null as any, null as any)).rejects.toThrow('Gift card not found');
  });

  it('syncGiftCardBalance persists updated balance and lastBalanceSync when webhook succeeds', async () => {
    mockSend
      .mockResolvedValueOnce({
        Item: {
          desc: JSON.stringify({
            cardNumber: '1234',
            balance: 50,
            currency: 'AUD',
            brandId: 'woolworths',
          }),
        },
      })
      .mockResolvedValueOnce({
        Item: {
          desc: JSON.stringify({
            balanceWebhookUrl: 'https://brand.example.com/balance',
            balanceWebhookSecret: 'secret',
          }),
        },
      })
      .mockResolvedValueOnce({});

    mockHttpsRequest.mockImplementation((options: any, callback: (res: any) => void) => {
      const res = {
        on: (event: string, handler: (chunk?: Buffer) => void) => {
          if (event === 'data') handler(Buffer.from(JSON.stringify({ balance: 42.5, currency: 'AUD' })));
          if (event === 'end') handler();
        },
      };
      callback(res);
      return {
        on: vi.fn(),
        write: vi.fn(),
        end: vi.fn(),
      };
    });

    const event = {
      info: { fieldName: 'syncGiftCardBalance' },
      identity: { claims: { 'custom:permULID': 'test-perm', 'cognito:username': 'test-user' } },
      arguments: { cardSK: 'GIFTCARD#delivery-01', brandId: 'woolworths' },
    } as any;

    const res: any = await handler(event, null as any, null as any);
    expect(res.synced).toBe(true);
    expect(res.balance).toBe(42.5);

    const updateCall = mockSend.mock.calls[2]?.[0]?.input;
    const updatedDesc = JSON.parse(String(updateCall.ExpressionAttributeValues[':desc']));
    expect(updatedDesc.balance).toBe(42.5);
    expect(updatedDesc.currency).toBe('AUD');
    expect(updatedDesc.lastBalanceSync).toBeDefined();
  });
});

describe('offer snooze (Phase 19)', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSend.mockReset();
  });

  it('stores brand-level offer snooze on the subscription record desc', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { desc: '{"offers":true}' } })
      .mockResolvedValueOnce({});

    const event = {
      info: { fieldName: 'snoozeOffers' },
      identity: { claims: { 'custom:permULID': 'test-perm', 'cognito:username': 'test-user' } },
      arguments: { brandId: 'woolworths', until: '2026-05-01T00:00:00.000Z' },
    } as any;

    const res: any = await handler(event, null as any, null as any);
    expect(res.success).toBe(true);

    const updateCall = mockSend.mock.calls[1]?.[0]?.input;
    expect(updateCall.Key.sK).toBe('SUBSCRIPTION#woolworths');
    expect(String(updateCall.ExpressionAttributeValues[':desc'])).toContain('offersSnoozeUntil');
  });

  it('stores global offer snooze on the preferences desc', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { desc: '{"reminders":{"offerExpiry":true}}' } })
      .mockResolvedValueOnce({});

    const event = {
      info: { fieldName: 'snoozeOffers' },
      identity: { claims: { 'custom:permULID': 'test-perm', 'cognito:username': 'test-user' } },
      arguments: { until: '2026-05-01T00:00:00.000Z' },
    } as any;

    const res: any = await handler(event, null as any, null as any);
    expect(res.success).toBe(true);

    const updateCall = mockSend.mock.calls[1]?.[0]?.input;
    expect(updateCall.Key.sK).toBe('PREFERENCES');
    expect(String(updateCall.ExpressionAttributeValues[':desc'])).toContain('offersGlobalSnoozeUntil');
  });
});

// Helper to make an AppSync event
function makeEvent(fieldName: string, args: Record<string, unknown> = {}, permULID = 'test-perm') {
  return {
    info:      { fieldName },
    identity:  { claims: { 'custom:permULID': permULID, 'cognito:username': 'test-user' } },
    arguments: args,
  } as any;
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 10 — QR Rotation & Tracking Severance (Patent Claims 75–86)
// ─────────────────────────────────────────────────────────────────────────────

describe('rotateQR (Phase 10)', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSend.mockReset();
    process.env.USER_TABLE  = 'UserDataEvent';
    process.env.ADMIN_TABLE = 'AdminDataEvent';
    process.env.REFDATA_TABLE = 'RefDataEvent';
  });

  it('full happy path: reads IDENTITY, creates new SCAN index, revokes old, updates IDENTITY, writes rotation log', async () => {
    // 1. GetCommand IDENTITY (with old secondaryULID + frequency)
    // 2. GetCommand old SCAN index
    // 3. PutCommand new SCAN index
    // 4. UpdateCommand revoke old SCAN index
    // 5. UpdateCommand update IDENTITY (conditional)
    // 6. PutCommand rotation log
    mockSend
      .mockResolvedValueOnce({
        Item: {
          secondaryULID: 'old-ulid',
          rotatesAt: '2020-01-01T00:00:00.000Z',
          desc: JSON.stringify({ rotationFrequency: 'every_24h' }),
        },
      })
      .mockResolvedValueOnce({
        Item: { desc: JSON.stringify({ cards: [{ brand: 'woolworths', cardId: 'CARD-1', isDefault: true }] }) },
      })
      .mockResolvedValueOnce({}) // PutCommand new SCAN index
      .mockResolvedValueOnce({}) // UpdateCommand revoke old
      .mockResolvedValueOnce({}) // UpdateCommand update IDENTITY
      .mockResolvedValueOnce({}); // PutCommand rotation log

    const res: any = await handler(makeEvent('rotateQR'), null as any, null as any);

    expect(res.success).toBe(true);
    expect(res.alreadyRotated).toBe(false);
    expect(res.newSecondaryULID).toBeDefined();
    expect(res.frequency).toBe('every_24h');
    expect(res.rotatesAt).toBeDefined();

    // Verify new SCAN index was written to AdminDataEvent
    const txCmd = mockSend.mock.calls.find(([cmd]: any[]) => cmd.__type === 'TransactWriteCommand');
    expect(txCmd).toBeDefined();
    const newScanPut = txCmd![0].input.TransactItems.find((item: any) => item.Put && String(item.Put.Item.pK).startsWith('SCAN#'));
    expect(newScanPut).toBeDefined();
    const scanItem = newScanPut.Put.Item;
    expect(scanItem.status).toBe('ACTIVE');
    expect(JSON.parse(scanItem.desc).cards).toHaveLength(1); // cards copied from old index

    // Verify old SCAN index was revoked
    const revokeUpdate = txCmd![0].input.TransactItems.find((item: any) => 
      item.Update && item.Update.ExpressionAttributeValues?.[':revoked'] === 'REVOKED'
    );
    expect(revokeUpdate).toBeDefined();
    expect(revokeUpdate.Update.Key.pK).toBe('SCAN#old-ulid');

    // Verify rotation log was written to UserDataEvent
    const rotationLog = mockSend.mock.calls.find(([cmd]: any[]) =>
      cmd.__type === 'PutCommand' && String(cmd.input?.Item?.sK).startsWith('ROTATION#'),
    );
    expect(rotationLog).toBeDefined();
    const logDesc = JSON.parse((rotationLog![0] as any).input.Item.desc);
    expect(logDesc.oldSecondaryULID).toBe('old-ulid');
    expect(logDesc.frequency).toBe('every_24h');
  });

  it('defaults to every_24h frequency when not stored in IDENTITY desc', async () => {
    mockSend
      .mockResolvedValueOnce({
        Item: { secondaryULID: 'old-ulid', desc: JSON.stringify({}) }, // no rotationFrequency
      })
      .mockResolvedValueOnce({ Item: { desc: JSON.stringify({ cards: [] }) } })
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({});

    const res: any = await handler(makeEvent('rotateQR'), null as any, null as any);
    expect(res.frequency).toBe('every_24h');
  });

  it('throws when IDENTITY record is missing', async () => {
    mockSend.mockResolvedValueOnce({ Item: null }); // IDENTITY missing
    await expect(handler(makeEvent('rotateQR'), null as any, null as any)).rejects.toThrow('IDENTITY record not found');
  });

  it('handles concurrent rotation (Race Guard): re-reads identity and returning authoritative values on conflict', async () => {
    // 1. GetCommand IDENTITY (returning old values)
    // 2. TransactWrite throws CancellationReasons[1].Code === 'ConditionalCheckFailed'
    // 3. Re-read GetCommand IDENTITY (returning values from the winner)
    mockSend
      .mockResolvedValueOnce({
        Item: { secondaryULID: 'old-ulid', desc: JSON.stringify({ rotationFrequency: 'every_24h' }) },
      })
      .mockResolvedValueOnce({ Item: { desc: '{}' } }) // Old SCAN index
      .mockRejectedValueOnce({
        name: 'TransactionCanceledException',
        CancellationReasons: [
          { Code: 'None' },
          { Code: 'ConditionalCheckFailed' }, // IDENTITY update failed
          { Code: 'None' }
        ]
      })
      .mockResolvedValueOnce({
        Item: { secondaryULID: 'winner-ulid', rotatesAt: '2026-04-12T10:00:00Z' }
      });

    const res: any = await handler(makeEvent('rotateQR'), null as any, null as any);
    
    expect(res.success).toBe(true);
    expect(res.alreadyRotated).toBe(true);
    expect(res.newSecondaryULID).toBe('winner-ulid');
    expect(res.rotatesAt).toBe('2026-04-12T10:00:00Z');
    
    // Check that we attempted a transaction with the correct oldULID as the condition
    const txCall = mockSend.mock.calls.find(([cmd]) => cmd.__type === 'TransactWriteCommand');
    const identityUpdate = (txCall![0] as any).input.TransactItems[1].Update;
    expect(identityUpdate.ConditionExpression).toContain('secondaryULID = :old');
    expect(identityUpdate.ExpressionAttributeValues[':old']).toBe('old-ulid');
  });
});

describe('setRotationFrequency (Phase 10)', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSend.mockReset();
    process.env.USER_TABLE    = 'UserDataEvent';
    process.env.ADMIN_TABLE   = 'AdminDataEvent';
    process.env.REFDATA_TABLE = 'RefDataEvent';
  });

  it.each(['every_scan', 'every_24h', 'every_7d', 'manual'] as const)(
    'accepts valid frequency: %s',
    async (frequency) => {
      mockSend
        .mockResolvedValueOnce({ Item: { desc: JSON.stringify({}) } }) // GetCommand IDENTITY
        .mockResolvedValueOnce({});                                      // UpdateCommand IDENTITY

      const res: any = await handler(makeEvent('setRotationFrequency', { frequency }), null as any, null as any);
      expect(res.success).toBe(true);
      expect(res.frequency).toBe(frequency);
      expect(res.rotatesAt).toBeDefined();

      // For every_scan: rotatesAt should be approximately now (≤ 1 second delta)
      if (frequency === 'every_scan') {
        const delta = Math.abs(new Date(res.rotatesAt).getTime() - Date.now());
        expect(delta).toBeLessThan(2000);
      }

      // For manual: rotatesAt should be ~100 years from now
      if (frequency === 'manual') {
        const yearsAhead = (new Date(res.rotatesAt).getTime() - Date.now()) / (365.25 * 24 * 60 * 60 * 1000);
        expect(yearsAhead).toBeGreaterThan(50);
      }

      // Verify frequency stored in IDENTITY desc
      const updateCall = mockSend.mock.calls.find(([cmd]: any[]) => cmd.__type === 'UpdateCommand');
      const desc = JSON.parse((updateCall![0] as any).input.ExpressionAttributeValues[':desc']);
      expect(desc.rotationFrequency).toBe(frequency);
    },
  );

  it('throws for an invalid frequency value', async () => {
    await expect(
      handler(makeEvent('setRotationFrequency', { frequency: 'every_minute' }), null as any, null as any),
    ).rejects.toThrow('Invalid frequency');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Phase 14 — Subscription Intelligence
// ─────────────────────────────────────────────────────────────────────────────

describe('addManualSubscription (Phase 14)', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSend.mockReset();
    process.env.USER_TABLE    = 'UserDataEvent';
    process.env.ADMIN_TABLE   = 'AdminDataEvent';
    process.env.REFDATA_TABLE = 'RefDataEvent';
  });

  it('writes RECURRING#manual# record to USER_TABLE', async () => {
    mockSend.mockResolvedValueOnce({}); // PutCommand

    const res: any = await handler(makeEvent('addManualSubscription', {
      brandName: 'Netflix',
      productName: 'Standard Plan',
      amount: 18.99,
      currency: 'AUD',
      frequency: 'monthly',
      nextBillingDate: '2026-05-01',
      category: 'entertainment',
    }), null as any, null as any);

    expect(res.success).toBe(true);
    expect(res.subSK).toMatch(/^RECURRING#manual#/);

    const putCall = mockSend.mock.calls.find(([cmd]: any[]) => cmd.__type === 'PutCommand');
    expect(putCall).toBeDefined();
    const item = (putCall![0] as any).input.Item;
    expect(item.pK).toBe('USER#test-perm');
    expect(item.sK).toMatch(/^RECURRING#manual#/);
    expect(item.eventType).toBe('SUBSCRIPTION');
    expect(item.status).toBe('ACTIVE');
    const desc = JSON.parse(item.desc);
    expect(desc.brandName).toBe('Netflix');
    expect(desc.amount).toBe(18.99);
    expect(desc.source).toBe('manual');
    expect(desc.currency).toBe('AUD');
  });

  it('defaults currency to AUD when not provided', async () => {
    mockSend.mockResolvedValueOnce({});
    const res: any = await handler(makeEvent('addManualSubscription', {
      brandName: 'Spotify', productName: 'Premium', amount: 12.99,
    }), null as any, null as any);
    expect(res.success).toBe(true);
    const putCall = mockSend.mock.calls[0]?.[0] as any;
    const desc = JSON.parse(putCall.input.Item.desc);
    expect(desc.currency).toBe('AUD');
  });
});

describe('cancelRecurring (Phase 14)', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSend.mockReset();
    mockCancelSubscription.mockReset();
    mockCancelSubscription.mockResolvedValue(undefined);
    process.env.USER_TABLE    = 'UserDataEvent';
    process.env.ADMIN_TABLE   = 'AdminDataEvent';
    process.env.REFDATA_TABLE = 'RefDataEvent';
  });

  it('delegates to cancelSubscription and returns CANCELLED_BY_USER status', async () => {
    const res: any = await handler(makeEvent('cancelRecurring', {
      subId: 'sub-001', brandId: 'netflix',
    }), null as any, null as any);

    expect(res.success).toBe(true);
    expect(res.status).toBe('CANCELLED_BY_USER');
    expect(mockCancelSubscription).toHaveBeenCalledWith('test-perm', 'netflix', 'sub-001', 'CANCELLED_BY_USER');
  });
});

describe('updatePreferences (Phase 14)', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSend.mockReset();
    process.env.USER_TABLE    = 'UserDataEvent';
    process.env.ADMIN_TABLE   = 'AdminDataEvent';
    process.env.REFDATA_TABLE = 'RefDataEvent';
  });

  it('merges reminder toggles into PREFERENCES record', async () => {
    const existingPrefs = { reminders: { offerExpiry: true, invoiceDue: true } };
    mockSend
      .mockResolvedValueOnce({ Item: { desc: JSON.stringify(existingPrefs) } }) // GetCommand PREFERENCES
      .mockResolvedValueOnce({});                                                  // UpdateCommand PREFERENCES

    const res: any = await handler(makeEvent('updatePreferences', {
      reminders: { offerExpiry: false, invoiceDue: true, giftCardExpiry: true },
    }), null as any, null as any);

    expect(res.success).toBe(true);

    const updateCall = mockSend.mock.calls.find(([cmd]: any[]) => cmd.__type === 'UpdateCommand');
    expect(updateCall).toBeDefined();
    const vals = (updateCall![0] as any).input.ExpressionAttributeValues;
    const desc = JSON.parse(vals[':desc']);
    expect(desc.reminders.offerExpiry).toBe(false);
    expect(desc.reminders.invoiceDue).toBe(true);
    expect(desc.reminders.giftCardExpiry).toBe(true);

    // Must write to the PREFERENCES sK
    expect((updateCall![0] as any).input.Key.sK).toBe('PREFERENCES');
  });
});

describe('invoice usage tracking (Phase 20)', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockSend.mockReset();
  });

  it('increments tenant invoice usage when invoice is created for a tenant brand', async () => {
    mockSend
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ Item: { desc: JSON.stringify({ tenantId: 'tenant-1' }) } })
      .mockResolvedValueOnce({
        Item: {
          status: 'ACTIVE',
          createdAt: '2026-04-01T00:00:00.000Z',
          desc: JSON.stringify({
            tier: 'engagement',
            billingStatus: 'ACTIVE',
            stripeCustomerId: 'cus_123',
            stripeSubscriptionId: 'sub_123',
          }),
        },
      })
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({
        Item: {
          usageCount: 1,
          desc: JSON.stringify({
            lastUpdatedAt: '2026-04-02T00:00:00.000Z',
            lastBrandId: 'woolworths',
          }),
        },
      });

    const event = {
      info: { fieldName: 'addInvoice' },
      identity: { claims: { 'custom:permULID': 'test-perm', 'cognito:username': 'test-user' } },
      arguments: {
        brandId: 'woolworths',
        supplier: 'Woolworths',
        amount: 42.5,
        dueDate: '2026-04-10',
        invoiceNumber: 'INV-001',
        category: 'groceries',
        notes: 'Tenant-supplied invoice',
        currency: 'AUD',
      },
    } as any;

    const res: any = await handler(event, null as any, null as any);
    expect(res.success).toBe(true);

    const usageUpdate = mockSend.mock.calls.find(([cmd]) =>
      cmd?.input?.Key?.pK === 'TENANT#tenant-1' && String(cmd?.input?.Key?.sK ?? '').includes('#invoices'));
    expect(usageUpdate).toBeDefined();
  });
});
