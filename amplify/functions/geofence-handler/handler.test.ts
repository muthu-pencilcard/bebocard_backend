import { vi, describe, it, expect, beforeEach } from 'vitest';

// ── Hoisted mocks ─────────────────────────────────────────────────────────────

const { mockDdbSend, mockFcmSend, mockGetTenantState, mockIncrementUsage } = vi.hoisted(() => ({
  mockDdbSend: vi.fn(),
  mockFcmSend: vi.fn(),
  mockGetTenantState: vi.fn(),
  mockIncrementUsage: vi.fn(),
}));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) { this.send = mockDdbSend; }),
  PutItemCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutItemCommand', input });
  }),
  QueryCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'QueryCommand', input });
  }),
  GetItemCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetItemCommand', input });
  }),
  AttributeValue: {},
}));

vi.mock('@aws-sdk/util-dynamodb', () => ({
  marshall: vi.fn((obj: Record<string, unknown>) => obj),
  unmarshall: vi.fn((obj: Record<string, unknown>) => obj),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn(() => ({ send: mockDdbSend })) },
}));

vi.mock('firebase-admin/app', () => ({
  initializeApp: vi.fn(),
  getApps: vi.fn(() => [{ name: 'app' }]),
  cert: vi.fn((x: unknown) => x),
}));

vi.mock('firebase-admin/messaging', () => ({
  getMessaging: vi.fn(() => ({ send: mockFcmSend })),
}));

vi.mock('../../shared/secure-handler-wrapper', () => ({
  withGraphQLHandler: (fn: unknown) => fn,
}));

vi.mock('../../shared/tenant-billing', () => ({
  getTenantStateForBrand: mockGetTenantState,
  incrementTenantUsageCounter: mockIncrementUsage,
}));

import { handler as rawHandler } from './handler.js';

// ── Helpers ───────────────────────────────────────────────────────────────────

type AppSyncEvent = Parameters<typeof rawHandler>[0];
const handler = ((event: AppSyncEvent) => rawHandler(event, {} as never, () => {})) as (event: AppSyncEvent) => Promise<unknown>;

function makeEvent(fieldName: string, args: Record<string, unknown>): AppSyncEvent {
  return {
    info: { fieldName },
    arguments: args,
    identity: null,
    source: null,
    request: { headers: {} } as never,
    prev: null,
    stash: {},
  } as unknown as AppSyncEvent;
}

// Valid timestamp within ±5 min of now
function nowIso(): string {
  return new Date().toISOString();
}

const ACTIVE_TENANT = { active: true, tier: 'intelligence', tenantId: 'tenant-001' };

beforeEach(() => {
  vi.clearAllMocks();
  process.env.USER_TABLE  = 'test-user-table';
  process.env.ADMIN_TABLE = 'test-admin-table';
  process.env.REF_TABLE   = 'test-ref-table';
  process.env.FIREBASE_SERVICE_ACCOUNT_JSON = JSON.stringify({ type: 'service_account' });
  mockFcmSend.mockResolvedValue('msg-id');
  mockGetTenantState.mockResolvedValue(ACTIVE_TENANT);
  mockIncrementUsage.mockResolvedValue(undefined);
});

// ── registerDeviceToken ───────────────────────────────────────────────────────

describe('registerDeviceToken', () => {
  it('returns QUEUED when permULID is absent', async () => {
    const result = await handler(makeEvent('registerDeviceToken', { token: 'fcm-tok', platform: 'ios' }));
    expect(result).toBe('QUEUED');
    expect(mockDdbSend).not.toHaveBeenCalled();
  });

  it('writes DEVICE_TOKEN record and returns OK', async () => {
    mockDdbSend.mockResolvedValue({});
    const result = await handler(makeEvent('registerDeviceToken', {
      token: 'fcm-tok',
      platform: 'android',
      permULID: 'PERM-001',
    }));
    expect(result).toBe('OK');
    expect(mockDdbSend).toHaveBeenCalledOnce();
    const call = mockDdbSend.mock.calls[0][0] as { __type: string; input: { Item: Record<string, unknown> } };
    expect(call.__type).toBe('PutItemCommand');
    expect(call.input.Item.sK).toBe('DEVICE_TOKEN');
  });
});

// ── unregisterDeviceToken ─────────────────────────────────────────────────────

describe('unregisterDeviceToken', () => {
  it('writes DEVICE_TOKEN record with INACTIVE status and returns OK', async () => {
    mockDdbSend.mockResolvedValue({});
    const result = await handler(makeEvent('unregisterDeviceToken', {
      token: 'fcm-tok',
      permULID: 'PERM-001',
    }));
    expect(result).toBe('OK');
    const call = mockDdbSend.mock.calls[0][0] as { __type: string; input: { Item: Record<string, unknown> } };
    expect(call.__type).toBe('PutItemCommand');
    expect(call.input.Item.status).toBe('INACTIVE');
  });
});

// ── getNearbyStores ───────────────────────────────────────────────────────────

describe('getNearbyStores', () => {
  it('throws on invalid latitude', async () => {
    await expect(handler(makeEvent('getNearbyStores', {
      brandId: 'woolworths', lat: 999, lng: 151, radiusKm: 1, limit: 5,
    }))).rejects.toThrow('Invalid latitude');
  });

  it('throws on invalid longitude', async () => {
    await expect(handler(makeEvent('getNearbyStores', {
      brandId: 'woolworths', lat: -33.8, lng: 999, radiusKm: 1, limit: 5,
    }))).rejects.toThrow('Invalid longitude');
  });

  it('throws when radiusKm exceeds 5', async () => {
    await expect(handler(makeEvent('getNearbyStores', {
      brandId: 'woolworths', lat: -33.8, lng: 151.2, radiusKm: 10, limit: 5,
    }))).rejects.toThrow('radiusKm');
  });

  it('returns nearby stores within radiusKm', async () => {
    // Store 0.1 km away (should be included), store 10 km away (should be excluded)
    mockDdbSend.mockResolvedValue({
      Items: [
        { sK: 'STORE#woolworths#s1', desc: JSON.stringify({ lat: -33.8001, lng: 151.2001 }) },
        { sK: 'STORE#woolworths#s2', desc: JSON.stringify({ lat: -34.0, lng: 151.5 }) },
      ],
    });

    const result = await handler(makeEvent('getNearbyStores', {
      brandId: 'woolworths', lat: -33.8, lng: 151.2, radiusKm: 1, limit: 5,
    })) as Array<{ sK: string }>;

    expect(result).toHaveLength(1);
    expect(result[0].sK).toBe('STORE#woolworths#s1');
  });

  it('returns empty array when no stores are in range', async () => {
    mockDdbSend.mockResolvedValue({ Items: [] });
    const result = await handler(makeEvent('getNearbyStores', {
      brandId: 'woolworths', lat: -33.8, lng: 151.2, radiusKm: 1, limit: 5,
    }));
    expect(result).toEqual([]);
  });
});

// ── reportGeofenceEntry ───────────────────────────────────────────────────────

describe('reportGeofenceEntry', () => {
  it('returns INVALID_ENTRY_TIME for a non-parseable timestamp', async () => {
    const result = await handler(makeEvent('reportGeofenceEntry', {
      secondaryULID: 'SEC-001',
      geofenceId: 'STORE#woolworths#s1',
      entryTime: 'not-a-date',
    }));
    expect(result).toBe('INVALID_ENTRY_TIME');
  });

  it('returns ENTRY_TIME_DRIFT for a timestamp more than 5 min in the past', async () => {
    const old = new Date(Date.now() - 6 * 60 * 1000).toISOString();
    const result = await handler(makeEvent('reportGeofenceEntry', {
      secondaryULID: 'SEC-001',
      geofenceId: 'STORE#woolworths#s1',
      entryTime: old,
    }));
    expect(result).toBe('ENTRY_TIME_DRIFT');
  });

  it('returns ULID_NOT_FOUND when secondaryULID has no SCAN# record', async () => {
    mockDdbSend.mockResolvedValue({ Items: [] });
    const result = await handler(makeEvent('reportGeofenceEntry', {
      secondaryULID: 'SEC-MISSING',
      geofenceId: 'STORE#woolworths#s1',
      entryTime: nowIso(),
    }));
    expect(result).toBe('ULID_NOT_FOUND');
  });

  it('returns INVALID_GEOFENCE for a malformed geofenceId', async () => {
    mockDdbSend.mockResolvedValue({ Items: [{ sK: 'PERM-001' }] });
    const result = await handler(makeEvent('reportGeofenceEntry', {
      secondaryULID: 'SEC-001',
      geofenceId: 'BAD_FORMAT',
      entryTime: nowIso(),
    }));
    expect(result).toBe('INVALID_GEOFENCE');
  });

  it('returns TENANT_BILLING_SUSPENDED when tenant is inactive', async () => {
    mockDdbSend.mockResolvedValue({ Items: [{ sK: 'PERM-001' }] });
    mockGetTenantState.mockResolvedValue({ active: false, tier: 'base', tenantId: 'tenant-001' });
    const result = await handler(makeEvent('reportGeofenceEntry', {
      secondaryULID: 'SEC-001',
      geofenceId: 'STORE#woolworths#s1',
      entryTime: nowIso(),
    }));
    expect(result).toBe('TENANT_BILLING_SUSPENDED');
  });

  it('returns TENANT_NOT_ELIGIBLE when tenant tier is not intelligence', async () => {
    mockDdbSend.mockResolvedValue({ Items: [{ sK: 'PERM-001' }] });
    mockGetTenantState.mockResolvedValue({ active: true, tier: 'base', tenantId: 'tenant-001' });
    const result = await handler(makeEvent('reportGeofenceEntry', {
      secondaryULID: 'SEC-001',
      geofenceId: 'STORE#woolworths#s1',
      entryTime: nowIso(),
    }));
    expect(result).toBe('TENANT_NOT_ELIGIBLE');
  });

  it('returns NO_TOKEN when user has no device token', async () => {
    let queryCount = 0;
    mockDdbSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') {
        queryCount++;
        // First QueryCommand = SCAN# lookup; subsequent = visit count or offer
        if (queryCount === 1) return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
        return Promise.resolve({ Count: 0, Items: [] });
      }
      if (cmd.__type === 'PutItemCommand') return Promise.resolve({});
      if (cmd.__type === 'GetItemCommand') return Promise.resolve({ Item: null }); // no device token
      return Promise.resolve({ Items: [] });
    });

    const result = await handler(makeEvent('reportGeofenceEntry', {
      secondaryULID: 'SEC-001',
      geofenceId: 'STORE#woolworths#s1',
      entryTime: nowIso(),
    }));
    expect(result).toBe('NO_TOKEN');
  });

  it('returns SENT and sends FCM push on happy path', async () => {
    let queryCount = 0;
    mockDdbSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') {
        queryCount++;
        if (queryCount === 1) return Promise.resolve({ Items: [{ sK: 'PERM-001' }] }); // SCAN# resolve
        if (queryCount === 2) return Promise.resolve({ Count: 1, Items: [] });           // visit count
        return Promise.resolve({ Items: [] }); // no broadcast offer
      }
      if (cmd.__type === 'PutItemCommand') return Promise.resolve({});
      if (cmd.__type === 'GetItemCommand') {
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'device-token' }) } });
      }
      return Promise.resolve({ Items: [] });
    });

    const result = await handler(makeEvent('reportGeofenceEntry', {
      secondaryULID: 'SEC-001',
      geofenceId: 'STORE#woolworths#s1',
      entryTime: nowIso(),
    }));
    expect(result).toBe('SENT');
    expect(mockFcmSend).toHaveBeenCalledOnce();
  });

  it('uses personalised offer message when visitCount > 2 and broadcast offer is present', async () => {
    let queryCount = 0;
    mockDdbSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') {
        queryCount++;
        if (queryCount === 1) return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
        if (queryCount === 2) return Promise.resolve({ Count: 3, Items: [] }); // 3 visits
        // broadcast offer
        return Promise.resolve({
          Items: [{
            sK: 'OFFER#123',
            desc: JSON.stringify({ brandId: 'woolworths', brandName: 'Woolworths', headline: 'Save 10%', voucherCode: 'VOU10' }),
          }],
        });
      }
      if (cmd.__type === 'PutItemCommand') return Promise.resolve({});
      if (cmd.__type === 'GetItemCommand') {
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'device-token' }) } });
      }
      return Promise.resolve({ Items: [] });
    });

    await handler(makeEvent('reportGeofenceEntry', {
      secondaryULID: 'SEC-001',
      geofenceId: 'STORE#woolworths#s1',
      entryTime: nowIso(),
    }));

    const fcmCall = mockFcmSend.mock.calls[0][0] as { notification: { title: string; body: string } };
    expect(fcmCall.notification.title).toContain('Loyal customer');
    expect(fcmCall.notification.body).toContain("You're a regular");
  });

  it('returns FCM_ERROR when Firebase send throws', async () => {
    let queryCount = 0;
    mockDdbSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'QueryCommand') {
        queryCount++;
        if (queryCount === 1) return Promise.resolve({ Items: [{ sK: 'PERM-001' }] });
        if (queryCount === 2) return Promise.resolve({ Count: 0, Items: [] });
        return Promise.resolve({ Items: [] });
      }
      if (cmd.__type === 'PutItemCommand') return Promise.resolve({});
      if (cmd.__type === 'GetItemCommand') {
        return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'device-token' }) } });
      }
      return Promise.resolve({ Items: [] });
    });
    mockFcmSend.mockRejectedValue(new Error('Firebase down'));

    const result = await handler(makeEvent('reportGeofenceEntry', {
      secondaryULID: 'SEC-001',
      geofenceId: 'STORE#woolworths#s1',
      entryTime: nowIso(),
    }));
    expect(result).toBe('FCM_ERROR');
  });
});

describe('unknown fieldName', () => {
  it('throws for an unrecognised GraphQL field', async () => {
    await expect(handler(makeEvent('unknownField', {}))).rejects.toThrow('Unknown field');
  });
});
