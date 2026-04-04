import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { APIGatewayProxyEvent } from 'aws-lambda';

// ─── MOCKS ───
const { mockSend, mockFcmSend } = vi.hoisted(() => ({
    mockSend: vi.fn(),
    mockFcmSend: vi.fn(),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
    DynamoDBDocumentClient: {
        from: () => ({ send: mockSend })
    },
    PutCommand: class { constructor(public input: any) { } },
    GetCommand: class { constructor(public input: any) { } },
    UpdateCommand: class { constructor(public input: any) { } },
    QueryCommand: class { constructor(public input: any) { } }
}));

vi.mock('firebase-admin/app', () => ({
    initializeApp: vi.fn(),
    getApps: () => [],
    cert: vi.fn()
}));
vi.mock('firebase-admin/messaging', () => ({
    getMessaging: () => ({ send: mockFcmSend })
}));


// Mock API Key Auth shared library
vi.mock('../../shared/api-key-auth', () => ({
    validateApiKey: vi.fn().mockResolvedValue({ brandId: 'test-brand' }),
    extractApiKey: vi.fn().mockReturnValue('test-key'),
}));

// Mock Audit Logger
vi.mock('../../shared/audit-logger', () => ({
    withAuditLog: (dynamo: any, handler: any) => handler
}));

// ─── IMPORT HANDLER ───
import { handler } from './handler';

describe('brand-api-handler - unit tests', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockSend.mockImplementation(async () => ({ Items: [], Item: null })); // Default mock
        process.env.USER_TABLE = 'UserTable';
        process.env.REFDATA_TABLE = 'RefDataTable';
        process.env.ADMIN_TABLE = 'AdminTable';
        process.env.FIREBASE_SERVICE_ACCOUNT_JSON = '{"project_id":"test"}';
    });

    it('non-portal origins receive the production PORTAL_ORIGIN header (hardened CORS)', async () => {
        // Localhost is not in the allow-list in production — brand-api-handler always
        // reflects back PORTAL_ORIGIN so the browser's CORS check passes for the portal
        // and rejects for third-party origins.
        const event: any = {
            httpMethod: 'GET',
            path: '/offers',
            headers: { Origin: 'http://localhost:3000' }
        };

        const result: any = await handler(event, {} as any, () => { });
        expect(result.headers['Access-Control-Allow-Origin']).toBe('https://business.bebocard.com.au');
    });

    it('rejects forbidden origins', async () => {
        const event: any = {
            httpMethod: 'GET',
            path: '/offers',
            headers: { Origin: 'https://malicious.site' }
        };

        const result: any = await handler(event, {} as any, () => { });
        // Should fallback to main production domain if origin not in allow-list
        expect(result.headers['Access-Control-Allow-Origin']).toBe('https://business.bebocard.com.au');
    });

    it('handles GET /offers and returns mapped items', async () => {
        mockSend.mockResolvedValueOnce({
            Items: [
                { pK: 'BRAND#test-brand', sK: 'OFFER#123', desc: JSON.stringify({ title: 'Test Offer' }), status: 'ACTIVE' }
            ]
        });

        const event: any = {
            httpMethod: 'GET',
            path: '/offers',
            headers: {}
        };

        const result: any = await handler(event, {} as any, () => { });
        const body = JSON.parse(result.body);

        expect(result.statusCode).toBe(200);
        expect(body.offers).toHaveLength(1);
        expect(body.offers[0].title).toBe('Test Offer');
    });

    it('POST /offers creates a record and triggers fan-out', async () => {
        mockSend
            .mockResolvedValueOnce({
                Item: { desc: JSON.stringify({ tenantId: 'tenant-1' }) }
            })
            .mockResolvedValueOnce({
                Item: { status: 'ACTIVE', desc: JSON.stringify({ tier: 'base', billingStatus: 'ACTIVE', includedEventsPerMonth: 250 }) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 20, desc: JSON.stringify({}) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 10, desc: JSON.stringify({}) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 5, desc: JSON.stringify({}) }
            })
            .mockResolvedValueOnce({}) // PutCommand RefData
            .mockResolvedValueOnce({}) // UpdateCommand usage increment
            .mockResolvedValueOnce({
                Item: { usageCount: 21, desc: JSON.stringify({ lastUpdatedAt: '2026-04-02T00:00:00.000Z', lastBrandId: 'test-brand' }) }
            })
            .mockResolvedValueOnce({
            Items: [
                { pK: 'USER#user1', sK: 'SUBSCRIPTION#test-brand', status: 'ACTIVE', offers: true }
            ]
            })
            .mockResolvedValueOnce({
            Item: { desc: JSON.stringify({ token: 'fcm-token-1' }) }
            });
        mockFcmSend.mockResolvedValueOnce('msg-123');

        const event: any = {
            httpMethod: 'POST',
            path: '/offers',
            headers: {},
            body: JSON.stringify({
                title: 'New Discount!',
                validFrom: '2025-01-01',
                validTo: '2025-12-31'
            })
        };

        const result: any = await handler(event, {} as any, () => { });
        expect(result.statusCode).toBe(200);

        // Check if DynamoDB was called to store the offer
        expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({
            input: expect.objectContaining({
                Item: expect.objectContaining({ eventType: 'OFFER' })
            })
        }));
    });

    it('rejects unauthorized requests if API key is missing', async () => {
        const API_AUTH = await import('../../shared/api-key-auth');
        (API_AUTH.extractApiKey as any).mockReturnValueOnce(null);

        const event: any = {
            httpMethod: 'GET',
            path: '/offers',
            headers: {}
        };

        const result: any = await handler(event, {} as any, () => { });
        expect(result.statusCode).toBe(401);
    });

    it('GET /usage returns current tenant usage summary', async () => {
        mockSend
            .mockResolvedValueOnce({
                Item: { desc: JSON.stringify({ tenantId: 'tenant-1' }) }
            })
            .mockResolvedValueOnce({
                Item: { status: 'ACTIVE', desc: JSON.stringify({ tier: 'base', billingStatus: 'ACTIVE' }) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 2, desc: JSON.stringify({ lastUpdatedAt: '2026-04-02T00:00:00.000Z', lastBrandId: 'test-brand' }) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 1, desc: JSON.stringify({ lastUpdatedAt: '2026-04-02T00:00:00.000Z', lastBrandId: 'test-brand' }) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 0, desc: JSON.stringify({ lastUpdatedAt: '2026-04-02T00:00:00.000Z', lastBrandId: 'test-brand' }) }
            });

        const event: any = {
            httpMethod: 'GET',
            path: '/usage',
            headers: {}
        };

        const result: any = await handler(event, {} as any, () => { });
        const body = JSON.parse(result.body);

        expect(result.statusCode).toBe(200);
        expect(body.tenantId).toBe('tenant-1');
        expect(body.usage).toHaveLength(6);
        expect(body.usage[0].type).toBe('offers');
    });

    it('blocks billable sends when tenant billing is suspended', async () => {
        mockSend
            .mockResolvedValueOnce({
                Item: { desc: JSON.stringify({ tenantId: 'tenant-1' }) }
            })
            .mockResolvedValueOnce({
                Item: { status: 'ACTIVE', desc: JSON.stringify({ tier: 'base', billingStatus: 'SUSPENDED' }) }
            });

        const event: any = {
            httpMethod: 'POST',
            path: '/offers',
            headers: {},
            body: JSON.stringify({
                title: 'New Discount!',
                validFrom: '2025-01-01',
                validTo: '2025-12-31'
            })
        };

        const result: any = await handler(event, {} as any, () => { });

        expect(result.statusCode).toBe(403);
        expect(JSON.parse(result.body).error).toContain('suspended');
    });

    it('blocks base tier sends when monthly quota is exceeded', async () => {
        mockSend
            .mockResolvedValueOnce({
                Item: { desc: JSON.stringify({ tenantId: 'tenant-1' }) }
            })
            .mockResolvedValueOnce({
                Item: { status: 'ACTIVE', desc: JSON.stringify({ tier: 'base', billingStatus: 'ACTIVE', includedEventsPerMonth: 2 }) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 1, desc: JSON.stringify({}) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 1, desc: JSON.stringify({}) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 0, desc: JSON.stringify({}) }
            });

        const event: any = {
            httpMethod: 'POST',
            path: '/offers',
            headers: {},
            body: JSON.stringify({
                title: 'New Discount!',
                validFrom: '2025-01-01',
                validTo: '2025-12-31'
            })
        };

        const result: any = await handler(event, {} as any, () => { });

        expect(result.statusCode).toBe(403);
        expect(JSON.parse(result.body).error).toContain('quota exceeded');
    });

    it('allows engagement tier overage sends', async () => {
        mockSend
            .mockResolvedValueOnce({
                Item: { desc: JSON.stringify({ tenantId: 'tenant-1' }) }
            })
            .mockResolvedValueOnce({
                Item: { status: 'ACTIVE', desc: JSON.stringify({ tier: 'engagement', billingStatus: 'ACTIVE', includedEventsPerMonth: 1 }) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 1, desc: JSON.stringify({}) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 1, desc: JSON.stringify({}) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 0, desc: JSON.stringify({}) }
            })
            .mockResolvedValueOnce({})
            .mockResolvedValueOnce({})
            .mockResolvedValueOnce({
                Item: { usageCount: 2, desc: JSON.stringify({ lastUpdatedAt: '2026-04-02T00:00:00.000Z', lastBrandId: 'test-brand' }) }
            })
            .mockResolvedValueOnce({
                Items: []
            });

        const event: any = {
            httpMethod: 'POST',
            path: '/offers',
            headers: {},
            body: JSON.stringify({
                title: 'New Discount!',
                validFrom: '2025-01-01',
                validTo: '2025-12-31'
            })
        };

        const result: any = await handler(event, {} as any, () => { });

        expect(result.statusCode).toBe(200);
        expect(JSON.parse(result.body).billing.tier).toBe('engagement');
    });
});
