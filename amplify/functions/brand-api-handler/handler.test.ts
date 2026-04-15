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
    PutCommand:    class { constructor(public input: any) { } },
    GetCommand:    class { constructor(public input: any) { } },
    UpdateCommand: class { constructor(public input: any) { } },
    QueryCommand:  class { constructor(public input: any) { } },
    ScanCommand:   class { constructor(public input: any) { } },
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
        mockSend.mockReset();
        mockSend.mockImplementation(async () => ({ Items: [], Item: null })); // Default mock
        process.env.USER_TABLE = 'UserTable';
        process.env.REFDATA_TABLE = 'RefDataTable';
        process.env.ADMIN_TABLE = 'AdminTable';
        process.env.FIREBASE_SERVICE_ACCOUNT_JSON = '{"project_id":"test"}';
        delete process.env.ADMIN_API_KEY; // reset between tests
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
        // ALL_USAGE_TYPES has 15 entries:
        // Core (7): offers, newsletters, catalogues, invoices, geolocation, payments, consent
        // Engagement (4): newsletter_reads, offer_engagements, catalogue_views, delivery_outcomes
        // Intelligence (4): consent_decisions, payment_decisions, enrollment_decisions, subscription_changes
        mockSend
            .mockResolvedValueOnce({
                Item: { desc: JSON.stringify({ tenantId: 'tenant-1' }) }
            })
            .mockResolvedValueOnce({
                Item: { status: 'ACTIVE', desc: JSON.stringify({ tier: 'base', billingStatus: 'ACTIVE' }) }
            })
            // 15 usage type GetCommands (offers through subscription_changes)
            .mockResolvedValueOnce({
                Item: { usageCount: 2, desc: JSON.stringify({ lastUpdatedAt: '2026-04-02T00:00:00.000Z', lastBrandId: 'test-brand' }) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 1, desc: JSON.stringify({ lastUpdatedAt: '2026-04-02T00:00:00.000Z', lastBrandId: 'test-brand' }) }
            })
            .mockResolvedValueOnce({
                Item: { usageCount: 0, desc: JSON.stringify({ lastUpdatedAt: '2026-04-02T00:00:00.000Z', lastBrandId: 'test-brand' }) }
            })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } })
            .mockResolvedValueOnce({ Item: { usageCount: 0, desc: JSON.stringify({}) } });

        const event: any = {
            httpMethod: 'GET',
            path: '/usage',
            headers: {}
        };

        const result: any = await handler(event, {} as any, () => { });
        const body = JSON.parse(result.body);

        expect(result.statusCode).toBe(200);
        expect(body.tenantId).toBe('tenant-1');
        expect(body.usage).toHaveLength(15);
        expect(body.usage[0].type).toBe('offers');
        expect(body.usage[6].type).toBe('consent');
        expect(body.usage[14].type).toBe('subscription_changes');
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

    it('POST /subscription-catalog creates entry and returns 201', async () => {
        mockSend
            .mockResolvedValueOnce({ Item: null }) // GetCommand — no existing entry
            .mockResolvedValueOnce({});             // PutCommand

        const event: any = {
            httpMethod: 'POST',
            path: '/subscription-catalog',
            headers: {},
            body: JSON.stringify({
                providerName: 'PowerCo',
                category: 'utilities',
                invoiceType: 'RECURRING_INVOICE',
                plans: [{ planName: 'Standard', amount: 150, frequency: 'monthly', currency: 'AUD' }],
            }),
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(201);
        const body = JSON.parse(result.body);
        expect(body.providerId).toBe('test-brand');
        expect(body.listingStatus).toBe('UNLISTED');

        const putCall = mockSend.mock.calls[1][0];
        expect(putCall.input.Item.source).toBe('tenant');
        expect(putCall.input.Item.primaryCat).toBe('subscription_catalog');
        const desc = JSON.parse(putCall.input.Item.desc);
        expect(desc.invoiceType).toBe('RECURRING_INVOICE');
        expect(desc.isAffiliate).toBe(false);
        expect(desc.isTenantLinked).toBe(true);
        expect(desc.listingStatus).toBe('UNLISTED');
    });

    it('POST /subscription-catalog → 409 if entry already exists', async () => {
        mockSend.mockResolvedValueOnce({ Item: { pK: 'SUBSCRIPTION_CATALOG#test-brand', desc: '{}' } });

        const event: any = {
            httpMethod: 'POST',
            path: '/subscription-catalog',
            headers: {},
            body: JSON.stringify({
                providerName: 'PowerCo',
                category: 'utilities',
                plans: [{ planName: 'Standard', amount: 150, frequency: 'monthly' }],
            }),
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(409);
    });

    it('POST /subscription-catalog → 400 on invalid body (missing plans)', async () => {
        const event: any = {
            httpMethod: 'POST',
            path: '/subscription-catalog',
            headers: {},
            body: JSON.stringify({ providerName: 'PowerCo', category: 'utilities' }),
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(400);
    });

    it('PUT /subscription-catalog updates providerName while preserving admin-controlled isAffiliate', async () => {
        const existingDesc = {
            providerId: 'test-brand', tenantBrandId: 'test-brand', providerName: 'OldName',
            plans: [], isAffiliate: true, affiliateUrl: 'https://example.com', listingStatus: 'ACTIVE',
        };
        mockSend
            .mockResolvedValueOnce({ Item: { desc: JSON.stringify(existingDesc) } }) // GetCommand
            .mockResolvedValueOnce({});                                               // UpdateCommand

        const event: any = {
            httpMethod: 'PUT',
            path: '/subscription-catalog',
            headers: {},
            body: JSON.stringify({ providerName: 'NewName' }),
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(200);
        const updateCall = mockSend.mock.calls[1][0];
        const updated = JSON.parse(updateCall.input.ExpressionAttributeValues[':desc']);
        expect(updated.providerName).toBe('NewName');
        expect(updated.isAffiliate).toBe(true);      // preserved from existing
        expect(updated.listingStatus).toBe('ACTIVE'); // preserved from existing
    });

    it('PUT /subscription-catalog → 404 if no existing entry', async () => {
        mockSend.mockResolvedValueOnce({ Item: null });

        const event: any = {
            httpMethod: 'PUT',
            path: '/subscription-catalog',
            headers: {},
            body: JSON.stringify({ providerName: 'Test' }),
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(404);
    });

    it('GET /subscription-catalog returns the catalog entry for the brand', async () => {
        const desc = { providerId: 'test-brand', providerName: 'PowerCo', listingStatus: 'UNLISTED' };
        mockSend.mockResolvedValueOnce({ Item: { desc: JSON.stringify(desc) } });

        const event: any = {
            httpMethod: 'GET',
            path: '/subscription-catalog',
            headers: {},
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(200);
        expect(JSON.parse(result.body).providerName).toBe('PowerCo');
    });

    it('GET /subscription-catalog → 404 when brand has no entry', async () => {
        mockSend.mockResolvedValueOnce({ Item: null });

        const event: any = {
            httpMethod: 'GET',
            path: '/subscription-catalog',
            headers: {},
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(404);
    });

    it('GET /admin/subscription-catalog → 401 without correct admin key', async () => {
        process.env.ADMIN_API_KEY = 'real-admin-key';

        const event: any = {
            httpMethod: 'GET',
            path: '/admin/subscription-catalog',
            headers: { 'x-admin-api-key': 'wrong-key' },
            queryStringParameters: null,
        };
        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(401);
    });

    it('GET /admin/subscription-catalog lists all providers with affiliate and listing status', async () => {
        process.env.ADMIN_API_KEY = 'real-admin-key';
        mockSend.mockResolvedValueOnce({
            Items: [
                { pK: 'SUBSCRIPTION_CATALOG#netflix', sK: 'profile', status: 'ACTIVE', source: 'sync',
                  desc: JSON.stringify({ providerId: 'netflix', isAffiliate: true, listingStatus: 'ACTIVE', source: 'sync' }) },
                { pK: 'SUBSCRIPTION_CATALOG#myenergy', sK: 'profile', status: 'ACTIVE', source: 'tenant',
                  desc: JSON.stringify({ providerId: 'myenergy', isAffiliate: false, listingStatus: 'UNLISTED', source: 'tenant' }) },
            ],
        });

        const event: any = {
            httpMethod: 'GET',
            path: '/admin/subscription-catalog',
            headers: { 'x-admin-api-key': 'real-admin-key' },
            queryStringParameters: null,
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(200);
        const body = JSON.parse(result.body);
        expect(body.total).toBe(2);
        expect(body.catalog.find((c: any) => c.providerId === 'netflix').isAffiliate).toBe(true);
        expect(body.catalog.find((c: any) => c.providerId === 'myenergy').listingStatus).toBe('UNLISTED');
    });

    it('GET /admin/subscription-catalog filters by listingStatus=ACTIVE', async () => {
        process.env.ADMIN_API_KEY = 'real-admin-key';
        mockSend.mockResolvedValueOnce({
            Items: [
                { pK: 'SUBSCRIPTION_CATALOG#a', sK: 'profile', status: 'ACTIVE', source: 'sync',
                  desc: JSON.stringify({ providerId: 'a', listingStatus: 'ACTIVE', isAffiliate: true }) },
                { pK: 'SUBSCRIPTION_CATALOG#b', sK: 'profile', status: 'ACTIVE', source: 'tenant',
                  desc: JSON.stringify({ providerId: 'b', listingStatus: 'UNLISTED', isAffiliate: false }) },
            ],
        });

        const event: any = {
            httpMethod: 'GET',
            path: '/admin/subscription-catalog',
            headers: { 'x-admin-api-key': 'real-admin-key' },
            queryStringParameters: { listingStatus: 'ACTIVE' },
        };

        const result: any = await handler(event, {} as any, () => {});
        const body = JSON.parse(result.body);
        expect(body.total).toBe(1);
        expect(body.catalog[0].providerId).toBe('a');
    });

    it('PUT /admin/subscription-catalog activates listing and grants affiliate status', async () => {
        process.env.ADMIN_API_KEY = 'real-admin-key';
        const existingDesc = { providerId: 'myenergy', isAffiliate: false, listingStatus: 'UNLISTED', isTenantLinked: true };
        mockSend
            .mockResolvedValueOnce({ Item: { desc: JSON.stringify(existingDesc) } }) // GetCommand
            .mockResolvedValueOnce({});                                               // UpdateCommand

        const event: any = {
            httpMethod: 'PUT',
            path: '/admin/subscription-catalog',
            headers: { 'x-admin-api-key': 'real-admin-key' },
            pathParameters: { providerId: 'myenergy' },
            body: JSON.stringify({ listingStatus: 'ACTIVE', isAffiliate: true }),
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(200);
        const body = JSON.parse(result.body);
        expect(body.listingStatus).toBe('ACTIVE');
        expect(body.isAffiliate).toBe(true);
        const updateCall = mockSend.mock.calls[1][0];
        expect(updateCall.input.ExpressionAttributeValues[':status']).toBe('ACTIVE');
    });

    it('PUT /admin/subscription-catalog → 404 when provider not found', async () => {
        process.env.ADMIN_API_KEY = 'real-admin-key';
        mockSend.mockResolvedValueOnce({ Item: null });

        const event: any = {
            httpMethod: 'PUT',
            path: '/admin/subscription-catalog',
            headers: { 'x-admin-api-key': 'real-admin-key' },
            body: JSON.stringify({ providerId: 'ghost', listingStatus: 'ACTIVE' }),
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(404);
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

    it('POST /invoice-status updates invoice status when event is newer', async () => {
        mockSend
            .mockResolvedValueOnce({})
            .mockResolvedValueOnce({
                Items: [{
                    pK: 'USER#perm-001',
                    sK: 'INVOICE#abc',
                    desc: JSON.stringify({
                        brandId: 'test-brand',
                        status: 'unpaid',
                        lastStateEventAt: '2026-04-15T10:00:00.000Z'
                    })
                }]
            })
            .mockResolvedValueOnce({});

        const event: any = {
            httpMethod: 'POST',
            path: '/invoice-status',
            headers: { 'x-idempotency-key': 'evt-1' },
            body: JSON.stringify({
                invoiceSK: 'INVOICE#abc',
                status: 'paid',
                paidDate: '2026-04-16T09:00:00.000Z',
                eventTime: '2026-04-16T09:00:00.000Z'
            })
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(200);

        const body = JSON.parse(result.body);
        expect(body.updated).toBe(true);
        expect(body.status).toBe('paid');

        const updateCall = mockSend.mock.calls[2][0];
        expect(updateCall.input.Key).toEqual({ pK: 'USER#perm-001', sK: 'INVOICE#abc' });
        expect(updateCall.input.ExpressionAttributeValues[':status']).toBe('PAID');
    });

    it('POST /invoice-status ignores stale state events', async () => {
        mockSend
            .mockResolvedValueOnce({})
            .mockResolvedValueOnce({
                Items: [{
                    pK: 'USER#perm-001',
                    sK: 'INVOICE#abc',
                    desc: JSON.stringify({
                        brandId: 'test-brand',
                        status: 'unpaid',
                        lastStateEventAt: '2026-04-16T10:00:00.000Z'
                    })
                }]
            });

        const event: any = {
            httpMethod: 'POST',
            path: '/invoice-status',
            headers: { 'x-idempotency-key': 'evt-2' },
            body: JSON.stringify({
                invoiceSK: 'INVOICE#abc',
                status: 'overdue',
                eventTime: '2026-04-16T09:00:00.000Z'
            })
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(200);

        const body = JSON.parse(result.body);
        expect(body.updated).toBe(false);
        expect(body.staleIgnored).toBe(true);
        expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it('POST /invoice-status returns idempotent success on duplicate idempotency key', async () => {
        const duplicateErr = Object.assign(new Error('duplicate'), { name: 'ConditionalCheckFailedException' });
        mockSend.mockRejectedValueOnce(duplicateErr);

        const event: any = {
            httpMethod: 'POST',
            path: '/invoice-status',
            headers: { 'x-idempotency-key': 'evt-duplicate' },
            body: JSON.stringify({
                invoiceSK: 'INVOICE#abc',
                status: 'paid'
            })
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(200);
        const body = JSON.parse(result.body);
        expect(body.idempotent).toBe(true);
        expect(body.updated).toBe(false);
        expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it('POST /invoice-payment-session returns stored paymentUrl when invoice already has it', async () => {
        mockSend.mockResolvedValueOnce({
            Items: [{
                pK: 'USER#perm-001',
                sK: 'INVOICE#abc',
                desc: JSON.stringify({
                    brandId: 'test-brand',
                    status: 'unpaid',
                    amount: 42.5,
                    currency: 'AUD',
                    supplier: 'EnergyCo',
                    paymentUrl: 'https://pay.example.com/invoice/abc',
                }),
            }],
        });

        const event: any = {
            httpMethod: 'POST',
            path: '/invoice-payment-session',
            headers: {},
            body: JSON.stringify({ invoiceSK: 'INVOICE#abc' }),
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(200);
        const body = JSON.parse(result.body);
        expect(body.paymentMode).toBe('hosted_link');
        expect(body.checkoutUrl).toBe('https://pay.example.com/invoice/abc');
    });

    it('POST /invoice-payment-session builds hosted_link URL from brand settings', async () => {
        mockSend
            .mockResolvedValueOnce({
                Items: [{
                    pK: 'USER#perm-001',
                    sK: 'INVOICE#abc',
                    desc: JSON.stringify({
                        brandId: 'test-brand',
                        status: 'unpaid',
                        amount: 99.95,
                        currency: 'AUD',
                        supplier: 'WaterCo',
                        invoiceNumber: 'INV-42',
                    }),
                }],
            })
            .mockResolvedValueOnce({
                Item: {
                    desc: JSON.stringify({
                        invoicePaymentMode: 'hosted_link',
                        invoicePaymentBaseUrl: 'https://pay.brand.com/invoice/{invoiceNumber}?amount={amount}&currency={currency}',
                    }),
                },
            });

        const event: any = {
            httpMethod: 'POST',
            path: '/invoice-payment-session',
            headers: {},
            body: JSON.stringify({ invoiceSK: 'INVOICE#abc' }),
        };

        const result: any = await handler(event, {} as any, () => {});
        expect(result.statusCode).toBe(200);
        const body = JSON.parse(result.body);
        expect(body.paymentMode).toBe('hosted_link');
        expect(body.checkoutUrl).toContain('https://pay.brand.com/invoice/INV-42');
    });
});
