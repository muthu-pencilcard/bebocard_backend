import { describe, it, expect, vi, beforeEach } from 'vitest';
import { handler } from './handler';

const { mockSend } = vi.hoisted(() => ({ mockSend: vi.fn() }));

vi.mock('../../shared/api-key-auth', async () => {
    const actual = await vi.importActual('../../shared/api-key-auth') as any;
    return {
        ...actual,
        extractApiKey: vi.fn(),
        validateApiKey: vi.fn(),
    };
});

import { extractApiKey, validateApiKey } from '../../shared/api-key-auth';

vi.mock('@aws-sdk/lib-dynamodb', () => {
    return {
        DynamoDBDocumentClient: {
            from: () => ({ send: mockSend }),
        },
        GetCommand: class GetCommand { constructor(public input: any) { } },
        PutCommand: class PutCommand { constructor(public input: any) { } },
        QueryCommand: class QueryCommand { constructor(public input: any) { } },
    };
});

vi.mock('firebase-admin', () => ({
    default: {
        initializeApp: vi.fn(),
        credential: { cert: vi.fn() },
        messaging: vi.fn(() => ({ send: vi.fn().mockResolvedValue('msg-id') })),
        apps: [],
    }
}));

describe('scan-handler tests', () => {
    beforeEach(() => {
        vi.resetAllMocks();
        mockSend.mockReset();
    });

    it('POST /scan with no API key returns 401', async () => {
        (extractApiKey as any).mockReturnValue(null);
        const event = {
            httpMethod: 'POST',
            path: '/scan',
            headers: {},
            body: JSON.stringify({ secondaryULID: 'sec-ulid' }),
        } as any;

        const res: any = await handler(event, null as any, null as any);
        expect(res.statusCode).toBe(401);
    });

    it('POST /scan with valid key and valid secondaryULID returns user token', async () => {
        (extractApiKey as any).mockReturnValue('valid-key');
        (validateApiKey as any).mockResolvedValue({ brandId: 'Test Brand', scopes: ['scan:read'] });

        mockSend.mockImplementation(async (cmd: any) => {
            if (cmd?.input?.TableName === process.env.ADMIN_TABLE) {
                if (cmd.input.KeyConditionExpression) {
                    return { Items: [{ pK: 'SCAN#sec-ulid', sK: 'perm-ulid-123', status: 'ACTIVE', desc: JSON.stringify({ cards: [{ brand: 'Test Brand', cardId: '12345', isDefault: true }] }) }] };
                }
                return { Item: { sK: 'perm-ulid-123', status: 'ACTIVE', desc: '{}' } };
            }
            if (cmd?.input?.TableName === process.env.USER_TABLE) {
                return { Item: { status: 'ACTIVE', desc: '{}' } };
            }
            return { Items: [] };
        });

        const event = {
            httpMethod: 'POST',
            path: '/scan',
            headers: { 'x-api-key': 'valid-key' },
            body: JSON.stringify({ secondaryULID: 'sec-ulid', storeBrandLoyaltyName: 'Test Brand' }),
        } as any;

        const res: any = await handler(event, null as any, null as any);
        if (res.statusCode !== 200) console.log(res);
        expect(res.statusCode).toBe(200);
        const body = JSON.parse(res.body);
        expect(body.hasLoyaltyCard).toBe(true);
    });
});
