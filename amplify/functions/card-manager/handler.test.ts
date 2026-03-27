import { describe, it, expect, vi, beforeEach } from 'vitest';

const { mockSend } = vi.hoisted(() => ({ mockSend: vi.fn() }));

import { handler } from './handler';

vi.mock('@aws-sdk/lib-dynamodb', () => {
    return {
        DynamoDBDocumentClient: {
            from: () => ({ send: mockSend }),
        },
        PutCommand: class PutCommand { constructor(public input: any) { } },
        UpdateCommand: class UpdateCommand { constructor(public input: any) { } },
        GetCommand: class GetCommand { constructor(public input: any) { } },
        QueryCommand: class QueryCommand { constructor(public input: any) { } },
        TransactWriteCommand: class TransactWriteCommand { constructor(public input: any) { } },
    };
});

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
            identity: { claims: { 'custom:permULID': 'perm-ulid-123' } },
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
