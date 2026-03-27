import { describe, it, expect, vi, beforeEach } from 'vitest';

const { mockSend } = vi.hoisted(() => ({ mockSend: vi.fn() }));

import { handler } from './handler';

vi.mock('@aws-sdk/lib-dynamodb', () => {
    return {
        DynamoDBDocumentClient: {
            from: () => ({
                send: mockSend,
            }),
        },
        GetCommand: class GetCommand { constructor(public input: any) { } },
        PutCommand: class PutCommand { constructor(public input: any) { } },
        QueryCommand: class QueryCommand { constructor(public input: any) { } },
    };
});

describe('segment-processor privacy tests', () => {
    beforeEach(() => {
        vi.resetAllMocks();
        mockSend.mockReset();

        mockSend.mockImplementation(async (cmd) => {
            if (cmd.constructor.name === 'QueryCommand') {
                return {
                    Items: [
                        { desc: JSON.stringify({ amount: 50, purchaseDate: '2026-03-27' }) }
                    ]
                };
            }
            if (cmd.constructor.name === 'GetCommand') {
                return { Item: { pk: 'USER#perm-ulid', sk: 'SUBSCRIPTION#brand123', status: 'ACTIVE' } };
            }
            return {};
        });
    });

    it('computes segment correctly with subscribed = true if subscription exists', async () => {
        const event = {
            Records: [
                {
                    eventName: 'INSERT',
                    dynamodb: {
                        NewImage: {
                            pK: { S: 'USER#perm-ulid-123' },
                            sK: { S: 'RECEIPT#brand123#12345' },
                            subCategory: { S: 'brand123' },
                        }
                    }
                }
            ]
        } as any;

        await handler(event, null as any, null as any);

        const callCount = mockSend.mock.calls.length;
        expect(callCount).toBeGreaterThan(0);

        const putCall = mockSend.mock.calls.find((call: any[]) => call[0].constructor.name === 'PutCommand');
        expect(putCall).toBeDefined();

        const putItem = putCall![0].input.Item;
        expect(putItem.pK).toBe('USER#perm-ulid-123');
        expect(putItem.sK).toBe('SEGMENT#brand123');

        const parsedDesc = JSON.parse(putItem.desc);
        expect(parsedDesc.subscribed).toBe(true);
        expect(parsedDesc.spendBucket).toBeDefined();
    });
});
