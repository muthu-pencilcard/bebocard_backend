import { describe, it, expect, vi, beforeEach } from 'vitest';
import { handler } from './handler.js';

const { mockDdbSend } = vi.hoisted(() => ({
  mockDdbSend: vi.fn(),
}));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function () {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockDdbSend }) },
  GetCommand:    vi.fn(function (this: any, input: any) { Object.assign(this, { __type: 'GetCommand', input }); }),
  PutCommand:    vi.fn(function (this: any, input: any) { Object.assign(this, { __type: 'PutCommand', input }); }),
  QueryCommand:  vi.fn(function (this: any, input: any) { Object.assign(this, { __type: 'QueryCommand', input }); }),
  UpdateCommand: vi.fn(function (this: any, input: any) { Object.assign(this, { __type: 'UpdateCommand', input }); }),
}));

describe('Segment Processor (Phase 3 Intelligence)', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    process.env.USER_TABLE = 'UserDataEvent';
  });

  it('correctly identifies a Brand Loyalist and a Traveler', async () => {
    const receipts = [
      { pK: 'USER#1', sK: 'RECEIPT#1', desc: JSON.stringify({ amount: 100, category: 'travel', brandId: 'qantas', purchaseDate: '2026-04-01' }), status: 'ACTIVE' },
      { pK: 'USER#1', sK: 'RECEIPT#2', desc: JSON.stringify({ amount: 100, category: 'travel', brandId: 'qantas', purchaseDate: '2026-04-02' }), status: 'ACTIVE' },
      { pK: 'USER#1', sK: 'RECEIPT#3', desc: JSON.stringify({ amount: 100, category: 'travel', brandId: 'qantas', purchaseDate: '2026-04-03' }), status: 'ACTIVE' },
      { pK: 'USER#1', sK: 'RECEIPT#4', desc: JSON.stringify({ amount: 100, category: 'travel', brandId: 'qantas', purchaseDate: '2026-04-04' }), status: 'ACTIVE' },
      { pK: 'USER#1', sK: 'RECEIPT#5', desc: JSON.stringify({ amount: 100, category: 'travel', brandId: 'qantas', purchaseDate: '2026-04-05' }), status: 'ACTIVE' },
      { pK: 'USER#1', sK: 'RECEIPT#6', desc: JSON.stringify({ amount: 100, category: 'travel', brandId: 'qantas', purchaseDate: '2026-04-06' }), status: 'ACTIVE' },
    ];

    // Trigger on first receipt
    const streamEvent: any = {
      Records: [{
        eventName: 'INSERT',
        dynamodb: {
          NewImage: {
            pK: { S: 'USER#1' },
            sK: { S: 'RECEIPT#1' },
            subCategory: { S: 'qantas' },
            owner: { S: 'owner-1' }
          }
        }
      }]
    };

    mockDdbSend
      .mockResolvedValueOnce({ Item: { owner: 'owner-1' } }) // getOwner for brand segment
      .mockResolvedValueOnce({ Items: receipts, LastEvaluatedKey: null }) // recomputeSegment receipts
      .mockResolvedValueOnce({ Item: { pK: 'USER#1', sK: 'SUBSCRIPTION#qantas' } }) // check consent
      .mockResolvedValueOnce({}) // Put SEGMENT#qantas
      .mockResolvedValueOnce({ Items: receipts, LastEvaluatedKey: null }) // recomputeGlobalSegment receipts
      .mockResolvedValueOnce({}); // Put SEGMENT#global

    await handler(streamEvent);

    const globalPut = mockDdbSend.mock.calls.find(c => c[0].input.Item?.sK === 'SEGMENT#global');
    const desc = JSON.parse(globalPut[0].input.Item.desc);
    
    expect(desc.persona).toContain('brand_loyalist');
    expect(desc.persona).toContain('traveler');
    expect(desc.spendBucket).toBe('500+');
  });

  it('correctly identifies a Deal Hunter', async () => {
    const manySmallReceipts = Array.from({ length: 11 }, (_, i) => ({
      pK: 'USER#1', sK: `RECEIPT#${i}`, desc: JSON.stringify({ amount: 10, category: 'groceries', brandId: 'woolworths', purchaseDate: '2026-04-01' }), status: 'ACTIVE'
    }));

    const streamEvent: any = {
      Records: [{
        eventName: 'INSERT',
        dynamodb: {
          NewImage: { pK: { S: 'USER#1' }, sK: { S: 'RECEIPT#1' }, subCategory: { S: 'woolworths' }, owner: { S: 'owner-1' } }
        }
      }]
    };

    mockDdbSend
      .mockResolvedValueOnce({ Item: { owner: 'owner-1' } }) // getOwner
      .mockResolvedValueOnce({ Items: manySmallReceipts }) 
      .mockResolvedValueOnce({ Item: { pK: 'USER#1', sK: 'SUBSCRIPTION#woolworths' } })
      .mockResolvedValueOnce({}) // Put brand segment
      .mockResolvedValueOnce({ Items: manySmallReceipts }) 
      .mockResolvedValueOnce({}); // Put global segment

    await handler(streamEvent);

    const globalPut = mockDdbSend.mock.calls.find(c => c[0].input.Item?.sK === 'SEGMENT#global');
    const desc = JSON.parse(globalPut[0].input.Item.desc);
    
    expect(desc.persona).toContain('deal_hunter');
  });
});
