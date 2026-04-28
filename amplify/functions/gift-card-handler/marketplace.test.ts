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
  UpdateCommand: vi.fn(function (this: any, input: any) { Object.assign(this, { __type: 'UpdateCommand', input }); }),
  QueryCommand:  vi.fn(function (this: any, input: any) { Object.assign(this, { __type: 'QueryCommand', input }); }),
  TransactWriteCommand: vi.fn(function (this: any, input: any) { Object.assign(this, { __type: 'TransactWriteCommand', input }); }),
}));

function makeAppsyncEvent(fieldName: string, args: Record<string, unknown>, sub = 'cognito-sub-001') {
  return {
    info: { fieldName },
    identity: { sub },
    arguments: args,
  };
}

describe('Gift Card Marketplace (Phase 3)', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    process.env.USER_TABLE = 'UserDataEvent';
    process.env.REFDATA_TABLE = 'RefDataEvent';
  });

  describe('listYourGiftCardForSale', () => {
    it('locks the card and creates a resale listing', async () => {
      // 1. resolvePermULID -> 2. GetCommand card
      mockDdbSend
        .mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-seller' }] })
        .mockResolvedValueOnce({ 
          Item: { 
            sK: 'GIFTCARD#123',
            status: 'ACTIVE',
            desc: JSON.stringify({ brandName: 'Woolworths', denomination: 50 })
          } 
        })
        .mockResolvedValueOnce({}) // UpdateCommand (lock)
        .mockResolvedValueOnce({}); // PutCommand (listing)

      const res = await handler(makeAppsyncEvent('listYourGiftCardForSale', {
        cardSK: 'GIFTCARD#123',
        askingPrice: 45,
        currency: 'AUD'
      }));

      expect(res.status).toBe('LISTED');
      expect(res.resaleId).toBeDefined();

      // Verify card was locked
      const lockCall = mockDdbSend.mock.calls.find(c => c[0].__type === 'UpdateCommand');
      expect(lockCall[0].input.ExpressionAttributeValues[':locked']).toBe('LOCKED_FOR_SALE');
    });

    it('denies listing if card is not active', async () => {
      mockDdbSend
        .mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-seller' }] })
        .mockResolvedValueOnce({ Item: { sK: 'GIFTCARD#123', status: 'USED' } });

      await expect(handler(makeAppsyncEvent('listYourGiftCardForSale', {
        cardSK: 'GIFTCARD#123', askingPrice: 45, currency: 'AUD'
      }))).rejects.toThrow('Only active cards can be listed');
    });
  });

  describe('purchaseResoldCard', () => {
    it('transfers card from seller to buyer', async () => {
      // 1. resolvePermULID (buyer) -> 2. GetCommand listing -> 3. GetCommand seller identity -> 4+ (TransactWrite)
      mockDdbSend
        .mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-buyer' }] })
        .mockResolvedValueOnce({ 
          Item: { 
            status: 'ACTIVE',
            brandId: 'woolworths',
            faceValue: 50,
            askingPrice: 45,
            currency: 'AUD',
            sellerPermULID: 'perm-seller',
            cardSK: 'GIFTCARD#seller-card'
          } 
        })
        .mockResolvedValueOnce({ Item: { owner: 'seller-cognito-id' } }) // seller identity
        .mockResolvedValueOnce({ Item: { desc: JSON.stringify({ cardNumber: '1234', pin: '5555' }) } }) // seller card detail
        .mockResolvedValueOnce({}); // TransactWriteCommand

      const res = await handler(makeAppsyncEvent('purchaseResoldCard', {
        resaleId: 'RESALE#123'
      }));

      expect(res.status).toBe('SUCCESS');
      expect(res.cardSK).toBeDefined();

      // Verify one transaction was sent (marking listing sold, deleting seller card, adding buyer card etc)
      const transactCall = mockDdbSend.mock.calls.find(c => c[0].__type === 'UpdateCommand' && c[0].input.ExpressionAttributeValues[':sold'] === 'SOLD');
      expect(transactCall).toBeDefined();
    });
  });
});
