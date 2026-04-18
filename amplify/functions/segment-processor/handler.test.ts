import { vi, describe, it, expect, beforeEach } from 'vitest';

const { mockSend } = vi.hoisted(() => ({ mockSend: vi.fn() }));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockSend }) },
  GetCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetCommand', input });
  }),
  PutCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutCommand', input });
  }),
  QueryCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'QueryCommand', input });
  }),
  UpdateCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'UpdateCommand', input });
  }),
}));

// ── Import handler after mocks ─────────────────────────────────────────────────
import { handler } from './handler.js';
import type { DynamoDBStreamEvent, StreamRecord } from 'aws-lambda';

// ── Helpers ────────────────────────────────────────────────────────────────────

type EventName = 'INSERT' | 'MODIFY' | 'REMOVE';

function makeStreamRecord(
  eventName: EventName,
  pK: string,
  sK: string,
  overrides: Record<string, unknown> = {},
): DynamoDBStreamEvent['Records'][0] {
  const image = {
    pK: { S: pK },
    sK: { S: sK },
    subCategory: { S: (overrides.subCategory as string) ?? '' },
    status: { S: (overrides.status as string) ?? 'ACTIVE' },
    ...((overrides.extraFields as Record<string, unknown>) ?? {}),
  };

  return {
    eventName,
    eventSource: 'aws:dynamodb',
    eventVersion: '1.1',
    eventID: 'test-event-id',
    eventSourceARN: 'arn:aws:dynamodb:ap-southeast-2:123456789:table/test',
    awsRegion: 'ap-southeast-2',
    dynamodb: {
      StreamViewType: 'NEW_AND_OLD_IMAGES',
      NewImage: eventName !== 'REMOVE' ? image : undefined,
      OldImage: eventName === 'REMOVE' ? image : undefined,
      Keys: { pK: { S: pK }, sK: { S: sK } },
      SequenceNumber: '000000000000000001',
      SizeBytes: 100,
      ApproximateCreationDateTime: Date.now() / 1000,
    } as StreamRecord,
  };
}

function makeReceiptRecord(
  permULID: string,
  brandId: string,
  purchaseDate: string,
  amount: number,
  eventName: EventName = 'INSERT',
) {
  return makeStreamRecord(
    eventName,
    `USER#${permULID}`,
    `RECEIPT#${purchaseDate}#TEST`,
    { subCategory: brandId },
  );
}

function makeSubscriptionRecord(
  permULID: string,
  brandId: string,
  eventName: EventName,
  status = 'ACTIVE',
) {
  return makeStreamRecord(
    eventName,
    `USER#${permULID}`,
    `SUBSCRIPTION#${brandId}`,
    { subCategory: brandId, status },
  );
}

// Build receipt items for QueryCommand responses
function makeReceiptItems(
  brandId: string,
  entries: Array<{ amount: number; purchaseDate: string }>,
) {
  return entries.map((e) => ({
    pK: 'USER#PERM-001',
    sK: `RECEIPT#${e.purchaseDate}#TEST`,
    status: 'ACTIVE',
    subCategory: brandId,
    desc: JSON.stringify({ amount: e.amount, purchaseDate: e.purchaseDate }),
  }));
}

// ── Tests ──────────────────────────────────────────────────────────────────────

describe('segment-processor handler', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.USER_TABLE = 'test-user-table';
  });

  // ── Filtering / routing ───────────────────────────────────────────────────

  it('skips records that do not start with USER#', async () => {
    const event: DynamoDBStreamEvent = {
      Records: [makeStreamRecord('INSERT', 'BRAND#woolworths', 'profile')],
    };
    await handler(event, {} as never, () => {});
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('skips REMOVE events for RECEIPT# records', async () => {
    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', '2026-03-20', 50, 'REMOVE')],
    };
    await handler(event, {} as never, () => {});
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('calls recomputeSegment for RECEIPT# INSERT', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({
          Items: makeReceiptItems('woolworths', [{ amount: 50, purchaseDate: '2026-03-20' }]),
        });
      }
      if (cmd.__type === 'GetCommand') {
        if ((cmd.input?.Key as { sK?: string } | undefined)?.sK === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', '2026-03-20', 50)],
    };
    await handler(event, {} as never, () => {});

    const putCalls = mockSend.mock.calls.filter(([cmd]) => cmd.__type === 'PutCommand');
    expect(putCalls.length).toBeGreaterThan(0);
    const segmentPut = putCalls.find(([cmd]) => cmd.input?.Item?.eventType === 'SEGMENT');
    expect(segmentPut).toBeDefined();
  });

  // ── spendBucket classification ─────────────────────────────────────────────

  it("spendBucket is '<100' for total spend < 100", async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({
          Items: makeReceiptItems('woolworths', [{ amount: 75, purchaseDate: '2026-03-20' }]),
        });
      }
      if (cmd.__type === 'GetCommand') {
        if ((cmd.input?.Key as { sK?: string } | undefined)?.sK === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', '2026-03-20', 75)],
    };
    await handler(event, {} as never, () => {});

    const segmentPut = mockSend.mock.calls
      .filter(([cmd]) => cmd.__type === 'PutCommand')
      .find(([cmd]) => cmd.input?.Item?.eventType === 'SEGMENT');
    const desc = JSON.parse(segmentPut![0].input.Item.desc);
    expect(desc.spendBucket).toBe('<100');
  });

  it("spendBucket is '100-200' for total spend 100–199", async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({
          Items: makeReceiptItems('woolworths', [
            { amount: 80, purchaseDate: '2026-03-18' },
            { amount: 70, purchaseDate: '2026-03-20' },
          ]),
        });
      }
      if (cmd.__type === 'GetCommand') {
        if ((cmd.input?.Key as { sK?: string } | undefined)?.sK === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', '2026-03-20', 70)],
    };
    await handler(event, {} as never, () => {});

    const segmentPut = mockSend.mock.calls
      .filter(([cmd]) => cmd.__type === 'PutCommand')
      .find(([cmd]) => cmd.input?.Item?.eventType === 'SEGMENT');
    const desc = JSON.parse(segmentPut![0].input.Item.desc);
    expect(desc.spendBucket).toBe('100-200');
  });

  it("spendBucket is '200-500' for total spend 200–499", async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({
          Items: makeReceiptItems('woolworths', [
            { amount: 150, purchaseDate: '2026-03-18' },
            { amount: 100, purchaseDate: '2026-03-20' },
          ]),
        });
      }
      if (cmd.__type === 'GetCommand') {
        if ((cmd.input?.Key as { sK?: string } | undefined)?.sK === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', '2026-03-20', 100)],
    };
    await handler(event, {} as never, () => {});

    const segmentPut = mockSend.mock.calls
      .filter(([cmd]) => cmd.__type === 'PutCommand')
      .find(([cmd]) => cmd.input?.Item?.eventType === 'SEGMENT');
    const desc = JSON.parse(segmentPut![0].input.Item.desc);
    expect(desc.spendBucket).toBe('200-500');
  });

  it("spendBucket is '500+' for total spend >= 500", async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({
          Items: makeReceiptItems('woolworths', [
            { amount: 300, purchaseDate: '2026-03-18' },
            { amount: 250, purchaseDate: '2026-03-20' },
          ]),
        });
      }
      if (cmd.__type === 'GetCommand') {
        if ((cmd.input?.Key as { sK?: string } | undefined)?.sK === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', '2026-03-20', 250)],
    };
    await handler(event, {} as never, () => {});

    const segmentPut = mockSend.mock.calls
      .filter(([cmd]) => cmd.__type === 'PutCommand')
      .find(([cmd]) => cmd.input?.Item?.eventType === 'SEGMENT');
    const desc = JSON.parse(segmentPut![0].input.Item.desc);
    expect(desc.spendBucket).toBe('500+');
  });

  // ── visitFrequency classification ──────────────────────────────────────────

  it("visitFrequency is 'new' for fewer than 3 visits", async () => {
    const recentDate = new Date(Date.now() - 10 * 86_400_000).toISOString().substring(0, 10);
    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({
          Items: makeReceiptItems('woolworths', [
            { amount: 20, purchaseDate: recentDate },
            { amount: 30, purchaseDate: recentDate },
          ]),
        });
      }
      if (cmd.__type === 'GetCommand') {
        if ((cmd.input?.Key as { sK?: string } | undefined)?.sK === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', recentDate, 30)],
    };
    await handler(event, {} as never, () => {});

    const segmentPut = mockSend.mock.calls
      .filter(([cmd]) => cmd.__type === 'PutCommand')
      .find(([cmd]) => cmd.input?.Item?.eventType === 'SEGMENT');
    const desc = JSON.parse(segmentPut![0].input.Item.desc);
    expect(desc.visitFrequency).toBe('new');
  });

  it("visitFrequency is 'frequent' for 12+ visits within 90 days", async () => {
    const recentDate = new Date(Date.now() - 5 * 86_400_000).toISOString().substring(0, 10);
    const entries = Array.from({ length: 12 }, (_, i) => ({
      amount: 20,
      purchaseDate: new Date(Date.now() - (i + 1) * 86_400_000).toISOString().substring(0, 10),
    }));

    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({ Items: makeReceiptItems('woolworths', entries) });
      }
      if (cmd.__type === 'GetCommand') {
        if ((cmd.input?.Key as { sK?: string } | undefined)?.sK === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', recentDate, 20)],
    };
    await handler(event, {} as never, () => {});

    const segmentPut = mockSend.mock.calls
      .filter(([cmd]) => cmd.__type === 'PutCommand')
      .find(([cmd]) => cmd.input?.Item?.eventType === 'SEGMENT');
    const desc = JSON.parse(segmentPut![0].input.Item.desc);
    expect(desc.visitFrequency).toBe('frequent');
  });

  it("visitFrequency is 'lapsed' for last visit > 180 days ago", async () => {
    const oldDate = new Date(Date.now() - 200 * 86_400_000).toISOString().substring(0, 10);
    const entries = Array.from({ length: 15 }, (_, i) => ({
      amount: 30,
      purchaseDate: new Date(Date.now() - (200 + i) * 86_400_000).toISOString().substring(0, 10),
    }));

    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({ Items: makeReceiptItems('woolworths', entries) });
      }
      if (cmd.__type === 'GetCommand') {
        if ((cmd.input?.Key as { sK?: string } | undefined)?.sK === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', oldDate, 30)],
    };
    await handler(event, {} as never, () => {});

    const segmentPut = mockSend.mock.calls
      .filter(([cmd]) => cmd.__type === 'PutCommand')
      .find(([cmd]) => cmd.input?.Item?.eventType === 'SEGMENT');
    const desc = JSON.parse(segmentPut![0].input.Item.desc);
    expect(desc.visitFrequency).toBe('lapsed');
  });

  it("visitFrequency is 'occasional' for 3-11 visits within 90 days", async () => {
    const entries = Array.from({ length: 6 }, (_, i) => ({
      amount: 25,
      purchaseDate: new Date(Date.now() - (i + 1) * 86_400_000).toISOString().substring(0, 10),
    }));

    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({ Items: makeReceiptItems('woolworths', entries) });
      }
      if (cmd.__type === 'GetCommand') {
        if ((cmd.input?.Key as { sK?: string } | undefined)?.sK === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const recentDate = new Date(Date.now() - 1 * 86_400_000).toISOString().substring(0, 10);
    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', recentDate, 25)],
    };
    await handler(event, {} as never, () => {});

    const segmentPut = mockSend.mock.calls
      .filter(([cmd]) => cmd.__type === 'PutCommand')
      .find(([cmd]) => cmd.input?.Item?.eventType === 'SEGMENT');
    const desc = JSON.parse(segmentPut![0].input.Item.desc);
    expect(desc.visitFrequency).toBe('occasional');
  });

  // ── Subscription consent ───────────────────────────────────────────────────

  it('sets subscribed=true in desc when SUBSCRIPTION# record exists', async () => {
    const recentDate = new Date(Date.now() - 5 * 86_400_000).toISOString().substring(0, 10);

    mockSend.mockImplementation((cmd: { __type: string; input?: { Key?: { sK?: string } } }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({
          Items: makeReceiptItems('woolworths', [{ amount: 50, purchaseDate: recentDate }]),
        });
      }
      if (cmd.__type === 'GetCommand') {
        const sk = (cmd.input?.Key as { sK?: string } | undefined)?.sK ?? '';
        if (sk === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        if (sk === 'SUBSCRIPTION#woolworths') {
          return Promise.resolve({ Item: { pK: 'USER#PERM-001', sK: sk, status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', recentDate, 50)],
    };
    await handler(event, {} as never, () => {});

    const segmentPut = mockSend.mock.calls
      .filter(([cmd]) => cmd.__type === 'PutCommand')
      .find(([cmd]) => cmd.input?.Item?.eventType === 'SEGMENT');
    const desc = JSON.parse(segmentPut![0].input.Item.desc);
    expect(desc.subscribed).toBe(true);
  });

  it('sets subscribed=false in desc when no SUBSCRIPTION# record', async () => {
    const recentDate = new Date(Date.now() - 5 * 86_400_000).toISOString().substring(0, 10);

    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') {
        return Promise.resolve({
          Items: makeReceiptItems('woolworths', [{ amount: 50, purchaseDate: recentDate }]),
        });
      }
      if (cmd.__type === 'GetCommand') {
        if ((cmd.input?.Key as { sK?: string } | undefined)?.sK === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', recentDate, 50)],
    };
    await handler(event, {} as never, () => {});

    const segmentPut = mockSend.mock.calls
      .filter(([cmd]) => cmd.__type === 'PutCommand')
      .find(([cmd]) => cmd.input?.Item?.eventType === 'SEGMENT');
    const desc = JSON.parse(segmentPut![0].input.Item.desc);
    expect(desc.subscribed).toBe(false);
  });

  it('skips recompute and does not write if no receipts found', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') return Promise.resolve({ Items: [] });
      return Promise.resolve({});
    });

    const recentDate = new Date().toISOString().substring(0, 10);
    const event: DynamoDBStreamEvent = {
      Records: [makeReceiptRecord('PERM-001', 'woolworths', recentDate, 50)],
    };
    await handler(event, {} as never, () => {});

    const putCalls = mockSend.mock.calls.filter(([cmd]) => cmd.__type === 'PutCommand');
    expect(putCalls).toHaveLength(0);
  });

  // ── Subscription patch path ────────────────────────────────────────────────

  it('patches subscribed=true on SUBSCRIPTION# INSERT with status ACTIVE', async () => {
    const existingDesc = {
      spendBucket: '100-200',
      visitFrequency: 'frequent',
      totalSpend: 150,
      visitCount: 8,
      lastVisit: '2026-03-20',
      subscribed: false,
      computedAt: '2026-03-01T00:00:00.000Z',
    };

    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'GetCommand') {
        return Promise.resolve({
          Item: {
            pK: 'USER#PERM-001',
            sK: 'SEGMENT#woolworths',
            desc: JSON.stringify(existingDesc),
          },
        });
      }
      if (cmd.__type === 'UpdateCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeSubscriptionRecord('PERM-001', 'woolworths', 'INSERT', 'ACTIVE')],
    };
    await handler(event, {} as never, () => {});

    const updateCalls = mockSend.mock.calls.filter(([cmd]) => cmd.__type === 'UpdateCommand');
    expect(updateCalls).toHaveLength(1);
    const expressionValues = updateCalls[0][0].input.ExpressionAttributeValues;
    const updatedDesc = JSON.parse(expressionValues[':desc']);
    expect(updatedDesc.subscribed).toBe(true);
    // All other fields must be preserved
    expect(updatedDesc.spendBucket).toBe('100-200');
    expect(updatedDesc.visitFrequency).toBe('frequent');
  });

  it('patches subscribed=false on SUBSCRIPTION# REMOVE', async () => {
    const existingDesc = {
      spendBucket: '<100',
      visitFrequency: 'new',
      totalSpend: 40,
      visitCount: 1,
      lastVisit: '2026-03-20',
      subscribed: true,
      computedAt: '2026-03-01T00:00:00.000Z',
    };

    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'GetCommand') {
        return Promise.resolve({
          Item: {
            pK: 'USER#PERM-001',
            sK: 'SEGMENT#woolworths',
            desc: JSON.stringify(existingDesc),
          },
        });
      }
      if (cmd.__type === 'UpdateCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeSubscriptionRecord('PERM-001', 'woolworths', 'REMOVE', 'ACTIVE')],
    };
    await handler(event, {} as never, () => {});

    const updateCalls = mockSend.mock.calls.filter(([cmd]) => cmd.__type === 'UpdateCommand');
    expect(updateCalls).toHaveLength(1);
    const expressionValues = updateCalls[0][0].input.ExpressionAttributeValues;
    const updatedDesc = JSON.parse(expressionValues[':desc']);
    expect(updatedDesc.subscribed).toBe(false);
  });

  it('is a no-op if no SEGMENT# exists during subscription patch', async () => {
    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'GetCommand') return Promise.resolve({ Item: null });
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [makeSubscriptionRecord('PERM-001', 'woolworths', 'INSERT', 'ACTIVE')],
    };
    await handler(event, {} as never, () => {});

    const updateCalls = mockSend.mock.calls.filter(([cmd]) => cmd.__type === 'UpdateCommand');
    expect(updateCalls).toHaveLength(0);
  });

  it('one failed record does not block others in the batch', async () => {
    let callCount = 0;
    const recentDate = new Date(Date.now() - 3 * 86_400_000).toISOString().substring(0, 10);

    mockSend.mockImplementation((cmd: { __type: string; input?: Record<string, unknown> }) => {
      if (cmd.__type === 'QueryCommand') {
        callCount++;
        if (callCount === 1) {
          // First record fails
          return Promise.reject(new Error('DynamoDB throttle'));
        }
        // Second record succeeds
        return Promise.resolve({
          Items: makeReceiptItems('coles', [{ amount: 60, purchaseDate: recentDate }]),
        });
      }
      if (cmd.__type === 'GetCommand') {
        if ((cmd.input?.Key as { sK?: string } | undefined)?.sK === 'IDENTITY') {
          return Promise.resolve({ Item: { owner: 'test-user', status: 'ACTIVE' } });
        }
        return Promise.resolve({ Item: null });
      }
      if (cmd.__type === 'PutCommand') return Promise.resolve({});
      return Promise.resolve({});
    });

    const event: DynamoDBStreamEvent = {
      Records: [
        makeReceiptRecord('PERM-001', 'woolworths', recentDate, 50),
        makeReceiptRecord('PERM-002', 'coles', recentDate, 60),
      ],
    };

    // Must not throw even though first record failed
    await expect(handler(event, {} as never, () => {})).resolves.not.toThrow();

    // Second record should still have been processed — a SEGMENT PutCommand was issued
    const putCalls = mockSend.mock.calls.filter(
      ([cmd]) => cmd.__type === 'PutCommand' && cmd.input?.Item?.eventType === 'SEGMENT',
    );
    expect(putCalls.length).toBeGreaterThan(0);
  });
});
