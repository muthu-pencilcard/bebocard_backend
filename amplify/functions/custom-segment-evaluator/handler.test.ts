import { vi, describe, it, expect, beforeEach } from 'vitest';

const { mockSend } = vi.hoisted(() => {
  process.env.USER_TABLE = 'UserDataEvent-test';
  process.env.REFDATA_TABLE = 'RefDataEvent-test';
  return { mockSend: vi.fn() };
});

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockSend }) },
  ScanCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'ScanCommand', input });
  }),
  QueryCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'QueryCommand', input });
  }),
  BatchWriteCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'BatchWriteCommand', input });
  }),
  UpdateCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'UpdateCommand', input });
  }),
}));

import { handler } from './handler.js';

// ─── Helpers ──────────────────────────────────────────────────────────────────

function makeSegmentDef(overrides: Record<string, unknown> = {}) {
  return {
    pK: 'TENANT#tenant-1',
    sK: 'SEGMENT_DEF#seg-abc',
    brandId: 'woolworths',
    desc: JSON.stringify({
      name: 'High spenders',
      rules: [{ metric: 'total_spend', period: 'month', operator: 'gte', value: 300 }],
      logicalOperator: 'AND',
      scope: 'brand',
      brandId: 'woolworths',
      active: true,
    }),
    ...overrides,
  };
}

function makeReceiptItem(amount: number, purchaseDate: string, brandId = 'woolworths') {
  return {
    pK: 'USER#perm-001',
    sK: `RECEIPT#2024-01-01T00:00:00.000Z`,
    purchaseDate,
    desc: JSON.stringify({ amount, purchaseDate, brandId }),
  };
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('custom-segment-evaluator handler', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('does nothing when no segment defs exist', async () => {
    mockSend.mockResolvedValueOnce({ Items: [], LastEvaluatedKey: undefined }); // getAllSegmentDefs scan

    await handler(undefined);

    // Only the initial scan — no BatchWrite calls
    const batchWriteCalls = mockSend.mock.calls.filter(([cmd]: [Record<string, unknown>]) => cmd.__type === 'BatchWriteCommand');
    expect(batchWriteCalls).toHaveLength(0);
  });

  it('skips inactive segment defs', async () => {
    const inactiveDef = makeSegmentDef({
      desc: JSON.stringify({
        name: 'Inactive',
        rules: [{ metric: 'visit_count', period: 'week', operator: 'gte', value: 2 }],
        logicalOperator: 'AND',
        scope: 'brand',
        brandId: 'woolworths',
        active: false, // inactive
      }),
    });

    mockSend.mockResolvedValueOnce({ Items: [inactiveDef], LastEvaluatedKey: undefined });

    await handler(undefined);

    const batchWriteCalls = mockSend.mock.calls.filter(([cmd]: [Record<string, unknown>]) => cmd.__type === 'BatchWriteCommand');
    expect(batchWriteCalls).toHaveLength(0);
  });

  it('marks a user ACTIVE when they meet the spend threshold', async () => {
    // 1. Scan returns one segment def
    mockSend.mockResolvedValueOnce({ Items: [makeSegmentDef()], LastEvaluatedKey: undefined });
    // 2. Subscribers query — returns perm-001
    mockSend.mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-001' }], LastEvaluatedKey: undefined });
    // 3. Receipts query for perm-001 — $350 in last month
    const thirtyDaysAgo = new Date(Date.now() - 25 * 24 * 60 * 60 * 1000).toISOString();
    mockSend.mockResolvedValueOnce({ Items: [makeReceiptItem(350, thirtyDaysAgo)], LastEvaluatedKey: undefined });
    // 4. BatchWrite membership
    mockSend.mockResolvedValueOnce({});
    // 5. UpdateCommand for stats
    mockSend.mockResolvedValueOnce({});

    await handler(undefined);

    const batchCall = mockSend.mock.calls.find(([cmd]: [Record<string, unknown>]) => cmd.__type === 'BatchWriteCommand');
    expect(batchCall).toBeDefined();
    const items = (batchCall![0] as { input: { RequestItems: Record<string, Array<{ PutRequest: { Item: Record<string, unknown> } }>> } }).input.RequestItems['UserDataEvent-test'];
    expect(items[0].PutRequest.Item.sK).toBe('SEGMENT#woolworths#CUSTOM#seg-abc');
    expect(items[0].PutRequest.Item.status).toBe('ACTIVE');
  });

  it('marks a user INACTIVE when they do not meet the spend threshold', async () => {
    // 1. Scan — one def
    mockSend.mockResolvedValueOnce({ Items: [makeSegmentDef()], LastEvaluatedKey: undefined });
    // 2. Subscribers
    mockSend.mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-002' }], LastEvaluatedKey: undefined });
    // 3. Receipts — only $50 in last month (below 300 threshold)
    const recentDate = new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString();
    mockSend.mockResolvedValueOnce({ Items: [makeReceiptItem(50, recentDate)], LastEvaluatedKey: undefined });
    // 4. BatchWrite
    mockSend.mockResolvedValueOnce({});
    // 5. UpdateCommand
    mockSend.mockResolvedValueOnce({});

    await handler(undefined);

    const batchCall = mockSend.mock.calls.find(([cmd]: [Record<string, unknown>]) => cmd.__type === 'BatchWriteCommand');
    expect(batchCall).toBeDefined();
    const items = (batchCall![0] as { input: { RequestItems: Record<string, Array<{ PutRequest: { Item: Record<string, unknown> } }>> } }).input.RequestItems['UserDataEvent-test'];
    expect(items[0].PutRequest.Item.status).toBe('INACTIVE');
  });

  it('evaluates OR logic — user meets at least one rule', async () => {
    const orDef = makeSegmentDef({
      desc: JSON.stringify({
        name: 'Frequent OR high-spend',
        rules: [
          { metric: 'visit_count', period: 'week', operator: 'gte', value: 5 },   // user won't meet this
          { metric: 'total_spend', period: 'month', operator: 'gte', value: 100 }, // user will meet this
        ],
        logicalOperator: 'OR',
        scope: 'brand',
        brandId: 'woolworths',
        active: true,
      }),
    });

    // Scan
    mockSend.mockResolvedValueOnce({ Items: [orDef], LastEvaluatedKey: undefined });
    // Subscribers
    mockSend.mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-003' }], LastEvaluatedKey: undefined });
    // Receipts — 2 visits, $150 total (passes 2nd rule but not 1st)
    const d1 = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString();
    const d2 = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString();
    mockSend.mockResolvedValueOnce({ Items: [makeReceiptItem(80, d1), makeReceiptItem(70, d2)], LastEvaluatedKey: undefined });
    // BatchWrite
    mockSend.mockResolvedValueOnce({});
    // Update
    mockSend.mockResolvedValueOnce({});

    await handler(undefined);

    const batchCall = mockSend.mock.calls.find(([cmd]: [Record<string, unknown>]) => cmd.__type === 'BatchWriteCommand');
    const items = (batchCall![0] as { input: { RequestItems: Record<string, Array<{ PutRequest: { Item: Record<string, unknown> } }>> } }).input.RequestItems['UserDataEvent-test'];
    expect(items[0].PutRequest.Item.status).toBe('ACTIVE');
  });

  it('evaluates AND logic — user must meet all rules', async () => {
    const andDef = makeSegmentDef({
      desc: JSON.stringify({
        name: 'Frequent AND high-spend',
        rules: [
          { metric: 'visit_count', period: 'week', operator: 'gte', value: 3 },
          { metric: 'total_spend', period: 'month', operator: 'gte', value: 300 },
        ],
        logicalOperator: 'AND',
        scope: 'brand',
        brandId: 'woolworths',
        active: true,
      }),
    });

    // Scan
    mockSend.mockResolvedValueOnce({ Items: [andDef], LastEvaluatedKey: undefined });
    // Subscribers
    mockSend.mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-004' }], LastEvaluatedKey: undefined });
    // Receipts — 1 visit (fails visit_count rule), $500 total (passes spend rule)
    const d1 = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString();
    mockSend.mockResolvedValueOnce({ Items: [makeReceiptItem(500, d1)], LastEvaluatedKey: undefined });
    // BatchWrite
    mockSend.mockResolvedValueOnce({});
    // Update
    mockSend.mockResolvedValueOnce({});

    await handler(undefined);

    const batchCall = mockSend.mock.calls.find(([cmd]: [Record<string, unknown>]) => cmd.__type === 'BatchWriteCommand');
    const items = (batchCall![0] as { input: { RequestItems: Record<string, Array<{ PutRequest: { Item: Record<string, unknown> } }>> } }).input.RequestItems['UserDataEvent-test'];
    // visit_count = 1 but threshold is 3 → fails AND logic
    expect(items[0].PutRequest.Item.status).toBe('INACTIVE');
  });

  it('handles between operator', async () => {
    const betweenDef = makeSegmentDef({
      desc: JSON.stringify({
        name: 'Mid-range spenders',
        rules: [{ metric: 'total_spend', period: 'month', operator: 'between', value: 100, value2: 500 }],
        logicalOperator: 'AND',
        scope: 'brand',
        brandId: 'woolworths',
        active: true,
      }),
    });

    mockSend.mockResolvedValueOnce({ Items: [betweenDef], LastEvaluatedKey: undefined });
    mockSend.mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-005' }], LastEvaluatedKey: undefined });
    const d1 = new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString();
    mockSend.mockResolvedValueOnce({ Items: [makeReceiptItem(250, d1)], LastEvaluatedKey: undefined });
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({});

    await handler(undefined);

    const batchCall = mockSend.mock.calls.find(([cmd]: [Record<string, unknown>]) => cmd.__type === 'BatchWriteCommand');
    const items = (batchCall![0] as { input: { RequestItems: Record<string, Array<{ PutRequest: { Item: Record<string, unknown> } }>> } }).input.RequestItems['UserDataEvent-test'];
    expect(items[0].PutRequest.Item.status).toBe('ACTIVE');
  });

  it('writes global BeboCard segment membership with brandId=bebocard', async () => {
    const globalDef = {
      pK: 'TENANT#bebocard',
      sK: 'SEGMENT_DEF#global-seg',
      brandId: 'bebocard',
      desc: JSON.stringify({
        name: 'Cross-brand power users',
        rules: [{ metric: 'total_spend', period: 'quarter', operator: 'gte', value: 1000 }],
        logicalOperator: 'AND',
        scope: 'global',
        brandId: 'bebocard',
        active: true,
      }),
    };

    mockSend.mockResolvedValueOnce({ Items: [globalDef], LastEvaluatedKey: undefined });
    mockSend.mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-006' }], LastEvaluatedKey: undefined });
    const d1 = new Date(Date.now() - 20 * 24 * 60 * 60 * 1000).toISOString();
    mockSend.mockResolvedValueOnce({ Items: [makeReceiptItem(1200, d1, 'bigw'), makeReceiptItem(200, d1, 'woolworths')], LastEvaluatedKey: undefined });
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({});

    await handler(undefined);

    const batchCall = mockSend.mock.calls.find(([cmd]: [Record<string, unknown>]) => cmd.__type === 'BatchWriteCommand');
    const items = (batchCall![0] as { input: { RequestItems: Record<string, Array<{ PutRequest: { Item: Record<string, unknown> } }>> } }).input.RequestItems['UserDataEvent-test'];
    // sK should use brandId=bebocard for global segments
    expect(items[0].PutRequest.Item.sK).toBe('SEGMENT#bebocard#CUSTOM#global-seg');
    expect(items[0].PutRequest.Item.status).toBe('ACTIVE');
  });

  it('updates segment def stats after evaluation', async () => {
    mockSend.mockResolvedValueOnce({ Items: [makeSegmentDef()], LastEvaluatedKey: undefined });
    mockSend.mockResolvedValueOnce({ Items: [{ pK: 'USER#perm-007' }], LastEvaluatedKey: undefined });
    const d1 = new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString();
    mockSend.mockResolvedValueOnce({ Items: [makeReceiptItem(400, d1)], LastEvaluatedKey: undefined });
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({}); // UpdateCommand for stats

    await handler(undefined);

    const updateCall = mockSend.mock.calls.find(([cmd]: [Record<string, unknown>]) => cmd.__type === 'UpdateCommand');
    expect(updateCall).toBeDefined();
    const updateInput = (updateCall![0] as { input: { Key: Record<string, string>; ExpressionAttributeValues: Record<string, unknown> } }).input;
    expect(updateInput.Key.pK).toBe('TENANT#tenant-1');
    expect(updateInput.Key.sK).toBe('SEGMENT_DEF#seg-abc');
    expect(updateInput.ExpressionAttributeValues[':count']).toBe(1); // 1 active member
  });
});
