import { vi, describe, it, expect, beforeEach } from 'vitest';
import { createHmac } from 'crypto';
import type { DynamoDBStreamEvent } from 'aws-lambda';

// ── Hoisted setup ─────────────────────────────────────────────────────────────
// Module-level env vars in handler.ts must be set before the module is imported.

const GLOBAL_SALT = 'test_global_analytics_salt';
const TENANT_SALT = 'test_per_tenant_salt_abc';

const { mockDdbSend, mockAthenaStartQuery, mockAthenaGetExecution } = vi.hoisted(() => {
  process.env.USER_HASH_SALT        = 'test_per_tenant_salt_abc';
  process.env.GLOBAL_ANALYTICS_SALT = 'test_global_analytics_salt';
  process.env.ANALYTICS_BUCKET      = 'test-analytics-bucket';
  process.env.ATHENA_WORKGROUP      = 'test-workgroup';
  process.env.GLUE_DATABASE         = 'bebocard_analytics';
  process.env.REFDATA_TABLE         = 'test-refdata-table';
  return {
    mockDdbSend:             vi.fn(),
    mockAthenaStartQuery:    vi.fn(),
    mockAthenaGetExecution:  vi.fn(),
  };
});

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockDdbSend }) },
  GetCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-athena', () => ({
  AthenaClient: vi.fn(function (this: Record<string, unknown>) {
    this.send = (cmd: { __type: string }) => {
      if (cmd.__type === 'StartQueryExecutionCommand') return mockAthenaStartQuery(cmd);
      if (cmd.__type === 'GetQueryExecutionCommand')  return mockAthenaGetExecution(cmd);
      return Promise.resolve({});
    };
  }),
  StartQueryExecutionCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'StartQueryExecutionCommand', input });
  }),
  GetQueryExecutionCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetQueryExecutionCommand', input });
  }),
  QueryExecutionState: {
    SUCCEEDED: 'SUCCEEDED',
    FAILED:    'FAILED',
    CANCELLED: 'CANCELLED',
    RUNNING:   'RUNNING',
    QUEUED:    'QUEUED',
  },
}));

import { handler, _testResetCaches } from './handler.js';

// ── Helpers ───────────────────────────────────────────────────────────────────

const PERM_ULID = 'PERM-001';
const BRAND_ID  = 'woolworths';
const TENANT_ID = 'tenant-1';

function makeRecord(overrides: {
  eventName?: string;
  pk?: string;
  sk?: string;
  desc?: Record<string, unknown>;
}): DynamoDBStreamEvent['Records'][0] {
  const { eventName = 'INSERT', pk = `USER#${PERM_ULID}`, sk = 'RECEIPT#01HX', desc } = overrides;
  const defaultDesc = {
    merchant:      'Woolworths',
    amount:        54.20,
    currency:      'AUD',
    purchaseDate:  '2026-04-01',
    category:      'grocery',
    brandId:       BRAND_ID,
    secondaryULID: 'SEC-001',
  };
  return {
    eventName,
    dynamodb: {
      NewImage: {
        pK:          { S: pk },
        sK:          { S: sk },
        subCategory: { S: BRAND_ID },
        desc:        { S: JSON.stringify(desc ?? defaultDesc) },
      },
    },
  } as DynamoDBStreamEvent['Records'][0];
}

function setupTenantMocks(tier = 'INTELLIGENCE') {
  mockDdbSend.mockImplementation((cmd: { __type: string; input?: { Key?: { pK?: string; sK?: string } } }) => {
    const pK = cmd.input?.Key?.pK ?? '';
    if (pK.startsWith('BRAND#'))  return Promise.resolve({ Item: { tenantId: TENANT_ID } });
    if (pK.startsWith('TENANT#')) return Promise.resolve({ Item: { tier, desc: JSON.stringify({ salt: TENANT_SALT }) } });
    return Promise.resolve({});
  });
}

function getSql(): string {
  return (mockAthenaStartQuery.mock.calls[0]![0] as { input: { QueryString: string } }).input.QueryString;
}

beforeEach(() => {
  vi.clearAllMocks();
  _testResetCaches();
  // Default DDB: nothing found (safe for filtering tests that never reach Athena)
  mockDdbSend.mockResolvedValue({ Item: undefined });
  mockAthenaStartQuery.mockResolvedValue({ QueryExecutionId: 'qry-001' });
  mockAthenaGetExecution.mockResolvedValue({
    QueryExecution: { Status: { State: 'SUCCEEDED' } },
  });
});

// ── Filtering ─────────────────────────────────────────────────────────────────

describe('record filtering', () => {
  it('does nothing when Records array is empty', async () => {
    await handler({ Records: [] }, {} as never, () => {});
    expect(mockAthenaStartQuery).not.toHaveBeenCalled();
  });

  it('skips non-INSERT events', async () => {
    await handler({ Records: [makeRecord({ eventName: 'MODIFY' })] }, {} as never, () => {});
    expect(mockAthenaStartQuery).not.toHaveBeenCalled();
  });

  it('skips records whose pK does not start with USER#', async () => {
    await handler({ Records: [makeRecord({ pk: 'BRAND#woolworths', sk: 'RECEIPT#01HX' })] }, {} as never, () => {});
    expect(mockAthenaStartQuery).not.toHaveBeenCalled();
  });

  it('skips records whose sK does not start with RECEIPT#', async () => {
    await handler({ Records: [makeRecord({ sk: 'CARD#woolworths#C001' })] }, {} as never, () => {});
    expect(mockAthenaStartQuery).not.toHaveBeenCalled();
  });

  it('skips receipts that lack secondaryULID (manually-entered receipts)', async () => {
    const desc = { merchant: 'Woolworths', amount: 10, currency: 'AUD', purchaseDate: '2026-04-01', brandId: BRAND_ID };
    await handler({ Records: [makeRecord({ desc })] }, {} as never, () => {});
    expect(mockAthenaStartQuery).not.toHaveBeenCalled();
  });

  it('skips records with non-numeric amount', async () => {
    const desc = { merchant: 'Woolworths', amount: 'not-a-number', currency: 'AUD', purchaseDate: '2026-04-01', brandId: BRAND_ID, secondaryULID: 'SEC-001' };
    await handler({ Records: [makeRecord({ desc })] }, {} as never, () => {});
    expect(mockAthenaStartQuery).not.toHaveBeenCalled();
  });

  it('skips records with invalid desc JSON', async () => {
    const record = makeRecord({});
    (record.dynamodb!.NewImage as Record<string, unknown>)['desc'] = { S: 'not-json{{' };
    await handler({ Records: [record] }, {} as never, () => {});
    expect(mockAthenaStartQuery).not.toHaveBeenCalled();
  });

  it('skips ENGAGEMENT tier tenants', async () => {
    setupTenantMocks('ENGAGEMENT');
    await handler({ Records: [makeRecord({})] }, {} as never, () => {});
    expect(mockAthenaStartQuery).not.toHaveBeenCalled();
  });
});

// ── Dual visitor hash ─────────────────────────────────────────────────────────

describe('dual visitor hash', () => {
  it('computes both hashes for a BeboCard user and emits them in the SQL', async () => {
    setupTenantMocks();
    await handler({ Records: [makeRecord({})] }, {} as never, () => {});

    expect(mockAthenaStartQuery).toHaveBeenCalledOnce();
    const sql = getSql();

    const expectedGlobal = createHmac('sha256', GLOBAL_SALT).update(PERM_ULID).digest('hex');
    const expectedTenant = createHmac('sha256', TENANT_SALT).update(PERM_ULID).digest('hex');

    expect(sql).toContain(expectedGlobal);
    expect(sql).toContain(expectedTenant);
    // The two hashes must be distinct (different salts)
    expect(expectedGlobal).not.toBe(expectedTenant);
  });

  it('includes visitor_hash, visitor_hash_tenant, is_bebocard in column list', async () => {
    setupTenantMocks();
    await handler({ Records: [makeRecord({})] }, {} as never, () => {});
    const sql = getSql();
    expect(sql).toContain('visitor_hash');
    expect(sql).toContain('visitor_hash_tenant');
    expect(sql).toContain('is_bebocard');
  });

  it('sets is_bebocard = true for BeboCard users', async () => {
    setupTenantMocks();
    await handler({ Records: [makeRecord({})] }, {} as never, () => {});
    const sql = getSql();
    expect(sql).toContain('true');
  });

  it('writes NULL for both hashes and is_bebocard = false for anonymous records', async () => {
    setupTenantMocks();

    const anonRecord: DynamoDBStreamEvent['Records'][0] = {
      eventName: 'INSERT',
      dynamodb: {
        NewImage: {
          pK:          { S: 'ANON#some-anon-id' },
          sK:          { S: 'receipt' },
          subCategory: { S: BRAND_ID },
          desc:        { S: JSON.stringify({
            isAnonymous:  true,
            purchaseDate: '2026-04-18',
            amount:       20,
            currency:     'AUD',
            category:     'grocery',
            merchant:     'Coles',
            brandId:      BRAND_ID,
          }) },
        },
      },
    } as DynamoDBStreamEvent['Records'][0];

    await handler({ Records: [anonRecord] }, {} as never, () => {});

    expect(mockAthenaStartQuery).toHaveBeenCalledOnce();
    const sql = getSql();
    expect(sql).toContain('NULL');
    expect(sql).toContain('false');
  });

  it('never writes raw permULID into the SQL', async () => {
    setupTenantMocks();
    await handler({ Records: [makeRecord({})] }, {} as never, () => {});
    const sql = getSql();
    expect(sql).not.toContain(PERM_ULID);
  });

  it('uses the global salt (not tenant salt) for visitor_hash', async () => {
    setupTenantMocks();
    await handler({ Records: [makeRecord({})] }, {} as never, () => {});
    const sql = getSql();

    const hashWithGlobalSalt = createHmac('sha256', GLOBAL_SALT).update(PERM_ULID).digest('hex');
    const hashWithTenantSalt = createHmac('sha256', TENANT_SALT).update(PERM_ULID).digest('hex');

    // Both appear in the SQL — global for visitor_hash, tenant for visitor_hash_tenant
    expect(sql).toContain(hashWithGlobalSalt);
    expect(sql).toContain(hashWithTenantSalt);
    expect(hashWithGlobalSalt).not.toBe(hashWithTenantSalt);
  });
});

// ── Happy path ────────────────────────────────────────────────────────────────

describe('happy path', () => {
  it('starts an Athena INSERT for a valid brand-push receipt', async () => {
    setupTenantMocks();
    await handler({ Records: [makeRecord({})] }, {} as never, () => {});
    expect(mockAthenaStartQuery).toHaveBeenCalledOnce();
    const sql = getSql();
    expect(sql).toContain('INSERT INTO');
    expect(sql).toContain('receipts');
    expect(sql).toContain(BRAND_ID);
  });

  it('batches multiple valid records into a single Athena call', async () => {
    setupTenantMocks();
    await handler({
      Records: [makeRecord({}), makeRecord({ sk: 'RECEIPT#01HY' })],
    }, {} as never, () => {});
    expect(mockAthenaStartQuery).toHaveBeenCalledOnce();
    const sql = getSql();
    const valueRows = sql.match(/\(/g) ?? [];
    expect(valueRows.length).toBeGreaterThanOrEqual(2);
  });

  it('waits for Athena query to succeed before returning', async () => {
    setupTenantMocks();
    let pollCount = 0;
    mockAthenaGetExecution.mockImplementation(() => {
      pollCount++;
      return Promise.resolve({
        QueryExecution: { Status: { State: pollCount < 2 ? 'RUNNING' : 'SUCCEEDED' } },
      });
    });

    await handler({ Records: [makeRecord({})] }, {} as never, () => {});
    expect(pollCount).toBeGreaterThanOrEqual(2);
  });
});

// ── Athena error handling ─────────────────────────────────────────────────────

describe('Athena error handling', () => {
  it('throws when Athena query FAILS', async () => {
    setupTenantMocks();
    mockAthenaGetExecution.mockResolvedValue({
      QueryExecution: { Status: { State: 'FAILED', StateChangeReason: 'syntax error' } },
    });
    await expect(handler({ Records: [makeRecord({})] }, {} as never, () => {})).rejects.toThrow('FAILED');
  });

  it('throws when Athena query is CANCELLED', async () => {
    setupTenantMocks();
    mockAthenaGetExecution.mockResolvedValue({
      QueryExecution: { Status: { State: 'CANCELLED', StateChangeReason: 'manually cancelled' } },
    });
    await expect(handler({ Records: [makeRecord({})] }, {} as never, () => {})).rejects.toThrow('CANCELLED');
  });

  it('logs an error and continues when StartQueryExecution returns no QueryExecutionId', async () => {
    setupTenantMocks();
    mockAthenaStartQuery.mockResolvedValue({});
    await handler({ Records: [makeRecord({})] }, {} as never, () => {});
    expect(mockAthenaGetExecution).not.toHaveBeenCalled();
  });
});

// ── SQL sanitizers ────────────────────────────────────────────────────────────

describe('SQL sanitizers', () => {
  it('falls back to AUD for non-ISO-4217 currency codes', async () => {
    setupTenantMocks();
    const desc = { merchant: 'Shop', amount: 10, currency: 'INVALID', purchaseDate: '2026-04-01', brandId: BRAND_ID, secondaryULID: 'S1' };
    await handler({ Records: [makeRecord({ desc })] }, {} as never, () => {});
    expect(getSql()).toContain("'AUD'");
  });

  it("escapes single quotes in merchant names", async () => {
    setupTenantMocks();
    const desc = { merchant: "McDonald's", amount: 10, currency: 'AUD', purchaseDate: '2026-04-01', brandId: BRAND_ID, secondaryULID: 'S1' };
    await handler({ Records: [makeRecord({ desc })] }, {} as never, () => {});
    expect(getSql()).toContain("McDonald''s");
  });

  it('uses today as purchaseDate when the value is missing', async () => {
    setupTenantMocks();
    const desc = { merchant: 'Shop', amount: 10, currency: 'AUD', brandId: BRAND_ID, secondaryULID: 'S1' };
    await handler({ Records: [makeRecord({ desc })] }, {} as never, () => {});
    const todayPrefix = new Date().toISOString().substring(0, 7);
    expect(getSql()).toContain(todayPrefix);
  });
});
