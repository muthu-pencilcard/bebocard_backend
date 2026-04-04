import { vi, describe, it, expect, beforeEach } from 'vitest';
import type { DynamoDBStreamEvent } from 'aws-lambda';

// ── Hoisted setup ─────────────────────────────────────────────────────────────
// USER_HASH_SALT is read at module level in handler.ts, so it must be set
// before the module is imported (vi.hoisted runs before all imports).

const { mockAthenaStartQuery, mockAthenaGetExecution } = vi.hoisted(() => {
  process.env.USER_HASH_SALT     = 'test-salt-value-abc123';
  process.env.ANALYTICS_BUCKET   = 'test-analytics-bucket';
  process.env.ATHENA_WORKGROUP    = 'test-workgroup';
  process.env.GLUE_DATABASE       = 'bebocard_analytics';
  return {
    mockAthenaStartQuery: vi.fn(),
    mockAthenaGetExecution: vi.fn(),
  };
});

vi.mock('@aws-sdk/client-athena', () => ({
  AthenaClient: vi.fn(function (this: Record<string, unknown>) {
    this.send = (cmd: { __type: string }) => {
      if (cmd.__type === 'StartQueryExecutionCommand') return mockAthenaStartQuery(cmd);
      if (cmd.__type === 'GetQueryExecutionCommand') return mockAthenaGetExecution(cmd);
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
    SUCCEEDED:  'SUCCEEDED',
    FAILED:     'FAILED',
    CANCELLED:  'CANCELLED',
    RUNNING:    'RUNNING',
    QUEUED:     'QUEUED',
  },
}));

import { handler } from './handler.js';

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeRecord(overrides: {
  eventName?: string;
  pk?: string;
  sk?: string;
  desc?: Record<string, unknown>;
}): DynamoDBStreamEvent['Records'][0] {
  const { eventName = 'INSERT', pk = 'USER#PERM-001', sk = 'RECEIPT#01HX', desc } = overrides;
  const defaultDesc = {
    merchant: 'Woolworths',
    amount: 54.20,
    currency: 'AUD',
    purchaseDate: '2026-04-01',
    category: 'grocery',
    brandId: 'woolworths',
    secondaryULID: 'SEC-001',
  };
  return {
    eventName,
    dynamodb: {
      NewImage: {
        pK: { S: pk },
        sK: { S: sk },
        subCategory: { S: 'woolworths' },
        desc: { S: JSON.stringify(desc ?? defaultDesc) },
      },
    },
  } as DynamoDBStreamEvent['Records'][0];
}

beforeEach(() => {
  vi.clearAllMocks();
  process.env.ANALYTICS_BUCKET  = 'test-analytics-bucket';
  process.env.ATHENA_WORKGROUP   = 'test-workgroup';
  process.env.GLUE_DATABASE      = 'bebocard_analytics';
  process.env.USER_HASH_SALT     = 'test-salt-value-abc123';

  // Default: Athena query starts and succeeds immediately
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
    const desc = { merchant: 'Woolworths', amount: 10, currency: 'AUD', purchaseDate: '2026-04-01', brandId: 'woolworths' };
    await handler({ Records: [makeRecord({ desc })] }, {} as never, () => {});
    expect(mockAthenaStartQuery).not.toHaveBeenCalled();
  });

  it('skips records with non-numeric amount', async () => {
    const desc = { merchant: 'Woolworths', amount: 'not-a-number', currency: 'AUD', purchaseDate: '2026-04-01', brandId: 'woolworths', secondaryULID: 'SEC-001' };
    await handler({ Records: [makeRecord({ desc })] }, {} as never, () => {});
    expect(mockAthenaStartQuery).not.toHaveBeenCalled();
  });

  it('skips records with invalid desc JSON', async () => {
    const record = makeRecord({});
    (record.dynamodb!.NewImage as Record<string, unknown>)['desc'] = { S: 'not-json{{' };
    await handler({ Records: [record] }, {} as never, () => {});
    expect(mockAthenaStartQuery).not.toHaveBeenCalled();
  });
});

// ── Happy path ────────────────────────────────────────────────────────────────

describe('happy path', () => {
  it('starts an Athena INSERT for a valid brand-push receipt', async () => {
    await handler({ Records: [makeRecord({})] }, {} as never, () => {});
    expect(mockAthenaStartQuery).toHaveBeenCalledOnce();
    const sql = (mockAthenaStartQuery.mock.calls[0][0] as { input: { QueryString: string } }).input.QueryString;
    expect(sql).toContain('INSERT INTO');
    expect(sql).toContain('receipts');
    expect(sql).toContain('woolworths'); // brandId in SQL
  });

  it('pseudonymises the permULID — never appears in SQL', async () => {
    await handler({ Records: [makeRecord({})] }, {} as never, () => {});
    const sql = (mockAthenaStartQuery.mock.calls[0][0] as { input: { QueryString: string } }).input.QueryString;
    expect(sql).not.toContain('PERM-001');
  });

  it('batches multiple valid records into a single Athena call', async () => {
    await handler({
      Records: [makeRecord({}), makeRecord({ sk: 'RECEIPT#01HY' })],
    }, {} as never, () => {});
    expect(mockAthenaStartQuery).toHaveBeenCalledOnce();
    const sql = (mockAthenaStartQuery.mock.calls[0][0] as { input: { QueryString: string } }).input.QueryString;
    // Two value rows in the INSERT
    const valueLines = sql.match(/\('/g) ?? [];
    expect(valueLines.length).toBeGreaterThanOrEqual(2);
  });

  it('waits for Athena query to succeed before returning', async () => {
    // Simulate RUNNING → SUCCEEDED on second poll
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
    mockAthenaGetExecution.mockResolvedValue({
      QueryExecution: { Status: { State: 'FAILED', StateChangeReason: 'syntax error' } },
    });
    await expect(handler({ Records: [makeRecord({})] }, {} as never, () => {})).rejects.toThrow('FAILED');
  });

  it('throws when Athena query is CANCELLED', async () => {
    mockAthenaGetExecution.mockResolvedValue({
      QueryExecution: { Status: { State: 'CANCELLED', StateChangeReason: 'manually cancelled' } },
    });
    await expect(handler({ Records: [makeRecord({})] }, {} as never, () => {})).rejects.toThrow('CANCELLED');
  });

  it('throws when StartQueryExecution returns no QueryExecutionId', async () => {
    mockAthenaStartQuery.mockResolvedValue({});
    await expect(handler({ Records: [makeRecord({})] }, {} as never, () => {})).rejects.toThrow();
  });
});

// ── SQL sanitizers ────────────────────────────────────────────────────────────

describe('SQL sanitizer — currency', () => {
  it('falls back to AUD for non-ISO-4217 currency codes', async () => {
    const desc = { merchant: 'Shop', amount: 10, currency: 'INVALID', purchaseDate: '2026-04-01', brandId: 'brand', secondaryULID: 'S1' };
    await handler({ Records: [makeRecord({ desc })] }, {} as never, () => {});
    const sql = (mockAthenaStartQuery.mock.calls[0][0] as { input: { QueryString: string } }).input.QueryString;
    expect(sql).toContain("'AUD'");
  });
});

describe('SQL sanitizer — merchant name with single quote', () => {
  it('escapes single quotes in merchant names', async () => {
    const desc = { merchant: "McDonald's", amount: 10, currency: 'AUD', purchaseDate: '2026-04-01', brandId: 'brand', secondaryULID: 'S1' };
    await handler({ Records: [makeRecord({ desc })] }, {} as never, () => {});
    const sql = (mockAthenaStartQuery.mock.calls[0][0] as { input: { QueryString: string } }).input.QueryString;
    expect(sql).toContain("McDonald''s");
  });
});

describe('SQL sanitizer — purchaseDate fallback', () => {
  it('uses today as purchaseDate when the value is missing', async () => {
    const desc = { merchant: 'Shop', amount: 10, currency: 'AUD', brandId: 'brand', secondaryULID: 'S1' };
    await handler({ Records: [makeRecord({ desc })] }, {} as never, () => {});
    const sql = (mockAthenaStartQuery.mock.calls[0][0] as { input: { QueryString: string } }).input.QueryString;
    const todayPrefix = new Date().toISOString().substring(0, 7); // YYYY-MM
    expect(sql).toContain(todayPrefix);
  });
});
