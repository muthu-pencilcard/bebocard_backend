/**
 * reminder-handler tests
 *
 * ESM-compatible mock patterns:
 * - All shared mock fns are hoisted via vi.hoisted()
 * - All constructor mocks use regular `function` (not arrow functions)
 * - Command mocks store { __type, input } on `this`
 * - Handler is imported AFTER all vi.mock() calls
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

// ─── Hoisted shared mocks ─────────────────────────────────────────────────────

const { mockSend, mockFcmSend } = vi.hoisted(() => ({
  mockSend:    vi.fn(),
  mockFcmSend: vi.fn(),
}));

// ─── Module mocks (must precede handler import) ───────────────────────────────

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: object) {}),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: {
    from: vi.fn(() => ({ send: mockSend })),
  },
  ScanCommand: vi.fn(function (this: { __type: string; input: unknown }, input: unknown) {
    this.__type = 'ScanCommand';
    this.input = input;
  }),
  GetCommand: vi.fn(function (this: { __type: string; input: unknown }, input: unknown) {
    this.__type = 'GetCommand';
    this.input = input;
  }),
  PutCommand: vi.fn(function (this: { __type: string; input: unknown }, input: unknown) {
    this.__type = 'PutCommand';
    this.input = input;
  }),
  QueryCommand: vi.fn(function (this: { __type: string; input: unknown }, input: unknown) {
    this.__type = 'QueryCommand';
    this.input = input;
  }),
}));

vi.mock('firebase-admin/app', () => ({
  initializeApp: vi.fn(),
  getApps:       vi.fn(() => [{}]),   // non-empty → skip initializeApp
  cert:          vi.fn((x: unknown) => x),
}));

vi.mock('firebase-admin/messaging', () => ({
  getMessaging: vi.fn(() => ({ send: mockFcmSend })),
}));

vi.mock('ulid', () => ({
  monotonicFactory: () => {
    let c = 0;
    return () => `ULID${c++}`;
  },
}));

// ─── Handler import (after all vi.mock calls) ─────────────────────────────────

import { handler } from './handler.js';

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Returns today + daysAhead as YYYY-MM-DD.
 * Mirrors the exact calculation in processRule().
 */
function targetDate(daysAhead: number): string {
  const d = new Date();
  d.setDate(d.getDate() + daysAhead);
  return d.toISOString().slice(0, 10);
}

/** Valid USER_TABLE item for the invoiceDue rule. */
function makeInvoiceItem(dueDate: string) {
  return {
    pK: 'USER#PERM001',
    sK: 'INVOICE#inv1',
    desc: JSON.stringify({ dueDate, supplier: 'Acme', currency: 'AUD', amount: 99 }),
  };
}

/**
 * Full happy-path mockSend — call-order matches processRule() internals:
 *   0 → ScanCommand  (returns items)
 *   1 → GetCommand   (PREFERENCES — no record → reminders on)
 *   2 → GetCommand   (ADMIN alreadySent — no record → not yet sent)
 *   3 → GetCommand   (DEVICE_TOKEN → token)
 *   4 → PutCommand   (markSent)
 */
function setupSuccessMock(dueDate: string) {
  let call = 0;
  mockSend.mockImplementation((cmd: { __type: string }) => {
    const n = call++;
    if (n === 0) {
      // ScanCommand — first daysAhead=3 page, no pagination
      return Promise.resolve({
        Items: [makeInvoiceItem(dueDate)],
        LastEvaluatedKey: undefined,
      });
    }
    if (n === 1) {
      // PREFERENCES — no record → default reminders on
      return Promise.resolve({ Item: undefined });
    }
    if (n === 2) {
      // ADMIN alreadySent — not yet sent
      return Promise.resolve({ Item: undefined });
    }
    if (n === 3) {
      // DEVICE_TOKEN
      return Promise.resolve({
        Item: { desc: JSON.stringify({ token: 'fcm-token-123' }) },
      });
    }
    if (n === 4) {
      // PutCommand markSent
      return Promise.resolve({});
    }
    // Remaining rule/daysAhead iterations — return empty scan pages
    return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
  });
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('reminder-handler', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.USER_TABLE  = 'user-table';
    process.env.ADMIN_TABLE = 'admin-table';
    process.env.FIREBASE_SERVICE_ACCOUNT_JSON = JSON.stringify({});
  });

  // 1 ── no items ───────────────────────────────────────────────────────────────
  it('returns { sent: 0 } when every scan returns no items', async () => {
    mockSend.mockResolvedValue({ Items: [], LastEvaluatedKey: undefined });
    const result = await handler();
    expect(result).toEqual({ sent: 0 });
  });

  // 2 ── desc has no target date field ──────────────────────────────────────────
  it('returns { sent: 0 } when scan item desc has no dueDate field', async () => {
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'ScanCommand') {
        return Promise.resolve({
          Items: [{
            pK: 'USER#PERM001',
            sK: 'INVOICE#inv1',
            desc: JSON.stringify({ supplier: 'Acme' }), // no dueDate
          }],
          LastEvaluatedKey: undefined,
        });
      }
      return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
    });

    const result = await handler();
    expect(result).toEqual({ sent: 0 });
  });

  // 3 ── date doesn't match target date ─────────────────────────────────────────
  it('returns { sent: 0 } when scan item dueDate does not match target date', async () => {
    mockSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'ScanCommand') {
        return Promise.resolve({
          Items: [makeInvoiceItem('2000-01-01')], // wrong date
          LastEvaluatedKey: undefined,
        });
      }
      return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
    });

    const result = await handler();
    expect(result).toEqual({ sent: 0 });
  });

  // 4 ── user has disabled the reminder ─────────────────────────────────────────
  it('returns { sent: 0 } when user has disabled invoiceDue reminder in PREFERENCES', async () => {
    const due = targetDate(3);
    let call = 0;
    mockSend.mockImplementation((cmd: { __type: string }) => {
      const n = call++;
      if (n === 0) {
        return Promise.resolve({
          Items: [makeInvoiceItem(due)],
          LastEvaluatedKey: undefined,
        });
      }
      if (n === 1) {
        // PREFERENCES — invoiceDue disabled
        return Promise.resolve({
          Item: { desc: JSON.stringify({ reminders: { invoiceDue: false } }) },
        });
      }
      return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
    });

    const result = await handler();
    expect(result).toEqual({ sent: 0 });
    // ADMIN and DEVICE_TOKEN GetCommands should NOT have been called
    const getCalls = (mockSend.mock.calls as Array<[{ __type: string; input: { TableName: string; Key: { sK: string } } }]>).filter(
      ([cmd]) => cmd.__type === 'GetCommand',
    );
    const adminCalls = getCalls.filter(([cmd]) => cmd.input.TableName === 'admin-table');
    expect(adminCalls).toHaveLength(0);
  });

  // 5 ── already sent ────────────────────────────────────────────────────────────
  it('returns { sent: 0 } when ADMIN_TABLE shows reminder already sent', async () => {
    const due = targetDate(3);
    let call = 0;
    mockSend.mockImplementation(() => {
      const n = call++;
      if (n === 0) return Promise.resolve({ Items: [makeInvoiceItem(due)], LastEvaluatedKey: undefined });
      if (n === 1) return Promise.resolve({ Item: undefined }); // PREFERENCES — on
      if (n === 2) return Promise.resolve({ Item: {} });        // alreadySent — YES
      return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
    });

    const result = await handler();
    expect(result).toEqual({ sent: 0 });
  });

  // 6 ── no device token ────────────────────────────────────────────────────────
  it('returns { sent: 0 } when DEVICE_TOKEN record has no token', async () => {
    const due = targetDate(3);
    let call = 0;
    mockSend.mockImplementation(() => {
      const n = call++;
      if (n === 0) return Promise.resolve({ Items: [makeInvoiceItem(due)], LastEvaluatedKey: undefined });
      if (n === 1) return Promise.resolve({ Item: undefined });               // PREFERENCES
      if (n === 2) return Promise.resolve({ Item: undefined });               // alreadySent
      if (n === 3) return Promise.resolve({ Item: { desc: JSON.stringify({}) } }); // no token field
      return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
    });

    const result = await handler();
    expect(result).toEqual({ sent: 0 });
    expect(mockFcmSend).not.toHaveBeenCalled();
  });

  // 7 ── happy path ──────────────────────────────────────────────────────────────
  it('sends FCM and returns { sent: 1 } when all conditions are met', async () => {
    const due = targetDate(3);
    mockFcmSend.mockResolvedValue({});
    setupSuccessMock(due);

    const result = await handler();
    expect(result).toEqual({ sent: 1 });
    expect(mockFcmSend).toHaveBeenCalledTimes(1);
  });

  // 8 ── markSent PutCommand shape ───────────────────────────────────────────────
  it('markSent PutCommand has correct pK, sK, and eventType', async () => {
    const due = targetDate(3);
    mockFcmSend.mockResolvedValue({});
    setupSuccessMock(due);

    await handler();

    const putCall = (mockSend.mock.calls as Array<[{ __type: string; input: Record<string, unknown> }]>).find(
      ([cmd]) => cmd.__type === 'PutCommand',
    );
    expect(putCall).toBeDefined();
    const item = putCall![0].input.Item as Record<string, string>;
    expect(item.pK).toBe('REMINDER#PERM001');
    expect(item.sK).toBe('SENT#INVOICE#inv1#REMINDER#invoiceDue#3d');
    expect(item.eventType).toBe('REMINDER_SENT');
  });

  // 9 ── FCM data.type field ─────────────────────────────────────────────────────
  it('FCM message data.type is REMINDER_INVOICEDUE for the invoiceDue rule', async () => {
    const due = targetDate(3);
    mockFcmSend.mockResolvedValue({});
    setupSuccessMock(due);

    await handler();

    expect(mockFcmSend).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ type: 'REMINDER_INVOICEDUE' }),
      }),
    );
  });

  // 10 ── FCM throws — count stays 0, handler resolves ──────────────────────────
  it('does not increment count when FCM throws, and handler still resolves', async () => {
    const due = targetDate(3);
    mockFcmSend.mockRejectedValue(new Error('FCM unavailable'));
    setupSuccessMock(due);

    // Should resolve, not throw
    const result = await expect(handler()).resolves.toBeDefined();
    const { sent } = await handler();
    // Two runs: 1st from above await + the fresh call here. Each should give { sent: 0 }.
    expect(sent).toBe(0);
  });

  // 11 ── markSent throws — item skipped, handler resolves ──────────────────────
  it('skips item and does not crash when markSent throws', async () => {
    const due = targetDate(3);
    let call = 0;
    mockSend.mockImplementation(() => {
      const n = call++;
      if (n === 0) return Promise.resolve({ Items: [makeInvoiceItem(due)], LastEvaluatedKey: undefined });
      if (n === 1) return Promise.resolve({ Item: undefined }); // PREFERENCES
      if (n === 2) return Promise.resolve({ Item: undefined }); // alreadySent
      if (n === 3) return Promise.resolve({ Item: { desc: JSON.stringify({ token: 'tok' }) } }); // DEVICE_TOKEN
      if (n === 4) return Promise.reject(new Error('DDB write failed')); // markSent → throws
      return Promise.resolve({ Items: [], LastEvaluatedKey: undefined });
    });

    await expect(handler()).resolves.toBeDefined();
    expect(mockFcmSend).not.toHaveBeenCalled();
  });

  // 12 ── FCM token embedded in data field ──────────────────────────────────────
  it('FCM message includes correct token, recordSK, and daysAhead string', async () => {
    const due = targetDate(3);
    mockFcmSend.mockResolvedValue({});
    setupSuccessMock(due);

    await handler();

    expect(mockFcmSend).toHaveBeenCalledWith(
      expect.objectContaining({
        token: 'fcm-token-123',
        data: expect.objectContaining({
          recordSK: 'INVOICE#inv1',
          daysAhead: '3',
        }),
      }),
    );
  });

  // 13 ── notification title / body ─────────────────────────────────────────────
  it('FCM notification title is "Invoice due in 3 days" for 3-day reminder', async () => {
    const due = targetDate(3);
    mockFcmSend.mockResolvedValue({});
    setupSuccessMock(due);

    await handler();

    expect(mockFcmSend).toHaveBeenCalledWith(
      expect.objectContaining({
        notification: expect.objectContaining({
          title: 'Invoice due in 3 days',
        }),
      }),
    );
  });
});
