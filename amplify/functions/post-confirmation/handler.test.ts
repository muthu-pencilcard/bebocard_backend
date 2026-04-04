import { vi, describe, it, expect, beforeEach } from 'vitest';

// vi.hoisted ensures mockSend is available inside vi.mock() factories,
// which are hoisted before all variable declarations.
const { mockSend } = vi.hoisted(() => ({ mockSend: vi.fn() }));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) { }),
}));

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockSend }) },
  PutCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-cognito-identity-provider', () => ({
  CognitoIdentityProviderClient: vi.fn(function (this: Record<string, unknown>) {
    this.send = mockSend;
  }),
  AdminUpdateUserAttributesCommand: vi.fn(function (
    this: Record<string, unknown>,
    input: unknown,
  ) {
    Object.assign(this, { __type: 'AdminUpdateUserAttributesCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-ssm', () => ({
  SSMClient: vi.fn(function (this: Record<string, unknown>) {
    this.send = mockSend;
  }),
  GetParameterCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetParameterCommand', input });
  }),
}));

let ulidCounter = 0;
vi.mock('ulid', () => ({
  monotonicFactory: () => () => `ULID${String(ulidCounter++).padStart(4, '0')}`,
}));

// Import handler AFTER mocks
import { handler } from './handler.js';

// ── Helpers ──────────────────────────────────────────────────────────────────

function makeCognitoEvent(overrides: Record<string, unknown> = {}) {
  return {
    version: '1',
    triggerSource: 'PostConfirmation_ConfirmSignUp' as const,
    region: 'ap-southeast-2',
    userPoolId: 'ap-southeast-2_TEST',
    userName: 'test-user-123',
    callerContext: { awsSdkVersion: '1', clientId: 'test-client' },
    request: { userAttributes: {} },
    response: {},
    ...overrides,
  } as Parameters<typeof handler>[0];
}

function setupSuccessfulSend() {
  mockSend.mockImplementation((cmd: { __type: string; input: unknown }) => {
    if (cmd.__type === 'GetParameterCommand') {
      const name = (cmd as unknown as { input: { Name: string } }).input.Name;
      if (name.includes('user')) return Promise.resolve({ Parameter: { Value: 'user-table-name' } });
      return Promise.resolve({ Parameter: { Value: 'admin-table-name' } });
    }
    return Promise.resolve({});
  });
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('post-confirmation handler', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    ulidCounter = 0;
    process.env.USER_TABLE_PARAM = '/bebo/user-table';
    process.env.ADMIN_TABLE_PARAM = '/bebo/admin-table';
  });

  it('throws if SSM returns empty table names', async () => {
    // Must run FIRST — the handler caches SSM table names at module level.
    // An empty-string response triggers the guard before the cache is set.
    mockSend.mockResolvedValue({ Parameter: { Value: '' } });
    await expect(
      handler(makeCognitoEvent(), {} as never, () => { }),
    ).rejects.toThrow('Failed to fetch table names from SSM parameters');
  });

  it('creates IDENTITY record with correct pK and sK', async () => {
    setupSuccessfulSend();
    await handler(makeCognitoEvent(), {} as never, () => { });

    const calls = mockSend.mock.calls as [Record<string, unknown>][];
    const identityCall = calls.find(
      ([cmd]: [Record<string, unknown>]) =>
        cmd.__type === 'PutCommand' &&
        (cmd as { input: { Item?: { sK?: string } } }).input?.Item?.sK === 'IDENTITY',
    );
    expect(identityCall).toBeDefined();
    const item = (identityCall![0] as { input: { Item: Record<string, string> } }).input.Item;
    expect(item.pK).toMatch(/^USER#ULID/);
    expect(item.sK).toBe('IDENTITY');
  });

  it('creates SCAN_INDEX record with correct pK pattern', async () => {
    setupSuccessfulSend();
    await handler(makeCognitoEvent(), {} as never, () => { });

    const scanCalls = mockSend.mock.calls as [Record<string, unknown>][];
    const scanCall = scanCalls.find(
      ([cmd]: [Record<string, unknown>]) =>
        cmd.__type === 'PutCommand' &&
        (cmd as { input: { Item?: { eventType?: string } } }).input?.Item?.eventType === 'SCAN_INDEX',
    );
    expect(scanCall).toBeDefined();
    const item = (scanCall![0] as { input: { Item: Record<string, string> } }).input.Item;
    expect(item.pK).toMatch(/^SCAN#ULID/);
    expect(item.sK).toMatch(/^ULID/);
  });

  it('sets custom:permULID Cognito attribute', async () => {
    setupSuccessfulSend();
    await handler(makeCognitoEvent(), {} as never, () => { });

    const cognitoCalls = mockSend.mock.calls as [Record<string, unknown>][];
    const cognitoCall = cognitoCalls.find(
      ([cmd]: [Record<string, unknown>]) =>
        cmd.__type === 'AdminUpdateUserAttributesCommand',
    );
    expect(cognitoCall).toBeDefined();
    const attrs = (cognitoCall![0] as { input: { UserAttributes: Array<{ Name: string; Value: string }> } }).input.UserAttributes;
    const permAttr = attrs.find((a) => a.Name === 'custom:permULID');
    expect(permAttr).toBeDefined();
    expect(permAttr!.Value).toMatch(/^ULID/);
  });

  it('rotatesAt is approximately 24 hours from now', async () => {
    setupSuccessfulSend();
    const before = Date.now();
    await handler(makeCognitoEvent(), {} as never, () => { });
    const after = Date.now();

    const calls2 = mockSend.mock.calls as [Record<string, unknown>][];
    const identityCall = calls2.find(
      ([cmd]: [Record<string, unknown>]) =>
        cmd.__type === 'PutCommand' &&
        (cmd as { input: { Item?: { sK?: string } } }).input?.Item?.sK === 'IDENTITY',
    );
    const rotatesAt = new Date(
      (identityCall![0] as { input: { Item: { rotatesAt: string } } }).input.Item.rotatesAt,
    ).getTime();
    const expected = 24 * 60 * 60 * 1000;
    expect(rotatesAt).toBeGreaterThanOrEqual(before + expected - 2000);
    expect(rotatesAt).toBeLessThanOrEqual(after + expected + 2000);
  });

  it('IDENTITY record has secondaryULID as a top-level field', async () => {
    setupSuccessfulSend();
    await handler(makeCognitoEvent(), {} as never, () => { });

    const calls3 = mockSend.mock.calls as [Record<string, unknown>][];
    const identityCall = calls3.find(
      ([cmd]: [Record<string, unknown>]) =>
        cmd.__type === 'PutCommand' &&
        (cmd as { input: { Item?: { sK?: string } } }).input?.Item?.sK === 'IDENTITY',
    );
    const item = (identityCall![0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(item).toHaveProperty('secondaryULID');
    expect(typeof item.secondaryULID).toBe('string');
  });

  it('SCAN_INDEX desc contains empty cards array', async () => {
    setupSuccessfulSend();
    await handler(makeCognitoEvent(), {} as never, () => { });

    const scanCalls2 = mockSend.mock.calls as [Record<string, unknown>][];
    const scanCall = scanCalls2.find(
      ([cmd]: [Record<string, unknown>]) =>
        cmd.__type === 'PutCommand' &&
        (cmd as { input: { Item?: { eventType?: string } } }).input?.Item?.eventType === 'SCAN_INDEX',
    );
    const desc = JSON.parse(
      (scanCall![0] as { input: { Item: { desc: string } } }).input.Item.desc,
    );
    expect(desc.cards).toEqual([]);
  });

  it('returns the original event object', async () => {
    setupSuccessfulSend();
    const event = makeCognitoEvent();
    const result = await handler(event, {} as never, () => { });
    expect(result).toBe(event);
  });

  it('SCAN_INDEX sK equals the permULID from the IDENTITY record', async () => {
    setupSuccessfulSend();
    await handler(makeCognitoEvent(), {} as never, () => { });

    const calls4 = mockSend.mock.calls as [Record<string, unknown>][];
    const identityCall = calls4.find(
      ([cmd]: [Record<string, unknown>]) =>
        cmd.__type === 'PutCommand' &&
        (cmd as { input: { Item?: { sK?: string } } }).input?.Item?.sK === 'IDENTITY',
    );
    const scanCall = calls4.find(
      ([cmd]: [Record<string, unknown>]) =>
        cmd.__type === 'PutCommand' &&
        (cmd as { input: { Item?: { eventType?: string } } }).input?.Item?.eventType === 'SCAN_INDEX',
    );

    const permULID = (identityCall![0] as { input: { Item: { pK: string } } }).input.Item.pK.replace('USER#', '');
    expect((scanCall![0] as { input: { Item: { sK: string } } }).input.Item.sK).toBe(permULID);
  });

  it('SCAN_INDEX pK matches secondaryULID from IDENTITY record', async () => {
    setupSuccessfulSend();
    await handler(makeCognitoEvent(), {} as never, () => { });

    const calls5 = mockSend.mock.calls as [Record<string, unknown>][];
    const identityCall = calls5.find(
      ([cmd]: [Record<string, unknown>]) =>
        cmd.__type === 'PutCommand' &&
        (cmd as { input: { Item?: { sK?: string } } }).input?.Item?.sK === 'IDENTITY',
    );
    const scanCall = calls5.find(
      ([cmd]: [Record<string, unknown>]) =>
        cmd.__type === 'PutCommand' &&
        (cmd as { input: { Item?: { eventType?: string } } }).input?.Item?.eventType === 'SCAN_INDEX',
    );

    const secondaryULID = (identityCall![0] as { input: { Item: { secondaryULID: string } } }).input.Item.secondaryULID;
    expect((scanCall![0] as { input: { Item: { pK: string } } }).input.Item.pK).toBe(`SCAN#${secondaryULID}`);
  });

  it('sends AdminUpdateUserAttributesCommand with correct UserPoolId and Username', async () => {
    setupSuccessfulSend();
    const event = makeCognitoEvent({ userName: 'my-user', userPoolId: 'us-east-1_POOL' });
    await handler(event, {} as never, () => { });

    const calls6 = mockSend.mock.calls as [Record<string, unknown>][];
    const cognitoCall = calls6.find(
      ([cmd]: [Record<string, unknown>]) => cmd.__type === 'AdminUpdateUserAttributesCommand',
    );
    const callInput = (cognitoCall![0] as { input: { UserPoolId: string; Username: string } }).input;
    expect(callInput.UserPoolId).toBe('us-east-1_POOL');
    expect(callInput.Username).toBe('my-user');
  });
});
