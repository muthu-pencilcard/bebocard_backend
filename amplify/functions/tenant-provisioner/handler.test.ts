import { vi, describe, it, expect, beforeEach } from 'vitest';

const { mockDdbSend, mockApigwSend, mockSsmSend, mockGlueSend, mockS3Send } = vi.hoisted(() => ({
  mockDdbSend: vi.fn(),
  mockApigwSend: vi.fn(),
  mockSsmSend: vi.fn(),
  mockGlueSend: vi.fn(),
  mockS3Send: vi.fn(),
}));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) {}),
}));
vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockDdbSend }) },
  UpdateCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'UpdateCommand', input });
  }),
  PutCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutCommand', input });
  }),
}));
vi.mock('@aws-sdk/client-api-gateway', () => ({
  APIGatewayClient: vi.fn(function (this: Record<string, unknown>) {
    this.send = mockApigwSend;
  }),
  CreateApiKeyCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'CreateApiKeyCommand', input });
  }),
  CreateUsagePlanKeyCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'CreateUsagePlanKeyCommand', input });
  }),
  DeleteApiKeyCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'DeleteApiKeyCommand', input });
  }),
}));
vi.mock('@aws-sdk/client-ssm', () => ({
  SSMClient: vi.fn(function (this: Record<string, unknown>) {
    this.send = mockSsmSend;
  }),
  GetParameterCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetParameterCommand', input });
  }),
}));
vi.mock('@aws-sdk/client-glue', () => ({
  GlueClient: vi.fn(function (this: Record<string, unknown>) {
    this.send = mockGlueSend;
  }),
  GetTableCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetTableCommand', input });
  }),
  CreateTableCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'CreateTableCommand', input });
  }),
  EntityNotFoundException: class EntityNotFoundException extends Error {
    name = 'EntityNotFoundException';
  },
}));
vi.mock('@aws-sdk/client-s3', () => ({
  S3Client: vi.fn(function (this: Record<string, unknown>) {
    this.send = mockS3Send;
  }),
  HeadBucketCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'HeadBucketCommand', input });
  }),
  CreateBucketCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'CreateBucketCommand', input });
  }),
  PutBucketPolicyCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'PutBucketPolicyCommand', input });
  }),
}));
vi.mock('@aws-sdk/util-dynamodb', () => ({
  // Return the plain object — the test already passes plain objects in NewImage
  unmarshall: vi.fn((x: unknown) => x),
}));

import { handler } from './handler.js';

// ── Helpers ──────────────────────────────────────────────────────────────────

function makeStreamEvent(
  eventName: 'INSERT' | 'MODIFY',
  tenantId: string,
  tier: string,
  descOverrides: Record<string, unknown> = {},
) {
  return {
    Records: [
      {
        eventName,
        dynamodb: {
          NewImage: {
            pK: `TENANT#${tenantId}`,
            sK: 'profile',
            tier,
            primaryCat: 'tenant',
            desc: JSON.stringify({ apiKeyPlaintext: 'bebo_TESTULIDKEY.abc123secret', ...descOverrides }),
          },
        },
      },
    ],
  } as Parameters<typeof handler>[0];
}

function setupSsmPlanIds() {
  mockSsmSend.mockImplementation((cmd: { input: { Name: string } }) => {
    const name = cmd.input.Name;
    if (name.includes('SCAN_BASE'))         return Promise.resolve({ Parameter: { Value: 'plan-scan-base' } });
    if (name.includes('SCAN_ENGAGEMENT'))   return Promise.resolve({ Parameter: { Value: 'plan-scan-engagement' } });
    if (name.includes('SCAN_INTELLIGENCE')) return Promise.resolve({ Parameter: { Value: 'plan-scan-intelligence' } });
    if (name.includes('SCAN_ENTERPRISE'))   return Promise.resolve({ Parameter: { Value: 'plan-scan-enterprise' } });
    if (name.includes('ANALYTICS_ENGAGEMENT'))   return Promise.resolve({ Parameter: { Value: 'plan-analytics-engagement' } });
    if (name.includes('ANALYTICS_INTELLIGENCE')) return Promise.resolve({ Parameter: { Value: 'plan-analytics-intelligence' } });
    if (name.includes('ANALYTICS_ENTERPRISE'))   return Promise.resolve({ Parameter: { Value: 'plan-analytics-enterprise' } });
    return Promise.resolve({ Parameter: { Value: 'plan-unknown' } });
  });
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('tenant-provisioner — provisionApiKey', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reset module-level caches between tests
    process.env.GLUE_DATABASE = 'bebo_analytics_test';
    process.env.ANALYTICS_BUCKET = 'bebo-analytics-bucket';
    process.env.REFDATA_TABLE = 'RefDataEvent-test';
    process.env.USAGE_PLAN_SCAN_BASE = '/bebo/test/USAGE_PLAN_SCAN_BASE';
    process.env.USAGE_PLAN_SCAN_ENGAGEMENT = '/bebo/test/USAGE_PLAN_SCAN_ENGAGEMENT';
    process.env.USAGE_PLAN_SCAN_INTELLIGENCE = '/bebo/test/USAGE_PLAN_SCAN_INTELLIGENCE';
    process.env.USAGE_PLAN_SCAN_ENTERPRISE = '/bebo/test/USAGE_PLAN_SCAN_ENTERPRISE';
    process.env.USAGE_PLAN_ANALYTICS_ENGAGEMENT = '/bebo/test/USAGE_PLAN_ANALYTICS_ENGAGEMENT';
    process.env.USAGE_PLAN_ANALYTICS_INTELLIGENCE = '/bebo/test/USAGE_PLAN_ANALYTICS_INTELLIGENCE';
    process.env.USAGE_PLAN_ANALYTICS_ENTERPRISE = '/bebo/test/USAGE_PLAN_ANALYTICS_ENTERPRISE';

    // Default mock returns
    mockApigwSend.mockResolvedValue({ id: 'key-id-001' });
    mockDdbSend.mockResolvedValue({});
    mockGlueSend.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'GetTableCommand') {
        // Pretend the table already exists — skip Glue provisioning
        return Promise.resolve({});
      }
      return Promise.resolve({});
    });
    setupSsmPlanIds();
  });

  it('INSERT: creates API GW key and associates with scan + analytics plans for ENGAGEMENT tier', async () => {
    await handler(makeStreamEvent('INSERT', 'woolworths', 'ENGAGEMENT'), {} as never, () => {});

    const apigwCalls = mockApigwSend.mock.calls as [{ __type: string; input: Record<string, unknown> }][];

    const createKeyCall = apigwCalls.find(([cmd]) => cmd.__type === 'CreateApiKeyCommand');
    expect(createKeyCall).toBeDefined();
    expect(createKeyCall![0].input.value).toBe('bebo_TESTULIDKEY.abc123secret');
    expect(createKeyCall![0].input.enabled).toBe(true);

    const planKeyAssocs = apigwCalls.filter(([cmd]) => cmd.__type === 'CreateUsagePlanKeyCommand');
    expect(planKeyAssocs).toHaveLength(2); // scan + analytics

    const planIds = planKeyAssocs.map(([cmd]) => cmd.input.usagePlanId);
    expect(planIds).toContain('plan-scan-engagement');
    expect(planIds).toContain('plan-analytics-engagement');
  });

  it('INSERT: BASE tier associates only with scan plan (no analytics)', async () => {
    await handler(makeStreamEvent('INSERT', 'smallcafe', 'BASE'), {} as never, () => {});

    const apigwCalls = mockApigwSend.mock.calls as [{ __type: string; input: Record<string, unknown> }][];
    const planKeyAssocs = apigwCalls.filter(([cmd]) => cmd.__type === 'CreateUsagePlanKeyCommand');

    expect(planKeyAssocs).toHaveLength(1);
    expect(planKeyAssocs[0][0].input.usagePlanId).toBe('plan-scan-base');
  });

  it('INSERT: INTELLIGENCE tier uses intelligence plans', async () => {
    await handler(makeStreamEvent('INSERT', 'coles', 'INTELLIGENCE'), {} as never, () => {});

    const apigwCalls = mockApigwSend.mock.calls as [{ __type: string; input: Record<string, unknown> }][];
    const planIds = apigwCalls
      .filter(([cmd]) => cmd.__type === 'CreateUsagePlanKeyCommand')
      .map(([cmd]) => cmd.input.usagePlanId);

    expect(planIds).toContain('plan-scan-intelligence');
    expect(planIds).toContain('plan-analytics-intelligence');
  });

  it('INSERT: clears apiKeyPlaintext and persists apigwKeyId in tenant record', async () => {
    await handler(makeStreamEvent('INSERT', 'woolworths', 'ENGAGEMENT'), {} as never, () => {});

    // deriveAndSaveCompliance writes first; provisionApiKey writes second.
    // Find the UpdateCommand that contains apigwKeyId (the provisioner's write).
    const ddbCalls = mockDdbSend.mock.calls as [{ __type: string; input: Record<string, unknown> }][];
    const provisionerWrite = ddbCalls.find(([cmd]) => {
      if (cmd.__type !== 'UpdateCommand') return false;
      const attrVals = cmd.input.ExpressionAttributeValues as Record<string, string>;
      try { return JSON.parse(attrVals[':d']).apigwKeyId !== undefined; }
      catch { return false; }
    });
    expect(provisionerWrite).toBeDefined();

    const savedDesc = JSON.parse(
      (provisionerWrite![0].input.ExpressionAttributeValues as Record<string, string>)[':d']
    );
    expect(savedDesc.apiKeyPlaintext).toBeUndefined();
    expect(savedDesc.apigwKeyId).toBe('key-id-001');
    expect(savedDesc.apigwKeyTier).toBe('ENGAGEMENT');
  });

  it('INSERT: skips API key creation when apiKeyPlaintext is absent', async () => {
    const event = makeStreamEvent('INSERT', 'noplaintext', 'ENGAGEMENT', { apiKeyPlaintext: undefined });
    await handler(event, {} as never, () => {});

    const apigwCalls = mockApigwSend.mock.calls as unknown[];
    expect(apigwCalls).toHaveLength(0);
  });

  it('MODIFY: same tier — skips re-provisioning (idempotent)', async () => {
    const event = makeStreamEvent('MODIFY', 'woolworths', 'ENGAGEMENT', {
      apigwKeyId: 'existing-key-id',
      apigwKeyTier: 'ENGAGEMENT',
      apiKeyPlaintext: 'bebo_TESTULIDKEY.abc123secret',
    });
    await handler(event, {} as never, () => {});

    const apigwCalls = mockApigwSend.mock.calls as unknown[];
    expect(apigwCalls).toHaveLength(0);
  });

  it('MODIFY: tier upgrade — deletes old key and provisions new one', async () => {
    const event = makeStreamEvent('MODIFY', 'woolworths', 'INTELLIGENCE', {
      apigwKeyId: 'old-key-id',
      apigwKeyTier: 'ENGAGEMENT',
      apiKeyPlaintext: 'bebo_TESTULIDKEY.abc123secret',
    });
    await handler(event, {} as never, () => {});

    const apigwCalls = mockApigwSend.mock.calls as [{ __type: string; input: Record<string, unknown> }][];

    const deleteCall = apigwCalls.find(([cmd]) => cmd.__type === 'DeleteApiKeyCommand');
    expect(deleteCall).toBeDefined();
    expect(deleteCall![0].input.apiKey).toBe('old-key-id');

    const createCall = apigwCalls.find(([cmd]) => cmd.__type === 'CreateApiKeyCommand');
    expect(createCall).toBeDefined();

    const planIds = apigwCalls
      .filter(([cmd]) => cmd.__type === 'CreateUsagePlanKeyCommand')
      .map(([cmd]) => cmd.input.usagePlanId);
    expect(planIds).toContain('plan-scan-intelligence');
    expect(planIds).toContain('plan-analytics-intelligence');
  });

  it('MODIFY: DeleteApiKeyCommand NotFoundException is swallowed gracefully', async () => {
    const notFoundError = Object.assign(new Error('Not found'), { name: 'NotFoundException' });
    mockApigwSend.mockImplementationOnce(() => Promise.reject(notFoundError));
    mockApigwSend.mockResolvedValue({ id: 'new-key-id' });

    const event = makeStreamEvent('MODIFY', 'woolworths', 'INTELLIGENCE', {
      apigwKeyId: 'stale-key-id',
      apigwKeyTier: 'BASE',
      apiKeyPlaintext: 'bebo_TESTULIDKEY.abc123secret',
    });
    await expect(handler(event, {} as never, () => {})).resolves.not.toThrow();

    const apigwCalls = mockApigwSend.mock.calls as [{ __type: string }][];
    const createCall = apigwCalls.find(([cmd]) => cmd.__type === 'CreateApiKeyCommand');
    expect(createCall).toBeDefined();
  });

  it('ignores records that are not TENANT# profile', async () => {
    const event = {
      Records: [
        {
          eventName: 'INSERT',
          dynamodb: {
            NewImage: {
              pK: 'BRAND#woolworths',
              sK: 'profile',
              tier: 'ENGAGEMENT',
              primaryCat: 'brand',
              desc: JSON.stringify({}),
            },
          },
        },
      ],
    } as Parameters<typeof handler>[0];

    await handler(event, {} as never, () => {});
    expect(mockApigwSend).not.toHaveBeenCalled();
    expect(mockDdbSend).not.toHaveBeenCalled();
  });
});
