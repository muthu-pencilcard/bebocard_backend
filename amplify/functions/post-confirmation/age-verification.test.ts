import { vi, describe, it, expect, beforeEach } from 'vitest';

const { mockSend } = vi.hoisted(() => ({ mockSend: vi.fn() }));

vi.mock('@aws-sdk/client-dynamodb', () => ({ DynamoDBClient: vi.fn() }));
vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn().mockReturnValue({ send: mockSend }) },
  PutCommand: vi.fn(function (this: any, input: any) { Object.assign(this, { __type: 'PutCommand', input }); }),
}));
vi.mock('@aws-sdk/client-cognito-identity-provider', () => ({
  CognitoIdentityProviderClient: vi.fn(function (this: any) { this.send = mockSend; }),
  AdminUpdateUserAttributesCommand: vi.fn(function (this: any, input: any) { Object.assign(this, { __type: 'AdminUpdateUserAttributesCommand', input }); }),
}));
vi.mock('@aws-sdk/client-ssm', () => ({
  SSMClient: vi.fn(function (this: any) { this.send = mockSend; }),
  GetParameterCommand: vi.fn(function (this: any, input: any) { Object.assign(this, { __type: 'GetParameterCommand', input }); }),
}));
vi.mock('ulid', () => ({ monotonicFactory: () => () => 'TEST-ULID' }));

import { handler } from './handler.js';

describe('Post-Confirmation Age Verification (P2-6)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockImplementation(async (cmd) => {
      if (cmd.__type === 'GetParameterCommand') return { Parameter: { Value: 'test-table' } };
      return {};
    });
  });

  const makeEvent = (birthdate: string, parentEmail?: string) => ({
    userName: 'test-user',
    userPoolId: 'pool-123',
    request: {
      userAttributes: {
        birthdate,
        ...(parentEmail ? { 'custom:parentEmail': parentEmail } : {}),
      }
    },
    response: {},
  } as any);

  it('assigns <13 bucket and PENDING_CONSENT status for a child', async () => {
    const event = makeEvent('2020-01-01'); // 6 years old
    await handler(event, {} as any, () => {});

    // Check Cognito Attribute
    const cognitoCall = mockSend.mock.calls.find(c => c[0].__type === 'AdminUpdateUserAttributesCommand');
    const ageAttr = cognitoCall![0].input.UserAttributes.find((a: { Name: string }) => a.Name === 'custom:ageBucket');
    expect(ageAttr!.Value).toBe('<13');

    // Check DynamoDB Identity status
    const ddbCall = mockSend.mock.calls.find(c => c[0].__type === 'PutCommand' && c[0].input.Item.sK === 'IDENTITY');
    expect(ddbCall![0].input.Item.status).toBe('PENDING_CONSENT');
  });

  it('assigns 13-17 bucket and PENDING_CONSENT for a teen', async () => {
    const event = makeEvent('2010-01-01'); // ~16 years old
    await handler(event, {} as any, () => {});

    const ageAttr = mockSend.mock.calls.find(c => c[0].__type === 'AdminUpdateUserAttributesCommand')![0].input.UserAttributes.find((a: { Name: string }) => a.Name === 'custom:ageBucket');
    expect(ageAttr!.Value).toBe('13-17');

    const ddbCall = mockSend.mock.calls.find(c => c[0].__type === 'PutCommand' && c[0].input.Item.sK === 'IDENTITY');
    expect(ddbCall![0].input.Item.status).toBe('PENDING_CONSENT');
  });

  it('assigns 25-34 bucket and ACTIVE status for an adult', async () => {
    const event = makeEvent('1995-01-01'); // ~31 years old
    await handler(event, {} as any, () => {});

    const ageAttr = mockSend.mock.calls.find(c => c[0].__type === 'AdminUpdateUserAttributesCommand')![0].input.UserAttributes.find((a: { Name: string }) => a.Name === 'custom:ageBucket');
    expect(ageAttr!.Value).toBe('25-34');

    const ddbCall = mockSend.mock.calls.find(c => c[0].__type === 'PutCommand' && c[0].input.Item.sK === 'IDENTITY');
    expect(ddbCall![0].input.Item.status).toBe('ACTIVE');
  });
});
