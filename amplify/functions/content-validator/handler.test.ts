import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { S3Event } from 'aws-lambda';

// ── Hoisted mock functions ────────────────────────────────────────────────────

const { mockS3Send, mockRekSend, mockDdbSend } = vi.hoisted(() => ({
  mockS3Send: vi.fn(),
  mockRekSend: vi.fn(),
  mockDdbSend: vi.fn(),
}));

// ── Mocks ─────────────────────────────────────────────────────────────────────

vi.mock('@aws-sdk/client-s3', () => ({
  S3Client: vi.fn(function (this: Record<string, unknown>) { this.send = mockS3Send; }),
  HeadObjectCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'HeadObjectCommand', input });
  }),
  GetObjectCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'GetObjectCommand', input });
  }),
  CopyObjectCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'CopyObjectCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-rekognition', () => ({
  RekognitionClient: vi.fn(function (this: Record<string, unknown>) { this.send = mockRekSend; }),
  DetectModerationLabelsCommand: vi.fn(function (this: Record<string, unknown>, input: unknown) {
    Object.assign(this, { __type: 'DetectModerationLabelsCommand', input });
  }),
}));

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(function (this: Record<string, unknown>) { }),
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

// ── Set env vars before importing handler (top-level consts are captured at import time) ──

process.env.TENANT_UPLOADS_BUCKET = 'staging-bucket';
process.env.APP_REFERENCE_BUCKET = 'ref-bucket';
process.env.REFDATA_TABLE = 'refdata-table';
process.env.ADMIN_TABLE = 'admin-table';

// ── Import handler AFTER all mocks ────────────────────────────────────────────

const { handler } = await import('./handler.js');

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeS3Event(key: string): S3Event {
  // Encode slashes leniently — key path uses / so don't re-encode them
  const encodedKey = key.split('/').map(encodeURIComponent).join('/');
  return {
    Records: [{
      eventVersion: '2.1',
      eventSource: 'aws:s3',
      awsRegion: 'ap-southeast-2',
      eventTime: new Date().toISOString(),
      eventName: 'ObjectCreated:Put',
      userIdentity: { principalId: 'test' },
      requestParameters: { sourceIPAddress: '1.2.3.4' },
      responseElements: { 'x-amz-request-id': 'req', 'x-amz-id-2': 'id2' },
      s3: {
        s3SchemaVersion: '1.0',
        configurationId: 'test',
        bucket: { name: 'staging-bucket', ownerIdentity: { principalId: 'owner' }, arn: 'arn:aws:s3:::staging-bucket' },
        object: { key: encodedKey, size: 100_000, eTag: 'etag', sequencer: '0' },
      },
    }],
  } as unknown as S3Event;
}

function setupPassMocks() {
  mockS3Send.mockImplementation((cmd: { __type: string }) => {
    if (cmd.__type === 'HeadObjectCommand') {
      return Promise.resolve({ ContentLength: 100_000, ContentType: 'image/jpeg' });
    }
    // CopyObjectCommand
    return Promise.resolve({});
  });
  mockRekSend.mockResolvedValue({ ModerationLabels: [] });
  mockDdbSend.mockResolvedValue({});
}

// ── Setup ─────────────────────────────────────────────────────────────────────

beforeEach(() => {
  vi.clearAllMocks();
});

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('content-validator — key format guards', () => {
  it('skips key with unexpected format (no brands prefix)', async () => {
    await handler(makeS3Event('uploads/woolworths/logo/image.jpg'));
    expect(mockS3Send).not.toHaveBeenCalled();
  });

  it('skips key with fewer than 4 parts', async () => {
    await handler(makeS3Event('brands/woolworths/logo'));
    expect(mockS3Send).not.toHaveBeenCalled();
  });
});

describe('content-validator — rejection cases', () => {
  it('rejects unknown contentType (video)', async () => {
    // Unknown type triggers reject path — CopyObjectCommand to rejected/ prefix
    mockS3Send.mockResolvedValue({});
    mockDdbSend.mockResolvedValue({});
    const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
    await handler(makeS3Event('brands/woolworths/video/file.mp4'));
    const copyCalls = (CopyObjectCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
    const rejectedCopy = copyCalls.find(
      (args) => (args[0] as { Key: string }).Key?.includes('rejected'),
    );
    expect(rejectedCopy).toBeDefined();
  });

  it('rejects if MIME type is not allowed (image/gif)', async () => {
    mockS3Send.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'HeadObjectCommand') {
        return Promise.resolve({ ContentLength: 100_000, ContentType: 'image/gif' });
      }
      return Promise.resolve({});
    });
    mockDdbSend.mockResolvedValue({});
    const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
    await handler(makeS3Event('brands/woolworths/logo/banner.gif'));
    const copyCalls = (CopyObjectCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
    const rejectedCopy = copyCalls.find(
      (args) => (args[0] as { Key: string }).Key?.includes('rejected'),
    );
    expect(rejectedCopy).toBeDefined();
  });

  it('rejects if file size > 5 MB', async () => {
    const oversizeBytes = 5 * 1024 * 1024 + 1;
    mockS3Send.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'HeadObjectCommand') {
        return Promise.resolve({ ContentLength: oversizeBytes, ContentType: 'image/jpeg' });
      }
      return Promise.resolve({});
    });
    mockDdbSend.mockResolvedValue({});
    const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
    await handler(makeS3Event('brands/woolworths/logo/big.jpg'));
    const copyCalls = (CopyObjectCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
    const rejectedCopy = copyCalls.find(
      (args) => (args[0] as { Key: string }).Key?.includes('rejected'),
    );
    expect(rejectedCopy).toBeDefined();
  });

  it('rejects if Rekognition returns a label with confidence > 75', async () => {
    mockS3Send.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'HeadObjectCommand') {
        return Promise.resolve({ ContentLength: 100_000, ContentType: 'image/jpeg' });
      }
      return Promise.resolve({});
    });
    mockRekSend.mockResolvedValue({ ModerationLabels: [{ Name: 'Nudity', Confidence: 90 }] });
    mockDdbSend.mockResolvedValue({});
    const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
    await handler(makeS3Event('brands/woolworths/logo/bad.jpg'));
    const copyCalls = (CopyObjectCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
    const rejectedCopy = copyCalls.find(
      (args) => (args[0] as { Key: string }).Key?.includes('rejected'),
    );
    expect(rejectedCopy).toBeDefined();
  });

  it('on reject: CopyObjectCommand goes to rejected/ prefix in STAGING_BUCKET', async () => {
    mockS3Send.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'HeadObjectCommand') {
        return Promise.resolve({ ContentLength: 100_000, ContentType: 'image/gif' });
      }
      return Promise.resolve({});
    });
    mockDdbSend.mockResolvedValue({});
    const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
    await handler(makeS3Event('brands/woolworths/logo/test.gif'));
    const copyCalls = (CopyObjectCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
    const rejectedCopy = copyCalls.find(
      (args) => (args[0] as { Bucket: string; Key: string }).Key?.includes('rejected'),
    );
    expect(rejectedCopy).toBeDefined();
    expect((rejectedCopy![0] as { Bucket: string }).Bucket).toBe('staging-bucket');
    expect((rejectedCopy![0] as { Key: string }).Key).toMatch(/^brands\/woolworths\/rejected\//);
  });
});

describe('content-validator — pass cases', () => {
  it('passes and copies to REF_BUCKET for a valid jpeg with no moderation hits', async () => {
    setupPassMocks();
    const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
    await handler(makeS3Event('brands/woolworths/logo/logo.jpg'));
    const copyCalls = (CopyObjectCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
    const passCopy = copyCalls.find(
      (args) => (args[0] as { Bucket: string }).Bucket === 'ref-bucket',
    );
    expect(passCopy).toBeDefined();
  });

  it('passes for all three allowed MIME types (jpeg, png, webp)', async () => {
    for (const [ext, mime] of [['jpg', 'image/jpeg'], ['png', 'image/png'], ['webp', 'image/webp']]) {
      vi.clearAllMocks();
      mockS3Send.mockImplementation((cmd: { __type: string }) => {
        if (cmd.__type === 'HeadObjectCommand') {
          return Promise.resolve({ ContentLength: 50_000, ContentType: mime });
        }
        return Promise.resolve({});
      });
      mockRekSend.mockResolvedValue({ ModerationLabels: [] });
      mockDdbSend.mockResolvedValue({});

      const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
      await handler(makeS3Event(`brands/woolworths/banner/image.${ext}`));
      const copyCalls = (CopyObjectCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
      const passCopy = copyCalls.find(
        (args) => (args[0] as { Bucket: string }).Bucket === 'ref-bucket',
      );
      expect(passCopy, `Expected pass copy for ${mime}`).toBeDefined();
    }
  });

  it('on pass: CopyObjectCommand goes to REF_BUCKET with correct Key', async () => {
    setupPassMocks();
    const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
    await handler(makeS3Event('brands/woolworths/logo/logo.jpg'));
    const copyCalls = (CopyObjectCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
    const passCopy = copyCalls.find(
      (args) => (args[0] as { Bucket: string }).Bucket === 'ref-bucket',
    );
    expect((passCopy![0] as { Key: string }).Key).toBe('brands/woolworths/logo/logo.jpg');
  });

  it('on pass: UpdateCommand to REFDATA_TABLE has correct brandId in key', async () => {
    setupPassMocks();
    const { UpdateCommand } = await import('@aws-sdk/lib-dynamodb');
    await handler(makeS3Event('brands/woolworths/logo/logo.jpg'));
    const updateCalls = (UpdateCommand as unknown as ReturnType<typeof vi.fn>).mock.calls;
    expect(updateCalls.length).toBeGreaterThan(0);
    const updateInput = updateCalls[0][0] as { Key: { pK: string } };
    expect(updateInput.Key.pK).toBe('BRAND#woolworths');
  });

  it('passes catalogue PDF uploads without Rekognition moderation', async () => {
    mockS3Send.mockImplementation((cmd: { __type: string }) => {
      if (cmd.__type === 'HeadObjectCommand') {
        return Promise.resolve({ ContentLength: 500_000, ContentType: 'application/pdf' });
      }
      return Promise.resolve({});
    });
    mockDdbSend.mockResolvedValue({});

    await handler(makeS3Event('brands/woolworths/catalogue/catalogue.pdf'));

    expect(mockRekSend).not.toHaveBeenCalled();
  });

  it('passes widget_branding image uploads and writes the matching profile field', async () => {
    setupPassMocks();
    const { UpdateCommand } = await import('@aws-sdk/lib-dynamodb');
    await handler(makeS3Event('brands/woolworths/widget_branding/widget.png'));
    const updateCalls = (UpdateCommand as unknown as ReturnType<typeof vi.fn>).mock.calls;
    const updateInput = updateCalls[0][0] as { ExpressionAttributeNames: Record<string, string> };
    expect(updateInput.ExpressionAttributeNames['#cdnField']).toBe('widgetBrandingUrl');
  });
});

describe('content-validator — S3 head & error handling', () => {
  it('HeadObjectCommand uses correct Bucket (TENANT_UPLOADS_BUCKET)', async () => {
    setupPassMocks();
    const { HeadObjectCommand } = await import('@aws-sdk/client-s3');
    await handler(makeS3Event('brands/woolworths/logo/logo.jpg'));
    const headCalls = (HeadObjectCommand as unknown as ReturnType<typeof vi.fn>).mock.calls;
    expect(headCalls.length).toBeGreaterThan(0);
    expect(headCalls[0][0].Bucket).toBe('staging-bucket');
  });

  it('handles HeadObjectCommand throwing without crashing', async () => {
    mockS3Send.mockRejectedValue(new Error('S3 unavailable'));
    // Should not throw — handler logs and returns
    await expect(handler(makeS3Event('brands/woolworths/logo/logo.jpg'))).resolves.toBeUndefined();
  });

  it('processes multiple S3 records in one invocation', async () => {
    setupPassMocks();
    const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
    const multiEvent: S3Event = {
      Records: [
        ...makeS3Event('brands/woolworths/logo/logo.jpg').Records,
        ...makeS3Event('brands/bigw/banner/banner.jpg').Records,
      ],
    } as unknown as S3Event;
    await handler(multiEvent);
    const copyCalls = (CopyObjectCommand as unknown as ReturnType<typeof vi.fn>).mock.calls as unknown[][];
    const passCopies = copyCalls.filter(
      (args) => (args[0] as { Bucket: string }).Bucket === 'ref-bucket',
    );
    expect(passCopies.length).toBe(2);
  });
});
