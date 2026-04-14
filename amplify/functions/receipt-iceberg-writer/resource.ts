import { defineFunction } from '@aws-amplify/backend';

export const receiptIcebergWriterFn = defineFunction({
  name: 'bebo-receipt-iceberg-writer',
  entry: './handler.ts',
  timeoutSeconds: 60,
  environment: {
    ANALYTICS_BUCKET:  process.env.ANALYTICS_BUCKET  ?? '',
    ATHENA_WORKGROUP:  process.env.ATHENA_WORKGROUP   ?? '',
    GLUE_DATABASE:     process.env.GLUE_DATABASE      ?? '',
    USER_HASH_SALT:    process.env.USER_HASH_SALT     ?? '',
  },
  // DynamoDB Streams trigger and IAM grants are wired in backend.ts.
  // USER_HASH_SALT must be set via: amplify secret set USER_HASH_SALT
});
