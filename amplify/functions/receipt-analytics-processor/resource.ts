import { defineFunction } from '@aws-amplify/backend';

export const receiptAnalyticsProcessorFn = defineFunction({
  name: 'receipt-analytics-processor',
  entry: './handler.ts',
  timeoutSeconds: 60,
  // SQS trigger and IAM grants are wired in backend.ts.
  // GLOBAL_ANALYTICS_SALT must be set via: amplify secret set GLOBAL_ANALYTICS_SALT
});
