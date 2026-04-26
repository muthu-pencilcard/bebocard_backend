import { defineFunction, secret } from '@aws-amplify/backend';

export const receiptAnalyticsProcessorFn = defineFunction({
  name: 'receipt-analytics-processor',
  entry: './handler.ts',
  timeoutSeconds: 60,
  environment: {
    GLOBAL_ANALYTICS_SALT: secret('GLOBAL_ANALYTICS_SALT'),
  }
  // SQS trigger and IAM grants are wired in backend.ts.
});
