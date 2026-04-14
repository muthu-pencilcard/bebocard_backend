import { defineFunction } from '@aws-amplify/backend';

export const paymentRouterFn = defineFunction({
  resourceGroupName: "functions",
  name: 'bebo-payment-router',
  entry: './handler.ts',
  timeoutSeconds: 30,
  environment: {
    ADMIN_TABLE: process.env.ADMIN_TABLE ?? '',
    USER_TABLE: process.env.USER_TABLE ?? '',
    REF_TABLE: process.env.REF_TABLE ?? '',
    TIMEOUT_QUEUE_URL: process.env.TIMEOUT_QUEUE_URL ?? '',
    // FIREBASE_SERVICE_ACCOUNT_JSON — set via: amplify secret set FIREBASE_SERVICE_ACCOUNT_JSON
  },
});
