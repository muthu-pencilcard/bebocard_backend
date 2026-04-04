import { defineFunction } from '@aws-amplify/backend';

export const subscriptionProxyFn = defineFunction({
  name: 'bebo-subscription-proxy',
  entry: './handler.ts',
  resourceGroupName: 'data',
  timeoutSeconds: 30,
  environment: {
    ADMIN_TABLE: process.env.ADMIN_TABLE ?? '',
    USER_TABLE:  process.env.USER_TABLE ?? '',
    REF_TABLE:   process.env.REF_TABLE ?? '',
    // FIREBASE_SERVICE_ACCOUNT_JSON — set via: amplify secret set FIREBASE_SERVICE_ACCOUNT_JSON
  },
});
