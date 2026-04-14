import { defineFunction } from '@aws-amplify/backend';

export const giftCardRouterFn = defineFunction({
  resourceGroupName: 'data',
  name: 'bebo-gift-card-router',
  entry: './handler.ts',
  timeoutSeconds: 30,
  environment: {
    ADMIN_TABLE: process.env.ADMIN_TABLE ?? '',
    USER_TABLE:  process.env.USER_TABLE ?? '',
    REF_TABLE:   process.env.REF_TABLE ?? '',
    // FIREBASE_SERVICE_ACCOUNT_JSON — set via: amplify secret set FIREBASE_SERVICE_ACCOUNT_JSON
  },
});
