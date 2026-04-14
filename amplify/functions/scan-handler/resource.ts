import { defineFunction } from '@aws-amplify/backend';

export const scanHandlerFn = defineFunction({
  resourceGroupName: "functions",
  name: 'bebo-scan-handler',
  entry: './handler.ts',
  environment: {
    ADMIN_TABLE: process.env.ADMIN_TABLE ?? '',
    USER_TABLE: process.env.USER_TABLE ?? '',
    REFDATA_TABLE: process.env.REFDATA_TABLE ?? '',
    // FIREBASE_SERVICE_ACCOUNT_JSON — set via: amplify secret set FIREBASE_SERVICE_ACCOUNT_JSON
  },
  // This is a public Lambda — no auth required from POS side
});
