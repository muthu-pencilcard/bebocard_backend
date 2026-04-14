import { defineFunction } from '@aws-amplify/backend';

export const subscriptionNegotiator = defineFunction({
  resourceGroupName: "functions",
    name: 'subscription-negotiator',
    entry: './handler.ts',
    timeoutSeconds: 300,
    environment: {
        USER_TABLE: process.env.USER_TABLE ?? '',
        REF_TABLE: process.env.REF_TABLE ?? '',
        FIREBASE_SERVICE_ACCOUNT_JSON: process.env.FIREBASE_SERVICE_ACCOUNT_JSON ?? '',
    },
});
