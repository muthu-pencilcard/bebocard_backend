import { defineFunction } from '@aws-amplify/backend';

export const consentHandlerFn = defineFunction({
  resourceGroupName: "data",
  name: 'bebo-consent-handler',
  entry: './handler.ts',
  timeoutSeconds: 30,
  environment: {
    ADMIN_TABLE: process.env.ADMIN_TABLE ?? '',
    USER_TABLE:  process.env.USER_TABLE ?? '',
    REF_TABLE:   process.env.REF_TABLE ?? '',
    CONSENT_TIMEOUT_QUEUE_URL: process.env.CONSENT_TIMEOUT_QUEUE_URL ?? '',
    // FIREBASE_SERVICE_ACCOUNT_JSON — set via: amplify secret set FIREBASE_SERVICE_ACCOUNT_JSON
  },
});
