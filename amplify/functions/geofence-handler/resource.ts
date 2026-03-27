import { defineFunction } from '@aws-amplify/backend';

export const geofenceHandlerFn = defineFunction({
  name: 'bebo-geofence-handler',
  entry: './handler.ts',
  resourceGroupName: 'data',
  // Needs more memory for Firebase Admin SDK initialisation
  memoryMB: 512,
  timeoutSeconds: 15,
  environment: {
    USER_TABLE:  process.env.USER_TABLE  ?? '',
    REF_TABLE:   process.env.REF_TABLE   ?? '',
    ADMIN_TABLE: process.env.ADMIN_TABLE ?? '', // needed to resolve secondaryULID → permULID
    // Set via: amplify secret set FIREBASE_SERVICE_ACCOUNT_JSON
    FIREBASE_SERVICE_ACCOUNT_JSON: process.env.FIREBASE_SERVICE_ACCOUNT_JSON ?? '',
  },
});
