import { defineFunction } from '@aws-amplify/backend';

export const tenantLinker = defineFunction({
  name: 'tenant-linker',
  entry: './handler.ts',
  resourceGroupName: 'data',
  environment: {
    // API base — update with your actual API Gateway URL after deploy
    API_BASE_URL: 'https://api.bebocard.app',
    APP_SUCCESS_URL: 'https://bebocard.app/link-success',
    APP_FAILURE_URL: 'https://bebocard.app/link-failed',
    OAUTH_STATE_TTL_SECONDS: '600',
    // Brand OAuth credentials — set via `amplify secret set` or SSM
    // WOOLWORTHS_CLIENT_ID, WOOLWORTHS_CLIENT_SECRET
    // FLYBUYS_CLIENT_ID, FLYBUYS_CLIENT_SECRET
    // VELOCITY_CLIENT_ID, VELOCITY_CLIENT_SECRET
    // QANTAS_CLIENT_ID, QANTAS_CLIENT_SECRET
  },
  runtime: 20,
  timeoutSeconds: 30,
});
