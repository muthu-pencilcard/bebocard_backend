import { defineFunction } from '@aws-amplify/backend';

export const cognitoExportFn = defineFunction({
  resourceGroupName: "functions",
  name: 'bebo-cognito-export',
  entry: './handler.ts',
  timeoutSeconds: 300, // 5 min — large pools may have many users
  environment: {
    USER_POOL_ID: process.env.USER_POOL_ID ?? '',
    EXPORT_BUCKET: process.env.COGNITO_EXPORT_BUCKET ?? '',
  },
  // EventBridge weekly cron wired in backend.ts:
  //   every Sunday at 02:00 UTC (after analytics compaction, low-traffic window)
});
