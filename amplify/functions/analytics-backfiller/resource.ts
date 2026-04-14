import { defineFunction } from '@aws-amplify/backend';

export const analyticsBackfillerFn = defineFunction({
    name: 'bebo-analytics-backfiller',
  entry: './handler.ts',
  timeoutSeconds: 300, 
  environment: {
    ANALYTICS_BUCKET:  process.env.ANALYTICS_BUCKET  ?? '',
    ATHENA_WORKGROUP:  process.env.ATHENA_WORKGROUP   ?? '',
    GLUE_DATABASE:     process.env.GLUE_DATABASE      ?? '',
    USER_HASH_SALT:    process.env.USER_HASH_SALT     ?? '',
  },
});
