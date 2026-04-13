import { defineFunction } from '@aws-amplify/backend';

export const affiliateFeedSyncFn = defineFunction({
  name: 'affiliate-feed-sync',
  entry: './handler.ts',
  timeoutSeconds: 120, // Fetching external APIs can be slow
  memoryMB: 512,
});
