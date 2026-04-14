import { defineFunction } from '@aws-amplify/backend';

export const catalogSubscriptionSyncFn = defineFunction({
  resourceGroupName: 'data',
  name: 'catalog-subscription-sync',
  entry: './handler.ts',
  runtime: 20,
  timeoutSeconds: 300,
  memoryMB: 256,
});
