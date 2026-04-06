import { defineFunction } from '@aws-amplify/backend';

export const catalogSubscriptionSyncFn = defineFunction({
  name: 'catalog-subscription-sync',
  entry: './handler.ts',
  resourceGroupName: 'data',
  runtime: 20,
  timeoutSeconds: 300,
  memoryMB: 256,
});
