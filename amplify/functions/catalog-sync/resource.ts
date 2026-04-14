import { defineFunction } from '@aws-amplify/backend';

export const catalogSyncFn = defineFunction({
  resourceGroupName: "data",
  name: 'bebo-catalog-sync',
  entry: './handler.ts',
  runtime: 20,
  timeoutSeconds: 900,
  memoryMB: 512,
});
