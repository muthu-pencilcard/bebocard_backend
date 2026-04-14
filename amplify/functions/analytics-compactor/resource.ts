import { defineFunction } from '@aws-amplify/backend';

export const analyticsCompactorFn = defineFunction({
  resourceGroupName: "data",
  name: 'analytics-compactor',
  entry: './handler.ts',
  timeoutSeconds: 900, // Compaction is long-running
  memoryMB: 1024,
});
