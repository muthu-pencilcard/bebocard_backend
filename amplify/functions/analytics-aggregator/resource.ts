import { defineFunction } from '@aws-amplify/backend';

export const analyticsAggregatorFn = defineFunction({
  name: 'analytics-aggregator',
  entry: './handler.ts',
  timeoutSeconds: 300,
  memoryMB: 512,
});
