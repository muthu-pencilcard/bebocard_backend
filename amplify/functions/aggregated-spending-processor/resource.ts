import { defineFunction } from '@aws-amplify/backend';

export const aggregatedSpendingProcessorFn = defineFunction({
  resourceGroupName: "data",
  name: 'bebo-aggregated-spending-processor',
  entry: './handler.ts',
  timeoutSeconds: 300,
  environment: {
    USER_TABLE: process.env.USER_TABLE ?? '',
  },
});
