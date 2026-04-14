import { defineFunction } from '@aws-amplify/backend';

export const receiptProcessorFn = defineFunction({
  resourceGroupName: 'data',
  name: 'receipt-processor',
  entry: './handler.ts',
  timeoutSeconds: 30, // Visibility timeout of queue should match or be greater
});
