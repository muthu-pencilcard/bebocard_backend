import { defineFunction } from '@aws-amplify/backend';

export const receiptClaimHandlerFn = defineFunction({
  resourceGroupName: "data",
  name: 'receipt-claim-handler',
  entry: './handler.ts'
});
