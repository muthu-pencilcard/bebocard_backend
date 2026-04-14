import { defineFunction } from '@aws-amplify/backend';

export const receiptClaimHandlerFn = defineFunction({
  resourceGroupName: "functions",
  name: 'receipt-claim-handler',
  entry: './handler.ts'
});
