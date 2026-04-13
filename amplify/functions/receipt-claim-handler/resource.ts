import { defineFunction } from '@aws-amplify/backend';

export const receiptClaimHandlerFn = defineFunction({
  name: 'receipt-claim-handler',
  entry: './handler.ts'
});
