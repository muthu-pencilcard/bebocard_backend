import { defineFunction } from '@aws-amplify/backend';

export const giftCardRefundFn = defineFunction({
  resourceGroupName: "functions",
    name: 'gift-card-refund',
    entry: './handler.ts',
    timeoutSeconds: 300, // 5 minutes for scanning/refunding
});
