import { defineFunction } from '@aws-amplify/backend';

export const giftCardRefundFn = defineFunction({
    name: 'gift-card-refund',
    entry: './handler.ts',
    resourceGroupName: 'data',
    timeoutSeconds: 300, // 5 minutes for scanning/refunding
});
