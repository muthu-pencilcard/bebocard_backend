import { defineFunction } from '@aws-amplify/backend';

export const giftCardHandlerFn = defineFunction({
  name: 'bebo-gift-card',
  entry: './handler.ts',
  resourceGroupName: 'data',
  runtime: 20,
  timeoutSeconds: 300,
  memoryMB: 512,
});
