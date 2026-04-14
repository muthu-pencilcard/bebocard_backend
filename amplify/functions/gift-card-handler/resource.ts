import { defineFunction } from '@aws-amplify/backend';

export const giftCardHandlerFn = defineFunction({
  resourceGroupName: 'data',
  name: 'bebo-gift-card',
  entry: './handler.ts',
  runtime: 20,
  timeoutSeconds: 300,
  memoryMB: 512,
});
