import { defineFunction } from '@aws-amplify/backend';

export const discoveryHandlerFn = defineFunction({
  name: 'bebo-discovery',
  entry: './handler.ts',
  runtime: 20,
  timeoutSeconds: 30,
  memoryMB: 256,
});
