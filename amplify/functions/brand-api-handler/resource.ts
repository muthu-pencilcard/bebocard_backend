import { defineFunction } from '@aws-amplify/backend';

export const brandApiHandlerFn = defineFunction({
  resourceGroupName: "data",
  name: 'bebo-brand-api',
  entry: './handler.ts',
  runtime: 20,
  timeoutSeconds: 30,
  memoryMB: 256,
});
