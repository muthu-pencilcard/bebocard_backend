import { defineFunction } from '@aws-amplify/backend';

export const templateManagerFn = defineFunction({
  name: 'bebo-template-manager',
  entry: './handler.ts',
  runtime: 20,
  timeoutSeconds: 30,
  memoryMB: 256,
});
