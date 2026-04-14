import { defineFunction } from '@aws-amplify/backend';

export const templateManagerFn = defineFunction({
  resourceGroupName: "functions",
  name: 'bebo-template-manager',
  entry: './handler.ts',
  runtime: 20,
  timeoutSeconds: 30,
  memoryMB: 256,
});
