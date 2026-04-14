import { defineFunction } from '@aws-amplify/backend';

export const billingRunHandlerFn = defineFunction({
  resourceGroupName: "functions",
  name: 'bebo-billing-run',
  entry: './handler.ts',
  runtime: 20,
  timeoutSeconds: 300,
  memoryMB: 512,
});
