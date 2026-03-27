import { defineFunction } from '@aws-amplify/backend';

export const reminderHandlerFn = defineFunction({
  name: 'bebo-reminder',
  entry: './handler.ts',
  resourceGroupName: 'data',
  runtime: 20,
  timeoutSeconds: 300,
  memoryMB: 512,
});
