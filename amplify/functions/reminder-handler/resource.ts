import { defineFunction } from '@aws-amplify/backend';

export const reminderHandlerFn = defineFunction({
  name: 'bebo-reminder',
  entry: './handler.ts',
  runtime: 20,
  timeoutSeconds: 300,
  memoryMB: 512,
});
