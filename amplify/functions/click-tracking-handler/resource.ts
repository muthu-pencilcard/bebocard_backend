import { defineFunction } from '@aws-amplify/backend';

export const clickTrackingHandlerFn = defineFunction({
  name: 'click-tracking-handler',
  entry: './handler.ts',
  resourceGroupName: 'data',
  timeoutSeconds: 5,
  memoryMB: 256,
});
