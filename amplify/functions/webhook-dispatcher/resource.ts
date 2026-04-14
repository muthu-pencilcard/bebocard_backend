import { defineFunction } from '@aws-amplify/backend';

export const webhookDispatcherFn = defineFunction({
  resourceGroupName: "data",
  name: 'webhook-dispatcher',
  entry: './handler.ts',
  timeoutSeconds: 30,
});
