import { defineFunction } from '@aws-amplify/backend';

export const webhookDispatcherFn = defineFunction({
  resourceGroupName: "functions",
  name: 'webhook-dispatcher',
  entry: './handler.ts',
  timeoutSeconds: 30,
});
