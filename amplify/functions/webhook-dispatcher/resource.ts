import { defineFunction } from '@aws-amplify/backend';

export const webhookDispatcherFn = defineFunction({
    name: 'webhook-dispatcher',
  entry: './handler.ts',
  timeoutSeconds: 30,
});
