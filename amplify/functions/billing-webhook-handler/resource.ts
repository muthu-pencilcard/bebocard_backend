import { defineFunction } from '@aws-amplify/backend';

export const billingWebhookHandlerFn = defineFunction({
  resourceGroupName: 'data',
  name: 'billing-webhook-handler',
  entry: './handler.ts',
  timeoutSeconds: 30,
});
