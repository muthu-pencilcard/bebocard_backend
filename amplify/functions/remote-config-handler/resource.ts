import { defineFunction } from '@aws-amplify/backend';

export const remoteConfigHandlerFn = defineFunction({
  resourceGroupName: 'data',
  name: 'remote-config-handler',
  entry: './handler.ts',
  timeoutSeconds: 5,
});
