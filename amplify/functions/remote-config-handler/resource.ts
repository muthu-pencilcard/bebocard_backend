import { defineFunction } from '@aws-amplify/backend';

export const remoteConfigHandlerFn = defineFunction({
  resourceGroupName: "functions",
  name: 'remote-config-handler',
  entry: './handler.ts',
  timeoutSeconds: 5,
});
