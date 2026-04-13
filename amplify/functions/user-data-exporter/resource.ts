import { defineFunction } from '@aws-amplify/backend';

export const exporterFn = defineFunction({
  name: 'user-data-exporter',
  entry: './handler.ts',
  resourceGroupName: 'data',
  timeoutSeconds: 300,
  memoryMB: 1024, // Needed for ZIP generation and JSON processing
});
