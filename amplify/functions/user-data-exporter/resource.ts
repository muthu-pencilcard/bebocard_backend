import { defineFunction } from '@aws-amplify/backend';

export const exporterFn = defineFunction({
  resourceGroupName: "data",
    name: 'user-data-exporter',
  entry: './handler.ts',
  timeoutSeconds: 300,
  memoryMB: 1024, // Needed for ZIP generation and JSON processing
});
