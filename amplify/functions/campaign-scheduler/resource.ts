import { defineFunction } from '@aws-amplify/backend';

export const campaignSchedulerFn = defineFunction({
  resourceGroupName: 'data',
  name: 'campaign-scheduler',
  entry: './handler.ts',
  timeoutSeconds: 300, // 5 minutes for batch processing
  memoryMB: 512,
});
