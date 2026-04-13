import { defineFunction } from '@aws-amplify/backend';

export const brandHealthMonitorFn = defineFunction({
  name: 'bebo-brand-health-monitor',
  entry: './handler.ts',
  timeoutSeconds: 300,
  environment: {
    REFDATA_TABLE: process.env.REFDATA_TABLE ?? '',
    USER_TABLE: process.env.USER_TABLE ?? '',
    CSM_SNS_TOPIC_ARN: process.env.CSM_SNS_TOPIC_ARN ?? '',
  },
  // EventBridge weekly cron wired in backend.ts:
  //   every Monday at 08:00 UTC
});
