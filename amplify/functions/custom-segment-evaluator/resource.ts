import { defineFunction } from '@aws-amplify/backend';

export const customSegmentEvaluatorFn = defineFunction({
  resourceGroupName: 'data',
  name: 'bebo-custom-segment-evaluator',
  entry: './handler.ts',
  timeoutSeconds: 900, // 15 minutes — may evaluate large subscriber cohorts
  environment: {
    USER_TABLE: process.env.USER_TABLE ?? '',
    REFDATA_TABLE: process.env.REFDATA_TABLE ?? '',
  },
  // EventBridge cron trigger wired in backend.ts:
  //   nightly at 00:30 UTC (after segment-processor stream catches up, before compaction at 02:00)
});
