import { defineFunction } from '@aws-amplify/backend';

export const tenantAnalyticsFn = defineFunction({
  resourceGroupName: "data",
  name: 'bebo-tenant-analytics',
  entry: './handler.ts',
  environment: {
    USER_TABLE:   process.env.USER_TABLE   ?? '',
    REFDATA_TABLE: process.env.REFDATA_TABLE ?? '',
    // MIN_COHORT_THRESHOLD — optional override, defaults to 50 in handler
  },
  // Phase 2 — tenant-authenticated pull API.
  // API Gateway usage plan (per tenant tier) enforces rate limiting
  // before this Lambda is invoked. Handler does not re-implement throttling.
});
