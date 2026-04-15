import { defineFunction } from '@aws-amplify/backend';

export const pointsExpiryForecastFn = defineFunction({
  resourceGroupName: "data",
  name: 'bebo-points-expiry-forecast',
  entry: './handler.ts',
  timeoutSeconds: 300,
  environment: {
    USER_TABLE: process.env.USER_TABLE ?? '',
    REFDATA_TABLE: process.env.REFDATA_TABLE ?? '',
  },
});
