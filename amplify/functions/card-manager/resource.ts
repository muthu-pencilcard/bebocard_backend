import { defineFunction } from '@aws-amplify/backend';

export const cardManagerFn = defineFunction({
  resourceGroupName: 'data',
  name: 'bebo-card-manager',
  entry: './handler.ts',
  environment: {
    USER_TABLE: process.env.USER_TABLE ?? '',
    REFDATA_TABLE: process.env.REFDATA_TABLE ?? '',
    ADMIN_TABLE: process.env.ADMIN_TABLE ?? '',
  },
});
