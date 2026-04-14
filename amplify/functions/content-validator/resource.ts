import { defineFunction } from '@aws-amplify/backend';

export const contentValidatorFn = defineFunction({
  resourceGroupName: "functions",
  name: 'bebo-content-validator',
  entry: './handler.ts',
  runtime: 20,
  timeoutSeconds: 30,
  memoryMB: 512,
  environment: {
    REFDATA_TABLE:         process.env.REFDATA_TABLE ?? '',
    ADMIN_TABLE:           process.env.ADMIN_TABLE   ?? '',
    APP_REFERENCE_BUCKET:  process.env.APP_REFERENCE_BUCKET ?? '',
    TENANT_UPLOADS_BUCKET: process.env.TENANT_UPLOADS_BUCKET ?? '',
  },
});
