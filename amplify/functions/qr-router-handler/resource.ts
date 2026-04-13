import { defineFunction } from '@aws-amplify/backend';

export const qrRouterHandlerFn = defineFunction({
  name: 'qr-router-handler',
  entry: './handler.ts',
  timeoutSeconds: 10,
  environment: {
    REFDATA_TABLE: process.env.REFDATA_TABLE || '',
    USER_TABLE: process.env.USER_TABLE || '',
  }
});
