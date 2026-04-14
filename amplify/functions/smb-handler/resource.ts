import { defineFunction } from '@aws-amplify/backend';

export const smbHandlerFn = defineFunction({
  resourceGroupName: "functions",
  name: 'bebo-smb-handler',
  entry: './handler.ts',
  timeoutSeconds: 30,
  environment: {
    ADMIN_TABLE: process.env.ADMIN_TABLE ?? '',
    USER_TABLE:  process.env.USER_TABLE  ?? '',
    REF_TABLE:   process.env.REF_TABLE   ?? '',
  },
});
