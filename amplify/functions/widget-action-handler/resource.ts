import { defineFunction } from '@aws-amplify/backend';

export const widgetActionHandlerFn = defineFunction({
  resourceGroupName: "functions",
  name: 'bebo-widget-action-handler',
  entry: './handler.ts',
  timeoutSeconds: 30,
  environment: {
    USER_TABLE: process.env.USER_TABLE ?? '',
    REFDATA_TABLE: process.env.REFDATA_TABLE ?? '',
    ADMIN_TABLE: process.env.ADMIN_TABLE ?? '',
    COGNITO_REGION: process.env.COGNITO_REGION ?? '',
    COGNITO_USER_POOL_ID: process.env.COGNITO_USER_POOL_ID ?? '',
    WIDGET_TOKEN_SECRET: process.env.WIDGET_TOKEN_SECRET ?? '',
  },
});
