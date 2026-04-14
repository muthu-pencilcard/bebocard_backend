import { defineFunction } from '@aws-amplify/backend';

export const parentalConsentHandlerFn = defineFunction({
  resourceGroupName: "functions",
  name: 'parental-consent-handler',
});
