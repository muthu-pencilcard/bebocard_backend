import { defineFunction } from '@aws-amplify/backend';

export const parentalConsentHandlerFn = defineFunction({
  resourceGroupName: "data",
  name: 'parental-consent-handler',
});
