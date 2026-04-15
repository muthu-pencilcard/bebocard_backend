import { defineFunction } from '@aws-amplify/backend';

export const parentalConsentHandlerFn = defineFunction({
  resourceGroupName: "scanApi",
  name: 'parental-consent-handler',
});
