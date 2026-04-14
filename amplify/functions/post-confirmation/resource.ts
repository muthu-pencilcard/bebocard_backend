import { defineFunction } from '@aws-amplify/backend';

export const postConfirmationFn = defineFunction({
  resourceGroupName: "auth",
  name: 'bebo-post-confirmation',
  entry: './handler.ts',
});
