import { defineFunction } from '@aws-amplify/backend';

export const postConfirmationFn = defineFunction({
  name: 'bebo-post-confirmation',
  entry: './handler.ts',
});
