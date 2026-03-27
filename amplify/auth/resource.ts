import { defineAuth } from '@aws-amplify/backend';
import { postConfirmationFn } from '../functions/post-confirmation/resource';

export const auth = defineAuth({
  loginWith: {
    email: {
      verificationEmailStyle: 'CODE',
      verificationEmailSubject: 'Welcome to BeboCard!',
      verificationEmailBody: (createCode) =>
        `Your BeboCard verification code is ${createCode()}`,
    },
  },
  userAttributes: {
    email: { required: true, mutable: true },
    'custom:permULID': { dataType: 'String', mutable: false },
  },
  triggers: {
    postConfirmation: postConfirmationFn,
  },
});
