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
  multifactor: {
    mode: 'OPTIONAL',
    totp: true,
  },
  userAttributes: {
    email: { required: true, mutable: true },
    name: { required: true, mutable: true },
    birthdate: { required: true, mutable: true },
    // custom:permULID is set by post-confirmation Lambda, not during sign-up.
    'custom:permULID': { dataType: 'String', mutable: false },
    'custom:ageBucket': { dataType: 'String', mutable: true },
    'custom:parentEmail': { dataType: 'String', mutable: true },
  },
  triggers: {
    postConfirmation: postConfirmationFn,
  },
});
