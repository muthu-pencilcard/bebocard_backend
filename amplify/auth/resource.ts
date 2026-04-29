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
    // birthdate cannot be set required on an existing UserPool (standard attributes are immutable after creation).
    // Enforce age/birthdate at the app layer in post-confirmation Lambda instead.
    // custom:permULID is set by post-confirmation Lambda, not during sign-up.
    'custom:permULID': { dataType: 'String', mutable: false },
    'custom:ageBucket': { dataType: 'String', mutable: true },
    'custom:parentEmail': { dataType: 'String', mutable: true },
  },
  triggers: {
    postConfirmation: postConfirmationFn,
  },
});
