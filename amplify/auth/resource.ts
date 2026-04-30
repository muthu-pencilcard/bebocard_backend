import { defineAuth, secret } from '@aws-amplify/backend';
import { postConfirmationFn } from '../functions/post-confirmation/resource';

export const auth = defineAuth({
  loginWith: {
    email: {
      verificationEmailStyle: 'CODE',
      verificationEmailSubject: 'Welcome to BeboCard!',
      verificationEmailBody: (createCode) =>
        `Your BeboCard verification code is ${createCode()}`,
    },
    externalProviders: {
      google: {
        clientId: secret('GOOGLE_CLIENT_ID'),
        clientSecret: secret('GOOGLE_CLIENT_SECRET'),
        scopes: ['email', 'profile'],
        attributeMapping: {
          email: 'email',
          fullname: 'name',
        },
      },
      signInWithApple: {
        clientId: secret('SIWA_CLIENT_ID'),
        keyId: secret('SIWA_KEY_ID'),
        privateKey: secret('SIWA_PRIVATE_KEY'),
        teamId: secret('SIWA_TEAM_ID'),
        scopes: ['email', 'name'],
        attributeMapping: {
          email: 'email',
          fullname: 'name',
        },
      },
      callbackUrls: [
        'bebocard://callback/',
        'https://bebocard.com.au/callback/',
      ],
      logoutUrls: [
        'bebocard://signout/',
        'https://bebocard.com.au/signout/',
      ],
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
