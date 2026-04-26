import type { PostConfirmationTriggerHandler } from 'aws-lambda';
import { CognitoIdentityProviderClient, AdminUpdateUserAttributesCommand } from '@aws-sdk/client-cognito-identity-provider';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { monotonicFactory } from 'ulid';
import { SESClient, SendEmailCommand } from '@aws-sdk/client-ses';
import * as crypto from 'crypto';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const cognito = new CognitoIdentityProviderClient({});
const ssm = new SSMClient({});
const ses = new SESClient({});
const ulid = monotonicFactory();

let userTableCache = '';
let adminTableCache = '';

async function getTableNames() {
  if (userTableCache && adminTableCache) {
    return { USER_TABLE: userTableCache, ADMIN_TABLE: adminTableCache };
  }

  const [userRes, adminRes] = await Promise.all([
    ssm.send(new GetParameterCommand({ Name: process.env.USER_TABLE_PARAM! })),
    ssm.send(new GetParameterCommand({ Name: process.env.ADMIN_TABLE_PARAM! })),
  ]);

  userTableCache = userRes.Parameter?.Value ?? '';
  adminTableCache = adminRes.Parameter?.Value ?? '';

  if (!userTableCache || !adminTableCache) {
    throw new Error('Failed to fetch table names from SSM parameters');
  }

  return { USER_TABLE: userTableCache, ADMIN_TABLE: adminTableCache };
}
let scanApiUrlCache = '';
async function getScanApiUrl() {
  if (scanApiUrlCache) return scanApiUrlCache;
  if (process.env.SCAN_API_URL_PARAM) {
    const res = await ssm.send(new GetParameterCommand({ Name: process.env.SCAN_API_URL_PARAM }));
    scanApiUrlCache = res.Parameter?.Value ?? 'https://api.bebocard.com';
  } else {
    scanApiUrlCache = process.env.SCAN_API_URL ?? 'https://api.bebocard.com';
  }
  return scanApiUrlCache;
}

export const handler: PostConfirmationTriggerHandler = async (event) => {
  const attrs = event.request.userAttributes;
  const userId = event.userName;

  // Gap 1 fix — Idempotency: Cognito may retry on network failure.
  // If custom:permULID is already present, the trigger already ran — return early.
  if (attrs['custom:permULID']) {
    console.log(`[post-confirmation] Already processed — user=${userId} permULID=${attrs['custom:permULID']}`);
    event.response = {};
    return event;
  }

  const permULID = ulid();
  const secondaryULID = ulid();
  const now = new Date().toISOString();
  const rotatesAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();

  console.log(`[post-confirmation] START user=${userId}`);

  const SCAN_API_URL = await getScanApiUrl();

  const birthdate = attrs['birthdate'] ?? '';
  const parentEmail = attrs['custom:parentEmail'] ?? '';
  const ageBucket = calculateAgeBucket(birthdate);

  const isMinor = ageBucket === '<13' || ageBucket === '13-17';

  // Gap 4 fix — UNKNOWN birthdate: require age verification before activation.
  // Gap 2 fix — Minor with no parentEmail: block sign-up entirely.
  //   Cognito surfaces this as a sign-up failure; the client should prompt for parentEmail.
  if (isMinor && !parentEmail) {
    throw new Error(`[post-confirmation] Minor account (${ageBucket}) requires parentEmail — sign-up blocked for user=${userId}`);
  }

  const status = isMinor
    ? 'PENDING_CONSENT'
    : ageBucket === 'UNKNOWN'
      ? 'PENDING_AGE_VERIFICATION'
      : 'ACTIVE';

  let USER_TABLE: string;
  let ADMIN_TABLE: string;
  try {
    ({ USER_TABLE, ADMIN_TABLE } = await getTableNames());
  } catch (err) {
    console.error('[post-confirmation] FAILED to get table names:', err);
    throw err;
  }

  event.response = {};

  // 1. Update Cognito: set permULID + ageBucket.
  //    This must happen before the DynamoDB writes so that a retry sees
  //    custom:permULID set and skips re-generation (idempotency guard above).
  await cognito.send(new AdminUpdateUserAttributesCommand({
    UserPoolId: event.userPoolId,
    Username: userId,
    UserAttributes: [
      { Name: 'custom:permULID', Value: permULID },
      { Name: 'custom:ageBucket', Value: ageBucket },
    ],
  }));

  // 2. Write IDENTITY record.
  //    ConditionExpression prevents a double-write if the Cognito update above
  //    succeeded on a prior attempt but the DynamoDB write did not.
  try {
    await dynamo.send(new PutCommand({
      TableName: USER_TABLE,
      ConditionExpression: 'attribute_not_exists(pK)',
      Item: {
        pK: `USER#${permULID}`,
        sK: 'IDENTITY',
        eventType: 'IDENTITY',
        status,
        primaryCat: 'identity',
        subCategory: 'wallet',
        secondaryULID,
        rotatesAt,
        owner: userId,
        desc: JSON.stringify({
          permULID,
          cognitoUserId: userId,
          birthdate,
          ageBucket,
          parentEmail,
          createdAt: now,
        }),
        createdAt: now,
        updatedAt: now,
      },
    }));
  } catch (err: any) {
    if (err.name === 'ConditionalCheckFailedException') {
      console.log(`[post-confirmation] IDENTITY already exists for permULID=${permULID} — skipping`);
    } else {
      throw err;
    }
  }

  // 3. If minor, send parental consent email.
  //    Gap 3 fix — consent secret failure is separated from SES failure:
  //    a missing SSM secret re-throws (blocks return), keeping the account
  //    in PENDING_CONSENT rather than silently succeeding with no email path.
  if (status === 'PENDING_CONSENT' && parentEmail) {
    const secret = await getConsentSecret();
    const token = generateConsentToken(permULID, secret);
    const approvalLink = `${SCAN_API_URL}/v1/consent/parental/approve?token=${token}`;

    try {
      await ses.send(new SendEmailCommand({
        Source: 'no-reply@bebocard.com',
        Destination: { ToAddresses: [parentEmail] },
        Message: {
          Subject: { Data: 'BeboCard: Parental Consent Required' },
          Body: {
            Html: {
              Data: `
                <div style="font-family: sans-serif; background: #0F172A; color: white; padding: 2rem; border-radius: 1rem;">
                  <h1 style="color: #2DD4BF;">Parental Consent Required</h1>
                  <p>A BeboCard account has been created for your child.</p>
                  <p>As they are under 18, we require your approval before their account can be activated.</p>
                  <div style="margin: 2rem 0;">
                    <a href="${approvalLink}" style="background: #2DD4BF; color: #0F172A; padding: 1rem 2rem; border-radius: 0.5rem; text-decoration: none; font-weight: bold; display: inline-block;">Approve Account</a>
                  </div>
                  <p style="color: #94A3B8; font-size: 0.8rem;">Link expires in 7 days.</p>
                </div>
              `
            }
          }
        }
      }));
      console.log(`[post-confirmation] Consent email sent to ${parentEmail}`);
    } catch (err) {
      // SES transient failure is non-fatal: account stays PENDING_CONSENT and support
      // can re-trigger. Consent-secret failure is already separated out above.
      console.error('[post-confirmation] FAILED to send consent email (SES):', err);
    }
  }

  // 4. Pre-create scan index.
  //    ConditionExpression prevents duplicate SCAN# entries on retry.
  try {
    await dynamo.send(new PutCommand({
      TableName: ADMIN_TABLE,
      ConditionExpression: 'attribute_not_exists(pK)',
      Item: {
        pK: `SCAN#${secondaryULID}`,
        sK: permULID,
        eventType: 'SCAN_INDEX',
        status: 'ACTIVE',
        owner: userId,
        desc: JSON.stringify({ cards: [], createdAt: now }),
        createdAt: now,
        updatedAt: now,
      },
    }));
  } catch (err: any) {
    if (err.name === 'ConditionalCheckFailedException') {
      console.log(`[post-confirmation] SCAN# already exists for secondaryULID=${secondaryULID} — skipping`);
    } else {
      throw err;
    }
  }

  return event;
};

function calculateAgeBucket(birthdate: string): string {
  if (!birthdate) return 'UNKNOWN';
  const bday = new Date(birthdate);
  if (isNaN(bday.getTime())) return 'UNKNOWN';

  const now = new Date();
  let age = now.getFullYear() - bday.getFullYear();
  const m = now.getMonth() - bday.getMonth();
  if (m < 0 || (m === 0 && now.getDate() < bday.getDate())) age--;

  if (age < 13) return '<13';
  if (age < 18) return '13-17';
  if (age < 25) return '18-24';
  if (age < 35) return '25-34';
  if (age < 45) return '35-44';
  return '45+';
}

async function getConsentSecret(): Promise<string> {
  const res = await ssm.send(new GetParameterCommand({
    Name: '/amplify/shared/PARENTAL_CONSENT_SECRET',
    WithDecryption: true
  }));
  const secret = res.Parameter?.Value;
  if (!secret) throw new Error('PARENTAL_CONSENT_SECRET not found in SSM — cannot generate consent token');
  return secret;
}

function generateConsentToken(permULID: string, secret: string): string {
  const payload = {
    permULID,
    exp: Date.now() + 7 * 24 * 60 * 60 * 1000,
  };
  const payloadBase64 = Buffer.from(JSON.stringify(payload)).toString('base64');
  const signature = crypto.createHmac('sha256', secret).update(payloadBase64).digest('hex');
  return `${payloadBase64}.${signature}`;
}
