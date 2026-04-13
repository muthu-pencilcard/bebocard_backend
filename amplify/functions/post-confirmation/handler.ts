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

export const handler: PostConfirmationTriggerHandler = async (event) => {
  const permULID = ulid();
  const secondaryULID = ulid();
  const now = new Date().toISOString();
  const rotatesAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
  const userId = event.userName;
  const attrs = event.request.userAttributes;

  console.log(`[post-confirmation] START user=${userId}`);

  const SCAN_API_URL = process.env.SCAN_API_URL ?? 'https://api.bebocard.com';

  const birthdate = attrs['birthdate'] ?? '';
  const parentEmail = attrs['custom:parentEmail'] ?? '';
  const ageBucket = calculateAgeBucket(birthdate);
  const status = ageBucket === '<13' || ageBucket === '13-17' ? 'PENDING_CONSENT' : 'ACTIVE';

  let USER_TABLE: string;
  let ADMIN_TABLE: string;
  try {
    ({ USER_TABLE, ADMIN_TABLE } = await getTableNames());
  } catch (err) {
    console.error('[post-confirmation] FAILED to get table names:', err);
    throw err;
  }

  event.response = {};

  // 1. Update Cognito: set permULID + ageBucket
  await cognito.send(new AdminUpdateUserAttributesCommand({
    UserPoolId: event.userPoolId,
    Username: userId,
    UserAttributes: [
      { Name: 'custom:permULID', Value: permULID },
      { Name: 'custom:ageBucket', Value: ageBucket },
    ],
  }));

  // 2. Write IDENTITY record (P2-6 status check)
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
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

  // 3. If minor, trigger parental consent email (P2-6)
  if (status === 'PENDING_CONSENT' && parentEmail) {
    try {
      const secret = await getConsentSecret();
      const token = generateConsentToken(permULID, secret);
      const approvalLink = `${SCAN_API_URL}/v1/consent/parental/approve?token=${token}`;

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
      console.error('[post-confirmation] FAILED to send email:', err);
    }
  }

  // 4. Pre-create scan index
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
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
  return res.Parameter?.Value ?? 'default_fallback_secret_change_me';
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
