import type { PostConfirmationTriggerHandler } from 'aws-lambda';
import { CognitoIdentityProviderClient, AdminUpdateUserAttributesCommand } from '@aws-sdk/client-cognito-identity-provider';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { monotonicFactory } from 'ulid';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const cognito = new CognitoIdentityProviderClient({});
const ssm = new SSMClient({});
const ulid = monotonicFactory();

// We fetch table names from SSM parameters generated in the backend stack
// to break the CloudFormation circular dependency (Auth -> Lambda -> Data -> Auth).
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

/**
 * Runs after a user confirms their email.
 * 1. Generates permULID (permanent — written to Cognito custom attribute)
 * 2. Generates secondaryULID (QR-facing — stored in DynamoDB only)
 * 3. Writes USER#<permULID> | IDENTITY to UserDataEvent table
 * 4. Writes SCAN#<secondaryULID> | INDEX to AdminDataEvent table (empty cards initially)
 */
export const handler: PostConfirmationTriggerHandler = async (event) => {
  const permULID = ulid();
  const secondaryULID = ulid();
  const now = new Date().toISOString();
  const rotatesAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(); // 24h
  const userId = event.userName;

  console.log(`[post-confirmation] START user=${userId} pool=${event.userPoolId}`);
  console.log(`[post-confirmation] ENV USER_TABLE_PARAM=${process.env.USER_TABLE_PARAM} ADMIN_TABLE_PARAM=${process.env.ADMIN_TABLE_PARAM}`);

  let USER_TABLE: string;
  let ADMIN_TABLE: string;
  try {
    ({ USER_TABLE, ADMIN_TABLE } = await getTableNames());
    console.log(`[post-confirmation] Tables resolved: USER=${USER_TABLE} ADMIN=${ADMIN_TABLE}`);
  } catch (err) {
    console.error('[post-confirmation] FAILED to get table names from SSM:', err);
    throw err;
  }

  event.response = {};

  await cognito.send(new AdminUpdateUserAttributesCommand({
    UserPoolId: event.userPoolId,
    Username: userId,
    UserAttributes: [
      { Name: 'custom:permULID', Value: permULID },
    ],
  }));

  // 2. Write IDENTITY record.
  //    secondaryULID stored as top-level field (not just in desc) so rotateQR
  //    can use a conditional update to guard against multi-device race conditions.
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: 'IDENTITY',
      eventType: 'IDENTITY',
      status: 'ACTIVE',
      primaryCat: 'identity',
      subCategory: 'wallet',
      secondaryULID,          // top-level — used for conditional update in rotateQR
      rotatesAt,              // top-level — read by device to know when to rotate
      owner: userId,          // required for AppSync 'allow: owner' authorization
      desc: JSON.stringify({
        permULID,
        cognitoUserId: userId,
        createdAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  // 3. Pre-create scan index (empty — populated as cards are added).
  //    sK = permULID so AdminDataEvent is queryable by permULID via GSI.
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `SCAN#${secondaryULID}`,
      sK: permULID,
      eventType: 'SCAN_INDEX',
      status: 'ACTIVE',
      owner: userId,          // required for potential future AppSync reads
      desc: JSON.stringify({ cards: [], createdAt: now }),
      createdAt: now,
      updatedAt: now,
    },
  }));

  console.log(`[post-confirmation] permULID=${permULID} secondaryULID=${secondaryULID} user=${userId}`);
  return event;
};
