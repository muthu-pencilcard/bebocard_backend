import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';

const USER_TABLE = 'UserDataEvent-bpearwbsprfmjp2tskn4mhp4la-NONE';
const ADMIN_TABLE = 'AdminDataEvent-bpearwbsprfmjp2tskn4mhp4la-NONE'; 
const EMAIL = 'tomuthuprabucm@gmail.com';
const USERNAME = '14c81478-60b1-702e-6803-51b130abeeaa';
const SUB = '14c81478-60b1-702e-6803-51b130abeeaa';

const ulid = monotonicFactory();
const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-east-1' }));

async function run() {
  const permULID = SUB; // Using SUB as permULID fallback
  const secondaryULID = ulid();
  const now = new Date().toISOString();
  const rotatesAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();

  console.log(`Fixing DynamoDB records for user ${EMAIL}...`);
  console.log(`Using permULID (SUB): ${permULID}`);
  console.log(`Generated secondaryULID: ${secondaryULID}`);

  // 1. Write IDENTITY
  await dynamo.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: 'IDENTITY',
      eventType: 'IDENTITY',
      status: 'ACTIVE',
      primaryCat: 'identity',
      secondaryULID,
      rotatesAt,
      desc: JSON.stringify({
        permULID,
        cognitoUserId: USERNAME,
        createdAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log('IDENTITY record written to UserDataEvent.');

  // 2. Write SCAN index
  await dynamo.send(new PutCommand({
    TableName: ADMIN_TABLE,
    Item: {
      pK: `SCAN#${secondaryULID}`,
      sK: permULID,
      eventType: 'SCAN_INDEX',
      status: 'ACTIVE',
      desc: JSON.stringify({ cards: [], createdAt: now }),
      createdAt: now,
      updatedAt: now,
    },
  }));
  console.log('SCAN index written to AdminDataEvent.');
}

run().catch(console.error);
