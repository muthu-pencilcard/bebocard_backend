import type { PostConfirmationTriggerHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid = monotonicFactory();

const USER_TABLE = process.env.USER_TABLE!;

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

  event.response = {};

  // 2. Write IDENTITY record.
  //    secondaryULID stored as top-level field (not just in desc) so rotateQR
  //    can use a conditional update to guard against multi-device race conditions.
  //    rotatesAt tells the device when to request a new secondaryULID from AdminDataEvent.
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
    TableName: process.env.ADMIN_TABLE!,
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

  console.log(`[post-confirmation] permULID=${permULID} secondaryULID=${secondaryULID} user=${userId}`);
  return event;
};
