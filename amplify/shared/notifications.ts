import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const ulid = monotonicFactory();

const USER_TABLE = process.env.USER_TABLE!;

export interface NotificationPayload {
  title: string;
  body: string;
  topic: string;
  relatedSK?: string;
  metadata?: Record<string, any>;
}

/**
 * Persists a notification to the UserDataEvent table for the mobile app's inbox.
 */
export async function persistNotification(
  permULID: string,
  data: NotificationPayload
): Promise<void> {
  const now = new Date().toISOString();
  await docClient.send(new PutCommand({
    TableName: USER_TABLE,
    Item: {
      pK: `USER#${permULID}`,
      sK: `NOTIFICATION#${ulid()}`,
      eventType: 'NOTIFICATION',
      status: 'UNREAD',
      primaryCat: 'notification',
      subCategory: data.topic,
      desc: JSON.stringify({
        title: data.title,
        body: data.body,
        relatedSK: data.relatedSK,
        metadata: data.metadata,
        createdAt: now,
      }),
      createdAt: now,
      updatedAt: now,
    },
  }));
}
