import { DynamoDBClient, QueryCommand, BatchWriteItemCommand, DeleteItemCommand } from '@aws-sdk/client-dynamodb';
import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { CognitoIdentityProviderClient, AdminDisableUserCommand, AdminUserGlobalSignOutCommand } from '@aws-sdk/client-cognito-identity-provider';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';
import { createHash } from 'crypto';
import JSZip from 'jszip';

const ddb = new DynamoDBClient({});
const s3 = new S3Client({});
const sqs = new SQSClient({});
const cognito = new CognitoIdentityProviderClient({});

const USER_TABLE = process.env.USER_TABLE!;
const ADMIN_TABLE = process.env.ADMIN_TABLE!;
const EXPORTS_BUCKET = process.env.EXPORTS_BUCKET!;
const WEBHOOK_QUEUE_URL = process.env.WEBHOOK_QUEUE_URL!;
const USER_POOL_ID = process.env.USER_POOL_ID!;

function getFirebaseAdmin() {
  if (getApps().length === 0) {
    const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    if (!serviceAccountJson) throw new Error('FIREBASE_SERVICE_ACCOUNT_JSON not set');
    initializeApp({ credential: cert(JSON.parse(serviceAccountJson)) });
  }
  return getMessaging();
}

export const handler = async (event: any) => {
  const { fieldName } = event.info;
  const permULID = event.identity?.claims?.['custom:permULID'];
  const externalId = event.identity?.claims?.['sub'];
  
  if (!permULID) throw new Error('Unauthorized: missing permULID');

  if (fieldName === 'startDataExport') {
    return handleExport(permULID);
  } else if (fieldName === 'deleteUserAccount') {
    if (!externalId) throw new Error('Unauthorized: missing externalId (sub)');
    return handleDelete(permULID, externalId);
  }
  
  throw new Error(`Unknown field: ${fieldName}`);
};

async function handleExport(permULID: string) {
  console.info(`[user-data-exporter] Starting export for ${permULID}`);
  
  // 1. Fetch all user data
  const result = await ddb.send(new QueryCommand({
    TableName: USER_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: marshall({ ':pk': `USER#${permULID}` }),
  }));

  const items = (result.Items ?? []).map(i => unmarshall(i));
  const data = JSON.stringify(items, null, 2);

  // 2. Generate ZIP
  const zip = new JSZip();
  zip.file('bebocard_data_export.json', data);
  zip.file('READ_ME.txt', 'This is your BeboCard data export. It contains all loyalty cards, receipts, and account settings associated with your identity.');
  
  const buffer = await zip.generateAsync({ type: 'nodebuffer' });

  // 3. Upload to S3
  const key = `${permULID}/export_${new Date().getTime()}.zip`;
  await s3.send(new PutObjectCommand({
    Bucket: EXPORTS_BUCKET,
    Key: key,
    Body: buffer,
    ContentType: 'application/zip',
  }));

  // 4. Generate Pre-signed URL (24 hours)
  const url = await getSignedUrl(s3, new GetObjectCommand({ Bucket: EXPORTS_BUCKET, Key: key }), { expiresIn: 86400 });

  // 5. Notify User via Push
  await sendPushNotification(permULID, 'Your data export is ready', 'Tap to download your BeboCard archive.', url);

  return 'EXPORT_QUEUED';
}

async function handleDelete(permULID: string, externalId: string) {
  console.info(`[user-data-exporter] Deletion request received for ${permULID}`);
  
  // 1. Disable the user in Cognito immediately
  // This prevents any further app use or barcode rotations during the grace period.
  try {
    const username = externalId; // Root sub is usually the username for Admin commands
    await cognito.send(new AdminUserGlobalSignOutCommand({
      UserPoolId: USER_POOL_ID,
      Username: username,
    }));
    await cognito.send(new AdminDisableUserCommand({
      UserPoolId: USER_POOL_ID,
      Username: username,
    }));
    console.info(`[user-data-exporter] Cognito user ${username} disabled.`);
  } catch (e) {
    console.error(`[user-data-exporter] Failed to disable Cognito user: ${e}`);
    // We continue even if Cognito fails — DynamoDB tombstone is the source of truth for erasure.
  }

  // 2. Fetch user identity to get the current barcode
  const identityRes = await ddb.send(new QueryCommand({
    TableName: USER_TABLE,
    KeyConditionExpression: 'pK = :pk AND sK = :sk',
    ExpressionAttributeValues: marshall({ ':pk': `USER#${permULID}`, ':sk': 'IDENTITY' }),
  }));

  const identity = identityRes.Items?.[0] ? unmarshall(identityRes.Items[0]) : null;
  const secondaryULID = identity?.secondaryULID;

  // 3. Write Deletion Tombstone to AdminDataEvent
  // This starts the 30-day countdown to hard erasure.
  const erasureAt = new Date();
  erasureAt.setDate(erasureAt.getDate() + 30);

  await ddb.send(new BatchWriteItemCommand({
    RequestItems: {
      [ADMIN_TABLE]: [
        {
          PutRequest: {
            Item: marshall({
              pK: `GDPR_DELETION#${permULID}`,
              sK: 'REQUEST',
              status: 'PENDING_ERASURE',
              secondaryULID: secondaryULID || 'NONE',
              requestedAt: new Date().toISOString(),
              erasureAt: erasureAt.toISOString(),
              desc: JSON.stringify({
                reason: 'USER_REQUEST_FROM_APP',
                authType: 'COGNITO_SELF',
                externalId: externalId
              })
            })
          }
        }
      ]
    }
  }));

  // 4. Update IDENTITY status to 'PENDING_ERASURE' to block scan resolutions
  if (identity) {
    await ddb.send(new BatchWriteItemCommand({
      RequestItems: {
        [USER_TABLE]: [
          {
            PutRequest: {
              Item: marshall({
                ...identity,
                status: 'PENDING_ERASURE',
                updatedAt: new Date().toISOString()
              })
            }
          }
        ]
      }
    }));
  }

  console.info(`[user-data-exporter] Deletion request registered. Erasure scheduled for ${erasureAt.toISOString()}.`);
  return 'DELETION_PENDING';
}

/**
 * Performs the actual hard erasure after the grace period.
 * Triggered by a scheduled rule scanning the AdminDataEvent table.
 */
async function performHardErasure(permULID: string) {
  console.info(`[user-data-exporter] Performing hard erasure for ${permULID}`);
  
  // 1. Fetch all records
  const result = await ddb.send(new QueryCommand({
    TableName: USER_TABLE,
    KeyConditionExpression: 'pK = :pk',
    ExpressionAttributeValues: marshall({ ':pk': `USER#${permULID}` }),
  }));

  const items = (result.Items ?? []).map(i => unmarshall(i));

  // 2. Revoke active barcode in AdminDataEvent
  const identityRecord = items.find(i => i.sK === 'IDENTITY');
  if (identityRecord && identityRecord.secondaryULID) {
    await ddb.send(new DeleteItemCommand({
      TableName: ADMIN_TABLE,
      Key: marshall({ pK: `SCAN#${identityRecord.secondaryULID}`, sK: permULID })
    })).catch(e => console.warn(`Failed to delete scan index: ${e}`));
  }

  // 3. Notify brands via Reliable Webhook Queue
  const subscriptions = items.filter(i => i.sK.startsWith('SUBSCRIPTION#'));
  const obfuscatedEmail = createHash('sha256').update(permULID + 'erasure').digest('hex').substring(0, 16) + '@bebocard.me';

  for (const sub of subscriptions) {
    const brandId = sub.sK.replace('SUBSCRIPTION#', '');
    await sqs.send(new SendMessageCommand({
      QueueUrl: WEBHOOK_QUEUE_URL,
      MessageBody: JSON.stringify({
        brandId,
        type: 'user.deleted',
        data: {
          permULID,
          obfuscatedEmail,
          erasedAt: new Date().toISOString(),
          reason: 'USER_REQUEST'
        }
      })
    })).catch(e => console.error(`Failed to queue webhook for ${brandId}`, e));
  }

  // 4. Delete Cognito User
  const deletionRecord = await ddb.send(new QueryCommand({
    TableName: ADMIN_TABLE,
    KeyConditionExpression: 'pK = :pk AND sK = :sk',
    ExpressionAttributeValues: marshall({ ':pk': `GDPR_DELETION#${permULID}`, ':sk': 'REQUEST' }),
  }));
  
  if (deletionRecord.Items?.[0]) {
    const tombstone = unmarshall(deletionRecord.Items[0]);
    const desc = JSON.parse(tombstone.desc || '{}');
    if (desc.externalId) {
      // Real Cognito Delete
      const { AdminDeleteUserCommand } = await import('@aws-sdk/client-cognito-identity-provider');
      await cognito.send(new AdminDeleteUserCommand({
        UserPoolId: USER_POOL_ID,
        Username: desc.externalId,
      })).catch(e => console.warn(`Failed to delete Cognito user: ${e}`));
    }
  }

  // 5. Batch delete all UserDataEvent records
  const rawItems = result.Items ?? [];
  for (let i = 0; i < rawItems.length; i += 25) {
    const batch = rawItems.slice(i, i + 25);
    await ddb.send(new BatchWriteItemCommand({
      RequestItems: {
        [USER_TABLE]: batch.map(item => ({
          DeleteRequest: { Key: { pK: item.pK, sK: item.sK } }
        }))
      }
    }));
  }

  // 6. Final Tombstone Update
  await ddb.send(new BatchWriteItemCommand({
    RequestItems: {
      [ADMIN_TABLE]: [
        {
          PutRequest: {
            Item: marshall({
              pK: `GDPR_DELETION#${permULID}`,
              sK: 'COMPLETED',
              status: 'ERASED',
              completedAt: new Date().toISOString()
            })
          }
        },
        {
          DeleteRequest: { Key: marshall({ pK: `GDPR_DELETION#${permULID}`, sK: 'REQUEST' }) }
        }
      ]
    }
  }));

  console.info(`[user-data-exporter] Hard erasure complete for ${permULID}`);
}

async function sendPushNotification(permULID: string, title: string, body: string, url: string) {
  // Fetch device token
  const res = await ddb.send(new QueryCommand({
    TableName: USER_TABLE,
    KeyConditionExpression: 'pK = :pk AND sK = :sk',
    ExpressionAttributeValues: marshall({ ':pk': `USER#${permULID}`, ':sk': 'DEVICE_TOKEN' }),
    Limit: 1
  }));

  if (!res.Items || res.Items.length === 0) {
    console.warn(`[user-data-exporter] No device token found for ${permULID}, skipping push.`);
    return;
  }

  const tokenRow = unmarshall(res.Items[0]);
  const desc = JSON.parse(tokenRow.desc || '{}');
  const token = desc.token;

  if (!token) return;

  try {
    const messaging = getFirebaseAdmin();
    await messaging.send({
      token,
      notification: { title, body },
      data: {
        type: 'data_export_ready',
        downloadUrl: url,
      },
      android: { priority: 'high' },
      apns: { payload: { aps: { alert: { title, body }, sound: 'default' } } }
    });
    console.info(`[user-data-exporter] Push notification sent to ${permULID}`);
  } catch (e) {
    console.error('[user-data-exporter] FCM failed', e);
  }
}
