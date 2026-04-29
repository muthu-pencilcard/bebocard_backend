import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, ScanCommand, UpdateCommand, QueryCommand, GetCommand } from '@aws-sdk/lib-dynamodb';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging } from 'firebase-admin/messaging';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ssm = new SSMClient({});
const secrets = new SecretsManagerClient({});

const REFDATA_TABLE_PARAM = process.env.REFDATA_TABLE_PARAM!;
const USER_TABLE_PARAM = process.env.USER_TABLE_PARAM!;

let refDataTableName: string | null = null;
let userDataTableName: string | null = null;

async function resolveTables() {
  if (refDataTableName && userDataTableName) return;
  const [refRes, userRes] = await Promise.all([
    ssm.send(new GetParameterCommand({ Name: REFDATA_TABLE_PARAM })),
    ssm.send(new GetParameterCommand({ Name: USER_TABLE_PARAM })),
  ]);
  refDataTableName = refRes.Parameter?.Value!;
  userDataTableName = userRes.Parameter?.Value!;
}

async function getFirebase() {
  if (getApps().length === 0) {
    const sec = await secrets.send(new GetSecretValueCommand({ SecretId: 'bebocard/firebase-service-account' }));
    if (!sec.SecretString) throw new Error('Firebase secret not found');
    initializeApp({ credential: cert(JSON.parse(sec.SecretString)) });
  }
  return getMessaging();
}

/**
 * campaign-scheduler
 */
export const handler = async () => {
  const now = new Date().toISOString();
  await resolveTables();
  console.info(`[campaign-scheduler] Starting sweep at ${now}`);

  try {
    const res = await dynamo.send(new ScanCommand({
      TableName: refDataTableName!,
      FilterExpression: '#status = :scheduled AND begins_with(sK, :prefix)',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: {
        ':scheduled': 'SCHEDULED',
        ':prefix': 'OFFER#',
      },
    }));

    const items = res.Items ?? [];
    let transitionedCount = 0;

    for (const item of items) {
      const desc = JSON.parse(item.desc ?? '{}');
      const scheduledFor = desc.scheduledFor as string | undefined;

      const shouldGoLive = (scheduledFor && scheduledFor <= now) || 
                          (!scheduledFor && desc.validFrom && desc.validFrom <= now.slice(0, 10));

      if (shouldGoLive) {
        console.info(`[campaign-scheduler] Flipping ${item.sK} to ACTIVE`);
        
        await dynamo.send(new UpdateCommand({
          TableName: refDataTableName!,
          Key: { pK: item.pK, sK: item.sK },
          UpdateExpression: 'SET #status = :active, updatedAt = :now',
          ExpressionAttributeNames: { '#status': 'status' },
          ExpressionAttributeValues: { ':active': 'ACTIVE', ':now': now },
        }));
        
        transitionedCount++;

        // 🔥 Phase 3: Push to matching users if this is a Discovery offer
        if (desc.isDiscovery && desc.targetPersona) {
          await pushToMatchingUsers(desc.targetPersona, desc.brandName, desc.title, item.sK);
        }
      }
    }

    return { success: true, transitionedCount };
  } catch (err) {
    console.error('[campaign-scheduler] Error:', err);
    throw err;
  }
};

async function pushToMatchingUsers(persona: string, brandName: string, title: string, offerSK: string) {
  console.info(`[campaign-scheduler] Pushing discovery offer ${offerSK} to persona: ${persona}`);
  
  let lastKey: Record<string, any> | undefined;
  const messaging = await getFirebase();

  do {
    const users = await dynamo.send(new QueryCommand({
      TableName: userDataTableName!,
      IndexName: 'userDataByPersona',
      KeyConditionExpression: 'persona = :p',
      ExpressionAttributeValues: { ':p': persona },
      ExclusiveStartKey: lastKey,
      Limit: 100,
    }));
    lastKey = users.LastEvaluatedKey;

    for (const segmentRecord of users.Items ?? []) {
      const permULID = (segmentRecord.pK as string).replace('USER#', '');
      
      // Get device token
      const tokenRes = await dynamo.send(new GetCommand({
        TableName: userDataTableName!,
        Key: { pK: `USER#${permULID}`, sK: 'DEVICE_TOKEN' },
      }));
      const token = JSON.parse(tokenRes.Item?.desc ?? '{}').token;

      if (token) {
        try {
          await messaging.send({
            token,
            notification: {
              title: `New discovery: ${brandName}`,
              body: `${title} matches your profile — check it out!`,
            },
            data: { type: 'DISCOVERY_OFFER', offerSK },
          });
        } catch (e) {
          console.error(`[campaign-scheduler] Push failed for ${permULID}:`, e);
        }
      }
    }
  } while (lastKey);
}
