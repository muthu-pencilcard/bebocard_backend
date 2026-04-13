import type { APIGatewayProxyHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand, DeleteCommand, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ulid = monotonicFactory();

const USER_TABLE = process.env.USER_TABLE!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;

/**
 * handleClaim: POST /receipt/claim
 * Allows a BeboCard user to claim an anonymous receipt via a claim token.
 */
export const handler: APIGatewayProxyHandler = async (event) => {
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  };

  // ── Authentication Check ──
  // Assume Amplify/Cognito Authorizer has already populated the identity
  const userSub = event.requestContext.authorizer?.claims?.sub;
  const permULID = event.requestContext.authorizer?.claims?.['custom:permULID'];

  if (!userSub || !permULID) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  try {
    const { receiptId, token } = JSON.parse(event.body ?? '{}');
    if (!receiptId || !token) {
      return { statusCode: 400, headers, body: JSON.stringify({ error: 'Missing receiptId or token' }) };
    }

    // 1. Fetch the anonymous receipt
    const anonRes = await dynamo.send(new GetCommand({
      TableName: REFDATA_TABLE,
      Key: { pK: `ANON#${receiptId}`, sK: 'receipt' }
    }));

    const anonItem = anonRes.Item;
    if (!anonItem) {
      return { statusCode: 404, headers, body: JSON.stringify({ error: 'Receipt not found or already claimed' }) };
    }

    const desc = JSON.parse(anonItem.desc ?? '{}');
    if (desc.claimToken !== token) {
      return { statusCode: 403, headers, body: JSON.stringify({ error: 'Invalid claim token' }) };
    }

    // 2. Prepare the new user receipt SK
    const purchaseDate = desc.purchaseDate || new Date().toISOString();
    const itemTag = anonItem.eventType || 'RECEIPT';
    const newUserSK = `${itemTag}#${purchaseDate.substring(0, 10)}#${ulid()}`;

    // 3. Atomically move record to UserDataEvent
    await dynamo.send(new TransactWriteCommand({
      TransactItems: [
        {
          Delete: {
            TableName: REFDATA_TABLE,
            Key: { pK: `ANON#${receiptId}`, sK: 'receipt' },
            ConditionExpression: 'attribute_exists(pK)'
          }
        },
        {
          Delete: {
            TableName: REFDATA_TABLE,
            Key: { pK: `ANON#${receiptId}`, sK: `${itemTag}_IDEM#${receiptId}` }, // Not quite right but sentinel should be cleaned if exists
          }
        },
        {
          Put: {
            TableName: USER_TABLE,
            Item: {
              pK: `USER#${permULID}`,
              sK: newUserSK,
              eventType: itemTag,
              status: 'CLAIMED',
              primaryCat: anonItem.primaryCat || 'receipt',
              subCategory: anonItem.subCategory || desc.brandId,
              desc: JSON.stringify({
                ...desc,
                claimedAt: new Date().toISOString(),
                originalAnonymousId: receiptId,
                isAnonymous: false,
                claimToken: undefined // Remove token once claimed
              }),
              createdAt: anonItem.createdAt,
              updatedAt: new Date().toISOString(),
              exp: Math.floor(Date.now() / 1000) + (60 * 60 * 24 * 365 * 7), // Extend to 7-year retention on claim
            }
          }
        }
      ]
    }));

    console.info(`[receipt-claim-handler] User ${permULID} successfully claimed anonymous receipt ${receiptId} as ${newUserSK}`);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ 
        success: true, 
        message: 'Receipt successfully added to your wallet',
        receiptSK: newUserSK 
      }),
    };

  } catch (err: any) {
    console.error('[receipt-claim-handler]', err);
    if (err.name === 'TransactionCanceledException') {
      return { statusCode: 409, headers, body: JSON.stringify({ error: 'Receipt already claimed or being processed' }) };
    }
    return { statusCode: 500, headers, body: JSON.stringify({ error: 'Internal server error' }) };
  }
};
