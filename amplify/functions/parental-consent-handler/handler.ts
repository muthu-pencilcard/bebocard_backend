import type { APIGatewayProxyHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import * as crypto from 'crypto';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ssm = new SSMClient({});

const USER_TABLE = process.env.USER_TABLE!;

export const handler: APIGatewayProxyHandler = async (event) => {
  const token = event.queryStringParameters?.token;

  if (!token) {
    return {
      statusCode: 400,
      headers: { 'Content-Type': 'text/html' },
      body: '<h1>Invalid Request</h1><p>Missing consent token.</p>',
    };
  }

  try {
    const secret = await getSecret();
    const payload = verifyToken(token, secret);

    if (!payload) {
      return {
        statusCode: 403,
        headers: { 'Content-Type': 'text/html' },
        body: '<h1>Invalid or Expired Link</h1><p>The approval link is invalid or has expired.</p>',
      };
    }

    const { permULID } = payload;

    // Update status to ACTIVE
    await dynamo.send(new UpdateCommand({
      TableName: USER_TABLE,
      Key: { pK: `USER#${permULID}`, sK: 'IDENTITY' },
      UpdateExpression: 'SET #s = :active, updatedAt = :now',
      ExpressionAttributeNames: { '#s': 'status' },
      ExpressionAttributeValues: {
        ':active': 'ACTIVE',
        ':pending': 'PENDING_CONSENT',
        ':now': new Date().toISOString(),
      },
      ConditionExpression: '#s = :pending',
    }));

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/html' },
      body: `
        <html>
          <head>
            <style>
              body { font-family: -apple-system, sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; background: #0F172A; color: white; }
              .card { background: #1E293B; padding: 2rem; border-radius: 1rem; text-align: center; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1); border: 1px solid #334155; }
              const { teal } = { teal: '#2DD4BF' };
              h1 { color: #2DD4BF; margin-bottom: 0.5rem; }
              p { color: #94A3B8; }
            </style>
          </head>
          <body>
            <div class="card">
              <h1>Approval Successful</h1>
              <p>Your child's BeboCard account is now active.</p>
              <p>They can now log in and start using their wallet.</p>
            </div>
          </body>
        </html>
      `,
    };
  } catch (err: any) {
    console.error('[parental-consent-handler] ERROR:', err);
    if (err.name === 'ConditionalCheckFailedException') {
       return {
        statusCode: 200,
        headers: { 'Content-Type': 'text/html' },
        body: '<h1>Already Active</h1><p>This account has already been approved.</p>',
      };
    }
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'text/html' },
      body: '<h1>Internal Error</h1><p>Something went wrong. Please try again later.</p>',
    };
  }
};

async function getSecret(): Promise<string> {
  const res = await ssm.send(new GetParameterCommand({ 
    Name: '/amplify/shared/PARENTAL_CONSENT_SECRET',
    WithDecryption: true
  }));
  return res.Parameter?.Value ?? 'default_fallback_secret_change_me';
}

function verifyToken(token: string, secret: string) {
  try {
    const [payloadBase64, signature] = token.split('.');
    const payloadJson = Buffer.from(payloadBase64, 'base64').toString('utf8');
    const payload = JSON.parse(payloadJson);

    // Verify signature
    const expectedSig = crypto.createHmac('sha256', secret).update(payloadBase64).digest('hex');
    if (signature !== expectedSig) return null;

    // Verify expiry
    if (payload.exp < Date.now()) return null;

    return payload;
  } catch {
    return null;
  }
}
