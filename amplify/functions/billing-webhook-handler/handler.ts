import type { APIGatewayProxyHandler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, UpdateCommand, PutCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { createHmac } from 'crypto';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const ssm = new SSMClient({});

const REFDATA_TABLE = process.env.REFDATA_TABLE!;

async function verifyStripeSignature(event: any, webhookSecret: string): Promise<boolean> {
  const signature = event.headers['stripe-signature'];
  if (!signature) return false;

  const pairs = (signature as string).split(',');
  const timestamp = pairs.find(p => p.startsWith('t='))?.split('=')[1];
  const sigs = pairs.filter(p => p.startsWith('v1=')).map(p => p.split('=')[1]);

  if (!timestamp || sigs.length === 0) return false;

  const signedPayload = `${timestamp}.${event.body}`;
  const hmac = createHmac('sha256', webhookSecret).update(signedPayload).digest('hex');

  return sigs.includes(hmac);
}

async function getStripeSecret(): Promise<string> {
  const res = await ssm.send(new GetParameterCommand({
    Name: '/amplify/shared/STRIPE_SECRET_KEY',
    WithDecryption: true,
  }));
  return res.Parameter?.Value ?? '';
}

async function getStripeWebhookSecret(): Promise<string> {
  const res = await ssm.send(new GetParameterCommand({
    Name: '/amplify/shared/STRIPE_WEBHOOK_SECRET',
    WithDecryption: true,
  }));
  return res.Parameter?.Value ?? '';
}

export const handler: APIGatewayProxyHandler = async (event) => {
  console.info('[billing-webhook] Received event:', event.headers['stripe-signature'] ? 'Signed' : 'Unsigned');

  const webhookSecret = await getStripeWebhookSecret();
  const isValid = await verifyStripeSignature(event, webhookSecret);

  if (!isValid) {
    console.error('[billing-webhook] INVALID STRIPE SIGNATURE. Access denied.');
    return { statusCode: 401, body: 'Invalid signature' };
  }

  let body: any;
  try {
    body = JSON.parse(event.body ?? '{}');
  } catch (e) {
    return { statusCode: 400, body: 'Invalid JSON' };
  }

  const type = body.type;
  const data = body.data?.object;

  console.info('[billing-webhook] Event Type:', type);

  switch (type) {
    case 'checkout.session.completed':
      await handleCheckoutCompleted(data);
      break;
    case 'customer.subscription.updated':
    case 'customer.subscription.deleted':
      await handleSubscriptionUpdated(data);
      break;
    case 'invoice.payment_failed':
      await handlePaymentFailed(data);
      break;
    default:
      console.info('[billing-webhook] Unhandled event type:', type);
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ received: true }),
  };
};

async function handleCheckoutCompleted(session: any) {
  const tenantId = session.metadata?.tenantId;
  const stripeCustomerId = session.customer;
  const stripeSubscriptionId = session.subscription;

  if (!tenantId) {
    console.error('[billing-webhook] No tenantId in session metadata');
    return;
  }

  console.info(`[billing-webhook] Linking tenant ${tenantId} to stripe customer ${stripeCustomerId}`);

  // Fetch existing profile to preserve other fields in desc string
  const getRes = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'PROFILE' },
  }));

  const existingDesc = JSON.parse(getRes.Item?.desc ?? '{}');
  const now = new Date().toISOString();
  
  const mergedDesc = {
    ...existingDesc,
    stripeCustomerId,
    stripeSubscriptionId,
    billingStatus: 'ACTIVE',
    tierStartDate: existingDesc.tierStartDate ?? now,
    updatedAt: now,
  };

  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'PROFILE' },
    UpdateExpression: 'SET #desc = :desc, updatedAt = :now',
    ExpressionAttributeNames: { '#desc': 'desc' },
    ExpressionAttributeValues: {
      ':desc': JSON.stringify(mergedDesc),
      ':now': now,
    },
  }));
}

async function handleSubscriptionUpdated(subscription: any) {
  const stripeCustomerId = subscription.customer;
  const status = subscription.status; // 'active', 'past_due', 'canceled', etc.
  const tenantId = subscription.metadata?.tenantId;

  if (!tenantId) {
    console.warn('[billing-webhook] Received subscription update without tenantId metadata');
    return;
  }

  const billingStatus = (status === 'active' || status === 'trialing') ? 'ACTIVE' : 'SUSPENDED';
  console.info(`[billing-webhook] Updating tenant ${tenantId} billingStatus to ${billingStatus} (${status})`);

  const getRes = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'PROFILE' },
  }));

  if (!getRes.Item) return;

  const existingDesc = JSON.parse(getRes.Item.desc ?? '{}');
  const now = new Date().toISOString();

  const mergedDesc = {
    ...existingDesc,
    billingStatus,
    stripeSubscriptionStatus: status,
    updatedAt: now,
  };

  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'PROFILE' },
    UpdateExpression: 'SET #desc = :desc, updatedAt = :now',
    ExpressionAttributeNames: { '#desc': 'desc' },
    ExpressionAttributeValues: {
      ':desc': JSON.stringify(mergedDesc),
      ':now': now,
    },
  }));
}

async function handlePaymentFailed(invoice: any) {
  const tenantId = invoice.subscription_details?.metadata?.tenantId || invoice.metadata?.tenantId;
  if (!tenantId) return;

  console.warn(`[billing-webhook] Payment failed for tenant ${tenantId}. Marking as OVERDUE.`);

  const getRes = await dynamo.send(new GetCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'PROFILE' },
  }));

  if (!getRes.Item) return;

  const existingDesc = JSON.parse(getRes.Item.desc ?? '{}');
  const now = new Date().toISOString();

  const mergedDesc = {
    ...existingDesc,
    billingStatus: 'OVERDUE',
    updatedAt: now,
  };

  await dynamo.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'PROFILE' },
    UpdateExpression: 'SET #desc = :desc, updatedAt = :now',
    ExpressionAttributeNames: { '#desc': 'desc' },
    ExpressionAttributeValues: {
      ':desc': JSON.stringify(mergedDesc),
      ':now': now,
    },
  }));
}
