import { defineBackend } from '@aws-amplify/backend';
import { auth } from './auth/resource';
import { data } from './data/resource';
import { postConfirmationFn } from './functions/post-confirmation/resource';
import { cardManagerFn } from './functions/card-manager/resource';
import { scanHandlerFn } from './functions/scan-handler/resource';
import { tenantLinker } from './functions/tenant-linker/resource';
import { geofenceHandlerFn } from './functions/geofence-handler/resource';
import { contentValidatorFn } from './functions/content-validator/resource';
import { brandApiHandlerFn } from './functions/brand-api-handler/resource';
import { reminderHandlerFn } from './functions/reminder-handler/resource';
import { tenantAnalyticsFn } from './functions/tenant-analytics/resource';
import { segmentProcessorFn } from './functions/segment-processor/resource';
import { receiptIcebergWriterFn } from './functions/receipt-iceberg-writer/resource';
import { paymentRouterFn } from './functions/payment-router/resource';
import { consentHandlerFn } from './functions/consent-handler/resource';
import { subscriptionProxyFn } from './functions/subscription-proxy/resource';
import { giftCardRouterFn } from './functions/gift-card-router/resource';
import { enrollmentHandlerFn } from './functions/enrollment-handler/resource';
import { smbHandlerFn } from './functions/smb-handler/resource';
import { giftCardHandlerFn } from './functions/gift-card-handler/resource';
import { catalogSyncFn } from './functions/catalog-sync/resource';
import { catalogSubscriptionSyncFn } from './functions/catalog-subscription-sync/resource';
import { giftCardRefundFn } from './functions/gift-card-refund/resource';
import { subscriptionNegotiator } from './functions/subscription-negotiator/resource';
import { widgetActionHandlerFn } from './functions/widget-action-handler/resource';
import { billingRunHandlerFn } from './functions/billing-run-handler/resource';
import { discoveryHandlerFn } from './functions/discovery-handler/resource';
import { Stack, Duration, RemovalPolicy, Tags, CfnOutput } from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as apigw from 'aws-cdk-lib/aws-apigateway';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import * as cfOrigins from 'aws-cdk-lib/aws-cloudfront-origins';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as wafv2 from 'aws-cdk-lib/aws-wafv2';
import { DynamoEventSource, SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as ssm from 'aws-cdk-lib/aws-ssm';

const backend = defineBackend({
  auth,
  data,
  postConfirmationFn,
  cardManagerFn,
  scanHandlerFn,
  tenantLinker,
  geofenceHandlerFn,
  contentValidatorFn,
  brandApiHandlerFn,
  reminderHandlerFn,
  tenantAnalyticsFn,
  segmentProcessorFn,
  receiptIcebergWriterFn,
  paymentRouterFn,
  consentHandlerFn,
  subscriptionProxyFn,
  giftCardRouterFn,
  enrollmentHandlerFn,
  smbHandlerFn,
  giftCardHandlerFn,
  catalogSyncFn,
  catalogSubscriptionSyncFn,
  giftCardRefundFn,
  subscriptionNegotiator,
  widgetActionHandlerFn,
  billingRunHandlerFn,
  discoveryHandlerFn,
});

// ── Grant DynamoDB access ────────────────────────────────────────────────────
const userTable = backend.data.resources.tables['UserDataEvent'];
const refTable = backend.data.resources.tables['RefDataEvent'];
const adminTable = backend.data.resources.tables['AdminDataEvent'];

const tableNames = {
  USER_TABLE: userTable.tableName,
  REFDATA_TABLE: refTable.tableName,
  ADMIN_TABLE: adminTable.tableName,
};

const cfnUserTable = (backend.data.resources as {
  cfnResources?: { cfnTables?: Record<string, dynamodb.CfnTable> };
}).cfnResources?.cfnTables?.['UserDataEvent'];

const cfnRefTable = (backend.data.resources as {
  cfnResources?: { cfnTables?: Record<string, dynamodb.CfnTable> };
}).cfnResources?.cfnTables?.['RefDataEvent'];

const cfnAdminTable = (backend.data.resources as {
  cfnResources?: { cfnTables?: Record<string, dynamodb.CfnTable> };
}).cfnResources?.cfnTables?.['AdminDataEvent'];

// ── Production Hardening: PITR ──
[cfnUserTable, cfnRefTable, cfnAdminTable].forEach(cfnTable => {
  if (cfnTable) {
    cfnTable.pointInTimeRecoverySpecification = { pointInTimeRecoveryEnabled: true };
  }
});

// ── sK-pK-index GSI on UserDataEvent ─────────────────────────────────────────
if (cfnUserTable) {
  const existingGSIs = (cfnUserTable.globalSecondaryIndexes ?? []) as dynamodb.CfnTable.GlobalSecondaryIndexProperty[];
  cfnUserTable.globalSecondaryIndexes = [
    ...existingGSIs,
    {
      indexName: 'sK-pK-index',
      keySchema: [
        { attributeName: 'sK', keyType: 'HASH' },
        { attributeName: 'pK', keyType: 'RANGE' },
      ],
      projection: {
        projectionType: 'INCLUDE',
        nonKeyAttributes: ['desc', 'status'],
      },
    },
  ];
}

// ── Post-confirmation: SSM-based table lookup (breaks circular dep) ──────────
const dataStack = Stack.of(userTable);
const branchName = dataStack.stackName.toLowerCase().includes('prod') ? 'prod' : 'sandbox';
// Use AWS_APP_ID + AWS_BRANCH (concrete strings at CodeBuild synthesis) to build a unique SSM
// path per Amplify deployment. dataStack.stackName is a CDK token for nested stacks and cannot
// be used in SSM parameter names (CDK cannot determine the ARN separator for unresolved tokens).
const amplifyAppId = process.env.AWS_APP_ID ?? 'local';
const amplifyBranch = process.env.AWS_BRANCH ?? 'sandbox';
const userTableParamName = `/bebocard/${amplifyAppId}/${amplifyBranch}/USER_TABLE`;
const adminTableParamName = `/bebocard/${amplifyAppId}/${amplifyBranch}/ADMIN_TABLE`;

new ssm.StringParameter(dataStack, 'UserTableNameParam', {
  parameterName: userTableParamName,
  stringValue: userTable.tableName,
});
new ssm.StringParameter(dataStack, 'AdminTableNameParam', {
  parameterName: adminTableParamName,
  stringValue: adminTable.tableName,
});

const postConfirmLambda = backend.postConfirmationFn.resources.lambda as lambda.Function;
postConfirmLambda.addEnvironment('USER_TABLE_PARAM', userTableParamName);
postConfirmLambda.addEnvironment('ADMIN_TABLE_PARAM', adminTableParamName);

postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  // Wildcard covers all bebocard SSM params regardless of appId/branch resolution at synthesis.
  // Prevents IAM denials if AWS_APP_ID/AWS_BRANCH env vars differ between deploy runs.
  resources: [
    `arn:aws:ssm:${dataStack.region}:${dataStack.account}:parameter/bebocard/*`,
  ],
}));

postConfirmLambda.role?.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonCognitoPowerUser'));

// Wildcard user pool ARN — avoids UserPool → Lambda → LambdaRolePolicy → UserPool cycle
// within the auth stack (UserPool lists Lambda as trigger; Lambda policy must not ref UserPool resource).
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['cognito-idp:AdminUpdateUserAttributes'],
  resources: [`arn:aws:cognito-idp:${dataStack.region}:${dataStack.account}:userpool/*`],
}));

// Use wildcard ARN patterns (not cross-stack token exports) to avoid auth → data circular dep.
// Tables follow the Amplify Gen 2 naming convention: <ModelName>-<appId>-<branch>-<env>
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['dynamodb:PutItem', 'dynamodb:UpdateItem'],
  resources: [
    `arn:aws:dynamodb:${dataStack.region}:${dataStack.account}:table/UserDataEvent-*`,
    `arn:aws:dynamodb:${dataStack.region}:${dataStack.account}:table/AdminDataEvent-*`,
  ],
}));

// ── Card manager: read/write all three tables ────────────────────────────────
const cardManagerLambda = backend.cardManagerFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => cardManagerLambda.addEnvironment(k, v));
userTable.grantReadWriteData(cardManagerLambda);
refTable.grantReadData(cardManagerLambda);
adminTable.grantReadWriteData(cardManagerLambda);

// Scan handler
const scanLambda = backend.scanHandlerFn.resources.lambda as lambda.Function;
scanLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
scanLambda.addEnvironment('USER_TABLE', userTable.tableName);
scanLambda.addEnvironment('REFDATA_TABLE', refTable.tableName);
adminTable.grantReadData(scanLambda);
userTable.grantReadWriteData(scanLambda);
refTable.grantReadData(scanLambda);

// ── Tenant linker ──
const tenantLinkerLambda = backend.tenantLinker.resources.lambda as lambda.Function;
Object.entries({ USER_TABLE: userTable.tableName, ADMIN_TABLE: adminTable.tableName })
  .forEach(([k, v]) => tenantLinkerLambda.addEnvironment(k, v));
userTable.grantReadWriteData(tenantLinkerLambda);
adminTable.grantReadWriteData(tenantLinkerLambda);

const tenantLinkerUrl = tenantLinkerLambda.addFunctionUrl({
  authType: lambda.FunctionUrlAuthType.NONE,
  cors: {
    allowedOrigins: ['*'],
    allowedMethods: [lambda.HttpMethod.GET],
    allowedHeaders: ['Content-Type'],
  },
});
new CfnOutput(Stack.of(tenantLinkerLambda), 'TenantLinkerFunctionUrl', {
  value: tenantLinkerUrl.url,
});

// ── Geofence handler ──
const geofenceLambda = backend.geofenceHandlerFn.resources.lambda as lambda.Function;
geofenceLambda.addEnvironment('USER_TABLE', userTable.tableName);
geofenceLambda.addEnvironment('REF_TABLE', refTable.tableName);
geofenceLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
userTable.grantReadWriteData(geofenceLambda);
refTable.grantReadData(geofenceLambda);
adminTable.grantReadData(geofenceLambda);

// ── Content pipeline ──
const stack = Stack.of(backend.contentValidatorFn.resources.lambda);
const stage = dataStack.stackName.toLowerCase().includes('prod') ? 'prod' : 'dev';

const tenantUploadsBucket = new s3.Bucket(stack, 'TenantUploads', {
  removalPolicy: RemovalPolicy.RETAIN,
  cors: [{
    allowedMethods: [s3.HttpMethods.PUT],
    allowedOrigins: ['https://business.bebocard.com.au', 'http://localhost:3000'],
    allowedHeaders: ['*'],
  }],
});

const appReferenceBucket = new s3.Bucket(stack, 'AppReference', {
  removalPolicy: RemovalPolicy.RETAIN,
  publicReadAccess: true,
  blockPublicAccess: new s3.BlockPublicAccess({
    blockPublicAcls: false,
    ignorePublicAcls: false,
    blockPublicPolicy: false,
    restrictPublicBuckets: false,
  }),
});

const validatorLambda = backend.contentValidatorFn.resources.lambda as lambda.Function;
tenantUploadsBucket.grantRead(validatorLambda);
tenantUploadsBucket.grantPut(validatorLambda);
appReferenceBucket.grantPut(validatorLambda);
validatorLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['rekognition:DetectModerationLabels'],
  resources: ['*'],
}));
refTable.grantReadWriteData(validatorLambda);
adminTable.grantWriteData(validatorLambda);

validatorLambda.addEnvironment('TENANT_UPLOADS_BUCKET', tenantUploadsBucket.bucketName);
validatorLambda.addEnvironment('APP_REFERENCE_BUCKET', appReferenceBucket.bucketName);
validatorLambda.addEnvironment('REFDATA_TABLE', refTable.tableName);
validatorLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);

tenantUploadsBucket.addEventNotification(
  s3.EventType.OBJECT_CREATED_PUT,
  new s3n.LambdaDestination(validatorLambda),
  { prefix: 'brands/' },
);

// ── Brand API handler ──
const brandApiLambda = backend.brandApiHandlerFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => brandApiLambda.addEnvironment(k, v));
userTable.grantReadWriteData(brandApiLambda);
refTable.grantReadWriteData(brandApiLambda);
adminTable.grantReadWriteData(brandApiLambda);

// ── Reminder handler ──
const reminderLambda = backend.reminderHandlerFn.resources.lambda as lambda.Function;
reminderLambda.addEnvironment('USER_TABLE', userTable.tableName);
reminderLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
userTable.grantReadData(reminderLambda);
adminTable.grantReadWriteData(reminderLambda);

new events.Rule(Stack.of(reminderLambda), 'DailyReminderRule', {
  schedule: events.Schedule.cron({ hour: '21', minute: '0' }),
  targets: [new eventsTargets.LambdaFunction(reminderLambda)],
});

// ── Segment processor ──
const segmentLambda = backend.segmentProcessorFn.resources.lambda as lambda.Function;
segmentLambda.addEnvironment('USER_TABLE', userTable.tableName);
userTable.grantReadWriteData(segmentLambda);
if (cfnUserTable) {
  cfnUserTable.streamSpecification = { streamViewType: 'NEW_IMAGE' };
}

const segmentDLQ = new sqs.Queue(stack, 'SegmentProcessorDLQ', {
  retentionPeriod: Duration.days(14),
});
Tags.of(segmentDLQ).add('CostCenter', 'tenant-side');

segmentLambda.addEventSource(new DynamoEventSource(userTable, {
  startingPosition: lambda.StartingPosition.TRIM_HORIZON,
  filters: [
    lambda.FilterCriteria.filter({
      eventName: lambda.FilterRule.or('INSERT', 'MODIFY', 'REMOVE'),
      dynamodb: {
        Keys: {
          sK: {
            S: [{ prefix: 'RECEIPT#' }, { prefix: 'INVOICE#' }, { prefix: 'SUBSCRIPTION#' }],
          },
        },
      },
    }),
  ],
  retryAttempts: 3,
}));

// Attach DLQ to segment-processor for unprocessable stream events
segmentLambda.addEnvironment('SEGMENT_DLQ_URL', segmentDLQ.queueUrl);
segmentDLQ.grantSendMessages(segmentLambda);

// ── Tenant analytics ──
const tenantAnalyticsLambda = backend.tenantAnalyticsFn.resources.lambda as lambda.Function;
tenantAnalyticsLambda.addEnvironment('USER_TABLE', userTable.tableName);
tenantAnalyticsLambda.addEnvironment('REFDATA_TABLE', refTable.tableName);
userTable.grantReadData(tenantAnalyticsLambda);
refTable.grantReadData(tenantAnalyticsLambda);

const analyticsApi = new apigw.RestApi(stack, 'TenantAnalyticsApi', {
  restApiName: `bebo-tenant-analytics-${stage}`,
});
const analyticsIntegration = new apigw.LambdaIntegration(tenantAnalyticsLambda);
analyticsApi.root.addResource('analytics').addResource('segments').addMethod('GET', analyticsIntegration, { apiKeyRequired: true });

// ── Scan API ──
const scanApi = new apigw.RestApi(stack, 'ScanApi', {
  restApiName: `bebo-scan-api-${stage}`,
});
const scanIntegration = new apigw.LambdaIntegration(scanLambda);
scanApi.root.addResource('scan').addMethod('POST', scanIntegration, { apiKeyRequired: true });
scanApi.root.addResource('receipt').addMethod('POST', scanIntegration, { apiKeyRequired: true });

// ── Brand portal API ──
const brandPortalApi = new apigw.RestApi(stack, 'BrandPortalApi', {
  restApiName: `bebo-brand-api-${stage}`,
});
const brandPortalIntegration = new apigw.LambdaIntegration(brandApiLambda);
brandPortalApi.root.addMethod('ANY', brandPortalIntegration, { apiKeyRequired: true });
brandPortalApi.root.addProxy({
  defaultIntegration: brandPortalIntegration,
  anyMethod: true,
  defaultMethodOptions: { apiKeyRequired: true },
});

// ── Receipt Iceberg writer ──
const receiptIcebergLambda = backend.receiptIcebergWriterFn.resources.lambda as lambda.Function;
receiptIcebergLambda.addEnvironment('ANALYTICS_BUCKET', `bebocard-receipt-analytics-${stage}`);
receiptIcebergLambda.addEnvironment('GLUE_DATABASE', 'bebo_analytics');

const icebergDLQ = new sqs.Queue(stack, 'ReceiptIcebergDLQ', {
  retentionPeriod: Duration.days(14),
});
Tags.of(icebergDLQ).add('CostCenter', 'tenant-side');

receiptIcebergLambda.addEventSource(new DynamoEventSource(userTable, {
  startingPosition: lambda.StartingPosition.LATEST,
  filters: [lambda.FilterCriteria.filter({ eventName: lambda.FilterRule.isEqual('INSERT') })],
  retryAttempts: 3,
}));

receiptIcebergLambda.addEnvironment('ICEBERG_DLQ_URL', icebergDLQ.queueUrl);
icebergDLQ.grantSendMessages(receiptIcebergLambda);

// ── Payment Router ──
const paymentRouterLambda = backend.paymentRouterFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => paymentRouterLambda.addEnvironment(k, v));
adminTable.grantReadWriteData(paymentRouterLambda);
userTable.grantReadData(paymentRouterLambda);
refTable.grantReadData(paymentRouterLambda);

const checkoutTimeoutQueue = new sqs.Queue(stack, 'CheckoutTimeoutQueue', {
  visibilityTimeout: Duration.seconds(60),
});
checkoutTimeoutQueue.grantSendMessages(paymentRouterLambda);
checkoutTimeoutQueue.grantConsumeMessages(paymentRouterLambda);
paymentRouterLambda.addEnvironment('TIMEOUT_QUEUE_URL', checkoutTimeoutQueue.queueUrl);
paymentRouterLambda.addEventSource(new SqsEventSource(checkoutTimeoutQueue, { batchSize: 1 }));

const paymentApi = new apigw.RestApi(stack, 'PaymentApi', {
  restApiName: `bebo-payment-api-${stage}`,
});
const paymentIntegration = new apigw.LambdaIntegration(paymentRouterLambda);
paymentApi.root.addResource('checkout').addMethod('POST', paymentIntegration, { apiKeyRequired: true });

// ── Widget Action Handler (Phase 17 — Tenant-Embedded Wallet Actions) ───────
const widgetActionLambda = backend.widgetActionHandlerFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => widgetActionLambda.addEnvironment(k, v));
widgetActionLambda.addEnvironment('COGNITO_REGION', dataStack.region);
widgetActionLambda.addEnvironment('COGNITO_USER_POOL_ID', backend.auth.resources.userPool.userPoolId);
userTable.grantReadWriteData(widgetActionLambda);
refTable.grantReadData(widgetActionLambda);
adminTable.grantReadWriteData(widgetActionLambda);

const widgetApi = new apigw.RestApi(stack, 'WidgetActionApi', {
  restApiName: `bebo-widget-api-${stage}`,
  description: 'Embedded wallet widget API — auth, invoice to wallet, gift card selection',
  defaultCorsPreflightOptions: {
    allowOrigins: apigw.Cors.ALL_ORIGINS,
    allowMethods: ['GET', 'POST', 'OPTIONS'],
    allowHeaders: ['Content-Type', 'Authorization'],
  },
});
const widgetIntegration = new apigw.LambdaIntegration(widgetActionLambda);
const widgetResource = widgetApi.root.addResource('widget');
widgetResource.addResource('auth').addMethod('POST', widgetIntegration);
widgetResource.addResource('invoice').addMethod('POST', widgetIntegration);
widgetResource.addResource('giftcards').addMethod('GET', widgetIntegration);
widgetResource.addResource('giftcard').addResource('select').addMethod('POST', widgetIntegration);

const widgetPlan = widgetApi.addUsagePlan('WidgetPlan', {
  name: 'widget-standard',
  throttle: { rateLimit: 50, burstLimit: 100 },
  quota: { limit: 5_000, period: apigw.Period.DAY },
});
widgetPlan.addApiStage({ api: widgetApi, stage: widgetApi.deploymentStage });

// ── Web Application Firewall (WAF) ──
const publicWebAcl = new wafv2.CfnWebACL(stack, 'PublicApiWebAcl', {
  defaultAction: { allow: {} },
  scope: 'REGIONAL',
  visibilityConfig: {
    cloudWatchMetricsEnabled: true,
    metricName: 'PublicApiWebAcl',
    sampledRequestsEnabled: true,
  },
  rules: [
    {
      name: 'AWS-AWSManagedRulesCommonRuleSet',
      priority: 0,
      statement: {
        managedRuleGroupStatement: {
          vendorName: 'AWS',
          name: 'AWSManagedRulesCommonRuleSet',
        },
      },
      overrideAction: { none: {} },
      visibilityConfig: {
        cloudWatchMetricsEnabled: true,
        metricName: 'AWSManagedRulesCommonRuleSet',
        sampledRequestsEnabled: true,
      },
    },
    {
      name: 'LimitRequests1000Per5Min',
      priority: 1,
      statement: {
        rateBasedStatement: {
          limit: 1000,
          aggregateKeyType: 'IP',
        },
      },
      action: { block: {} },
      visibilityConfig: {
        cloudWatchMetricsEnabled: true,
        metricName: 'LimitRequests1000Per5Min',
        sampledRequestsEnabled: true,
      },
    },
  ],
});

// Associate WAF with APIs
[scanApi, widgetApi, paymentApi, analyticsApi, brandPortalApi].forEach((api, idx) => {
  new wafv2.CfnWebACLAssociation(stack, `WafAssociation${idx}`, {
    resourceArn: `arn:aws:apigateway:${stack.region}::/restapis/${api.restApiId}/stages/${api.deploymentStage.stageName}`,
    webAclArn: publicWebAcl.attrArn,
  });
});

new CfnOutput(stack, 'WidgetApiUrl', {
  value: widgetApi.url,
  description: 'Widget API base URL — hosted iframe app calls /widget/*',
});

backend.addOutput({
  custom: {
    WidgetActionApi: {
      url: widgetApi.url,
    },
  },
});

// ── Tags ──
const tenantId = process.env.BEBO_TENANT_ID || 'core';
const tagEntries: Array<[string, string]> = [
  ['Project', 'bebocard'],
  ['Environment', branchName],
  ['TenantId', tenantId],
  ['ManagedBy', 'amplify-gen2'],
];
const identifiedStacks = new Set<Stack>([dataStack, stack]);
const functionTags: Array<[lambda.Function, string, string]> = [
  [postConfirmLambda, 'post-confirmation', 'user-side'],
  [cardManagerLambda, 'card-manager', 'user-side'],
  [scanLambda, 'scan-handler', 'tenant-side'],
  [tenantLinkerLambda, 'tenant-linker', 'user-side'],
  [geofenceLambda, 'geofence-handler', 'user-side'],
  [validatorLambda, 'content-validator', 'tenant-side'],
  [brandApiLambda, 'brand-api-handler', 'tenant-side'],
  [reminderLambda, 'reminder-handler', 'user-side'],
  [tenantAnalyticsLambda, 'tenant-analytics', 'tenant-side'],
  [segmentLambda, 'segment-processor', 'tenant-side'],
  [receiptIcebergLambda, 'receipt-iceberg', 'tenant-side'],
  [paymentRouterLambda, 'payment-router', 'tenant-side'],
  [widgetActionLambda, 'widget-action-handler', 'tenant-side'],
];

for (const fn of functionTags.map(t => t[0])) identifiedStacks.add(Stack.of(fn));
for (const s of identifiedStacks) {
  for (const [key, value] of tagEntries) Tags.of(s).add(key, value);
}
for (const [fn, functionName, costCenter] of functionTags) {
  Tags.of(fn).add('Function', functionName);
  Tags.of(fn).add('CostCenter', costCenter);
}

// ── Consent Handler (Phase 6 — Consent-Gated Identity Release) ───────────────
// SQS queue for 60-second consent timeout checks.
// Lambda handles REST (POST /consent-request) and SQS timeout events.

const consentLambda = backend.consentHandlerFn.resources.lambda as lambda.Function;
consentLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
consentLambda.addEnvironment('USER_TABLE', userTable.tableName);
consentLambda.addEnvironment('REF_TABLE', refTable.tableName);
adminTable.grantReadWriteData(consentLambda);
userTable.grantReadData(consentLambda);
refTable.grantReadData(consentLambda);

const consentTimeoutQueue = new sqs.Queue(stack, 'ConsentTimeoutQueue', {
  visibilityTimeout: Duration.seconds(60),
  deadLetterQueue: {
    queue: new sqs.Queue(stack, 'ConsentTimeoutDLQ', {
      retentionPeriod: Duration.days(7),
    }),
    maxReceiveCount: 3,
  },
});
consentTimeoutQueue.grantSendMessages(consentLambda);
consentTimeoutQueue.grantConsumeMessages(consentLambda);
consentLambda.addEnvironment('CONSENT_TIMEOUT_QUEUE_URL', consentTimeoutQueue.queueUrl);
consentLambda.addEventSource(new SqsEventSource(consentTimeoutQueue, { batchSize: 1 }));

const consentApi = new apigw.RestApi(stack, 'ConsentApi', {
  restApiName: `bebo-consent-api-${stage}`,
  description: 'Consent-Gated Identity Release API — brand POS backends request user identity fields',
  defaultCorsPreflightOptions: {
    allowOrigins: apigw.Cors.ALL_ORIGINS,
    allowMethods: ['GET', 'POST', 'OPTIONS'],
    allowHeaders: ['x-api-key', 'Content-Type'],
  },
});

const consentIntegration = new apigw.LambdaIntegration(consentLambda);
const consentRequestResource = consentApi.root.addResource('consent-request');
consentRequestResource.addMethod('POST', consentIntegration, { apiKeyRequired: true });
const consentIdResource = consentRequestResource.addResource('{requestId}');
consentIdResource.addResource('status').addMethod('GET', consentIntegration, { apiKeyRequired: true });

const consentPlan = consentApi.addUsagePlan('ConsentPlan', {
  name: 'consent-standard',
  throttle: { rateLimit: 100, burstLimit: 200 },
  quota: { limit: 10_000, period: apigw.Period.DAY },
});
consentPlan.addApiStage({ api: consentApi, stage: consentApi.deploymentStage });

new CfnOutput(stack, 'ConsentApiUrl', {
  value: consentApi.url,
  description: 'Consent API base URL — brand POS backends call POST /consent-request',
});

Tags.of(consentApi).add('CostCenter', 'tenant-side');
Tags.of(consentApi).add('Function', 'consent-api');
Tags.of(consentTimeoutQueue).add('CostCenter', 'tenant-side');

// ── Subscription Proxy (Phase 7 — Subscription Revocation Proxy) ─────────────
// REST API for brand-initiated recurring charge registration and user-initiated
// cancellation proxy. No SQS queue needed — cancellations are synchronous.

const subscriptionLambda = backend.subscriptionProxyFn.resources.lambda as lambda.Function;
subscriptionLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
subscriptionLambda.addEnvironment('USER_TABLE', userTable.tableName);
subscriptionLambda.addEnvironment('REF_TABLE', refTable.tableName);
adminTable.grantReadData(subscriptionLambda);
userTable.grantReadWriteData(subscriptionLambda);
refTable.grantReadData(subscriptionLambda);

const recurringApi = new apigw.RestApi(stack, 'RecurringApi', {
  restApiName: `bebo-recurring-api-${stage}`,
  description: 'Subscription Revocation Proxy API — brand registers recurring charges, user cancels via app',
  defaultCorsPreflightOptions: {
    allowOrigins: apigw.Cors.ALL_ORIGINS,
    allowMethods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
    allowHeaders: ['x-api-key', 'Content-Type'],
  },
});

const recurringIntegration = new apigw.LambdaIntegration(subscriptionLambda);
const recurringResource = recurringApi.root.addResource('recurring');
recurringResource.addResource('register').addMethod('POST', recurringIntegration, { apiKeyRequired: true });
const subIdResource = recurringResource.addResource('{subId}');
subIdResource.addMethod('DELETE', recurringIntegration, { apiKeyRequired: true });
subIdResource.addResource('status').addMethod('GET', recurringIntegration, { apiKeyRequired: true });

const recurringPlan = recurringApi.addUsagePlan('RecurringPlan', {
  name: 'recurring-standard',
  throttle: { rateLimit: 100, burstLimit: 200 },
  quota: { limit: 10_000, period: apigw.Period.DAY },
});
recurringPlan.addApiStage({ api: recurringApi, stage: recurringApi.deploymentStage });

new CfnOutput(stack, 'RecurringApiUrl', {
  value: recurringApi.url,
  description: 'Recurring Subscription API base URL — brand backends call POST /recurring/register',
});

Tags.of(recurringApi).add('CostCenter', 'tenant-side');
Tags.of(recurringApi).add('Function', 'recurring-api');

// ── Gift Card Router (Phase 8 — Federated Gift Card Delivery) ─────────────────
// REST API for brand-initiated gift card delivery via the scan channel.
// Brands deliver gift cards to users using secondaryULID — no PII exposed.

const giftCardLambda = backend.giftCardRouterFn.resources.lambda as lambda.Function;
giftCardLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
giftCardLambda.addEnvironment('USER_TABLE', userTable.tableName);
giftCardLambda.addEnvironment('REF_TABLE', refTable.tableName);
adminTable.grantReadData(giftCardLambda);
userTable.grantReadWriteData(giftCardLambda);
refTable.grantReadData(giftCardLambda);

const giftCardApi = new apigw.RestApi(stack, 'GiftCardApi', {
  restApiName: `bebo-gift-card-api-${stage}`,
  description: 'Federated Gift Card Delivery API — brand delivers gift cards to users via secondaryULID',
  defaultCorsPreflightOptions: {
    allowOrigins: apigw.Cors.ALL_ORIGINS,
    allowMethods: ['GET', 'POST', 'OPTIONS'],
    allowHeaders: ['x-api-key', 'Content-Type'],
  },
});

const giftCardIntegration = new apigw.LambdaIntegration(giftCardLambda);
const giftCardResource = giftCardApi.root.addResource('gift-card');
giftCardResource.addResource('deliver').addMethod('POST', giftCardIntegration, { apiKeyRequired: true });
const deliveryIdResource = giftCardResource.addResource('{deliveryId}');
deliveryIdResource.addResource('status').addMethod('GET', giftCardIntegration, { apiKeyRequired: true });

const giftCardPlan = giftCardApi.addUsagePlan('GiftCardPlan', {
  name: 'gift-card-standard',
  throttle: { rateLimit: 50, burstLimit: 100 },
  quota: { limit: 5_000, period: apigw.Period.DAY },
});
giftCardPlan.addApiStage({ api: giftCardApi, stage: giftCardApi.deploymentStage });

new CfnOutput(stack, 'GiftCardApiUrl', {
  value: giftCardApi.url,
  description: 'Gift Card Delivery API base URL — brand backends call POST /gift-card/deliver',
});

Tags.of(giftCardApi).add('CostCenter', 'tenant-side');
Tags.of(giftCardApi).add('Function', 'gift-card-api');

// ── Enrollment Handler (Phase 9 — Enrollment Marketplace) ────────────────────
// Brand-initiated: brand pushes enrollment offer via REST; user accepts/declines in app.
// User-initiated: user taps "Join" in app; alias generated and delivered to brand webhook.

const enrollmentLambda = backend.enrollmentHandlerFn.resources.lambda as lambda.Function;
enrollmentLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
enrollmentLambda.addEnvironment('USER_TABLE', userTable.tableName);
enrollmentLambda.addEnvironment('REF_TABLE', refTable.tableName);
adminTable.grantReadWriteData(enrollmentLambda);
userTable.grantReadWriteData(enrollmentLambda);
refTable.grantReadData(enrollmentLambda);

const enrollmentApi = new apigw.RestApi(stack, 'EnrollmentApi', {
  restApiName: `bebo-enrollment-api-${stage}`,
  description: 'Enrollment Marketplace API — brand sends enrollment offers, user accepts via app',
  defaultCorsPreflightOptions: {
    allowOrigins: apigw.Cors.ALL_ORIGINS,
    allowMethods: ['GET', 'POST', 'OPTIONS'],
    allowHeaders: ['x-api-key', 'Content-Type'],
  },
});

const enrollmentIntegration = new apigw.LambdaIntegration(enrollmentLambda);
const enrollResource = enrollmentApi.root.addResource('enroll');
enrollResource.addMethod('POST', enrollmentIntegration, { apiKeyRequired: true });
const enrollIdResource = enrollResource.addResource('{enrollmentId}');
enrollIdResource.addResource('status').addMethod('GET', enrollmentIntegration, { apiKeyRequired: true });

const enrollmentPlan = enrollmentApi.addUsagePlan('EnrollmentPlan', {
  name: 'enrollment-standard',
  throttle: { rateLimit: 100, burstLimit: 200 },
  quota: { limit: 10_000, period: apigw.Period.DAY },
});
enrollmentPlan.addApiStage({ api: enrollmentApi, stage: enrollmentApi.deploymentStage });

new CfnOutput(stack, 'EnrollmentApiUrl', {
  value: enrollmentApi.url,
  description: 'Enrollment API base URL — brand backends call POST /enroll',
});

Tags.of(enrollmentApi).add('CostCenter', 'tenant-side');
Tags.of(enrollmentApi).add('Function', 'enrollment-api');

// ── SMB Handler (Phase 11 — SMB Loyalty-as-a-Service) ─────────────────────────
// Stamp card loyalty program for small brands.
// Routes: POST /smb/stamp, POST /smb/redeem, GET /smb/card, GET /smb/analytics

const smbLambda = backend.smbHandlerFn.resources.lambda as lambda.Function;
smbLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
smbLambda.addEnvironment('USER_TABLE', userTable.tableName);
smbLambda.addEnvironment('REF_TABLE', refTable.tableName);
adminTable.grantReadWriteData(smbLambda);
userTable.grantReadWriteData(smbLambda);
refTable.grantReadWriteData(smbLambda);

const smbApi = new apigw.RestApi(stack, 'SmbApi', {
  restApiName: `bebo-smb-api-${stage}`,
  description: 'SMB Loyalty-as-a-Service API — stamp cards, redemptions, SMB lite analytics',
  defaultCorsPreflightOptions: {
    allowOrigins: apigw.Cors.ALL_ORIGINS,
    allowMethods: ['GET', 'POST', 'OPTIONS'],
    allowHeaders: ['x-api-key', 'Content-Type'],
  },
});

const smbIntegration = new apigw.LambdaIntegration(smbLambda);
const smbResource = smbApi.root.addResource('smb');
smbResource.addResource('stamp').addMethod('POST', smbIntegration, { apiKeyRequired: true });
smbResource.addResource('redeem').addMethod('POST', smbIntegration, { apiKeyRequired: true });
smbResource.addResource('card').addMethod('GET', smbIntegration, { apiKeyRequired: true });
smbResource.addResource('analytics').addMethod('GET', smbIntegration, { apiKeyRequired: true });

// Three-tier usage plans matching SMB pricing tiers
const smbStarterPlan = smbApi.addUsagePlan('SmbStarterPlan', {
  name: 'smb-starter',
  throttle: { rateLimit: 10, burstLimit: 20 },
  quota: { limit: 500, period: apigw.Period.MONTH },
});
smbStarterPlan.addApiStage({ api: smbApi, stage: smbApi.deploymentStage });

const smbGrowthPlan = smbApi.addUsagePlan('SmbGrowthPlan', {
  name: 'smb-growth',
  throttle: { rateLimit: 30, burstLimit: 60 },
  quota: { limit: 2000, period: apigw.Period.MONTH },
});
smbGrowthPlan.addApiStage({ api: smbApi, stage: smbApi.deploymentStage });

const smbBusinessPlan = smbApi.addUsagePlan('SmbBusinessPlan', {
  name: 'smb-business',
  throttle: { rateLimit: 100, burstLimit: 200 },
  // No hard monthly cap for business tier
});
smbBusinessPlan.addApiStage({ api: smbApi, stage: smbApi.deploymentStage });

new CfnOutput(stack, 'SmbApiUrl', {
  value: smbApi.url,
  description: 'SMB Loyalty API base URL — POST /smb/stamp to stamp a user card at checkout',
});

Tags.of(smbApi).add('CostCenter', 'tenant-side');
Tags.of(smbApi).add('Function', 'smb-api');
Tags.of(smbLambda).add('Function', 'smb-handler');
Tags.of(smbLambda).add('CostCenter', 'tenant-side');

// ── Gift Card Handler (Phase 13 — Gift Card Marketplace) ─────────────────────
// AppSync resolver (purchaseForSelf, purchaseAsGift, syncGiftCardBalance) +
// REST (POST /webhook for Stripe, GET /gift/:token for claim resolution).

const giftCardHandlerLambda = backend.giftCardHandlerFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => giftCardHandlerLambda.addEnvironment(k, v));
userTable.grantReadWriteData(giftCardHandlerLambda);
refTable.grantReadData(giftCardHandlerLambda);
adminTable.grantReadWriteData(giftCardHandlerLambda);

// KMS key for gift card PIN transit encryption
const giftCardKmsKey = new kms.Key(stack, 'GiftCardKmsKey', {
  description: 'Encrypts gift card cardNumber + PIN in transit (GIFT# AdminDataEvent records)',
  enableKeyRotation: true,
  removalPolicy: RemovalPolicy.RETAIN,
});
giftCardKmsKey.grantEncryptDecrypt(giftCardHandlerLambda);
giftCardHandlerLambda.addEnvironment('GIFT_CARD_KMS_KEY_ARN', giftCardKmsKey.keyArn);

// SES send permission
giftCardHandlerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ses:SendEmail', 'ses:SendRawEmail'],
  resources: ['*'],
}));

const giftCardMarketplaceApi = new apigw.RestApi(stack, 'GiftCardMarketplaceApi', {
  restApiName: `bebo-gift-card-marketplace-${stage}`,
  description: 'Gift Card Marketplace public REST endpoints — Stripe webhook + gift claim',
  defaultCorsPreflightOptions: {
    allowOrigins: apigw.Cors.ALL_ORIGINS,
    allowMethods: ['GET', 'POST', 'OPTIONS'],
    allowHeaders: ['Content-Type', 'stripe-signature'],
  },
});

const giftMarketIntegration = new apigw.LambdaIntegration(giftCardHandlerLambda);
giftCardMarketplaceApi.root
  .addResource('webhook')
  .addMethod('POST', giftMarketIntegration);

const giftResource = giftCardMarketplaceApi.root.addResource('gift');
giftResource
  .addResource('{token}')
  .addMethod('GET', giftMarketIntegration);

new CfnOutput(stack, 'GiftCardMarketplaceApiUrl', {
  value: giftCardMarketplaceApi.url,
  description: 'Gift Card Marketplace REST API — POST /webhook (Stripe), GET /gift/{token}',
});

Tags.of(giftCardHandlerLambda).add('Function', 'gift-card-handler');
Tags.of(giftCardHandlerLambda).add('CostCenter', 'marketplace');

// ── Catalog Sync (Phase 13 — Gift Card Catalog) ───────────────────────────────
// Weekly EventBridge cron pulls distributor catalogs and upserts GIFTCARD#
// records into RefDataEvent.

const catalogSyncLambda = backend.catalogSyncFn.resources.lambda as lambda.Function;
catalogSyncLambda.addEnvironment('REFDATA_TABLE', refTable.tableName);
refTable.grantReadWriteData(catalogSyncLambda);

new events.Rule(Stack.of(catalogSyncLambda), 'WeeklyCatalogSyncRule', {
  schedule: events.Schedule.cron({ weekDay: 'SUN', hour: '2', minute: '0' }),
  targets: [new eventsTargets.LambdaFunction(catalogSyncLambda)],
});

Tags.of(catalogSyncLambda).add('Function', 'catalog-sync');
Tags.of(catalogSyncLambda).add('CostCenter', 'marketplace');

// ── Subscription Catalog Sync ─────────────────────────────────────────────────
// Weekly cron (Sunday 03:00 UTC) — upserts subscription provider catalog and
// BENCHMARK# records into RefDataEvent for the marketplace page and negotiator.

const catalogSubscriptionSyncLambda = backend.catalogSubscriptionSyncFn.resources.lambda as lambda.Function;
catalogSubscriptionSyncLambda.addEnvironment('REFDATA_TABLE', refTable.tableName);
refTable.grantReadWriteData(catalogSubscriptionSyncLambda);

new events.Rule(Stack.of(catalogSubscriptionSyncLambda), 'WeeklySubscriptionCatalogSyncRule', {
  schedule: events.Schedule.cron({ weekDay: 'SUN', hour: '3', minute: '0' }),
  targets: [new eventsTargets.LambdaFunction(catalogSubscriptionSyncLambda)],
});

Tags.of(catalogSubscriptionSyncLambda).add('Function', 'catalog-subscription-sync');
Tags.of(catalogSubscriptionSyncLambda).add('CostCenter', 'marketplace');

// ── Gift Claim Web Fallback (Phase 13 — web claim page for non-app recipients) ─
// S3 bucket + CloudFront distribution serving the static claim page at
// app.bebocard.com/gift/* for recipients who don't have the app installed.
// iOS Universal Links / Android App Links intercepts the same URL for app users.

const giftClaimBucket = new s3.Bucket(stack, 'GiftClaimWebBucket', {
  removalPolicy: RemovalPolicy.RETAIN,
  // No public access — served exclusively via CloudFront OAC
  blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
});

const giftClaimDistribution = new cloudfront.Distribution(stack, 'GiftClaimDistribution', {
  comment: 'BeboCard gift claim web fallback — app.bebocard.com/gift/*',
  defaultBehavior: {
    origin: cfOrigins.S3BucketOrigin.withOriginAccessControl(giftClaimBucket),
    viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
    cachePolicy: cloudfront.CachePolicy.CACHING_DISABLED,
  },
  defaultRootObject: 'index.html',
  errorResponses: [
    // Route all paths (including /gift/<token>) to index.html — JS extracts the token
    {
      httpStatus: 403,
      responseHttpStatus: 200,
      responsePagePath: '/index.html',
    },
    {
      httpStatus: 404,
      responseHttpStatus: 200,
      responsePagePath: '/index.html',
    },
  ],
});

new s3deploy.BucketDeployment(stack, 'GiftClaimWebDeploy', {
  sources: [s3deploy.Source.asset('./gift-claim-web')],
  destinationBucket: giftClaimBucket,
  distribution: giftClaimDistribution,
  distributionPaths: ['/*'],
});

new CfnOutput(stack, 'GiftClaimWebUrl', {
  value: `https://${giftClaimDistribution.distributionDomainName}`,
  description: 'Gift claim web fallback — map app.bebocard.com/gift/* to this CloudFront distribution',
});

Tags.of(giftClaimBucket).add('CostCenter', 'marketplace');
Tags.of(giftClaimDistribution).add('CostCenter', 'marketplace');

// ── Discovery Handler (public brand/offer/catalogue/newsletter feeds) ─────────
// No API key required — public read-only catalog data for the app discovery tab.
// Rate-limited by WAF (shared rule group). Returns region-filtered paginated results.

const discoveryLambda = backend.discoveryHandlerFn.resources.lambda as lambda.Function;
discoveryLambda.addEnvironment('REFDATA_TABLE', refTable.tableName);
refTable.grantReadData(discoveryLambda);

const discoveryApi = new apigw.RestApi(stack, 'DiscoveryApi', {
  restApiName: 'bebo-discovery-api',
  defaultCorsPreflightOptions: {
    allowOrigins: apigw.Cors.ALL_ORIGINS,
    allowMethods: ['GET', 'OPTIONS'],
    allowHeaders: ['Authorization', 'Content-Type'],
  },
});

const discoveryIntegration = new apigw.LambdaIntegration(discoveryLambda);
const discoverResource = discoveryApi.root.addResource('discover');
discoverResource.addResource('brands').addMethod('GET', discoveryIntegration);
discoverResource.addResource('offers').addMethod('GET', discoveryIntegration);
discoverResource.addResource('catalogues').addMethod('GET', discoveryIntegration);
discoverResource.addResource('newsletters').addMethod('GET', discoveryIntegration);

new CfnOutput(stack, 'DiscoveryApiUrl', {
  value: discoveryApi.url,
  description: 'Discovery REST API — GET /discover/{brands|offers|catalogues|newsletters}',
});

Tags.of(discoveryLambda).add('Function', 'discovery-handler');
Tags.of(discoveryLambda).add('CostCenter', 'platform');

// ── Gift Card Refund (Phase 13 — Auto-refund unredeemed gifts) ───────────────
// Daily EventBridge cron queries AdminTable for expired unclaimed gifts, decrypts
// PIN via KMS, writes back to sender's UserTable wallet.

const giftCardRefundLambda = backend.giftCardRefundFn.resources.lambda as lambda.Function;
giftCardRefundLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
giftCardRefundLambda.addEnvironment('USER_TABLE', userTable.tableName);
giftCardRefundLambda.addEnvironment('GIFT_CARD_KMS_KEY_ARN', giftCardKmsKey.keyArn);
// Firebase is sent from the existing environment var

adminTable.grantReadWriteData(giftCardRefundLambda);
userTable.grantReadWriteData(giftCardRefundLambda);
giftCardKmsKey.grantEncryptDecrypt(giftCardRefundLambda);

new events.Rule(Stack.of(giftCardRefundLambda), 'DailyGiftCardRefundRule', {
  schedule: events.Schedule.cron({ hour: '3', minute: '0' }), // Nightly 3 AM UTC
  targets: [new eventsTargets.LambdaFunction(giftCardRefundLambda)],
});

Tags.of(giftCardRefundLambda).add('Function', 'gift-card-refund');
Tags.of(giftCardRefundLambda).add('CostCenter', 'marketplace');

// ── Subscription Negotiator (Phase 14 — Subscription Intelligence) ───────────

const subscriptionNegotiatorLambda = backend.subscriptionNegotiator.resources.lambda as lambda.Function;
subscriptionNegotiatorLambda.addEnvironment('USER_TABLE', userTable.tableName);
subscriptionNegotiatorLambda.addEnvironment('REF_TABLE', refTable.tableName);

userTable.grantReadWriteData(subscriptionNegotiatorLambda);
refTable.grantReadData(subscriptionNegotiatorLambda);

new events.Rule(Stack.of(subscriptionNegotiatorLambda), 'DailySubscriptionNegotiatorRule', {
  schedule: events.Schedule.cron({ hour: '2', minute: '0' }), // Nightly 2 AM UTC
  targets: [new eventsTargets.LambdaFunction(subscriptionNegotiatorLambda)],
});

Tags.of(subscriptionNegotiatorLambda).add('Function', 'subscription-negotiator');

// ── Billing Run Handler (Monthly Overage Invoicing) ──────────────────────────
// EventBridge cron on the 1st of each month at 3 AM UTC — iterates all active
// tenants, calculates per-category overage, creates Stripe invoice items, and
// sends billing summary emails via SES.

const billingRunLambda = backend.billingRunHandlerFn.resources.lambda as lambda.Function;
billingRunLambda.addEnvironment('REFDATA_TABLE', refTable.tableName);
refTable.grantReadWriteData(billingRunLambda);

// SES for billing summary emails
billingRunLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ses:SendEmail', 'ses:SendRawEmail'],
  resources: ['*'],
}));

new events.Rule(Stack.of(billingRunLambda), 'MonthlyBillingRunRule', {
  schedule: events.Schedule.cron({ day: '1', hour: '3', minute: '0' }),
  targets: [new eventsTargets.LambdaFunction(billingRunLambda)],
});

Tags.of(billingRunLambda).add('Function', 'billing-run-handler');
Tags.of(billingRunLambda).add('CostCenter', 'tenant-side');

export default backend;
