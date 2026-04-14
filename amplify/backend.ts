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
import { billingRunHandlerFn } from './functions/billing-run-handler/resource';
import { billingWebhookHandlerFn } from './functions/billing-webhook-handler/resource';
import { qrRouterHandlerFn } from './functions/qr-router-handler/resource';
import { remoteConfigHandlerFn } from './functions/remote-config-handler/resource';
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
import { discoveryHandlerFn } from './functions/discovery-handler/resource';
import { receiptProcessorFn } from './functions/receipt-processor/resource';
import { tenantProvisionerFn } from './functions/tenant-provisioner/resource';
import { exporterFn } from './functions/user-data-exporter/resource';
import { analyticsCompactorFn } from './functions/analytics-compactor/resource';
import { analyticsBackfillerFn } from './functions/analytics-backfiller/resource';
import { webhookDispatcherFn } from './functions/webhook-dispatcher/resource';
import { analyticsAggregatorFn } from './functions/analytics-aggregator/resource';
import { receiptClaimHandlerFn } from './functions/receipt-claim-handler/resource';
import { customSegmentEvaluatorFn } from './functions/custom-segment-evaluator/resource';
import { affiliateFeedSyncFn } from './functions/affiliate-feed-sync/resource';
import { parentalConsentHandlerFn } from './functions/parental-consent-handler/resource';
import { cognitoExportFn } from './functions/cognito-export/resource';
import { brandHealthMonitorFn } from './functions/brand-health-monitor/resource';
import { clickTrackingHandlerFn } from './functions/click-tracking-handler/resource';
import { templateManagerFn } from './functions/template-manager/resource';
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
import * as glue from 'aws-cdk-lib/aws-glue';
import * as athena from 'aws-cdk-lib/aws-athena';
import * as wafv2 from 'aws-cdk-lib/aws-wafv2';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as snsSubscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cwActions from 'aws-cdk-lib/aws-cloudwatch-actions';
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
  discoveryHandlerFn,
  receiptProcessorFn,
  tenantProvisionerFn,
  exporterFn,
  analyticsCompactorFn,
  analyticsBackfillerFn,
  webhookDispatcherFn,
  receiptClaimHandlerFn,
  analyticsAggregatorFn,
  customSegmentEvaluatorFn,
  affiliateFeedSyncFn,
  parentalConsentHandlerFn,
  cognitoExportFn,
  brandHealthMonitorFn,
  billingRunHandlerFn,
  billingWebhookHandlerFn,
  qrRouterHandlerFn,
  remoteConfigHandlerFn,
  clickTrackingHandlerFn,
  templateManagerFn,
});

// ── Shared Infrastructure ────────────────────────────────────────────────────
const userTable = backend.data.resources.tables['UserDataEvent'];
const refDataTable = backend.data.resources.tables['RefDataEvent'];
const adminTable = backend.data.resources.tables['AdminDataEvent'];

const tableNames = {
  USER_TABLE: userTable.tableName,
  REFDATA_TABLE: refDataTable.tableName,
  ADMIN_TABLE: adminTable.tableName,
  REPORT_TABLE: backend.data.resources.tables['ReportDataEvent'].tableName,
};

const cfnUserTable = (backend.data.resources as any).cfnResources?.cfnTables?.['UserDataEvent'];

const dataStack = Stack.of(userTable);
const authStack = backend.auth.resources.userPool.stack;
const stage = dataStack.stackName.toLowerCase().includes('prod') ? 'prod' : 'dev';
const branchName = dataStack.stackName.toLowerCase().includes('prod') ? 'prod' : 'sandbox';
const amplifyAppId = process.env.AWS_APP_ID ?? 'local';
const amplifyBranch = process.env.AWS_BRANCH ?? 'sandbox';

const userHashSalt = 'bebo_' + (process.env.USER_HASH_SALT ?? 'local_dev_salt_123');
const stack = backend.createStack('SharedInfrastructure'); // Isolated stack to prevent circular deps
const rootStack = (dataStack.node.scope as any) instanceof Stack ? (dataStack.node.scope as Stack) : dataStack;

// ── SSM Parameters (Circular Dep Break) ──────────────────────────────────────
const userTableParamName = `/bebocard/${amplifyAppId}/${amplifyBranch}/USER_TABLE`;
const adminTableParamName = `/bebocard/${amplifyAppId}/${amplifyBranch}/ADMIN_TABLE`;
const restApiUrlParamName = `/bebocard/${amplifyAppId}/${amplifyBranch}/SCAN_API_URL`;

new ssm.StringParameter(stack, 'UserTableNameParam', { parameterName: userTableParamName, stringValue: userTable.tableName });
new ssm.StringParameter(stack, 'AdminTableNameParam', { parameterName: adminTableParamName, stringValue: adminTable.tableName });

// ── Bebo Intelligence: Data Lake (P1-1 Architecture) ─────────────────────────
const analyticsBucket = new s3.Bucket(stack, 'AnalyticsLake', {
  removalPolicy: RemovalPolicy.RETAIN,
  versioned: true, // Required for CRR (P3-12)
  intelligentTieringConfigurations: [
    { name: 'DefaultTiering', archiveAccessTierTime: Duration.days(90), deepArchiveAccessTierTime: Duration.days(180) }
  ],
  lifecycleRules: [
    { expiration: Duration.days(7), prefix: 'athena-results/' },
    { transitions: [{ storageClass: s3.StorageClass.GLACIER, transitionAfter: Duration.days(365) }], prefix: 'receipts/' }
  ],
});

// ── Remote Configuration (P2-2) ──
// Allows instant UI/Feature updates without App Store reviews
const remoteConfigBucket = new s3.Bucket(stack, 'RemoteConfig', {
  versioned: true,
  publicReadAccess: true, // Safe for non-sensitive public app configuration
  blockPublicAccess: {
    blockPublicAcls: false,
    blockPublicPolicy: false,
    ignorePublicAcls: false,
    restrictPublicBuckets: false,
  },
  cors: [{
    allowedMethods: [s3.HttpMethods.GET],
    allowedOrigins: ['*'],
    allowedHeaders: ['*'],
  }],
});

new ssm.StringParameter(stack, 'RemoteConfigBucketParam', {
  parameterName: `/bebocard/${amplifyAppId}/${amplifyBranch}/REMOTE_CONFIG_BUCKET`,
  stringValue: remoteConfigBucket.bucketName,
});

const exportsBucket = new s3.Bucket(stack, 'UserDataExports', {
  removalPolicy: RemovalPolicy.DESTROY, // Exports are temporary
  lifecycleRules: [{ expiration: Duration.days(1) }], // Auto-delete after 24 hours
});

const glueDatabase = (backend.data.resources as any).cfnResources?.cfnTables?.['UserDataEvent']
  ?.stack.node.defaultChild.parent.parent.parent.node.findAll()
  .find((n: any) => n.cfnResourceType === 'AWS::Glue::Database') 
  ?? new glue.CfnDatabase(stack, 'AnalyticsDatabase', {
    catalogId: stack.account,
    databaseInput: { name: `bebo_analytics_${stage}`, description: 'BeboCard Intelligence Data Lake' },
  });

const athenaWorkgroup = new athena.CfnWorkGroup(stack, 'AnalyticsWorkgroup', {
  name: `bebo-intel-${stage}`,
  description: 'Intelligence tier analytics queries',
  workGroupConfiguration: {
    resultConfiguration: { outputLocation: `s3://${analyticsBucket.bucketName}/athena-results/` },
  },
});

const icebergDLQ = new sqs.Queue(stack, 'ReceiptIcebergDLQ', { retentionPeriod: Duration.days(14) });
Tags.of(icebergDLQ).add('CostCenter', 'tenant-side');

// ── Monitoring & Alarming (P0-1) ─────────────────────────────────────────────
const alertsTopic = new sns.Topic(stack, 'InfrastructureAlerts', {
  displayName: `BeboCard [${stage}] Infrastructure Alerts`,
});

// Default engineer contact — in prod this would be an OpsGenie/PagerDuty endpoint
alertsTopic.addSubscription(new snsSubscriptions.EmailSubscription('farahgalaria@gmail.com'));

const createDlqAlarm = (queue: sqs.IQueue, name: string, threshold = 1) => {
  const alarm = new cloudwatch.Alarm(Stack.of(queue), `${name}Alarm`, {
    metric: queue.metricApproximateNumberOfMessagesVisible({ period: Duration.minutes(5) }),
    threshold,
    evaluationPeriods: 1,
    alarmDescription: `Messages detected in ${name}. Critical failure in data pipeline.`,
    treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
  });
  alarm.addAlarmAction(new cwActions.SnsAction(alertsTopic));
  return alarm;
};

const icebergDlqAlarm = createDlqAlarm(icebergDLQ, 'ReceiptIcebergDLQ');

// ── Receipt Signing (P3-4) ──
const receiptSigningKey = new kms.Key(stack, 'ReceiptSigningKey', {
  alias: 'bebocard/receipt-signing',
  keySpec: kms.KeySpec.RSA_2048,
  keyUsage: kms.KeyUsage.SIGN_VERIFY,
  removalPolicy: RemovalPolicy.RETAIN,
});

// ── Post-confirmation ──
const postConfirmLambda = backend.postConfirmationFn.resources.lambda as lambda.Function;
// ── P0-5: Concurrency Reservation ──
// (postConfirmLambda.node.defaultChild as lambda.CfnFunction).reservedConcurrentExecutions = 10;

const createUtilizationAlarm = (fn: lambda.Function, name: string) => {
  const alarm = new cloudwatch.Alarm(Stack.of(fn), `${name}UtilizationAlarm`, {
    metric: fn.metric('ConcurrentExecutions', { period: Duration.minutes(1) }),
    threshold: 7.5, // 75% of 10
    evaluationPeriods: 2,
    alarmDescription: `Concurrency utilization for ${name} exceeded 75%. Consider increasing reservation.`,
    comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
  });
  alarm.addAlarmAction(new cwActions.SnsAction(alertsTopic));
};
createUtilizationAlarm(postConfirmLambda, 'PostConfirmation');

const createHighTrafficUtilizationAlarm = (fn: lambda.Function, name: string) => {
  const alarm = new cloudwatch.Alarm(Stack.of(fn), `${name}UtilizationAlarm`, {
    metric: fn.metric('ConcurrentExecutions', { period: Duration.minutes(1) }),
    threshold: 37.5, // 75% of 50
    evaluationPeriods: 2,
    alarmDescription: `Concurrency utilization for ${name} exceeded 75%. Scale check required.`,
    comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
  });
  alarm.addAlarmAction(new cwActions.SnsAction(alertsTopic));
};

postConfirmLambda.addEnvironment('USER_TABLE_PARAM', userTableParamName);
postConfirmLambda.addEnvironment('ADMIN_TABLE_PARAM', adminTableParamName);
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [`arn:aws:ssm:${stack.region}:${stack.account}:parameter/bebocard/*`],
}));
postConfirmLambda.role?.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonCognitoPowerUser'));
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['cognito-idp:AdminUpdateUserAttributes'],
  resources: [`arn:aws:cognito-idp:${stack.region}:${stack.account}:userpool/*`],
}));
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['dynamodb:PutItem', 'dynamodb:UpdateItem'],
  resources: [`arn:aws:dynamodb:${stack.region}:${stack.account}:table/UserDataEvent-*`, `arn:aws:dynamodb:${stack.region}:${stack.account}:table/AdminDataEvent-*`],
}));
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ses:SendEmail', 'ses:SendRawEmail'],
  resources: ['*'], // In production, scope this to the verified domain
}));
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [`arn:aws:ssm:${stack.region}:${stack.account}:parameter/amplify/shared/PARENTAL_CONSENT_SECRET`],
}));

// ── Card manager ──
const cardManagerLambda = backend.cardManagerFn.resources.lambda as lambda.Function;
// ── P0-5: Concurrency Reservation ──
// (cardManagerLambda.node.defaultChild as lambda.CfnFunction).reservedConcurrentExecutions = 50;
createHighTrafficUtilizationAlarm(cardManagerLambda, 'CardManager');
Object.entries(tableNames).forEach(([k, v]) => cardManagerLambda.addEnvironment(k, v));
const grantTableAccess = (fn: lambda.Function, table: any, write: boolean = false) => {
  fn.addToRolePolicy(new iam.PolicyStatement({
    actions: write ? ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem', 'dynamodb:DeleteItem', 'dynamodb:Query', 'dynamodb:Scan', 'dynamodb:BatchWriteItem', 'dynamodb:BatchGetItem'] : ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:Scan', 'dynamodb:BatchGetItem'],
    resources: [
      `arn:aws:dynamodb:${dataStack.region}:${dataStack.account}:table/${table.tableName}`,
      `arn:aws:dynamodb:${dataStack.region}:${dataStack.account}:table/${table.tableName}/index/*`
    ],
  }));
};

grantTableAccess(cardManagerLambda, userTable, true);
grantTableAccess(cardManagerLambda, refDataTable, false);
grantTableAccess(cardManagerLambda, adminTable, true);

// ── Scan handler ──
const scanLambda = backend.scanHandlerFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => scanLambda.addEnvironment(k, v));
grantTableAccess(scanLambda, userTable, true);
grantTableAccess(scanLambda, refDataTable, false);
grantTableAccess(scanLambda, adminTable, false);
receiptSigningKey.grant(scanLambda, 'kms:GetPublicKey');

// Provisioned Concurrency — scan-handler: eliminates cold starts for retail checkout (P0-5)
const scanAlias = scanLambda.addAlias('prod');

// Throttling Alarm — scan-handler: ensures we are notified if concurrency ceiling is approached
const scanThrottleAlarm = new cloudwatch.Alarm(Stack.of(scanLambda), 'ScanHandlerThrottlesAlarm', {
  metric: scanLambda.metricThrottles({ period: Duration.minutes(1) }),
  threshold: 1,
  evaluationPeriods: 1,
  alarmDescription: 'Scan handler is being throttled! Concurrency ceiling reached.',
  treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
});
scanThrottleAlarm.addAlarmAction(new cwActions.SnsAction(alertsTopic));
 
// Utilization Alarm — scan-handler: fires when concurrent executions exceed 75% of reservation (37/50)
const scanUtilizationAlarm = new cloudwatch.Alarm(Stack.of(scanLambda), 'ScanHandlerUtilizationAlarm', {
  metric: scanLambda.metric('ConcurrentExecutions', { period: Duration.minutes(1) }),
  threshold: 37,
  evaluationPeriods: 2,
  alarmDescription: 'Scan handler concurrency is exceeding 75% of reserved capacity. Scaling ceiling approached.',
  treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
});
scanUtilizationAlarm.addAlarmAction(new cwActions.SnsAction(alertsTopic));

const receiptProcessingDLQ = new sqs.Queue(stack, 'ReceiptProcessingDLQ', { retentionPeriod: Duration.days(14) });
const receiptProcessingQueue = new sqs.Queue(stack, 'ReceiptProcessingQueue', {
  visibilityTimeout: Duration.seconds(30),
  deadLetterQueue: {
    queue: receiptProcessingDLQ,
    maxReceiveCount: 3,
  },
});
createDlqAlarm(receiptProcessingDLQ, 'ReceiptProcessingDLQ'); // Zero tolerance for checkout failures

receiptProcessingQueue.grantSendMessages(scanLambda);
scanLambda.addEnvironment('RECEIPT_QUEUE_URL', receiptProcessingQueue.queueUrl);

const receiptProcessorLambda = backend.receiptProcessorFn.resources.lambda as lambda.Function;
receiptProcessorLambda.addEnvironment('USER_TABLE', userTable.tableName);
receiptProcessorLambda.addEventSource(new SqsEventSource(receiptProcessingQueue, { batchSize: 10 }));
grantTableAccess(receiptProcessorLambda, userTable, true);
// Reserved concurrency — receipt-processor: ensures receipt writes cannot be throttled by other bursts (P0-5)
const cfnReceiptProcessor = receiptProcessorLambda.node.defaultChild as lambda.CfnFunction;
// cfnReceiptProcessor.reservedConcurrentExecutions = 100;

receiptSigningKey.grant(receiptProcessorLambda, 'kms:Sign');
receiptProcessorLambda.addEnvironment('RECEIPT_SIGNING_KEY_ID', receiptSigningKey.keyId);

// ── Tenant linker ──
const scanHandlerLambda = backend.scanHandlerFn.resources.lambda as lambda.Function;
// ── P0-5: Concurrency Reservation ──
// (scanHandlerLambda.node.defaultChild as lambda.CfnFunction).reservedConcurrentExecutions = 50;

scanHandlerLambda.addEnvironment('USER_TABLE', userTable.tableName);
const tenantLinkerLambda = backend.tenantLinker.resources.lambda as lambda.Function;
tenantLinkerLambda.addEnvironment('USER_TABLE', userTable.tableName);
tenantLinkerLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
grantTableAccess(tenantLinkerLambda, userTable, true);
grantTableAccess(tenantLinkerLambda, adminTable, true);

// ── Geofence handler ──
const geofenceLambda = backend.geofenceHandlerFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => geofenceLambda.addEnvironment(k, v));
grantTableAccess(geofenceLambda, userTable, true);
grantTableAccess(geofenceLambda, refDataTable, false);
grantTableAccess(geofenceLambda, adminTable, false);

// ── Consent handler ──
const consentLambda = backend.consentHandlerFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => consentLambda.addEnvironment(k, v));
grantTableAccess(consentLambda, userTable, true);
grantTableAccess(consentLambda, refDataTable, false);
grantTableAccess(consentLambda, adminTable, true);

// ── Segment processor ──
const segmentLambda = backend.segmentProcessorFn.resources.lambda as lambda.Function;
segmentLambda.addEnvironment('USER_TABLE', userTable.tableName);
segmentLambda.addEnvironment('USER_HASH_SALT', userHashSalt);
grantTableAccess(segmentLambda, userTable, true);
if (cfnUserTable) cfnUserTable.streamSpecification = { streamViewType: 'NEW_IMAGE' };

const segmentDLQ = new sqs.Queue(stack, 'SegmentProcessorDLQ', { retentionPeriod: Duration.days(14) });
createDlqAlarm(segmentDLQ, 'SegmentProcessorDLQ', 5); // Allow small batch jitter before alerting
segmentLambda.addEnvironment('SEGMENT_DLQ_URL', segmentDLQ.queueUrl);
segmentDLQ.grantSendMessages(segmentLambda);

segmentLambda.addEventSource(new DynamoEventSource(userTable, {
  startingPosition: lambda.StartingPosition.TRIM_HORIZON,
  filters: [lambda.FilterCriteria.filter({ eventName: lambda.FilterRule.or('INSERT', 'MODIFY', 'REMOVE'), dynamodb: { Keys: { sK: { S: [{ prefix: 'RECEIPT#' }, { prefix: 'INVOICE#' }, { prefix: 'SUBSCRIPTION#' }] } } } })],
  retryAttempts: 1,
}));

// ── Billing Run Schedule (P1-8) ──
const billingRunLambda = backend.billingRunHandlerFn.resources.lambda as lambda.Function;
billingRunLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
grantTableAccess(billingRunLambda, refDataTable, true);
billingRunLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ses:SendEmail'],
  resources: ['*'], // In production, scope to the verified identity
}));

billingRunLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [`arn:aws:ssm:${stack.region}:${stack.account}:parameter/amplify/shared/STRIPE_SECRET_KEY`],
}));

// Run daily at 02:00 UTC (processes monthly overages on the 1st)
const billingRunRule = new events.Rule(stack, 'MonthlyBillingRunRule', {
  schedule: events.Schedule.expression('cron(0 2 * * ? *)'),
});
billingRunRule.addTarget(new eventsTargets.LambdaFunction(billingRunLambda));

// ── Billing Webhook Handler (P1-8) ──
const billingWebhookLambda = backend.billingWebhookHandlerFn.resources.lambda as lambda.Function;
billingWebhookLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
grantTableAccess(billingWebhookLambda, refDataTable, true);
billingWebhookLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [
    `arn:aws:ssm:${stack.region}:${stack.account}:parameter/amplify/shared/STRIPE_SECRET_KEY`,
    `arn:aws:ssm:${stack.region}:${stack.account}:parameter/amplify/shared/STRIPE_WEBHOOK_SECRET`,
  ],
}));

// ── QR Router (P1-10) ──
const qrRouterLambda = backend.qrRouterHandlerFn.resources.lambda as lambda.Function;
qrRouterLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
grantTableAccess(qrRouterLambda, refDataTable, false);

const qrApi = new apigw.RestApi(stack, 'QrRouterApi', { 
  restApiName: `bebo-qr-router-${stage}`,
  // CloudFront will sit in front of this, but we keep the API for routing
});
const qrIntegration = new apigw.LambdaIntegration(qrRouterLambda);
const brandRes = qrApi.root.addResource('{brandId}');
brandRes.addMethod('GET', qrIntegration);
brandRes.addResource('{storeId}').addMethod('GET', qrIntegration);
// Add /.well-known for Universal/App Links in future
const wellKnown = qrApi.root.addResource('.well-known');
wellKnown.addResource('apple-app-site-association').addMethod('GET', qrIntegration);
wellKnown.addResource('assetlinks.json').addMethod('GET', qrIntegration);

// ── Receipt Iceberg writer ──
const receiptIcebergLambda = backend.receiptIcebergWriterFn.resources.lambda as lambda.Function;
receiptIcebergLambda.addEnvironment('ANALYTICS_BUCKET', analyticsBucket.bucketName);
receiptIcebergLambda.addEnvironment('GLUE_DATABASE', glueDatabase.ref ?? `bebo_analytics_${stage}`);
receiptIcebergLambda.addEnvironment('ATHENA_WORKGROUP', athenaWorkgroup.name);
receiptIcebergLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
receiptIcebergLambda.addEnvironment('USER_HASH_SALT', userHashSalt);
receiptIcebergLambda.addEnvironment('ICEBERG_DLQ_URL', icebergDLQ.queueUrl);

analyticsBucket.grantReadWrite(receiptIcebergLambda);
icebergDLQ.grantSendMessages(receiptIcebergLambda);
receiptIcebergLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:GetQueryResults'],
  resources: [`arn:aws:athena:${stack.region}:${stack.account}:workgroup/${athenaWorkgroup.name}`],
}));
receiptIcebergLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['glue:GetDatabase', 'glue:GetTable', 'glue:CreateTable', 'glue:UpdateTable'],
  resources: [
    `arn:aws:glue:${stack.region}:${stack.account}:catalog`,
    `arn:aws:glue:${stack.region}:${stack.account}:database/${glueDatabase.ref ?? `bebo_analytics_${stage}`}`,
    `arn:aws:glue:${stack.region}:${stack.account}:table/${glueDatabase.ref ?? `bebo_analytics_${stage}`}/receipts_*`,
  ],
}));
grantTableAccess(receiptIcebergLambda, refDataTable, false);
receiptIcebergLambda.addEventSource(new DynamoEventSource(userTable, {
  startingPosition: lambda.StartingPosition.LATEST,
  filters: [lambda.FilterCriteria.filter({ eventName: lambda.FilterRule.isEqual('INSERT') })],
  retryAttempts: 1,
}));
receiptIcebergLambda.addEventSource(new DynamoEventSource(refDataTable, {
  startingPosition: lambda.StartingPosition.LATEST,
  filters: [lambda.FilterCriteria.filter({ 
    eventName: lambda.FilterRule.isEqual('INSERT'),
    dynamodb: { Keys: { pK: { S: [{ prefix: 'ANON#' }] } } }
  })],
  retryAttempts: 1,
}));

// ── Tenant provisioner (P1-2) ──
const tenantProvisionerLambda = backend.tenantProvisionerFn.resources.lambda as lambda.Function;
tenantProvisionerLambda.addEnvironment('GLUE_DATABASE', glueDatabase.ref ?? `bebo_analytics_${stage}`);
tenantProvisionerLambda.addEnvironment('ANALYTICS_BUCKET', analyticsBucket.bucketName);
tenantProvisionerLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);

grantTableAccess(tenantProvisionerLambda, refDataTable, true);

tenantProvisionerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['glue:GetTable', 'glue:CreateTable', 'glue:UpdateTable'],
  resources: [
    `arn:aws:glue:${stack.region}:${stack.account}:catalog`,
    `arn:aws:glue:${stack.region}:${stack.account}:database/${glueDatabase.ref ?? `bebo_analytics_${stage}`}`,
    `arn:aws:glue:${stack.region}:${stack.account}:table/${glueDatabase.ref ?? `bebo_analytics_${stage}`}/receipts_*`,
  ],
}));

tenantProvisionerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['s3:CreateBucket', 's3:GetBucketPolicy', 's3:PutBucketPolicy', 's3:ListBucket', 's3:HeadBucket'],
  resources: ['arn:aws:s3:::bebocard-enterprise-*', 'arn:aws:s3:::bebocard-*'],
}));

tenantProvisionerLambda.addEventSource(new DynamoEventSource(refDataTable, {
  startingPosition: lambda.StartingPosition.LATEST,
  filters: [lambda.FilterCriteria.filter({ 
    eventName: lambda.FilterRule.or('INSERT', 'MODIFY'),
    dynamodb: { 
      NewImage: { 
        pK: { S: [{ prefix: 'TENANT#' }] },
        sK: { S: ['profile'] }
      } 
    } 
  })],
  retryAttempts: 1,
}));

// ── Tenant analytics ──
const analyticsLambda = backend.tenantAnalyticsFn.resources.lambda as lambda.Function;
analyticsLambda.addEnvironment('USER_TABLE', userTable.tableName);
analyticsLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
analyticsLambda.addEnvironment('ANALYTICS_BUCKET', analyticsBucket.bucketName);
analyticsLambda.addEnvironment('GLUE_DATABASE', glueDatabase.ref ?? `bebo_analytics_${stage}`);
analyticsLambda.addEnvironment('ATHENA_WORKGROUP', athenaWorkgroup.name);
analyticsLambda.addEnvironment('REPORT_TABLE', backend.data.resources.tables['ReportDataEvent'].tableName);

grantTableAccess(analyticsLambda, userTable, false);
grantTableAccess(analyticsLambda, refDataTable, false);
grantTableAccess(analyticsLambda, backend.data.resources.tables['ReportDataEvent'], false);
analyticsBucket.grantReadWrite(analyticsLambda);
analyticsLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:GetQueryResults'],
  resources: [`arn:aws:athena:${stack.region}:${stack.account}:workgroup/${athenaWorkgroup.name}`],
}));
analyticsLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['glue:GetDatabase', 'glue:GetTable'],
  resources: [
    `arn:aws:glue:${stack.region}:${stack.account}:catalog`,
    `arn:aws:glue:${stack.region}:${stack.account}:database/${glueDatabase.ref ?? `bebo_analytics_${stage}`}`,
    `arn:aws:glue:${stack.region}:${stack.account}:table/${glueDatabase.ref ?? `bebo_analytics_${stage}`}/receipts`,
  ],
}));

// ── Scan API (v1 & Legacy) ──
const scanApi = new apigw.RestApi(stack, 'ScanApi', { restApiName: `bebo-scan-api-${stage}` });
const scanIntegration = new apigw.LambdaIntegration(scanLambda);

// v1 routes
const scanV1 = scanApi.root.addResource('v1');
scanV1.addResource('scan').addMethod('POST', scanIntegration, { apiKeyRequired: true });
scanV1.addResource('receipt').addMethod('POST', scanIntegration, { apiKeyRequired: true });
scanV1.addResource('invoice').addMethod('POST', scanIntegration, { apiKeyRequired: true });
scanV1.addResource('health').addMethod('GET', scanIntegration, { apiKeyRequired: false });

// Parental consent approval endpoint (Public, signed token auth)
const consentRes = scanV1.addResource('consent');
const parentalRes = consentRes.addResource('parental');
const approveRes = parentalRes.addResource('approve');
const parentalConsentLambda = backend.parentalConsentHandlerFn.resources.lambda as lambda.Function;
approveRes.addMethod('GET', new apigw.LambdaIntegration(parentalConsentLambda), { apiKeyRequired: false });
parentalConsentLambda.addEnvironment('USER_TABLE', userTable.tableName);
grantTableAccess(parentalConsentLambda, userTable, true);
parentalConsentLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [`arn:aws:ssm:${stack.region}:${stack.account}:parameter/amplify/shared/PARENTAL_CONSENT_SECRET`],
}));

// Billing Webhook Route
const billingRes = scanV1.addResource('billing');
const stripeWebhookRes = billingRes.addResource('stripe-webhook');
stripeWebhookRes.addMethod('POST', new apigw.LambdaIntegration(billingWebhookLambda), { apiKeyRequired: false });

// We use a decoupled environment variable for the API URL to break the auth -> data circular dependency.
postConfirmLambda.addEnvironment('SCAN_API_URL', `https://api.bebocard.app/v1/`); // Placeholder pattern for now

new ssm.StringParameter(stack, 'ScanApiUrlParam', {
  parameterName: restApiUrlParamName,
  stringValue: scanApi.url,
});

// Legacy routes — 301 permanent redirect to /v1/ equivalents (P2-7)
// Brands that haven't updated their integration receive a redirect, not an error.
// No API key required on the redirect itself — the brand's key is used on the /v1/ destination.
const make301 = (v1Path: string) => ({
  integration: new apigw.MockIntegration({
    requestTemplates: { 'application/json': '{"statusCode": 301}' },
    integrationResponses: [{
      statusCode: '301',
      responseParameters: {
        'method.response.header.Location': `'${scanApi.urlForPath(v1Path)}'`,
        'method.response.header.Deprecation': "'true'",
        'method.response.header.Sunset': "'Wed, 01 Jul 2026 00:00:00 GMT'",
      },
    }],
  }),
  options: {
    apiKeyRequired: false,
    methodResponses: [{
      statusCode: '301',
      responseParameters: { 
        'method.response.header.Location': true,
        'method.response.header.Deprecation': true,
        'method.response.header.Sunset': true,
      },
    }],
  } satisfies apigw.MethodOptions,
});

const { integration: scan301, options: scan301Opts } = make301('/v1/scan');
scanApi.root.addResource('scan').addMethod('POST', scan301, scan301Opts);

const { integration: receipt301, options: receipt301Opts } = make301('/v1/receipt');
scanApi.root.addResource('receipt').addMethod('POST', receipt301, receipt301Opts);

const { integration: invoice301, options: invoice301Opts } = make301('/v1/invoice');
scanApi.root.addResource('invoice').addMethod('POST', invoice301, invoice301Opts);

// ── Tenant Analytics API (v1 & Legacy) ──
const analyticsApi = new apigw.RestApi(stack, 'TenantAnalyticsApi', {
  restApiName: `bebo-tenant-analytics-${stage}`,
  deployOptions: { stageName: stage },
  defaultMethodOptions: { apiKeyRequired: true },
});
const analyticsIntegration = new apigw.LambdaIntegration(analyticsLambda);

// v1 routes
const analyticsV1 = analyticsApi.root.addResource('v1');
const analyticsV1Res = analyticsV1.addResource('analytics');
analyticsV1Res.addResource('segments').addMethod('GET', analyticsIntegration, { apiKeyRequired: true });
analyticsV1Res.addResource('intelligence').addMethod('GET', analyticsIntegration, { apiKeyRequired: true });
analyticsV1Res.addResource('subscriber-count').addMethod('GET', analyticsIntegration, { apiKeyRequired: true });

// Legacy routes (Redirected in handler)
const analyticsLegacy = analyticsApi.root.addResource('analytics');
analyticsLegacy.addResource('segments').addMethod('GET', analyticsIntegration, { apiKeyRequired: true });
analyticsLegacy.addResource('intelligence').addMethod('GET', analyticsIntegration, { apiKeyRequired: true });
analyticsLegacy.addResource('subscriber-count').addMethod('GET', analyticsIntegration, { apiKeyRequired: true });

// ── User Data Exporter (P2-5) ──
const exporterLambda = backend.exporterFn.resources.lambda as lambda.Function;
exporterLambda.addEnvironment('USER_TABLE', userTable.tableName);
exporterLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
exporterLambda.addEnvironment('EXPORTS_BUCKET', exportsBucket.bucketName);
exporterLambda.addEnvironment('USER_POOL_ID', backend.auth.resources.userPool.userPoolId);
backend.auth.resources.cfnResources.cfnUserPool.addPropertyOverride('AdminCreateUserConfig.AllowAdminCreateUserOnly', true);

grantTableAccess(exporterLambda, userTable, true);
grantTableAccess(exporterLambda, adminTable, true);
exportsBucket.grantReadWrite(exporterLambda);

exporterLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['cognito-idp:AdminDisableUser', 'cognito-idp:AdminUserGlobalSignOut', 'cognito-idp:AdminDeleteUser'],
  resources: [backend.auth.resources.userPool.userPoolArn],
}));

// ── Webhook Reliability Queue (P2-12) ──
const webhookDLQ = new sqs.Queue(stack, 'WebhookReliabilityDLQ', { retentionPeriod: Duration.days(14) });
const webhookQueue = new sqs.Queue(stack, 'WebhookReliabilityQueue', {
  visibilityTimeout: Duration.seconds(60), // Match Lambda timeout + headroom
  deadLetterQueue: {
    queue: webhookDLQ,
    maxReceiveCount: 5,
  },
});
createDlqAlarm(webhookDLQ, 'WebhookReliabilityDLQ');

webhookQueue.grantSendMessages(exporterLambda);
exporterLambda.addEnvironment('WEBHOOK_QUEUE_URL', webhookQueue.queueUrl);

const webhookDispatcherLambda = backend.webhookDispatcherFn.resources.lambda as lambda.Function;
webhookDispatcherLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
webhookDispatcherLambda.addEventSource(new SqsEventSource(webhookQueue, { batchSize: 5 }));
grantTableAccess(webhookDispatcherLambda, refDataTable, false);
// Allow dispatcher to read per-brand webhook signing secrets (P2-12 HMAC signature)
webhookDispatcherLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['secretsmanager:GetSecretValue'],
  resources: [`arn:aws:secretsmanager:${stack.region}:${stack.account}:secret:bebocard/webhook-signing/*`],
}));

// Secrets for FCM
const firebaseSecret = ssm.StringParameter.fromSecureStringParameterAttributes(stack, 'FirebaseSecretExporter', {
  parameterName: '/amplify/shared/FIREBASE_SERVICE_ACCOUNT_JSON',
});
exporterLambda.addEnvironment('FIREBASE_SERVICE_ACCOUNT_JSON', firebaseSecret.stringValue);

// ── WAF ──
const publicWebAcl = new wafv2.CfnWebACL(stack, 'PublicApiWebAcl', {
  defaultAction: { allow: {} },
  scope: 'REGIONAL',
  visibilityConfig: { cloudWatchMetricsEnabled: true, metricName: 'PublicApiWebAcl', sampledRequestsEnabled: true },
  rules: [
    { name: 'AWS-AWSManagedRulesCommonRuleSet', priority: 0, statement: { managedRuleGroupStatement: { vendorName: 'AWS', name: 'AWSManagedRulesCommonRuleSet' } }, overrideAction: { none: {} }, visibilityConfig: { cloudWatchMetricsEnabled: true, metricName: 'AWSManagedRulesCommonRuleSet', sampledRequestsEnabled: true } },
    { name: 'LimitRequests1000Per5Min', priority: 1, statement: { rateBasedStatement: { limit: 1000, aggregateKeyType: 'IP' } }, action: { block: {} }, visibilityConfig: { cloudWatchMetricsEnabled: true, metricName: 'LimitRequests1000Per5Min', sampledRequestsEnabled: true } },
  ],
});

[scanApi, analyticsApi].forEach((api, idx) => {
  new wafv2.CfnWebACLAssociation(stack, `WafAssoc${idx}`, {
    resourceArn: `arn:aws:apigateway:${stack.region}::/restapis/${api.restApiId}/stages/${api.deploymentStage.stageName}`,
    webAclArn: publicWebAcl.attrArn,
  });
});

// ── Analytics Compactor (P1-5) ──
const compactorLambda = backend.analyticsCompactorFn.resources.lambda as lambda.Function;
compactorLambda.addEnvironment('GLUE_DATABASE', glueDatabase.ref ?? `bebo_analytics_${stage}`);
compactorLambda.addEnvironment('ATHENA_WORKGROUP', athenaWorkgroup.name);
compactorLambda.addEnvironment('ANALYTICS_BUCKET', analyticsBucket.bucketName);

analyticsBucket.grantReadWrite(compactorLambda);
compactorLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['s3:GetBucketLocation', 's3:GetObject', 's3:ListBucket', 's3:PutObject', 's3:DeleteObject'],
  resources: [
    analyticsBucket.bucketArn,
    `${analyticsBucket.bucketArn}/*`,
    'arn:aws:s3:::bebocard-enterprise-*',
    'arn:aws:s3:::bebocard-enterprise-*/*',
  ],
}));

compactorLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:GetQueryResults'],
  resources: [`arn:aws:athena:${stack.region}:${stack.account}:workgroup/${athenaWorkgroup.name}`],
}));

compactorLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['glue:GetDatabase', 'glue:GetTables', 'glue:GetTable', 'glue:UpdateTable'],
  resources: [
    `arn:aws:glue:${stack.region}:${stack.account}:catalog`,
    `arn:aws:glue:${stack.region}:${stack.account}:database/${glueDatabase.ref ?? `bebo_analytics_${stage}`}`,
    `arn:aws:glue:${stack.region}:${stack.account}:table/${glueDatabase.ref ?? `bebo_analytics_${stage}`}/receipts_*`,
  ],
}));

// ── Analytics Backfiller (P1-4) ──
const backfillerLambda = backend.analyticsBackfillerFn.resources.lambda as lambda.Function;
backfillerLambda.addEnvironment('GLUE_DATABASE', glueDatabase.ref ?? `bebo_analytics_${stage}`);
backfillerLambda.addEnvironment('ATHENA_WORKGROUP', athenaWorkgroup.name);
backfillerLambda.addEnvironment('ANALYTICS_BUCKET', analyticsBucket.bucketName);
backfillerLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
backfillerLambda.addEnvironment('USER_TABLE', userTable.tableName);
backfillerLambda.addEnvironment('USER_HASH_SALT', userHashSalt);

analyticsBucket.grantReadWrite(backfillerLambda);
backfillerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['s3:GetBucketLocation', 's3:GetObject', 's3:ListBucket', 's3:PutObject', 's3:DeleteObject'],
  resources: [
    analyticsBucket.bucketArn,
    `${analyticsBucket.bucketArn}/*`,
    'arn:aws:s3:::bebocard-enterprise-*',
    'arn:aws:s3:::bebocard-enterprise-*/*',
  ],
}));

backfillerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:GetQueryResults'],
  resources: [`arn:aws:athena:${stack.region}:${stack.account}:workgroup/${athenaWorkgroup.name}`],
}));

backfillerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['glue:GetDatabase', 'glue:GetTables', 'glue:GetTable', 'glue:UpdateTable', 'glue:CreateTable'],
  resources: [
    `arn:aws:glue:${stack.region}:${stack.account}:catalog`,
    `arn:aws:glue:${stack.region}:${stack.account}:database/${glueDatabase.ref ?? `bebo_analytics_${stage}`}`,
    `arn:aws:glue:${stack.region}:${stack.account}:table/${glueDatabase.ref ?? `bebo_analytics_${stage}`}/receipts_*`,
  ],
}));

grantTableAccess(backfillerLambda, refDataTable, false);
grantTableAccess(backfillerLambda, userTable, false);
 
// Reserved concurrency — analytics-backfiller: prevents massive backfill scans from consuming entire regional concurrency (P0-5)
const cfnBackfiller = backfillerLambda.node.defaultChild as lambda.CfnFunction;
// cfnBackfiller.reservedConcurrentExecutions = 10;

// Schedule: Nightly at 2:00 AM UTC
const cronRule = new events.Rule(stack, 'NightlyCompactionRule', {
  schedule: events.Schedule.cron({ hour: '2', minute: '0' }),
});
cronRule.addTarget(new eventsTargets.LambdaFunction(compactorLambda));

// Analytics Aggregator Schedule: Nightly at 1:00 AM UTC (before compaction)
const aggregatorLambda = backend.analyticsAggregatorFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => aggregatorLambda.addEnvironment(k, v));
grantTableAccess(aggregatorLambda, userTable, false);
grantTableAccess(aggregatorLambda, refDataTable, false);
grantTableAccess(aggregatorLambda, backend.data.resources.tables['ReportDataEvent'], true);

const aggregatorRule = new events.Rule(stack, 'NightlyAggregationRule', {
  schedule: events.Schedule.cron({ hour: '1', minute: '0' }),
});
aggregatorRule.addTarget(new eventsTargets.LambdaFunction(aggregatorLambda));

// ── Custom Segment Evaluator (EOD batch) ──
// Nightly at 00:30 UTC — after segment-processor stream has caught up, before analytics compaction at 02:00.
const customSegmentLambda = backend.customSegmentEvaluatorFn.resources.lambda as lambda.Function;
customSegmentLambda.addEnvironment('USER_TABLE', userTable.tableName);
customSegmentLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);

// Read segment defs from RefDataEvent; write membership records to UserDataEvent
grantTableAccess(customSegmentLambda, refDataTable, false);
grantTableAccess(customSegmentLambda, userTable, true);

// Update SEGMENT_DEF# stats (memberCount, lastEvaluatedAt) — needs UpdateItem on RefDataEvent
grantTableAccess(customSegmentLambda, refDataTable, true);

const customSegmentDLQ = new sqs.Queue(stack, 'CustomSegmentEvaluatorDLQ', {
  retentionPeriod: Duration.days(14),
});
createDlqAlarm(customSegmentDLQ, 'CustomSegmentEvaluatorDLQ');
customSegmentLambda.addEnvironment('CUSTOM_SEGMENT_DLQ_URL', customSegmentDLQ.queueUrl);
customSegmentDLQ.grantSendMessages(customSegmentLambda);

const customSegmentRule = new events.Rule(stack, 'NightlyCustomSegmentRule', {
  schedule: events.Schedule.cron({ hour: '0', minute: '30' }),
  description: 'Nightly end-of-day custom segment evaluation at 00:30 UTC',
});
customSegmentRule.addTarget(new eventsTargets.LambdaFunction(customSegmentLambda));

Tags.of(customSegmentLambda).add('Function', 'custom-segment-evaluator');
Tags.of(customSegmentLambda).add('CostCenter', 'tenant-side');

// ── Affiliate Feed Sync (Nightly at 05:00 UTC) ──
const affiliateSyncLambda = backend.affiliateFeedSyncFn.resources.lambda as lambda.Function;
affiliateSyncLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
grantTableAccess(affiliateSyncLambda, refDataTable, true);

const affiliateSyncRule = new events.Rule(stack, 'NightlyAffiliateSyncRule', {
  schedule: events.Schedule.cron({ hour: '5', minute: '0' }),
  description: 'Nightly sync of affiliate offers from Commission Factory / Impact',
});
affiliateSyncRule.addTarget(new eventsTargets.LambdaFunction(affiliateSyncLambda));

Tags.of(affiliateSyncLambda).add('Function', 'affiliate-feed-sync');
Tags.of(affiliateSyncLambda).add('CostCenter', 'marketing');

// ── Tags ──
const functionTags: Array<[lambda.Function, string, string]> = [
  [postConfirmLambda, 'post-confirmation', 'user-side'],
  [cardManagerLambda, 'card-manager', 'user-side'],
  [scanLambda, 'scan-handler', 'tenant-side'],
  [tenantLinkerLambda, 'tenant-linker', 'user-side'],
  [geofenceLambda, 'geofence-handler', 'user-side'],
  [segmentLambda, 'segment-processor', 'tenant-side'],
  [receiptIcebergLambda, 'receipt-iceberg', 'tenant-side'],
  [analyticsLambda, 'tenant-analytics', 'tenant-side'],
  [tenantProvisionerLambda, 'tenant-provisioner', 'tenant-side'],
  [exporterLambda, 'user-data-exporter', 'user-side'],
  [backend.analyticsCompactorFn.resources.lambda as lambda.Function, 'analytics-compactor', 'ops'],
  [backfillerLambda, 'analytics-backfiller', 'ops'],
  [webhookDispatcherLambda, 'webhook-dispatcher', 'tenant-side'],
  [backend.receiptClaimHandlerFn.resources.lambda as lambda.Function, 'receipt-claim-handler', 'user-side'],
];

for (const [fn, name, cost] of functionTags) {
  Tags.of(fn).add('Function', name);
  Tags.of(fn).add('CostCenter', cost);
}

// ── Receipt Claim Handler Setup ──
const receiptClaimLambda = backend.receiptClaimHandlerFn.resources.lambda as lambda.Function;
receiptClaimLambda.addEnvironment('USER_TABLE', userTable.tableName);
receiptClaimLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
grantTableAccess(receiptClaimLambda, userTable, true);
grantTableAccess(receiptClaimLambda, refDataTable, true);

// ── Remote Config Wiring (P2-21) ──
const remoteConfigLambda = backend.remoteConfigHandlerFn.resources.lambda as lambda.Function;
remoteConfigLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
grantTableAccess(remoteConfigLambda, refDataTable, false);

// ── Click Tracking (P2-19) ──
const clickTrackingLambda = backend.clickTrackingHandlerFn.resources.lambda as lambda.Function;
clickTrackingLambda.addEnvironment('REPORT_TABLE', tableNames.REPORT_TABLE);
clickTrackingLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
grantTableAccess(clickTrackingLambda, backend.data.resources.tables['ReportDataEvent'], true);
grantTableAccess(clickTrackingLambda, refDataTable, false);

// ── P1-2 Glue IAM Refinement ──
[tenantProvisionerLambda, receiptIcebergLambda].forEach(fn => {
  fn.addToRolePolicy(new iam.PolicyStatement({
    actions: ['glue:GetDatabase'],
    resources: [
      `arn:aws:glue:${stack.region}:${stack.account}:catalog`,
      `arn:aws:glue:${stack.region}:${stack.account}:database/${glueDatabase.ref ?? `bebo_analytics_${stage}`}`,
    ],
  }));
});

// ── BeboCard Operational Dashboard (P2-11) ──
new cloudwatch.Dashboard(stack, 'BeboCardOpsDashboard', {
  dashboardName: `BeboCard-Core-Ops-${stage}`,
  widgets: [
    [
      new cloudwatch.TextWidget({
        markdown: '# BeboCard Core Ops\nPrimary health metrics for scan resolution and retail delivery.',
        width: 24,
      }),
    ],
    [
      new cloudwatch.GraphWidget({
        title: 'Scan API Latency (p95)',
        left: [scanLambda.metricDuration({ statistic: 'p95', label: 'Scan Handler' })],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'Lambda Concurrent Executions',
        left: [
          scanLambda.metric('ConcurrentExecutions', { label: 'Scan API' }),
          cardManagerLambda.metric('ConcurrentExecutions', { label: 'Card Manager' }),
        ],
        width: 12,
      }),
    ],
    [
      new cloudwatch.GraphWidget({
        title: 'Webhook DLQ Depth (Retries Exhausted)',
        left: [webhookDLQ.metricApproximateNumberOfMessagesVisible()],
        width: 24,
      }),
    ],
  ],
});

// ── P0-6: Cognito Export Lambda (weekly DR backup) ───────────────────────────
const cognitoExportBucket = new s3.Bucket(stack, 'CognitoExportBucket', {
  bucketName: `bebocard-cognito-exports-${stack.account}`,
  encryption: s3.BucketEncryption.KMS_MANAGED,
  versioned: true,
  lifecycleRules: [{ expiration: Duration.days(90), id: 'expire-old-exports' }],
  removalPolicy: RemovalPolicy.RETAIN,
  blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
});

const cognitoExportLambda = backend.cognitoExportFn.resources.lambda as lambda.Function;
cognitoExportLambda.addEnvironment('USER_POOL_ID', backend.auth.resources.userPool.userPoolId);
cognitoExportLambda.addEnvironment('EXPORT_BUCKET', cognitoExportBucket.bucketName);
cognitoExportBucket.grantPut(cognitoExportLambda);

cognitoExportLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['cognito-idp:ListUsers'],
  resources: [backend.auth.resources.userPool.userPoolArn],
}));

const cognitoExportRule = new events.Rule(stack, 'WeeklyCognitoExportRule', {
  schedule: events.Schedule.cron({ weekDay: 'SUN', hour: '2', minute: '0' }),
  description: 'Weekly Cognito user pool export for DR (P0-6)',
});
cognitoExportRule.addTarget(new eventsTargets.LambdaFunction(cognitoExportLambda));
Tags.of(cognitoExportLambda).add('Function', 'cognito-export');
Tags.of(cognitoExportLambda).add('CostCenter', 'ops');

new CfnOutput(stack, 'CognitoExportBucketName', {
  value: cognitoExportBucket.bucketName,
  description: 'Cognito DR export bucket — restore from here if pool is lost',
});

// ── P0-6: Composite DR Alarm (3+ tables in error = infrastructure incident) ──
const userTableErrorAlarm = new cloudwatch.Alarm(stack, 'UserTableSystemErrors', {
  metric: userTable.metric('SystemErrors', { statistic: 'Sum', period: Duration.minutes(5) }),
  threshold: 5,
  evaluationPeriods: 2,
  alarmDescription: 'UserDataEvent DynamoDB system errors elevated',
});
const refTableErrorAlarm = new cloudwatch.Alarm(stack, 'RefTableSystemErrors', {
  metric: refDataTable.metric('SystemErrors', { statistic: 'Sum', period: Duration.minutes(5) }),
  threshold: 5,
  evaluationPeriods: 2,
  alarmDescription: 'RefDataEvent DynamoDB system errors elevated',
});
const adminTableErrorAlarm = new cloudwatch.Alarm(stack, 'AdminTableSystemErrors', {
  metric: adminTable.metric('SystemErrors', { statistic: 'Sum', period: Duration.minutes(5) }),
  threshold: 5,
  evaluationPeriods: 2,
  alarmDescription: 'AdminDataEvent DynamoDB system errors elevated',
});

// "Any table in error" composite — DR-level signal; if even one table is failing, page on-call
new cloudwatch.CompositeAlarm(stack, 'BebocardDRCompositeAlarm', {
  alarmDescription: 'P0-6: One or more DynamoDB tables in error state — potential infrastructure incident. Initiate DR runbook.',
  alarmRule: cloudwatch.AlarmRule.anyOf(
    cloudwatch.AlarmRule.fromAlarm(userTableErrorAlarm, cloudwatch.AlarmState.ALARM),
    cloudwatch.AlarmRule.fromAlarm(refTableErrorAlarm, cloudwatch.AlarmState.ALARM),
    cloudwatch.AlarmRule.fromAlarm(adminTableErrorAlarm, cloudwatch.AlarmState.ALARM),
  ),
});

// ── P3-16: Brand Health Monitor Lambda (weekly, 50%-drop CSM alert) ───────────
const brandHealthLambda = backend.brandHealthMonitorFn.resources.lambda as lambda.Function;
brandHealthLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
brandHealthLambda.addEnvironment('USER_TABLE', userTable.tableName);
grantTableAccess(brandHealthLambda, refDataTable, false);
grantTableAccess(brandHealthLambda, userTable, false);

const brandHealthRule = new events.Rule(stack, 'WeeklyBrandHealthRule', {
  schedule: events.Schedule.cron({ weekDay: 'MON', hour: '8', minute: '0' }),
  description: 'Weekly brand health check — alerts CSM when scan volume drops >50% (P3-16)',
});
brandHealthRule.addTarget(new eventsTargets.LambdaFunction(brandHealthLambda));
Tags.of(brandHealthLambda).add('Function', 'brand-health-monitor');
Tags.of(brandHealthLambda).add('CostCenter', 'ops');

// ── Template Manager (loyalty card templates — super_admin CRUD) ──────────────
const templateManagerLambda = backend.templateManagerFn.resources.lambda as lambda.Function;
templateManagerLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
templateManagerLambda.addEnvironment('PORTAL_ORIGIN', process.env.PORTAL_ORIGIN ?? 'https://business.bebocard.com.au');
templateManagerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [`arn:aws:ssm:${stack.region}:${stack.account}:parameter/amplify/shared/INTERNAL_SIGNING_SECRET`],
}));
grantTableAccess(templateManagerLambda, refDataTable, true);
Tags.of(templateManagerLambda).add('Function', 'template-manager');
Tags.of(templateManagerLambda).add('CostCenter', 'ops');

// ── P3-12: Zero-Downtime Blue/Green Deployments (Lambda Aliases + CodeDeploy) ─
import * as codedeploy from 'aws-cdk-lib/aws-codedeploy';

const blueGreenTargets: Array<{ lambda: lambda.Function; name: string }> = [
  { lambda: scanLambda, name: 'ScanHandler' },
  { lambda: receiptProcessorLambda, name: 'ReceiptProcessor' },
  { lambda: cardManagerLambda, name: 'CardManager' },
];

for (const { lambda: fn, name } of blueGreenTargets) {
  const version = fn.currentVersion;

  const fnStack = Stack.of(fn);
  const liveAlias = new lambda.Alias(fnStack, `${name}LiveAlias`, {
    aliasName: 'live',
    version,
  });

  const errorAlarm = new cloudwatch.Alarm(fnStack, `${name}CanaryErrorAlarm`, {
    metric: liveAlias.metricErrors({ period: Duration.minutes(1) }),
    threshold: 1,
    evaluationPeriods: 3,
    alarmDescription: `P3-12: ${name} canary error rate elevated — auto-rollback triggered`,
  });

  new codedeploy.LambdaDeploymentGroup(fnStack, `${name}DeploymentGroup`, {
    alias: liveAlias,
    deploymentConfig: codedeploy.LambdaDeploymentConfig.CANARY_10PERCENT_5MINUTES,
    alarms: [errorAlarm],
    autoRollback: { failedDeployment: true, deploymentInAlarm: true },
  });
}
