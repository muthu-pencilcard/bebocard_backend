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

// ── String-literal table names (no CDK tokens — breaks functions→data synthesis link) ──
const tableNames = {
  USER_TABLE: 'UserDataEvent',
  REFDATA_TABLE: 'RefDataEvent',
  ADMIN_TABLE: 'AdminDataEvent',
  REPORT_TABLE: 'ReportDataEvent',
};

const cfnUserTable = (backend.data.resources as any).cfnResources?.cfnTables?.['UserDataEvent'];

const amplifyAppId = process.env.AWS_APP_ID ?? 'local';
const amplifyBranch = process.env.AWS_BRANCH ?? 'sandbox';

const stage = amplifyBranch === 'main' ? 'prod' : 'dev';
const branchName = amplifyBranch === 'main' ? 'prod' : amplifyBranch;

const dataStack = Stack.of(userTable);
const authStack = backend.auth.resources.userPool.stack;

const userHashSalt = 'bebo_' + (process.env.USER_HASH_SALT ?? 'local_dev_salt_123');
// ── Infrastructure Stacks (Decoupled to prevent circular deps) ────────────────
const funcStack = backend.auth.resources.userPool.stack.node.scope as Stack; // The root amplify stack
const infraStack = funcStack; // Storage, Glue, SNS, KMS go in root scope to decouple from data
const mappingStack = funcStack; // Event sources pointing to lambdas MUST be in root scope to break circularity
const rootStack = (dataStack.node.scope as any) instanceof Stack ? (dataStack.node.scope as Stack) : dataStack;

// ── SSM Parameters (Circular Dep Break) ──────────────────────────────────────
const userTableParamName = `/bebocard/${amplifyAppId}/${amplifyBranch}/USER_TABLE`;
const adminTableParamName = `/bebocard/${amplifyAppId}/${amplifyBranch}/ADMIN_TABLE`;
const restApiUrlParamName = `/bebocard/${amplifyAppId}/${amplifyBranch}/SCAN_API_URL`;
const USER_POOL_ID_PARAM = `/bebocard/${amplifyAppId}/${amplifyBranch}/USER_POOL_ID`;

new ssm.StringParameter(infraStack, 'UserTableNameParam', { parameterName: userTableParamName, stringValue: userTable.tableName });
new ssm.StringParameter(infraStack, 'AdminTableNameParam', { parameterName: adminTableParamName, stringValue: adminTable.tableName });
// ── Store Cognito UserPoolId in auth stack's own SSM param (no data→auth token) ──
new ssm.StringParameter(authStack, 'UserPoolIdParam', {
  parameterName: USER_POOL_ID_PARAM,
  stringValue: backend.auth.resources.userPool.userPoolId,
});

// ── Bebo Intelligence: Data Lake (P1-1 Architecture) ─────────────────────────
const analyticsBucketName = `bebocard-analytics-${amplifyAppId}-${amplifyBranch}`.toLowerCase();
// Adoption Pattern: Attempt to use existing bucket to prevent CREATE_FAILED collision on failed re-runs
const analyticsBucket = s3.Bucket.fromBucketName(infraStack, 'AnalyticsLakeImport', analyticsBucketName) || new s3.Bucket(infraStack, 'AnalyticsLake', {
  bucketName: analyticsBucketName,
  removalPolicy: RemovalPolicy.RETAIN,
  versioned: true, 
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
// ── Remote Configuration (P2-2) ──
const remoteConfigBucketName = `bebocard-config-${amplifyAppId}-${amplifyBranch}`.toLowerCase();
const remoteConfigBucket = s3.Bucket.fromBucketName(infraStack, 'RemoteConfigImport', remoteConfigBucketName) || new s3.Bucket(infraStack, 'RemoteConfig', {
  bucketName: remoteConfigBucketName,
  versioned: true,
  publicReadAccess: true, 
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

new ssm.StringParameter(infraStack, 'RemoteConfigBucketParam', {
  parameterName: `/bebocard/${amplifyAppId}/${amplifyBranch}/REMOTE_CONFIG_BUCKET`,
  stringValue: remoteConfigBucket.bucketName,
});

const exportsBucketName = `bebocard-exports-${amplifyAppId}-${amplifyBranch}`.toLowerCase();
const exportsBucket = new s3.Bucket(infraStack, 'UserDataExports', {
  bucketName: exportsBucketName,
  removalPolicy: RemovalPolicy.DESTROY, // Exports are temporary
  lifecycleRules: [{ expiration: Duration.days(1) }], // Auto-delete after 24 hours
});

const glueDatabaseName = `bebo_analytics_${stage}`;
const athenaWorkgroupName = `bebo-intel-${stage}`;

const glueDatabase = (backend.data.resources as any).cfnResources?.cfnTables?.['UserDataEvent']
  ?.stack.node.defaultChild.parent.parent.parent.node.findAll()
  .find((n: any) => n.cfnResourceType === 'AWS::Glue::Database') 
  ?? new glue.CfnDatabase(infraStack, 'AnalyticsDatabase', {
    catalogId: infraStack.account,
    databaseInput: { name: glueDatabaseName, description: 'BeboCard Intelligence Data Lake' },
  });

const athenaWorkgroup = new athena.CfnWorkGroup(infraStack, 'AnalyticsWorkgroup', {
  name: athenaWorkgroupName,
  description: 'Intelligence tier analytics queries',
  workGroupConfiguration: {
    resultConfiguration: { outputLocation: `s3://${analyticsBucketName}/athena-results/` },
  },
});

const icebergDLQ = new sqs.Queue(infraStack, 'ReceiptIcebergDLQ', { retentionPeriod: Duration.days(14) });
Tags.of(icebergDLQ).add('CostCenter', 'tenant-side');

// ── Monitoring & Alarming (P0-1) ─────────────────────────────────────────────
const alertsTopic = new sns.Topic(authStack, 'InfrastructureAlerts', {
  displayName: `BeboCard ${stage.toUpperCase()} Infrastructure Alerts`,
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
const receiptSigningKey = new kms.Key(infraStack, 'ReceiptSigningKey', {
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
  resources: [`arn:aws:ssm:*:*:parameter/bebocard/*`],
}));
postConfirmLambda.role?.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonCognitoPowerUser'));
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['cognito-idp:AdminUpdateUserAttributes'],
  resources: [`arn:aws:cognito-idp:*:*:userpool/*`],
}));
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['dynamodb:PutItem', 'dynamodb:UpdateItem'],
  resources: [
    `arn:aws:dynamodb:*:*:table/UserDataEvent-*`, 
    `arn:aws:dynamodb:*:*:table/AdminDataEvent-*`
  ],
}));
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ses:SendEmail', 'ses:SendRawEmail'],
  resources: ['*'], // In production, scope this to the verified domain
}));
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [`arn:aws:ssm:*:*:parameter/amplify/shared/PARENTAL_CONSENT_SECRET`],
}));

// ── Card manager ──
const cardManagerLambda = backend.cardManagerFn.resources.lambda as lambda.Function;
// ── P0-5: Concurrency Reservation ──
// (cardManagerLambda.node.defaultChild as lambda.CfnFunction).reservedConcurrentExecutions = 50;
createHighTrafficUtilizationAlarm(cardManagerLambda, 'CardManager');
Object.entries(tableNames).forEach(([k, v]) => cardManagerLambda.addEnvironment(k, v));
const grantTableAccess = (fn: lambda.Function, tableNamePrefix: string, write: boolean = false) => {
  fn.addToRolePolicy(new iam.PolicyStatement({
    actions: write ? ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem', 'dynamodb:DeleteItem', 'dynamodb:Query', 'dynamodb:Scan', 'dynamodb:BatchWriteItem', 'dynamodb:BatchGetItem'] : ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:Scan', 'dynamodb:BatchGetItem'],
    resources: [
      `arn:aws:dynamodb:*:*:table/${tableNamePrefix}-*`,
      `arn:aws:dynamodb:*:*:table/${tableNamePrefix}-*/index/*`
    ],
  }));
};

const grantS3Access = (fn: lambda.Function, bucketName: string, actions: string[]) => {
  fn.addToRolePolicy(new iam.PolicyStatement({
    actions,
    resources: [`arn:aws:s3:::${bucketName}`, `arn:aws:s3:::${bucketName}/*`],
  }));
};

const grantSqsAccess = (fn: lambda.Function, queue: sqs.IQueue, actions: string[]) => {
  fn.addToRolePolicy(new iam.PolicyStatement({
    actions,
    resources: [`arn:aws:sqs:*:*:${queue.queueName}`],
  }));
};

const grantKmsAccess = (fn: lambda.Function, key: kms.IKey, actions: string[]) => {
  fn.addToRolePolicy(new iam.PolicyStatement({
    actions,
    resources: [`arn:aws:kms:*:*:key/${key.keyId}`],
  }));
};

grantTableAccess(cardManagerLambda, 'UserDataEvent', true);
grantTableAccess(cardManagerLambda, 'RefDataEvent', false);
grantTableAccess(cardManagerLambda, 'AdminDataEvent', true);

// ── Scan handler ──
const scanLambda = backend.scanHandlerFn.resources.lambda as lambda.Function;
grantTableAccess(scanLambda, 'UserDataEvent', true);
grantTableAccess(scanLambda, 'RefDataEvent', false);
grantTableAccess(scanLambda, 'AdminDataEvent', false);
grantKmsAccess(scanLambda, receiptSigningKey, ['kms:GetPublicKey']);

/*
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
*/

const receiptProcessingDLQ = new sqs.Queue(infraStack, 'ReceiptProcessingDLQ', { retentionPeriod: Duration.days(14) });
const receiptProcessingQueue = new sqs.Queue(infraStack, 'ReceiptProcessingQueue', {
  visibilityTimeout: Duration.seconds(30),
  deadLetterQueue: {
    queue: receiptProcessingDLQ,
    maxReceiveCount: 3,
  },
});
createDlqAlarm(receiptProcessingDLQ, 'ReceiptProcessingDLQ'); // Zero tolerance for checkout failures

grantSqsAccess(scanLambda, receiptProcessingQueue, ['sqs:SendMessage']);
scanLambda.addEnvironment('RECEIPT_QUEUE_URL', receiptProcessingQueue.queueUrl);

const receiptProcessorLambda = backend.receiptProcessorFn.resources.lambda as lambda.Function;
receiptProcessorLambda.addEnvironment('USER_TABLE', userTable.tableName);
new lambda.EventSourceMapping(mappingStack, 'ReceiptProcessorSQSSource', {
  target: receiptProcessorLambda,
  eventSourceArn: receiptProcessingQueue.queueArn,
  batchSize: 10,
});
receiptProcessingQueue.grantConsumeMessages(receiptProcessorLambda);
grantTableAccess(receiptProcessorLambda, 'UserDataEvent', true);
// Reserved concurrency — receipt-processor: ensures receipt writes cannot be throttled by other bursts (P0-5)
const cfnReceiptProcessor = receiptProcessorLambda.node.defaultChild as lambda.CfnFunction;
// cfnReceiptProcessor.reservedConcurrentExecutions = 100;

grantKmsAccess(receiptProcessorLambda, receiptSigningKey, ['kms:Sign']);
receiptProcessorLambda.addEnvironment('RECEIPT_SIGNING_KEY_ID', receiptSigningKey.keyId);

const tenantLinkerLambda = backend.tenantLinker.resources.lambda as lambda.Function;
tenantLinkerLambda.addEnvironment('USER_TABLE', 'UserDataEvent');
tenantLinkerLambda.addEnvironment('ADMIN_TABLE', 'AdminDataEvent');
grantTableAccess(tenantLinkerLambda, 'UserDataEvent', true);
grantTableAccess(tenantLinkerLambda, 'AdminDataEvent', true);

// ── Geofence handler ──
const geofenceLambda = backend.geofenceHandlerFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => geofenceLambda.addEnvironment(k, v));
grantTableAccess(geofenceLambda, 'UserDataEvent', true);
grantTableAccess(geofenceLambda, 'RefDataEvent', false);
grantTableAccess(geofenceLambda, 'AdminDataEvent', false);

// ── Consent handler ──
const consentLambda = backend.consentHandlerFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => consentLambda.addEnvironment(k, v));
grantTableAccess(consentLambda, 'UserDataEvent', true);
grantTableAccess(consentLambda, 'RefDataEvent', false);
grantTableAccess(consentLambda, 'AdminDataEvent', true);

// ── Segment processor ──
const segmentLambda = backend.segmentProcessorFn.resources.lambda as lambda.Function;
segmentLambda.addEnvironment('USER_TABLE', 'UserDataEvent');
segmentLambda.addEnvironment('USER_HASH_SALT', userHashSalt);
grantTableAccess(segmentLambda, 'UserDataEvent', true);
if (cfnUserTable) cfnUserTable.streamSpecification = { streamViewType: 'NEW_IMAGE' };

const segmentDLQ = new sqs.Queue(infraStack, 'SegmentProcessorDLQ', { retentionPeriod: Duration.days(14) });
createDlqAlarm(segmentDLQ, 'SegmentProcessorDLQ', 5); // Allow small batch jitter before alerting
segmentLambda.addEnvironment('SEGMENT_DLQ_URL', segmentDLQ.queueUrl);
grantSqsAccess(segmentLambda, segmentDLQ, ['sqs:SendMessage']);

new lambda.EventSourceMapping(mappingStack, 'SegmentLambdaDDBSource', {
  target: segmentLambda,
  eventSourceArn: userTable.tableStreamArn,
  startingPosition: lambda.StartingPosition.TRIM_HORIZON,
  filters: [lambda.FilterCriteria.filter({ eventName: lambda.FilterRule.or('INSERT', 'MODIFY', 'REMOVE'), dynamodb: { Keys: { sK: { S: [{ prefix: 'RECEIPT#' }, { prefix: 'INVOICE#' }, { prefix: 'SUBSCRIPTION#' }] } } } })],
  retryAttempts: 1,
});
grantTableAccess(segmentLambda, 'UserDataEvent', false); // P1-5: Read-only access for segment processing
userTable.grantStreamRead(segmentLambda);

// ── Billing Run Schedule (P1-8) ──
const billingRunLambda = backend.billingRunHandlerFn.resources.lambda as lambda.Function;
billingRunLambda.addEnvironment('REFDATA_TABLE', 'RefDataEvent');
grantTableAccess(billingRunLambda, 'RefDataEvent', true);
billingRunLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ses:SendEmail'],
  resources: ['*'], // In production, scope to the verified identity
}));

billingRunLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [`arn:aws:ssm:*:*:parameter/amplify/shared/STRIPE_SECRET_KEY`],
}));

// Run daily at 02:00 UTC (processes monthly overages on the 1st)
const billingRunRule = new events.Rule(infraStack, 'MonthlyBillingRunRule', {
  schedule: events.Schedule.expression('cron(0 2 * * ? *)'),
});
billingRunRule.addTarget(new eventsTargets.LambdaFunction(billingRunLambda));

// ── Billing Webhook Handler (P1-8) ──
const billingWebhookLambda = backend.billingWebhookHandlerFn.resources.lambda as lambda.Function;
billingWebhookLambda.addEnvironment('REFDATA_TABLE', 'RefDataEvent');
grantTableAccess(billingWebhookLambda, 'UserDataEvent', true);
billingWebhookLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [
    `arn:aws:ssm:*:*:parameter/amplify/shared/STRIPE_SECRET_KEY`,
    `arn:aws:ssm:*:*:parameter/amplify/shared/STRIPE_WEBHOOK_SECRET`,
  ],
}));

// ── QR Router (P1-10) ──
const qrRouterLambda = backend.qrRouterHandlerFn.resources.lambda as lambda.Function;
qrRouterLambda.addEnvironment('REFDATA_TABLE', 'RefDataEvent');
grantTableAccess(qrRouterLambda, 'RefDataEvent', false);

const qrApi = new apigw.RestApi(infraStack, 'QrRouterApi', { 
  restApiName: `bebo-qr-router-${stage}`,
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
receiptIcebergLambda.addEnvironment('ANALYTICS_BUCKET', analyticsBucketName);
receiptIcebergLambda.addEnvironment('GLUE_DATABASE', glueDatabase.ref ?? `bebo_analytics_${stage}`);
receiptIcebergLambda.addEnvironment('ATHENA_WORKGROUP', athenaWorkgroup.name);
receiptIcebergLambda.addEnvironment('REFDATA_TABLE', 'RefDataEvent');
receiptIcebergLambda.addEnvironment('USER_HASH_SALT', userHashSalt);
receiptIcebergLambda.addEnvironment('ICEBERG_DLQ_URL', icebergDLQ.queueUrl);

grantS3Access(receiptIcebergLambda, analyticsBucketName, ['s3:GetObject', 's3:PutObject', 's3:ListBucket']);
grantSqsAccess(receiptIcebergLambda, icebergDLQ, ['sqs:SendMessage']);
receiptIcebergLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:GetQueryResults'],
  resources: [`arn:aws:athena:*:*:workgroup/${athenaWorkgroupName}`],
}));
receiptIcebergLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['glue:GetDatabase', 'glue:GetTable', 'glue:CreateTable', 'glue:UpdateTable'],
  resources: [
    `arn:aws:glue:*:*:catalog`,
    `arn:aws:glue:*:*:database/${glueDatabaseName}`,
    `arn:aws:glue:*:*:table/${glueDatabaseName}/receipts_*`,
  ],
}));
grantTableAccess(receiptIcebergLambda, 'RefDataEvent', false);
new lambda.EventSourceMapping(mappingStack, 'ReceiptIcebergLambdaUserSource', {
  target: receiptIcebergLambda,
  eventSourceArn: userTable.tableStreamArn,
  startingPosition: lambda.StartingPosition.LATEST,
  filters: [lambda.FilterCriteria.filter({ eventName: lambda.FilterRule.isEqual('INSERT') })],
  retryAttempts: 1,
});
grantTableAccess(receiptIcebergLambda, 'UserDataEvent', false); 
grantTableAccess(receiptIcebergLambda, 'RefDataEvent', false);
userTable.grantStreamRead(receiptIcebergLambda);
refDataTable.grantStreamRead(receiptIcebergLambda);
new lambda.EventSourceMapping(mappingStack, 'ReceiptIcebergLambdaRefSource', {
  target: receiptIcebergLambda,
  eventSourceArn: refDataTable.tableStreamArn,
  startingPosition: lambda.StartingPosition.LATEST,
  filters: [lambda.FilterCriteria.filter({ 
    eventName: lambda.FilterRule.isEqual('INSERT'),
    dynamodb: { Keys: { pK: { S: [{ prefix: 'ANON#' }] } } }
  })],
  retryAttempts: 1,
});
refDataTable.grantStreamRead(receiptIcebergLambda);

// ── Tenant provisioner (P1-2) ──
const tenantProvisionerLambda = backend.tenantProvisionerFn.resources.lambda as lambda.Function;
tenantProvisionerLambda.addEnvironment('GLUE_DATABASE', glueDatabaseName);
tenantProvisionerLambda.addEnvironment('ANALYTICS_BUCKET', analyticsBucketName);
tenantProvisionerLambda.addEnvironment('REFDATA_TABLE', 'RefDataEvent');

grantTableAccess(tenantProvisionerLambda, 'RefDataEvent', true);

tenantProvisionerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['glue:GetTable', 'glue:CreateTable', 'glue:UpdateTable'],
  resources: [
    `arn:aws:glue:*:*:catalog`,
    `arn:aws:glue:*:*:database/${glueDatabaseName}`,
    `arn:aws:glue:*:*:table/${glueDatabaseName}/receipts_*`,
  ],
}));

tenantProvisionerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['s3:CreateBucket', 's3:GetBucketPolicy', 's3:PutBucketPolicy', 's3:ListBucket', 's3:HeadBucket'],
  resources: ['arn:aws:s3:::bebocard-enterprise-*', 'arn:aws:s3:::bebocard-*'],
}));

new lambda.EventSourceMapping(mappingStack, 'TenantProvisionerLambdaRefSource', {
  target: tenantProvisionerLambda,
  eventSourceArn: refDataTable.tableStreamArn,
  startingPosition: lambda.StartingPosition.LATEST,
  filters: [lambda.FilterCriteria.filter({ 
    eventName: lambda.FilterRule.or('INSERT', 'MODIFY'),
    dynamodb: { 
      NewImage: { 
        primaryCat: { S: ['tenant'] }
      } 
    } 
  })],
  retryAttempts: 1,
});
grantTableAccess(tenantProvisionerLambda, 'RefDataEvent', true);
refDataTable.grantStreamRead(tenantProvisionerLambda);

// ── Tenant analytics ──
const analyticsLambda = backend.tenantAnalyticsFn.resources.lambda as lambda.Function;
analyticsLambda.addEnvironment('USER_TABLE', 'UserDataEvent');
analyticsLambda.addEnvironment('REFDATA_TABLE', 'RefDataEvent');
analyticsLambda.addEnvironment('ANALYTICS_BUCKET', analyticsBucketName);
analyticsLambda.addEnvironment('GLUE_DATABASE', glueDatabaseName);
analyticsLambda.addEnvironment('ATHENA_WORKGROUP', athenaWorkgroupName);
analyticsLambda.addEnvironment('REPORT_TABLE', 'ReportDataEvent');

grantTableAccess(analyticsLambda, 'UserDataEvent', false);
grantTableAccess(analyticsLambda, 'RefDataEvent', false);
grantTableAccess(analyticsLambda, 'ReportDataEvent', false);
grantS3Access(analyticsLambda, analyticsBucketName, ['s3:GetObject', 's3:PutObject', 's3:ListBucket']);
analyticsLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:GetQueryResults'],
  resources: [`arn:aws:athena:*:*:workgroup/${athenaWorkgroupName}`],
}));
analyticsLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['glue:GetDatabase', 'glue:GetTable'],
  resources: [
    `arn:aws:glue:*:*:catalog`,
    `arn:aws:glue:*:*:database/${glueDatabaseName}`,
    `arn:aws:glue:*:*:table/${glueDatabaseName}/receipts`,
  ],
}));

// ── Scan API (v1 & Legacy) ──
const scanApiStack = backend.scanHandlerFn.resources.lambda.stack;
// Construct API in its own nested stack to co-locate with Lambdas (reduces cross-stack permission deps)
const scanApi = new apigw.RestApi(scanApiStack, 'ScanApi', { 
  restApiName: `bebo-scan-api-${stage}`,
  deployOptions: { stageName: 'prod' } 
});
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
parentalConsentLambda.addEnvironment('USER_TABLE', 'UserDataEvent');
grantTableAccess(parentalConsentLambda, 'UserDataEvent', true);
parentalConsentLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [`arn:aws:ssm:*:*:parameter/amplify/shared/PARENTAL_CONSENT_SECRET`],
}));

// Billing Webhook Route
const billingRes = scanV1.addResource('billing');
const stripeWebhookRes = billingRes.addResource('stripe-webhook');
stripeWebhookRes.addMethod('POST', new apigw.LambdaIntegration(billingWebhookLambda), { apiKeyRequired: false });

// We use a decoupled environment variable for the API URL to break the auth -> data circular dependency.
postConfirmLambda.addEnvironment('SCAN_API_URL', `https://api.bebocard.app/v1/`); // Placeholder pattern for now

// Break Circularity: Store the API URL in the ROOT stack (infraStack), not the scanApiStack.
// We use a literal ARN pattern — the URL is deterministic given restApiId and stage
const scanApiUrl = `https://${scanApi.restApiId}.execute-api.${Stack.of(scanApiStack).region}.amazonaws.com/prod/`;

new ssm.StringParameter(infraStack, 'ScanApiUrlParam', {
  parameterName: restApiUrlParamName,
  stringValue: scanApiUrl,
});

// Legacy routes — 301 permanent redirect to /v1/ equivalents (P2-7)
// Brands that haven't updated their integration receive a redirect, not an error.
// No API key required on the redirect itself — the brand's key is used on the /v1/ destination.
const make301 = (v1Path: string) => {
  const targetUrl = `https://${scanApi.restApiId}.execute-api.${Stack.of(scanApiStack).region}.amazonaws.com/prod${v1Path}`;
  return {
    integration: new apigw.MockIntegration({
      requestTemplates: { 'application/json': '{"statusCode": 301}' },
      integrationResponses: [{
        statusCode: '301',
        responseParameters: {
          'method.response.header.Location': `'${targetUrl}'`,
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
    } as apigw.MethodOptions,
  };
};

const { integration: scan301, options: scan301Opts } = make301('/v1/scan');
scanApi.root.addResource('scan').addMethod('POST', scan301, scan301Opts);

const { integration: receipt301, options: receipt301Opts } = make301('/v1/receipt');
scanApi.root.addResource('receipt').addMethod('POST', receipt301, receipt301Opts);

const { integration: invoice301, options: invoice301Opts } = make301('/v1/invoice');
scanApi.root.addResource('invoice').addMethod('POST', invoice301, invoice301Opts);

// ── Tenant Analytics API (v1 & Legacy) ──
const analyticsApi = new apigw.RestApi(infraStack, 'TenantAnalyticsApi', {
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
exporterLambda.addEnvironment('USER_TABLE', 'UserDataEvent');
exporterLambda.addEnvironment('ADMIN_TABLE', 'AdminDataEvent');
exporterLambda.addEnvironment('EXPORTS_BUCKET', exportsBucketName);
// USER_POOL_ID read from SSM at runtime — no synthesis-time auth→data token
exporterLambda.addEnvironment('USER_POOL_ID_PARAM', USER_POOL_ID_PARAM);
exporterLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [`arn:aws:ssm:*:*:parameter${USER_POOL_ID_PARAM}`],
}));
// cfnUserPool.addPropertyOverride removed — was creating data→auth cross-stack mutation.
// AllowAdminCreateUserOnly defaults to false but can be enforced via a separate auth stack customisation.

grantTableAccess(exporterLambda, 'UserDataEvent', true);
grantTableAccess(exporterLambda, 'AdminDataEvent', true);
grantS3Access(exporterLambda, exportsBucketName, ['s3:GetObject', 's3:PutObject', 's3:ListBucket']);

exporterLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['cognito-idp:AdminDisableUser', 'cognito-idp:AdminUserGlobalSignOut', 'cognito-idp:AdminDeleteUser'],
  resources: ['arn:aws:cognito-idp:*:*:userpool/*'],
}));

// ── Webhook Reliability Queue (P2-12) ──
const webhookDLQ = new sqs.Queue(infraStack, 'WebhookReliabilityDLQ', { retentionPeriod: Duration.days(14) });
const webhookQueue = new sqs.Queue(infraStack, 'WebhookReliabilityQueue', {
  visibilityTimeout: Duration.seconds(60), // Match Lambda timeout + headroom
  deadLetterQueue: {
    queue: webhookDLQ,
    maxReceiveCount: 5,
  },
});
createDlqAlarm(webhookDLQ, 'WebhookReliabilityDLQ');

grantSqsAccess(exporterLambda, webhookQueue, ['sqs:SendMessage']);
exporterLambda.addEnvironment('WEBHOOK_QUEUE_URL', webhookQueue.queueUrl);

const webhookDispatcherLambda = backend.webhookDispatcherFn.resources.lambda as lambda.Function;
webhookDispatcherLambda.addEnvironment('REFDATA_TABLE', 'RefDataEvent');
new lambda.EventSourceMapping(mappingStack, 'WebhookDispatcherSQSSource', {
  target: webhookDispatcherLambda,
  eventSourceArn: webhookQueue.queueArn,
  batchSize: 5,
});
webhookQueue.grantConsumeMessages(webhookDispatcherLambda);
grantTableAccess(webhookDispatcherLambda, 'RefDataEvent', false);
// Allow dispatcher to read per-brand webhook signing secrets (P2-12 HMAC signature)
webhookDispatcherLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['secretsmanager:GetSecretValue'],
  resources: [`arn:aws:secretsmanager:*:*:secret:bebocard/webhook-signing/*`],
}));

// Secrets for FCM
// Using environment variable with fallback to prevent stack synthesis crashes if the secret isn't pre-configured in SSM.
const firebaseSecretValue = process.env.FIREBASE_SERVICE_ACCOUNT_JSON || '{}';
exporterLambda.addEnvironment('FIREBASE_SERVICE_ACCOUNT_JSON', firebaseSecretValue);

// ── WAF ──
const publicWebAcl = new wafv2.CfnWebACL(infraStack, 'PublicApiWebAcl', {
  defaultAction: { allow: {} },
  scope: 'REGIONAL',
  visibilityConfig: { cloudWatchMetricsEnabled: true, metricName: 'PublicApiWebAcl', sampledRequestsEnabled: true },
  rules: [
    { name: 'AWS-AWSManagedRulesCommonRuleSet', priority: 0, statement: { managedRuleGroupStatement: { vendorName: 'AWS', name: 'AWSManagedRulesCommonRuleSet' } }, overrideAction: { none: {} }, visibilityConfig: { cloudWatchMetricsEnabled: true, metricName: 'AWSManagedRulesCommonRuleSet', sampledRequestsEnabled: true } },
    { name: 'LimitRequests1000Per5Min', priority: 1, statement: { rateBasedStatement: { limit: 1000, aggregateKeyType: 'IP' } }, action: { block: {} }, visibilityConfig: { cloudWatchMetricsEnabled: true, metricName: 'LimitRequests1000Per5Min', sampledRequestsEnabled: true } },
  ],
});

/*
[scanApi, analyticsApi].forEach((api, idx) => {
  new wafv2.CfnWebACLAssociation(infraStack, `WafAssoc${idx}`, {
    resourceArn: `arn:aws:apigateway:*::/restapis/${api.restApiId}/stages/${api.deploymentStage.stageName}`,
    webAclArn: publicWebAcl.attrArn,
  });
});
*/

// ── Analytics Compactor (P1-5) ──
const compactorLambda = backend.analyticsCompactorFn.resources.lambda as lambda.Function;
compactorLambda.addEnvironment('GLUE_DATABASE', glueDatabaseName);
compactorLambda.addEnvironment('ATHENA_WORKGROUP', athenaWorkgroupName);
compactorLambda.addEnvironment('ANALYTICS_BUCKET', analyticsBucketName);

grantS3Access(compactorLambda, analyticsBucketName, ['s3:GetObject', 's3:PutObject', 's3:ListBucket', 's3:DeleteObject']);
compactorLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['s3:GetBucketLocation', 's3:GetObject', 's3:ListBucket', 's3:PutObject', 's3:DeleteObject'],
  resources: [
    `arn:aws:s3:::${analyticsBucketName}`,
    `arn:aws:s3:::${analyticsBucketName}/*`,
    'arn:aws:s3:::bebocard-enterprise-*',
    'arn:aws:s3:::bebocard-enterprise-*/*',
  ],
}));

compactorLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:GetQueryResults'],
  resources: [`arn:aws:athena:*:*:workgroup/${athenaWorkgroupName}`],
}));

compactorLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['glue:GetDatabase', 'glue:GetTables', 'glue:GetTable', 'glue:UpdateTable'],
  resources: [
    `arn:aws:glue:*:*:catalog`,
    `arn:aws:glue:*:*:database/${glueDatabaseName}`,
    `arn:aws:glue:*:*:table/${glueDatabaseName}/receipts_*`,
  ],
}));

// ── Analytics Backfiller (P1-4) ──
const backfillerLambda = backend.analyticsBackfillerFn.resources.lambda as lambda.Function;
backfillerLambda.addEnvironment('GLUE_DATABASE', glueDatabaseName);
backfillerLambda.addEnvironment('ATHENA_WORKGROUP', athenaWorkgroupName);
backfillerLambda.addEnvironment('ANALYTICS_BUCKET', analyticsBucketName);
backfillerLambda.addEnvironment('REFDATA_TABLE', 'RefDataEvent');
backfillerLambda.addEnvironment('USER_TABLE', 'UserDataEvent');
backfillerLambda.addEnvironment('USER_HASH_SALT', userHashSalt);

grantS3Access(backfillerLambda, analyticsBucketName, ['s3:GetObject', 's3:PutObject', 's3:ListBucket']);
backfillerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['s3:GetBucketLocation', 's3:GetObject', 's3:ListBucket', 's3:PutObject', 's3:DeleteObject'],
  resources: [
    `arn:aws:s3:::${analyticsBucketName}`,
    `arn:aws:s3:::${analyticsBucketName}/*`,
    'arn:aws:s3:::bebocard-enterprise-*',
    'arn:aws:s3:::bebocard-enterprise-*/*',
  ],
}));

backfillerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:GetQueryResults'],
  resources: [`arn:aws:athena:*:*:workgroup/${athenaWorkgroup.name}`],
}));

backfillerLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['glue:GetDatabase', 'glue:GetTables', 'glue:GetTable', 'glue:UpdateTable', 'glue:CreateTable'],
  resources: [
    `arn:aws:glue:*:*:catalog`,
    `arn:aws:glue:*:*:database/${glueDatabase.ref ?? `bebo_analytics_${stage}`}`,
    `arn:aws:glue:*:*:table/${glueDatabase.ref ?? `bebo_analytics_${stage}`}/receipts_*`,
  ],
}));

grantTableAccess(backfillerLambda, 'RefDataEvent', false);
grantTableAccess(backfillerLambda, 'UserDataEvent', false);
 
// Reserved concurrency — analytics-backfiller: prevents massive backfill scans from consuming entire regional concurrency (P0-5)
const cfnBackfiller = backfillerLambda.node.defaultChild as lambda.CfnFunction;
// cfnBackfiller.reservedConcurrentExecutions = 10;

// Schedule: Nightly at 2:00 AM UTC
const cronRule = new events.Rule(infraStack, 'NightlyCompactionRule', {
  schedule: events.Schedule.expression('cron(0 2 * * ? *)'),
});
cronRule.addTarget(new eventsTargets.LambdaFunction(compactorLambda));

// Analytics Aggregator Schedule: Nightly at 1:00 AM UTC (before compaction)
const aggregatorLambda = backend.analyticsAggregatorFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => aggregatorLambda.addEnvironment(k, v));
grantTableAccess(aggregatorLambda, 'UserDataEvent', false);
grantTableAccess(aggregatorLambda, 'RefDataEvent', false);
grantTableAccess(aggregatorLambda, 'ReportDataEvent', true);

const aggregatorRule = new events.Rule(infraStack, 'NightlyAggregationRule', {
  schedule: events.Schedule.expression('cron(0 1 * * ? *)'),
});
aggregatorRule.addTarget(new eventsTargets.LambdaFunction(aggregatorLambda));

// ── Custom Segment Evaluator (EOD batch) ──
// Nightly at 00:30 UTC — after segment-processor stream has caught up, before analytics compaction at 02:00.
const customSegmentLambda = backend.customSegmentEvaluatorFn.resources.lambda as lambda.Function;
customSegmentLambda.addEnvironment('USER_TABLE', 'UserDataEvent');
customSegmentLambda.addEnvironment('REFDATA_TABLE', 'RefDataEvent');

// Read segment defs from RefDataEvent; write membership records to UserDataEvent
grantTableAccess(customSegmentLambda, 'RefDataEvent', false);
grantTableAccess(customSegmentLambda, 'UserDataEvent', true);

// Update SEGMENT_DEF# stats (memberCount, lastEvaluatedAt) — needs UpdateItem on RefDataEvent
grantTableAccess(customSegmentLambda, 'RefDataEvent', true);

const customSegmentDLQ = new sqs.Queue(infraStack, 'CustomSegmentEvaluatorDLQ', {
  retentionPeriod: Duration.days(14),
});
createDlqAlarm(customSegmentDLQ, 'CustomSegmentEvaluatorDLQ');
customSegmentLambda.addEnvironment('CUSTOM_SEGMENT_DLQ_URL', customSegmentDLQ.queueUrl);
grantSqsAccess(customSegmentLambda, customSegmentDLQ, ['sqs:SendMessage']);

const customSegmentRule = new events.Rule(infraStack, 'NightlyCustomSegmentRule', {
  schedule: events.Schedule.expression('cron(30 0 * * ? *)'),
  description: 'Nightly end-of-day custom segment evaluation at 00:30 UTC',
});
customSegmentRule.addTarget(new eventsTargets.LambdaFunction(customSegmentLambda));

Tags.of(customSegmentLambda).add('Function', 'custom-segment-evaluator');
Tags.of(customSegmentLambda).add('CostCenter', 'tenant-side');

// ── Affiliate Feed Sync (Nightly at 05:00 UTC) ──
const affiliateSyncLambda = backend.affiliateFeedSyncFn.resources.lambda as lambda.Function;
affiliateSyncLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
grantTableAccess(affiliateSyncLambda, 'RefDataEvent', true);

const affiliateSyncRule = new events.Rule(infraStack, 'NightlyAffiliateSyncRule', {
  schedule: events.Schedule.expression('cron(0 5 * * ? *)'),
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
grantTableAccess(receiptClaimLambda, 'UserDataEvent', true);
grantTableAccess(receiptClaimLambda, 'RefDataEvent', true);

// ── Remote Config Wiring (P2-21) ──
const remoteConfigLambda = backend.remoteConfigHandlerFn.resources.lambda as lambda.Function;
remoteConfigLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
grantTableAccess(remoteConfigLambda, 'RefDataEvent', false);

// ── Click Tracking (P2-19) ──
const clickTrackingLambda = backend.clickTrackingHandlerFn.resources.lambda as lambda.Function;
clickTrackingLambda.addEnvironment('REPORT_TABLE', tableNames.REPORT_TABLE);
clickTrackingLambda.addEnvironment('REFDATA_TABLE', refDataTable.tableName);
grantTableAccess(clickTrackingLambda, 'ReportDataEvent', true);
grantTableAccess(clickTrackingLambda, 'RefDataEvent', false);

// ── P1-2 Glue IAM Refinement ──
[tenantProvisionerLambda, receiptIcebergLambda].forEach(fn => {
  fn.addToRolePolicy(new iam.PolicyStatement({
    actions: ['glue:GetDatabase'],
    resources: [
      `arn:aws:glue:*:*:catalog`,
      `arn:aws:glue:*:*:database/${glueDatabaseName}`,
    ],
  }));
});

// ── BeboCard Operational Dashboard (P2-11) ──
new cloudwatch.Dashboard(infraStack, 'BeboCardOpsDashboard', {
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
const cognitoExportBucketName = `bebocard-cognito-exports-${amplifyAppId}-${amplifyBranch}`.toLowerCase();
const cognitoExportBucket = s3.Bucket.fromBucketName(infraStack, 'CognitoExportsImport', cognitoExportBucketName) || new s3.Bucket(infraStack, 'CognitoExports', {
  bucketName: cognitoExportBucketName,
  encryption: s3.BucketEncryption.S3_MANAGED,
  versioned: true,
  lifecycleRules: [{ expiration: Duration.days(90), id: 'expire-old-exports' }],
  removalPolicy: RemovalPolicy.RETAIN,
  blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
});

const cognitoExportLambda = backend.cognitoExportFn.resources.lambda as lambda.Function;
// USER_POOL_ID read from SSM at runtime — no functions→auth token
cognitoExportLambda.addEnvironment('USER_POOL_ID_PARAM', USER_POOL_ID_PARAM);
cognitoExportLambda.addEnvironment('EXPORT_BUCKET', cognitoExportBucketName);
grantS3Access(cognitoExportLambda, cognitoExportBucketName, ['s3:PutObject']);
cognitoExportLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [`arn:aws:ssm:*:*:parameter${USER_POOL_ID_PARAM}`],
}));

cognitoExportLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['cognito-idp:ListUsers'],
  resources: ['arn:aws:cognito-idp:*:*:userpool/*'],
}));

const cognitoExportRule = new events.Rule(infraStack, 'WeeklyCognitoExportRule', {
  schedule: events.Schedule.expression('cron(0 2 ? * SUN *)'),
  description: 'Weekly Cognito user pool export for DR (P0-6)',
});
cognitoExportRule.addTarget(new eventsTargets.LambdaFunction(cognitoExportLambda));
Tags.of(cognitoExportLambda).add('Function', 'cognito-export');
Tags.of(cognitoExportLambda).add('CostCenter', 'ops');

new CfnOutput(infraStack, 'CognitoExportBucketName', {
  value: cognitoExportBucket.bucketName,
  description: 'Cognito DR export bucket — restore from here if pool is lost',
});

// ── P0-6: Composite DR Alarm (3+ tables in error = infrastructure incident) ──
const userTableErrorAlarm = new cloudwatch.Alarm(infraStack, 'UserTableSystemErrors', {
  metric: userTable.metric('SystemErrors', { statistic: 'Sum', period: Duration.minutes(5) }),
  threshold: 5,
  evaluationPeriods: 2,
  alarmDescription: 'UserDataEvent DynamoDB system errors elevated',
});
const refTableErrorAlarm = new cloudwatch.Alarm(infraStack, 'RefTableSystemErrors', {
  metric: refDataTable.metric('SystemErrors', { statistic: 'Sum', period: Duration.minutes(5) }),
  threshold: 5,
  evaluationPeriods: 2,
  alarmDescription: 'RefDataEvent DynamoDB system errors elevated',
});
const adminTableErrorAlarm = new cloudwatch.Alarm(infraStack, 'AdminTableSystemErrors', {
  metric: adminTable.metric('SystemErrors', { statistic: 'Sum', period: Duration.minutes(5) }),
  threshold: 5,
  evaluationPeriods: 2,
  alarmDescription: 'AdminDataEvent DynamoDB system errors elevated',
});

// "Any table in error" composite — DR-level signal; if even one table is failing, page on-call
new cloudwatch.CompositeAlarm(infraStack, 'BebocardDRCompositeAlarm', {
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
grantTableAccess(brandHealthLambda, 'RefDataEvent', false);
grantTableAccess(brandHealthLambda, 'UserDataEvent', false);

const brandHealthRule = new events.Rule(infraStack, 'WeeklyBrandHealthRule', {
  schedule: events.Schedule.expression('cron(0 8 ? * MON *)'),
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
  resources: [`arn:aws:ssm:*:*:parameter/amplify/shared/INTERNAL_SIGNING_SECRET`],
}));
grantTableAccess(templateManagerLambda, 'RefDataEvent', true);
Tags.of(templateManagerLambda).add('Function', 'template-manager');
Tags.of(templateManagerLambda).add('CostCenter', 'ops');

/*
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
  });
}
*/
