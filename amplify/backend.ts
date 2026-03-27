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
import { Stack, Duration, RemovalPolicy, Tags, CfnOutput } from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as apigw from 'aws-cdk-lib/aws-apigateway';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as athena from 'aws-cdk-lib/aws-athena';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
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
// The post-confirmation Lambda uses SSM parameters to discover table names at
// runtime instead of direct CFN references, breaking the Auth→Data→Auth cycle.
const dataStack = Stack.of(userTable);
const branchName = dataStack.stackName.toLowerCase().includes('prod') ? 'prod' : 'sandbox';
const userTableParamName = `/bebocard/${branchName}/USER_TABLE`;
const adminTableParamName = `/bebocard/${branchName}/ADMIN_TABLE`;

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

// Grant SSM read and wildcard DynamoDB access to completely decouple CFN refs
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['ssm:GetParameter'],
  resources: [
    `arn:aws:ssm:${dataStack.region}:${dataStack.account}:parameter/bebocard/${branchName}/*`,
  ],
}));
postConfirmLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['dynamodb:PutItem', 'dynamodb:UpdateItem'],
  resources: [
    userTable.tableArn,
    adminTable.tableArn,
  ],
}));

// ── Card manager: read/write all three tables ────────────────────────────────
const cardManagerLambda = backend.cardManagerFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => cardManagerLambda.addEnvironment(k, v));
userTable.grantReadWriteData(cardManagerLambda);
refTable.grantReadData(cardManagerLambda);
adminTable.grantReadWriteData(cardManagerLambda);

// Scan handler: read AdminDataEvent (ULID resolution), read/write UserDataEvent (receipts + device token)
// Also reads RefDataEvent for API key validation
const scanLambda = backend.scanHandlerFn.resources.lambda as lambda.Function;
scanLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
scanLambda.addEnvironment('USER_TABLE', userTable.tableName);
scanLambda.addEnvironment('REFDATA_TABLE', refTable.tableName);
adminTable.grantReadData(scanLambda);
userTable.grantReadWriteData(scanLambda);
refTable.grantReadData(scanLambda);

// ── Tenant linker: REST OAuth flow (read/write UserDataEvent + AdminDataEvent) ──
const tenantLinkerLambda = backend.tenantLinker.resources.lambda as lambda.Function;
Object.entries({ USER_TABLE: userTable.tableName, ADMIN_TABLE: adminTable.tableName })
  .forEach(([k, v]) => tenantLinkerLambda.addEnvironment(k, v));
userTable.grantReadWriteData(tenantLinkerLambda);
adminTable.grantReadWriteData(tenantLinkerLambda);
// Brand OAuth secrets must be set separately via `amplify secret set`:
//   WOOLWORTHS_CLIENT_ID, WOOLWORTHS_CLIENT_SECRET
//   FLYBUYS_CLIENT_ID, FLYBUYS_CLIENT_SECRET
//   VELOCITY_CLIENT_ID, VELOCITY_CLIENT_SECRET
//   QANTAS_CLIENT_ID, QANTAS_CLIENT_SECRET

// Function URL — OAuth redirect flow requires a deterministic HTTPS URL.
// Register <url>/auth/callback/{brandId} as the redirect_uri in each brand's
// OAuth application configuration. The handler validates state tokens internally;
// no API Gateway authorizer needed.
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
  description: 'Tenant linker OAuth base URL — register <url>/auth/callback/{brandId} at each brand OAuth portal',
});

// ── Geofence handler: read/write UserDataEvent + read RefDataEvent + read AdminDataEvent ──
// AdminDataEvent: resolves secondaryULID → permULID on geofence entry.
// Also requires FIREBASE_SERVICE_ACCOUNT_JSON secret for FCM push sends.
// Set via: amplify secret set FIREBASE_SERVICE_ACCOUNT_JSON
const geofenceLambda = backend.geofenceHandlerFn.resources.lambda as lambda.Function;
geofenceLambda.addEnvironment('USER_TABLE', userTable.tableName);
geofenceLambda.addEnvironment('REF_TABLE', refTable.tableName);
geofenceLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
userTable.grantReadWriteData(geofenceLambda);
refTable.grantReadData(geofenceLambda);
adminTable.grantReadData(geofenceLambda);

// ── Content pipeline: S3 buckets + validator Lambda ─────────────────────────
const stack = Stack.of(backend.contentValidatorFn.resources.lambda);
const stage = backend.contentValidatorFn.resources.lambda.stack.stackName
  .toLowerCase().includes('prod') ? 'prod' : 'dev';

// Tenant staging bucket — brands upload here via presigned URL
const tenantUploadsBucket = new s3.Bucket(stack, 'TenantUploads', {
  bucketName: `bebocard-tenant-uploads-${stage}`,
  removalPolicy: RemovalPolicy.RETAIN,
  cors: [
    {
      allowedMethods: [s3.HttpMethods.PUT],
      allowedOrigins: ['https://business.bebocard.com.au', 'http://localhost:3000'],
      allowedHeaders: ['*'],
      exposedHeaders: ['ETag'],
      maxAge: 300,
    },
  ],
  // Auto-expire staging objects after 7 days (validator moves them to reference or rejected/)
  lifecycleRules: [
    {
      id: 'StagingCleanup',
      prefix: 'brands/',
      expiration: Duration.days(7),
    },
  ],
  blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
});

// App reference bucket — CDN-served, publicly readable, managed entirely by the validator Lambda
const appReferenceBucket = new s3.Bucket(stack, 'AppReference', {
  bucketName: `bebocard-app-reference-${stage}`,
  removalPolicy: RemovalPolicy.RETAIN,
  blockPublicAccess: new s3.BlockPublicAccess({
    blockPublicAcls: false,
    ignorePublicAcls: false,
    blockPublicPolicy: false,
    restrictPublicBuckets: false,
  }),
  publicReadAccess: true,
});

// Content validator Lambda permissions
const validatorLambda = backend.contentValidatorFn.resources.lambda as lambda.Function;

// Read from staging, write to staging (rejected/ prefix), write to reference
tenantUploadsBucket.grantRead(validatorLambda);
tenantUploadsBucket.grantPut(validatorLambda);  // for rejected/ copy
appReferenceBucket.grantPut(validatorLambda);

// Rekognition permission
validatorLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['rekognition:DetectModerationLabels'],
  resources: ['*'],
}));

// DynamoDB access
refTable.grantReadWriteData(validatorLambda);
adminTable.grantWriteData(validatorLambda);

// Environment variables
validatorLambda.addEnvironment('TENANT_UPLOADS_BUCKET', tenantUploadsBucket.bucketName);
validatorLambda.addEnvironment('APP_REFERENCE_BUCKET', appReferenceBucket.bucketName);
validatorLambda.addEnvironment('REFDATA_TABLE', refTable.tableName);
validatorLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);

// S3 event notification → content-validator on every new object in brands/ prefix
tenantUploadsBucket.addEventNotification(
  s3.EventType.OBJECT_CREATED_PUT,
  new s3n.LambdaDestination(validatorLambda),
  { prefix: 'brands/', suffix: undefined },
);

// Expose bucket names so the portal's presigned-URL API route can read them
validatorLambda.addEnvironment(
  'TENANT_UPLOADS_BUCKET', tenantUploadsBucket.bucketName,
);

// ── Brand API handler ─────────────────────────────────────────────────────────
const brandApiLambda = backend.brandApiHandlerFn.resources.lambda as lambda.Function;
Object.entries(tableNames).forEach(([k, v]) => brandApiLambda.addEnvironment(k, v));
userTable.grantReadWriteData(brandApiLambda);
refTable.grantReadWriteData(brandApiLambda);
adminTable.grantReadWriteData(brandApiLambda);
// FIREBASE_SERVICE_ACCOUNT_JSON must be set via: amplify secret set FIREBASE_SERVICE_ACCOUNT_JSON

// ── Reminder handler (EventBridge daily at 21:00 UTC) ────────────────────────
const reminderLambda = backend.reminderHandlerFn.resources.lambda as lambda.Function;
reminderLambda.addEnvironment('USER_TABLE', userTable.tableName);
reminderLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
userTable.grantReadData(reminderLambda);
adminTable.grantReadWriteData(reminderLambda);  // write sent-log to prevent duplicate pushes

const reminderStack = Stack.of(reminderLambda);
new events.Rule(reminderStack, 'DailyReminderRule', {
  schedule: events.Schedule.cron({ hour: '21', minute: '0' }),
  targets: [new eventsTargets.LambdaFunction(reminderLambda)],
  description: 'Triggers reminder-handler daily at 21:00 UTC',
});

// ── Segment processor (DynamoDB Streams → UserDataEvent) ─────────────────────
// Recomputes SEGMENT#<brandId> on every RECEIPT# write.
// Sets subscribed: boolean by checking SUBSCRIPTION#<brandId> at write time
// so tenant-analytics can filter without a per-user join.
const segmentLambda = backend.segmentProcessorFn.resources.lambda as lambda.Function;
segmentLambda.addEnvironment('USER_TABLE', userTable.tableName);
userTable.grantReadWriteData(segmentLambda);

// Enable DynamoDB Streams on UserDataEvent and attach INSERT/MODIFY records
// to the segment processor.
if (cfnUserTable) {
  cfnUserTable.streamSpecification = { streamViewType: 'NEW_IMAGE' };
}

segmentLambda.addEventSource(new DynamoEventSource(userTable, {
  startingPosition: lambda.StartingPosition.TRIM_HORIZON,
  batchSize: 100,
  bisectBatchOnError: true,
  retryAttempts: 2,
  // Only process inserts and updates — deletes don't change segment stats
  filters: [
    lambda.FilterCriteria.filter({
      eventName: lambda.FilterRule.isEqual('INSERT'),
    }),
    lambda.FilterCriteria.filter({
      eventName: lambda.FilterRule.isEqual('MODIFY'),
    }),
  ],
}));

// ── Tenant analytics: read UserDataEvent (segment queries) + read RefDataEvent (API key validation) ──
const tenantAnalyticsLambda = backend.tenantAnalyticsFn.resources.lambda as lambda.Function;
tenantAnalyticsLambda.addEnvironment('USER_TABLE', userTable.tableName);
tenantAnalyticsLambda.addEnvironment('REFDATA_TABLE', refTable.tableName);
userTable.grantReadData(tenantAnalyticsLambda);
refTable.grantReadData(tenantAnalyticsLambda);

// ── API Gateway — tenant analytics REST API ───────────────────────────────────
// Usage plans enforce per-tenant throttling + quota at gateway level before
// the Lambda is invoked. The Lambda still validates the bebo_* key for auth.
//
// Key provisioning at tenant onboarding (done via brand portal / admin CLI):
// 1. Call createTenantApiKey() → rawKey (bebo_<keyId>.<secret>)
// 2. Create API Gateway API key with value = rawKey, associate with the
//    correct usage plan. Now the same key enforces both throttling (gateway)
//    and identity/scope (Lambda).
const analyticsApi = new apigw.RestApi(stack, 'TenantAnalyticsApi', {
  restApiName: `bebo-tenant-analytics-${stage}`,
  description: 'Tenant-authenticated aggregate analytics pull API',
  defaultCorsPreflightOptions: {
    allowOrigins: apigw.Cors.ALL_ORIGINS,
    allowMethods: ['GET', 'OPTIONS'],
    allowHeaders: ['Authorization', 'x-api-key', 'Content-Type'],
  },
});

const analyticsIntegration = new apigw.LambdaIntegration(tenantAnalyticsLambda);
const analyticsRoot = analyticsApi.root.addResource('analytics');
analyticsRoot.addResource('segments').addMethod('GET', analyticsIntegration, {
  apiKeyRequired: true,
});

// Usage plan tiers — burst/rate in requests/sec; quota per day
const planDefs = [
  { id: 'Starter', name: 'starter', rate: 10, burst: 20, quota: 500 },
  { id: 'Growth', name: 'growth', rate: 50, burst: 100, quota: 5_000 },
  { id: 'Enterprise', name: 'enterprise', rate: 200, burst: 500, quota: 50_000 },
] as const;

for (const p of planDefs) {
  const plan = analyticsApi.addUsagePlan(`${p.id}Plan`, {
    name: p.name,
    throttle: { rateLimit: p.rate, burstLimit: p.burst },
    quota: { limit: p.quota, period: apigw.Period.DAY },
  });
  plan.addApiStage({ api: analyticsApi, stage: analyticsApi.deploymentStage });
}

// ── Scan API — public REST for brand POS backends ────────────────────────────
// POST /scan    → loyalty check at checkout (brand backend calls this)
// POST /receipt → receipt push after payment (brand backend calls this)
//
// The same bebo_* API key value serves both API Gateway throttling (native API
// key) and Lambda-level auth/scope validation. At brand onboarding, create an
// API Gateway key whose value equals the bebo_* key returned by createBrandApiKey(),
// then associate it with the appropriate scan usage plan.
const scanApi = new apigw.RestApi(stack, 'ScanApi', {
  restApiName: `bebo-scan-api-${stage}`,
  description: 'Public POS scan API — called by brand backends at checkout, not end-users',
  defaultCorsPreflightOptions: {
    allowOrigins: apigw.Cors.ALL_ORIGINS,
    allowMethods: ['POST', 'OPTIONS'],
    allowHeaders: ['x-api-key', 'Content-Type'],
  },
});

const scanIntegration = new apigw.LambdaIntegration(scanLambda);
scanApi.root.addResource('scan').addMethod('POST', scanIntegration, { apiKeyRequired: true });
scanApi.root.addResource('receipt').addMethod('POST', scanIntegration, { apiKeyRequired: true });

// Higher quotas than analytics — real POS scan traffic at checkout is bursty
const scanPlanDefs = [
  { id: 'ScanStarter', name: 'scan-starter', rate: 50, burst: 100, quota: 10_000 },
  { id: 'ScanGrowth', name: 'scan-growth', rate: 200, burst: 500, quota: 100_000 },
  { id: 'ScanEnterprise', name: 'scan-enterprise', rate: 1_000, burst: 2_000, quota: 1_000_000 },
] as const;

for (const p of scanPlanDefs) {
  const plan = scanApi.addUsagePlan(`${p.id}Plan`, {
    name: p.name,
    throttle: { rateLimit: p.rate, burstLimit: p.burst },
    quota: { limit: p.quota, period: apigw.Period.DAY },
  });
  plan.addApiStage({ api: scanApi, stage: scanApi.deploymentStage });
}

new CfnOutput(stack, 'ScanApiUrl', {
  value: scanApi.url,
  description: 'Scan API base URL — brand backends call POST /scan and POST /receipt',
});

// ── Brand portal API ───────────────────────────────────────────────────────────
// REST API for brand B2B portal: offer / newsletter / catalogue management,
// store management, analytics, presigned upload URLs, API key rotation.
// All routing is handled internally by brand-api-handler; the gateway acts as
// an authenticated proxy with usage-plan throttling.
const brandPortalApi = new apigw.RestApi(stack, 'BrandPortalApi', {
  restApiName: `bebo-brand-api-${stage}`,
  description: 'Brand portal B2B REST API — bebo_* key authenticated',
  defaultCorsPreflightOptions: {
    allowOrigins: ['https://business.bebocard.com.au', 'http://localhost:3000'],
    allowMethods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowHeaders: ['x-api-key', 'Content-Type'],
  },
});

const brandPortalIntegration = new apigw.LambdaIntegration(brandApiLambda);
// Root + greedy {proxy+} — handler owns all path-based routing
brandPortalApi.root.addMethod('ANY', brandPortalIntegration, { apiKeyRequired: true });
brandPortalApi.root.addProxy({
  defaultIntegration: brandPortalIntegration,
  anyMethod: true,
  defaultMethodOptions: { apiKeyRequired: true },
});

const brandPortalPlan = brandPortalApi.addUsagePlan('BrandPortalPlan', {
  name: 'brand-portal',
  throttle: { rateLimit: 100, burstLimit: 200 },
  quota: { limit: 10_000, period: apigw.Period.DAY },
});
brandPortalPlan.addApiStage({ api: brandPortalApi, stage: brandPortalApi.deploymentStage });

new CfnOutput(stack, 'BrandPortalApiUrl', {
  value: brandPortalApi.url,
  description: 'Brand portal API base URL',
});

// ── Receipt analytics — Iceberg on S3 (tenant-side, Phase 2) ─────────────────
//
// Every RECEIPT# write that originated via the brand POS scan path (/receipt
// endpoint) carries secondaryULID in its desc JSON.  The receipt-iceberg-writer
// Lambda reads the DynamoDB stream, extracts those records, hashes permULID with
// HMAC-SHA256, and inserts a pseudonymous row into the Iceberg table via Athena.
//
// Write condition: secondaryULID present in desc  (POS-path marker, set by scan-handler)
// Privacy:         permULID replaced with HMAC-SHA256(permULID, USER_HASH_SALT)
// Format:          Apache Iceberg v1 via Athena Engine v3 + Glue Data Catalog
// Partitioning:    brand_id, purchase_date (month truncation in table definition)
//
// USER_HASH_SALT must be set via: amplify secret set USER_HASH_SALT
// The salt must never change after receipts are written — changing it makes all
// existing user_hash values non-joinable.

// Receipt analytics S3 bucket — private, no public access
const receiptAnalyticsBucket = new s3.Bucket(stack, 'ReceiptAnalytics', {
  bucketName: `bebocard-receipt-analytics-${stage}`,
  removalPolicy: RemovalPolicy.RETAIN,
  blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
  lifecycleRules: [
    // Expire Athena result files after 30 days
    {
      id: 'AthenaResultsCleanup',
      prefix: 'athena-results/',
      expiration: Duration.days(30),
    },
  ],
});

// Glue Data Catalog database for all BeboCard analytics tables
const glueDatabase = new glue.CfnDatabase(stack, 'BeboAnalyticsDatabase', {
  catalogId: stack.account,
  databaseInput: {
    name: 'bebo_analytics',
    description: 'BeboCard pseudonymous receipt analytics — Iceberg tables, no PII',
  },
});

// Iceberg receipts table
// Athena Engine v3 reads/writes Iceberg format via these StorageDescriptor settings.
// INSERT is performed by the receipt-iceberg-writer Lambda via Athena API.
// DO NOT query directly with engine v2 — Iceberg metadata will be corrupted.
const icebergReceiptsTable = new glue.CfnTable(stack, 'ReceiptsIcebergTable', {
  catalogId: stack.account,
  databaseName: 'bebo_analytics',
  tableInput: {
    name: 'receipts',
    tableType: 'EXTERNAL_TABLE',
    parameters: {
      'table_type': 'ICEBERG',
      'metadata_location': `s3://${receiptAnalyticsBucket.bucketName}/iceberg/receipts/`,
      'write.target-file-size-bytes': '134217728',  // 128 MiB target part size
    },
    storageDescriptor: {
      location: `s3://${receiptAnalyticsBucket.bucketName}/iceberg/receipts/`,
      inputFormat: 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
      outputFormat: 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
      serdeInfo: {
        serializationLibrary: 'org.apache.iceberg.mr.hive.HiveIcebergSerDe',
      },
      columns: [
        { name: 'user_hash', type: 'string', comment: 'HMAC-SHA256(permULID, salt) — pseudonymous, never reversible' },
        { name: 'brand_id', type: 'string', comment: 'Brand identifier (e.g. woolworths)' },
        { name: 'purchase_date', type: 'date', comment: 'Transaction date YYYY-MM-DD' },
        { name: 'amount', type: 'double', comment: 'Transaction amount in local currency' },
        { name: 'currency', type: 'string', comment: 'ISO 4217 currency code (default AUD)' },
        { name: 'category', type: 'string', comment: 'Merchant category (brand-supplied)' },
        { name: 'merchant', type: 'string', comment: 'Merchant name (brand-supplied)' },
        { name: 'ingested_at', type: 'timestamp', comment: 'UTC ingestion timestamp' },
      ],
    },
  },
});
icebergReceiptsTable.addDependency(glueDatabase);

// Athena workgroup — Engine v3 required for Iceberg support
const analyticsWorkgroup = new athena.CfnWorkGroup(stack, 'BeboAnalyticsWorkgroup', {
  name: `bebo-analytics-${stage}`,
  description: 'Athena Engine v3 workgroup for BeboCard Iceberg analytics',
  workGroupConfiguration: {
    engineVersion: {
      selectedEngineVersion: 'Athena engine version 3',
    },
    resultConfiguration: {
      outputLocation: `s3://${receiptAnalyticsBucket.bucketName}/athena-results/`,
    },
    enforceWorkGroupConfiguration: true,
    publishCloudWatchMetricsEnabled: true,
  },
  state: 'ENABLED',
});

// Receipt Iceberg writer Lambda
const receiptIcebergLambda = backend.receiptIcebergWriterFn.resources.lambda as lambda.Function;
receiptIcebergLambda.addEnvironment('ANALYTICS_BUCKET', receiptAnalyticsBucket.bucketName);
receiptIcebergLambda.addEnvironment('ATHENA_WORKGROUP', analyticsWorkgroup.name);
receiptIcebergLambda.addEnvironment('GLUE_DATABASE', 'bebo_analytics');

// S3: write Iceberg data files + Athena result files
receiptAnalyticsBucket.grantReadWrite(receiptIcebergLambda);

// Athena: start queries + read results
receiptIcebergLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: [
    'athena:StartQueryExecution',
    'athena:GetQueryExecution',
    'athena:GetQueryResults',
  ],
  resources: [
    `arn:aws:athena:${stack.region}:${stack.account}:workgroup/bebo-analytics-${stage}`,
  ],
}));

// Glue: read table metadata for Iceberg manifest resolution
receiptIcebergLambda.addToRolePolicy(new iam.PolicyStatement({
  actions: [
    'glue:GetDatabase',
    'glue:GetTable',
    'glue:GetPartitions',
    'glue:UpdateTable',
    'glue:BatchCreatePartition',
  ],
  resources: [
    `arn:aws:glue:${stack.region}:${stack.account}:catalog`,
    `arn:aws:glue:${stack.region}:${stack.account}:database/bebo_analytics`,
    `arn:aws:glue:${stack.region}:${stack.account}:table/bebo_analytics/receipts`,
  ],
}));

// DynamoDB Streams trigger — shares the same UserDataEvent stream as segment-processor.
// Only INSERT events are processed; secondaryULID presence is checked inside the handler.
receiptIcebergLambda.addEventSource(new DynamoEventSource(userTable, {
  startingPosition: lambda.StartingPosition.LATEST,
  batchSize: 50,
  bisectBatchOnError: true,
  retryAttempts: 2,
  filters: [
    lambda.FilterCriteria.filter({
      eventName: lambda.FilterRule.isEqual('INSERT'),
    }),
  ],
}));

// ── Cost allocation and environment tags ──────────────────────────────────────
//
// Tags propagate to all child resources within each CDK Stack, satisfying:
//   - Cost Explorer filtering by Project / Environment
//   - AWS Cost Allocation Tag activation (must be enabled once in Billing console)
//   - Per-function cost breakdown via the Function tag in Cost Explorer
//
// CostCenter values: 'user-side' for consumer features, 'tenant-side' for B2B
// API / analytics features. Splitting here enables approximate cost attribution
// to Phase 1 (free tier) vs Phase 2 (tenant-billed) workloads.

const tagEntries: Array<[string, string]> = [
  ['Project', 'bebocard'],
  ['Environment', branchName],   // 'prod' | 'sandbox'
  ['ManagedBy', 'amplify-gen2'],
];

// Tag every identified stack so all resources within inherit the tags.
// Lambda functions may each live in their own nested stack — tag them individually.
const identifiedStacks = new Set<Stack>([dataStack, stack]);
for (const fn of [
  postConfirmLambda, cardManagerLambda, scanLambda, tenantLinkerLambda,
  geofenceLambda, validatorLambda, brandApiLambda, reminderLambda,
  tenantAnalyticsLambda, segmentLambda, receiptIcebergLambda,
]) {
  identifiedStacks.add(Stack.of(fn));
}
for (const s of identifiedStacks) {
  for (const [key, value] of tagEntries) Tags.of(s).add(key, value);
}

// Per-resource Function and CostCenter tags for granular Cost Explorer filtering.
// 'user-side' = Phase 1 free-tier; 'tenant-side' = Phase 2 B2B revenue workloads.
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
];
for (const [fn, functionName, costCenter] of functionTags) {
  Tags.of(fn).add('Function', functionName);
  Tags.of(fn).add('CostCenter', costCenter);
}

// S3 buckets and API Gateways
for (const [key, value] of tagEntries) {
  Tags.of(tenantUploadsBucket).add(key, value);
  Tags.of(appReferenceBucket).add(key, value);
  Tags.of(receiptAnalyticsBucket).add(key, value);
  Tags.of(analyticsApi).add(key, value);
  Tags.of(scanApi).add(key, value);
  Tags.of(brandPortalApi).add(key, value);
}
Tags.of(tenantUploadsBucket).add('CostCenter', 'tenant-side');
Tags.of(appReferenceBucket).add('CostCenter', 'tenant-side');
Tags.of(receiptAnalyticsBucket).add('CostCenter', 'tenant-side');
Tags.of(analyticsApi).add('CostCenter', 'tenant-side');
Tags.of(analyticsApi).add('Function', 'tenant-analytics-api');
Tags.of(scanApi).add('CostCenter', 'tenant-side');
Tags.of(scanApi).add('Function', 'scan-api');
Tags.of(brandPortalApi).add('CostCenter', 'tenant-side');
Tags.of(brandPortalApi).add('Function', 'brand-portal-api');

export default backend;
