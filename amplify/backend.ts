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
import { Stack, Duration, RemovalPolicy } from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';

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
});

// ── Wire post-confirmation trigger ──────────────────────────────────────────
const { cfnUserPool } = backend.auth.resources.cfnResources;
const postConfirmLambda = backend.postConfirmationFn.resources.lambda;

cfnUserPool.lambdaConfig = {
  postConfirmation: postConfirmLambda.functionArn,
};

// ── Grant DynamoDB access ────────────────────────────────────────────────────
const userTable = backend.data.resources.tables['UserDataEvent'];
const refTable = backend.data.resources.tables['RefDataEvent'];
const adminTable = backend.data.resources.tables['AdminDataEvent'];

const tableNames = {
  USER_TABLE: userTable.tableName,
  REFDATA_TABLE: refTable.tableName,
  ADMIN_TABLE: adminTable.tableName,
};

// Post-confirmation: write to UserDataEvent + AdminDataEvent
backend.postConfirmationFn.resources.lambda.addEnvironment('USER_TABLE', userTable.tableName);
backend.postConfirmationFn.resources.lambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
userTable.grantWriteData(postConfirmLambda);
adminTable.grantWriteData(postConfirmLambda);

// Card manager: read/write all three tables
const cardManagerLambda = backend.cardManagerFn.resources.lambda;
Object.entries(tableNames).forEach(([k, v]) => cardManagerLambda.addEnvironment(k, v));
userTable.grantReadWriteData(cardManagerLambda);
refTable.grantReadData(cardManagerLambda);
adminTable.grantReadWriteData(cardManagerLambda);

// Scan handler: read AdminDataEvent (ULID resolution), read/write UserDataEvent (receipts + device token)
// Also reads RefDataEvent for API key validation
const scanLambda = backend.scanHandlerFn.resources.lambda;
scanLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
scanLambda.addEnvironment('USER_TABLE', userTable.tableName);
scanLambda.addEnvironment('REFDATA_TABLE', refTable.tableName);
adminTable.grantReadData(scanLambda);
userTable.grantReadWriteData(scanLambda);
refTable.grantReadData(scanLambda);

// ── Tenant linker: REST OAuth flow (read/write UserDataEvent + AdminDataEvent) ──
const tenantLinkerLambda = backend.tenantLinker.resources.lambda;
Object.entries({ USER_TABLE: userTable.tableName, ADMIN_TABLE: adminTable.tableName })
  .forEach(([k, v]) => tenantLinkerLambda.addEnvironment(k, v));
userTable.grantReadWriteData(tenantLinkerLambda);
adminTable.grantReadWriteData(tenantLinkerLambda);
// Brand OAuth secrets must be set separately via `amplify secret set`:
//   WOOLWORTHS_CLIENT_ID, WOOLWORTHS_CLIENT_SECRET
//   FLYBUYS_CLIENT_ID, FLYBUYS_CLIENT_SECRET
//   VELOCITY_CLIENT_ID, VELOCITY_CLIENT_SECRET
//   QANTAS_CLIENT_ID, QANTAS_CLIENT_SECRET

// ── Geofence handler: read/write UserDataEvent + read RefDataEvent + read AdminDataEvent ──
// AdminDataEvent: resolves secondaryULID → permULID on geofence entry.
// Also requires FIREBASE_SERVICE_ACCOUNT_JSON secret for FCM push sends.
// Set via: amplify secret set FIREBASE_SERVICE_ACCOUNT_JSON
const geofenceLambda = backend.geofenceHandlerFn.resources.lambda;
geofenceLambda.addEnvironment('USER_TABLE',  userTable.tableName);
geofenceLambda.addEnvironment('REF_TABLE',   refTable.tableName);
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
      allowedMethods:  [s3.HttpMethods.PUT],
      allowedOrigins:  ['https://business.bebocard.com.au', 'http://localhost:3000'],
      allowedHeaders:  ['*'],
      exposedHeaders:  ['ETag'],
      maxAge:          300,
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
    blockPublicAcls:       false,
    ignorePublicAcls:      false,
    blockPublicPolicy:     false,
    restrictPublicBuckets: false,
  }),
  publicReadAccess: true,
});

// Content validator Lambda permissions
const validatorLambda = backend.contentValidatorFn.resources.lambda;

// Read from staging, write to staging (rejected/ prefix), write to reference
tenantUploadsBucket.grantRead(validatorLambda);
tenantUploadsBucket.grantPut(validatorLambda);  // for rejected/ copy
appReferenceBucket.grantPut(validatorLambda);

// Rekognition permission
validatorLambda.addToRolePolicy(new iam.PolicyStatement({
  actions:   ['rekognition:DetectModerationLabels'],
  resources: ['*'],
}));

// DynamoDB access
refTable.grantReadWriteData(validatorLambda);
adminTable.grantWriteData(validatorLambda);

// Environment variables
validatorLambda.addEnvironment('TENANT_UPLOADS_BUCKET', tenantUploadsBucket.bucketName);
validatorLambda.addEnvironment('APP_REFERENCE_BUCKET',  appReferenceBucket.bucketName);
validatorLambda.addEnvironment('REFDATA_TABLE',         refTable.tableName);
validatorLambda.addEnvironment('ADMIN_TABLE',           adminTable.tableName);

// S3 event notification → content-validator on every new object in brands/ prefix
tenantUploadsBucket.addEventNotification(
  s3.EventType.OBJECT_CREATED_PUT,
  new s3n.LambdaDestination(validatorLambda),
  { prefix: 'brands/', suffix: undefined },
);

// Expose bucket names so the portal's presigned-URL API route can read them
backend.contentValidatorFn.resources.lambda.addEnvironment(
  'TENANT_UPLOADS_BUCKET', tenantUploadsBucket.bucketName,
);

// ── Brand API handler ─────────────────────────────────────────────────────────
const brandApiLambda = backend.brandApiHandlerFn.resources.lambda;
Object.entries(tableNames).forEach(([k, v]) => brandApiLambda.addEnvironment(k, v));
userTable.grantReadWriteData(brandApiLambda);
refTable.grantReadWriteData(brandApiLambda);
adminTable.grantReadWriteData(brandApiLambda);
// FIREBASE_SERVICE_ACCOUNT_JSON must be set via: amplify secret set FIREBASE_SERVICE_ACCOUNT_JSON

// ── Reminder handler (EventBridge daily 9am AEST = 21:00 UTC) ────────────────
const reminderLambda = backend.reminderHandlerFn.resources.lambda;
reminderLambda.addEnvironment('USER_TABLE', userTable.tableName);
reminderLambda.addEnvironment('ADMIN_TABLE', adminTable.tableName);
userTable.grantReadData(reminderLambda);
adminTable.grantReadWriteData(reminderLambda);  // write sent-log to prevent duplicate pushes

const reminderStack = Stack.of(reminderLambda);
new events.Rule(reminderStack, 'DailyReminderRule', {
  schedule: events.Schedule.cron({ hour: '21', minute: '0' }),
  targets: [new eventsTargets.LambdaFunction(reminderLambda)],
  description: 'Triggers reminder-handler daily at 9am AEST (21:00 UTC)',
});

// ── Public Function URL for scan-handler (POS access) ───────────────────────
// In production use API Gateway. Function URL is sufficient for dev/staging.
export default backend;
