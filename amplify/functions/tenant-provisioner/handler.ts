import { DynamoDBStreamHandler } from 'aws-lambda';
import { GlueClient, CreateTableCommand, GetTableCommand, EntityNotFoundException } from '@aws-sdk/client-glue';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { S3Client, PutBucketPolicyCommand, CreateBucketCommand, HeadBucketCommand } from '@aws-sdk/client-s3';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { randomBytes } from 'crypto';

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const glue = new GlueClient({});
const GLUE_DATABASE = process.env.GLUE_DATABASE!;
const ANALYTICS_BUCKET = process.env.ANALYTICS_BUCKET!;
const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const CURRENT_COMPLIANCE_VERSION = 2; // Incremented for EU AI Act 2026 updates (P3-13)

export const handler: DynamoDBStreamHandler = async (event) => {
  for (const record of event.Records) {
    if (record.eventName !== 'INSERT' && record.eventName !== 'MODIFY') continue;

    const newItem = unmarshall(record.dynamodb?.NewImage as any);
    const pK = newItem.pK as string;
    const sK = newItem.sK as string;

    // We only care about Tenant profiles
    if (!pK.startsWith('TENANT#') || sK !== 'profile') continue;

    const tenantId = pK.replace('TENANT#', '');
    const tier = newItem.tier || 'ENGAGEMENT';
    const desc = JSON.parse(newItem.desc || '{}');
    const jurisdictions: string[] = desc.jurisdictions || ['AU'];
    const tenantComplianceVersion = desc.complianceVersion || 0;

    console.info(`[tenant-provisioner] Processing tenant ${tenantId} (Tier: ${tier}). Ver: ${tenantComplianceVersion} vs ${CURRENT_COMPLIANCE_VERSION}`);

    // Trigger update if record is new, modified, or has a stale compliance version (P3-13)
    const needsUpdate = record.eventName === 'INSERT' || tenantComplianceVersion < CURRENT_COMPLIANCE_VERSION;

    if (needsUpdate) {
      await deriveAndSaveCompliance(tenantId, jurisdictions, desc);
    }

    if (tier === 'INTELLIGENCE' || tier === 'ENTERPRISE') {
      await Promise.all([
        provisionAnalytics(tenantId, tier, desc.analyticsConfig || {}, jurisdictions),
        ensureTenantSalt(tenantId, newItem)
      ]);
    }
  }
};

async function deriveAndSaveCompliance(tenantId: string, jurisdictions: string[], currentDesc: any) {
  const flags = {
    requiresDPA: jurisdictions.some(j => ['AU', 'EU', 'UK', 'UAE'].includes(j)),
    requiresCCPASPA: jurisdictions.includes('US'),
    requiresBIPADisclosure: jurisdictions.includes('US'),
    requiresAgeVerification: jurisdictions.some(j => ['IN', 'UAE'].includes(j)),
    requiresDPDPA: jurisdictions.includes('IN'),
  };

  const regions: Record<string, string> = {};
  if (jurisdictions.includes('AU')) regions['AU'] = 'ap-southeast-2';
  if (jurisdictions.includes('US')) regions['US'] = 'us-east-1';
  if (jurisdictions.includes('EU') || jurisdictions.includes('UK')) regions['GLOBAL'] = 'eu-west-1';
  if (jurisdictions.includes('UAE')) regions['UAE'] = 'me-south-1';
  if (jurisdictions.includes('IN')) regions['IN'] = 'ap-south-1';

  // Deletion window is the max required across selected jurisdictions
  const deletionWindow = jurisdictions.includes('US') ? 45 : 30;

  const updatedDesc = {
    ...currentDesc,
    complianceVersion: CURRENT_COMPLIANCE_VERSION,
    complianceFlags: flags,
    dataResidencyRegions: regions,
    deletionResponseWindowDays: deletionWindow,
    // ...
    taxTypes: jurisdictions.map(j => ({
      jurisdiction: j,
      type: j === 'AU' ? 'ABN' : j === 'US' ? 'EIN' : j === 'IN' ? 'GSTIN' : j === 'UAE' ? 'TRN' : 'VAT'
    }))
  };

  await ddb.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'profile' },
    UpdateExpression: 'SET #desc = :d, updatedAt = :now',
    ExpressionAttributeNames: { '#desc': 'desc' },
    ExpressionAttributeValues: {
      ':d': JSON.stringify(updatedDesc),
      ':now': new Date().toISOString()
    }
  }));
}

async function ensureTenantSalt(tenantId: string, item: any) {
  const desc = JSON.parse(item.desc || '{}');
  if (desc.salt) return;

  const salt = randomBytes(32).toString('hex');
  const updatedDesc = JSON.stringify({ ...desc, salt });

  console.info(`[tenant-provisioner] Generating unique analytics salt for tenant ${tenantId}`);

  await ddb.send(new UpdateCommand({
    TableName: REFDATA_TABLE,
    Key: { pK: `TENANT#${tenantId}`, sK: 'profile' },
    UpdateExpression: 'SET #desc = :d, updatedAt = :now',
    ExpressionAttributeNames: { '#desc': 'desc' },
    ExpressionAttributeValues: {
      ':d': updatedDesc,
      ':now': new Date().toISOString()
    }
  }));
}

async function provisionAnalytics(tenantId: string, tier: string, config: any, jurisdictions: string[]) {
  const tableName = `receipts_${tenantId.toLowerCase().replace(/[^a-z0-9_]/g, '_')}`;
  
  // Jurisdiction-aware location with region override support (P3-2)
  const regionMap: Record<string, string> = {
    'AU': 'ap-southeast-2',
    'US': 'us-east-1',
    'EU': 'eu-west-1',
    'UK': 'eu-west-1',
    'UAE': 'me-south-1',
    'IN': 'ap-south-1'
  };
  
  const targetRegion = config.dataResidencyRegion || regionMap[jurisdictions[0] || 'AU'] || 'ap-southeast-2';
  const s3Client = new S3Client({ region: targetRegion });

  let s3Location = `s3://${ANALYTICS_BUCKET}/${tenantId}/receipts/`;
  let bucketName = ANALYTICS_BUCKET;
  
  if (tier === 'ENTERPRISE' && config.customBucketArn) {
    bucketName = config.customBucketArn.split(':').pop();
    s3Location = `s3://${bucketName}/bebocard-receipts/`;
  } else if (tier === 'ENTERPRISE' && config.bucketType === 'DEDICATED') {
    bucketName = `bebocard-enterprise-${tenantId.toLowerCase()}-${targetRegion}`;
    s3Location = `s3://${bucketName}/receipts/`;
    
    // Ensure dedicated bucket exists in the target region (P3-2)
    try {
      await s3Client.send(new HeadBucketCommand({ Bucket: bucketName }));
    } catch (e) {
      console.info(`[tenant-provisioner] Creating dedicated bucket ${bucketName} in ${targetRegion}`);
      await s3Client.send(new CreateBucketCommand({ 
        Bucket: bucketName,
        CreateBucketConfiguration: targetRegion === 'us-east-1' ? undefined : { LocationConstraint: targetRegion as any }
      }));
    }

    // Apply Cross-Account S3 Access (P3-1)
    if (config.crossAccountRoleArn) {
      console.info(`[tenant-provisioner] Applying cross-account policy for role ${config.crossAccountRoleArn}`);
      const policy = {
        Version: '2012-10-17',
        Statement: [
          {
            Sid: 'CrossAccountAccess',
            Effect: 'Allow',
            Principal: { AWS: config.crossAccountRoleArn },
            Action: ['s3:GetObject', 's3:ListBucket'],
            Resource: [`arn:aws:s3:::${bucketName}`, `arn:aws:s3:::${bucketName}/*`]
          }
        ]
      };
      await s3Client.send(new PutBucketPolicyCommand({
        Bucket: bucketName,
        Policy: JSON.stringify(policy)
      }));
    }
  }

  try {
    await glue.send(new GetTableCommand({
      DatabaseName: GLUE_DATABASE,
      Name: tableName,
    }));
  } catch (err) {
    if (err instanceof EntityNotFoundException) {
      console.info(`[tenant-provisioner] Provisioning Glue table ${tableName} in ${targetRegion} S3 residency...`);
      await createGlueTable(tableName, s3Location);
    } else {
      throw err;
    }
  }
}

async function createGlueTable(tableName: string, s3Location: string) {
  await glue.send(new CreateTableCommand({
    DatabaseName: GLUE_DATABASE,
    TableInput: {
      Name: tableName,
      TableType: 'EXTERNAL_TABLE',
      Parameters: {
        'classification': 'parquet',
        'typeOfData': 'file',
        'has_encrypted_data': 'false',
        'EXTERNAL': 'TRUE',
        'parquet.compression': 'SNAPPY',
        'bebocard:tenant_id': tableName.replace('receipts_', ''),
        'bebocard:bucket_name': s3Location.split('/')[2],
        'bebocard:provisioned_at': new Date().toISOString()
      },
      StorageDescriptor: {
        Location: s3Location,
        InputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        OutputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        SerdeInfo: {
          SerializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          Parameters: { 'serialization.format': '1' }
        },
        Columns: [
          { Name: 'receipt_id', Type: 'string' },
          { Name: 'visitor_hash', Type: 'string' },
          { Name: 'brand_id', Type: 'string' },
          { Name: 'merchant', Type: 'string' },
          { Name: 'amount', Type: 'double' },
          { Name: 'currency', Type: 'string' },
          { Name: 'category', Type: 'string' },
          { Name: 'purchase_date', Type: 'string' },
          { Name: 'items', Type: 'string' },
          { Name: 'ingested_at', Type: 'string' }
        ]
      },
      PartitionKeys: [
        { Name: 'year', Type: 'string' },
        { Name: 'month', Type: 'string' },
        { Name: 'day', Type: 'string' }
      ]
    }
  }));
}
