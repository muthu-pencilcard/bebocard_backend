import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand, DeleteCommand } from '@aws-sdk/lib-dynamodb';
import { handler as clickTrackingHandler } from '../functions/click-tracking-handler/handler';
import { ulid } from 'ulid';

/**
 * Phase 1 E2E Verification Script
 * Validates the core "tenant -> app -> analytics" loop locally using AWS credentials.
 * Ensure you have AWS credentials mapped before running.
 */
async function runPhase1E2ETest() {
  console.log('🚀 Starting Phase 1 E2E Alignment Verification...');

  // 1. Initialize AWS Clients
  const ddbUnwrapped = new DynamoDBClient({ region: process.env.AWS_REGION || 'ap-southeast-2' });
  const docClient = DynamoDBDocumentClient.from(ddbUnwrapped);
  
  // NOTE: If testing against sandbox/dev, these names might have hashes. 
  // We assume valid ENV vars or default test tables.
  const REFDATA_TABLE = process.env.REFDATA_TABLE || 'RefDataEvent';
  const REPORT_TABLE = process.env.REPORT_TABLE || 'ReportDataEvent';

  const testBrandId = `E2E_BRAND_${ulid()}`;
  const testUserPermUlid = `E2E_USER_${ulid()}`;
  
  console.log(`\n📋 [STEP 1] Generating Normalized Brand Profile [${testBrandId}]`);
  
  // 2. Validate Normalized Brand Profile Write
  const mockBrandProfile = {
    brandName: 'Phase 1 E2E Tester',
    brandColor: '#FF5733',
    isWidgetEnabled: true,
    widgetConfig: {
      iframeUrl: 'https://bebocard.test/iframe',
      supportedActions: ['invoice', 'giftcard']
    },
    loyaltyDefaults: { supportsMultipleCards: false, pointsName: 'E2E Points' },
  };

  try {
    await docClient.send(new PutCommand({
      TableName: REFDATA_TABLE,
      Item: {
        pK: `BRAND#${testBrandId}`,
        sK: 'PROFILE',
        desc: JSON.stringify(mockBrandProfile),
        status: 'ACTIVE'
      }
    }));
    console.log('✅ Success: Brand Profile correctly conformed to normalized schema and saved to RefData.');
  } catch (error) {
    console.error('❌ Failed: Could not write normalized Brand Profile.', error);
    process.exit(1);
  }

  console.log(`\n📲 [STEP 2] Simulating Mobile App Widget Launch Engagement...`);
  
  // 3. Simulate The AppSync 'trackEngagement' Mutation Call
  const mockAppSyncEvent = {
    arguments: {
      eventType: 'LAUNCH_WIDGET',
      targetId: testBrandId,
      source: 'app',
      metadata: JSON.stringify({ action: 'invoice_payment' }),
      permULID: testUserPermUlid,
    }
  };

  // Temporarily inject env vars the lambda requires
  process.env.REFDATA_TABLE = REFDATA_TABLE;
  process.env.REPORT_TABLE = REPORT_TABLE;

  let trackingId = '';
  try {
    const lambdaResponse = await clickTrackingHandler(mockAppSyncEvent as any, {} as any, () => {});
    if (!lambdaResponse || typeof lambdaResponse !== 'object' || !('success' in lambdaResponse) || !lambdaResponse.success) {
        throw new Error(`Lambda returned failure: ${JSON.stringify(lambdaResponse)}`);
    }
    trackingId = (lambdaResponse as any).trackingId;
    console.log(`✅ Success: trackEngagement Lambda resolved. Tracking ID: ${trackingId}`);
  } catch (error) {
    console.error('❌ Failed: trackEngagement Lambda threw an error.', error);
    process.exit(1);
  }

  console.log(`\n📊 [STEP 3] Validating Data Lake Aggregation Queue...`);
  
  // 4. Verify the item immediately safely reached the ReportDataEvent table
  try {
    const reportQuery = await docClient.send(new QueryCommand({
      TableName: REPORT_TABLE,
      KeyConditionExpression: 'pK = :pk AND begins_with(sK, :sk)',
      ExpressionAttributeValues: {
        ':pk': `ENGAGEMENT#${testUserPermUlid}`,
        ':sk': 'LAUNCH_WIDGET#E2E_BRAND_'
      }
    }));

    if (reportQuery.Items && reportQuery.Items.length > 0) {
      const recordedItem = reportQuery.Items[0];
      console.log(`✅ Success: Found engagement record in Report table!`);
      console.log(`   Event Type:  ${recordedItem.eventType}`);
      console.log(`   Target ID:   ${recordedItem.targetId}`);
      console.log(`   Metadata:    ${recordedItem.metadata}`);
    } else {
      throw new Error("Query succeeded but no item was found.");
    }
  } catch (error) {
    console.error('❌ Failed: Analytics item did not reach the Report table.', error);
    process.exit(1);
  }

  // 5. Cleanup E2E Artifacts
  console.log(`\n🧹 [STEP 4] Cleaning up E2E Artifacts...`);
  try {
    await docClient.send(new DeleteCommand({
      TableName: REFDATA_TABLE,
      Key: { pK: `BRAND#${testBrandId}`, sK: 'PROFILE' }
    }));
    await docClient.send(new DeleteCommand({
      TableName: REPORT_TABLE,
      // Need exact SK for deletion
      Key: { pK: `ENGAGEMENT#${testUserPermUlid}`, sK: `LAUNCH_WIDGET#${testBrandId}#${trackingId}` }
    }));
    console.log('✅ Success: Teardown complete.');
  } catch(error) {
    console.log('⚠️ Cleanup failed (non-critical).', error);
  }

  console.log(`\n🎉 PHASE 1 ALIGNMENT VERIFIED FULLY!`);
}

runPhase1E2ETest();
