import { Handler } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

const REFDATA_TABLE = process.env.REFDATA_TABLE!;
const REPORT_TABLE = process.env.REPORT_TABLE!;

export const handler: Handler = async (event) => {
  console.log('Received click tracking event', JSON.stringify(event, null, 2));

  // If invoked via AppSync, the arguments are usually in event.arguments
  const args = event.arguments || event;
  const { offerId, source, affiliateId, permULID } = args;

  if (!offerId || !permULID) {
    return { success: false, message: 'Missing offerId or permULID' };
  }

  const trackingId = `${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;

  try {
    // Write an analytical record to ReportDataEvent table
    await docClient.send(new PutCommand({
      TableName: REPORT_TABLE,
      Item: {
        pK: `CLICK#${permULID}`,
        sK: `OFFER#${offerId}#${trackingId}`,
        offerId,
        source: source || 'app',
        affiliateId: affiliateId || 'NONE',
        timestamp: new Date().toISOString(),
      }
    }));

    return { 
      success: true, 
      trackingId,
      message: 'Click tracked successfully' 
    };
  } catch (error) {
    console.error('Error tracking click:', error);
    throw new Error('Failed to track click');
  }
};
