import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const REFDATA_TABLE = process.env.REFDATA_TABLE!;

/**
 * campaign-scheduler
 * Triggered every 5 minutes by EventBridge (scheduled rule).
 * Scans for SCHEDULED offers that are due to go live and flips them to ACTIVE.
 */
export const handler = async () => {
  const now = new Date().toISOString();
  console.info(`[campaign-scheduler] Starting sweep at ${now}`);

  try {
    // 1. Find all SCHEDULED offers in RefDataEvent
    // At enterprise scale (< 100k offers total), a filtered scan every 5 mins 
    // is cost-effective compared to maintaining a dedicated GSI for scheduling.
    const res = await dynamo.send(new ScanCommand({
      TableName: REFDATA_TABLE,
      FilterExpression: '#status = :scheduled AND begins_with(sK, :prefix)',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: {
        ':scheduled': 'SCHEDULED',
        ':prefix': 'OFFER#',
      },
    }));

    const items = res.Items ?? [];
    console.info(`[campaign-scheduler] Found ${items.length} candidates for lifecycle transition`);

    let transitionedCount = 0;

    for (const item of items) {
      const desc = JSON.parse(item.desc ?? '{}');
      const scheduledFor = desc.scheduledFor as string | undefined;

      // Transition if:
      // a) scheduledFor is passed (precision scheduling)
      // b) validFrom is passed (fallback for legacy/simple scheduling)
      const shouldGoLive = (scheduledFor && scheduledFor <= now) || 
                          (!scheduledFor && desc.validFrom && desc.validFrom <= now.slice(0, 10));

      if (shouldGoLive) {
        console.info(`[campaign-scheduler] Flipping ${item.sK} to ACTIVE`);
        
        await dynamo.send(new UpdateCommand({
          TableName: REFDATA_TABLE,
          Key: { pK: item.pK, sK: item.sK },
          UpdateExpression: 'SET #status = :active, updatedAt = :now',
          ExpressionAttributeNames: { '#status': 'status' },
          ExpressionAttributeValues: {
            ':active': 'ACTIVE',
            ':now': now,
          },
        }));
        
        transitionedCount++;
      }
    }

    console.info(`[campaign-scheduler] Successfully transitioned ${transitionedCount} offers to ACTIVE.`);
    return { success: true, transitionedCount };

  } catch (err) {
    console.error('[campaign-scheduler] Error during lifecycle sweep:', err);
    throw err;
  }
};
