import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb';

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const REFDATA_TABLE = process.env.REFDATA_TABLE!;

export const handler = async (event: any) => {
  const { platform, appVersion } = event.arguments || {};
  
  console.info(`[remote-config] Fetching config for ${platform}@${appVersion}`);

  try {
    const res = await ddb.send(new GetCommand({
      TableName: REFDATA_TABLE,
      Key: { pK: 'CONFIG#GLOBAL', sK: 'DEFAULT' }
    }));

    const config = JSON.parse(res.Item?.desc ?? '{}');

    // Default fallbacks if table is empty
    return {
      is_rotation_enabled: config.is_rotation_enabled ?? true,
      min_scan_interval_ms: config.min_scan_interval_ms ?? 30000,
      enforce_biometrics: config.enforce_biometrics ?? false,
      support_email: config.support_email ?? 'hello@bebocard.app',
      feature_flags: config.feature_flags ?? {
        gift_cards_v2: true,
        receipt_ocr_enhanced: false,
        discovery_region_lock: true
      },
      server_time: new Date().toISOString()
    };
  } catch (err) {
    console.error('[remote-config] Failed to fetch config', err);
    throw err;
  }
};
