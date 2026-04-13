import { DynamoDBDocumentClient, PutCommand, GetCommand } from '@aws-sdk/lib-dynamodb';

/**
 * Performs an idempotent Put operation in DynamoDB.
 * If the record already exists, returns the existing record.
 * Otherwise, writes the new record.
 */
export async function idempotentPut(
  dynamo: DynamoDBDocumentClient,
  tableName: string,
  item: Record<string, any>,
  idempotencyKeyField: string = 'idempotencyKey'
) {
  const pK = item.pK;
  const sK = item.sK;

  if (!pK || !sK) {
    throw new Error('Item must have pK and sK for idempotentPut');
  }

  try {
    await dynamo.send(new PutCommand({
      TableName: tableName,
      Item: item,
      ConditionExpression: 'attribute_not_exists(pK)',
    }));
    return { success: true, item };
  } catch (err: any) {
    if (err.name === 'ConditionalCheckFailedException') {
      // Re-read to return the authoritative record
      const result = await dynamo.send(new GetCommand({
        TableName: tableName,
        Key: { pK, sK },
      }));
      return { success: false, duplicate: true, item: result.Item };
    }
    throw err;
  }
}
