import { defineFunction } from '@aws-amplify/backend';

export const segmentProcessorFn = defineFunction({
  resourceGroupName: 'data',
  name: 'bebo-segment-processor',
  entry: './handler.ts',
  environment: {
    USER_TABLE: process.env.USER_TABLE ?? '',
  },
  // DynamoDB Streams trigger is wired in backend.ts using CDK escape hatch:
  //   userTable.addEventSource(new DynamoDBEventSource(userTable, { ... }))
  // Streams must also be enabled on the UserDataEvent table via CDK.
});
