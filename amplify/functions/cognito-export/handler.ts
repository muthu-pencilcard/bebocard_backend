import { Handler } from 'aws-lambda';
import {
  CognitoIdentityProviderClient,
  ListUsersCommand,
  ListUsersCommandOutput,
} from '@aws-sdk/client-cognito-identity-provider';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

const cognitoClient = new CognitoIdentityProviderClient({});
const s3Client = new S3Client({});

const USER_POOL_ID = process.env.USER_POOL_ID!;
const EXPORT_BUCKET = process.env.EXPORT_BUCKET!;

/**
 * Weekly Cognito user pool export to S3 (P0-6 DR).
 *
 * Exports all users as a JSON array to:
 *   s3://EXPORT_BUCKET/pool-exports/YYYY-MM-DD/users.json
 *
 * Retains 90 days of exports (S3 lifecycle policy configured in backend.ts).
 * Used as the recovery source if the Cognito pool is accidentally deleted.
 *
 * NOTE: Cognito does not export password hashes. Users will need to reset
 * passwords on first login after a pool recovery. All custom attributes
 * (custom:permULID, custom:userId, etc.) ARE exported and preserved.
 */
export const handler: Handler = async () => {
  const exportDate = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
  const allUsers: Record<string, unknown>[] = [];
  let paginationToken: string | undefined;

  console.log(`[cognito-export] Starting export for pool ${USER_POOL_ID} on ${exportDate}`);

  // Paginate through all users
  do {
    const response: ListUsersCommandOutput = await cognitoClient.send(
      new ListUsersCommand({
        UserPoolId: USER_POOL_ID,
        Limit: 60,
        PaginationToken: paginationToken,
      })
    );

    for (const user of response.Users ?? []) {
      const attrs: Record<string, string> = {};
      for (const attr of user.Attributes ?? []) {
        if (attr.Name) attrs[attr.Name] = attr.Value ?? '';
      }
      allUsers.push({
        Username: user.Username,
        UserStatus: user.UserStatus,
        UserCreateDate: user.UserCreateDate?.toISOString(),
        UserLastModifiedDate: user.UserLastModifiedDate?.toISOString(),
        Enabled: user.Enabled,
        Attributes: attrs,
      });
    }

    paginationToken = response.PaginationToken;
  } while (paginationToken);

  const exportKey = `pool-exports/${exportDate}/users.json`;
  const exportPayload = JSON.stringify({
    exportedAt: new Date().toISOString(),
    userPoolId: USER_POOL_ID,
    userCount: allUsers.length,
    users: allUsers,
  });

  await s3Client.send(
    new PutObjectCommand({
      Bucket: EXPORT_BUCKET,
      Key: exportKey,
      Body: exportPayload,
      ContentType: 'application/json',
      ServerSideEncryption: 'aws:kms', // KMS-encrypted at rest
    })
  );

  console.log(`[cognito-export] Exported ${allUsers.length} users to s3://${EXPORT_BUCKET}/${exportKey}`);

  return {
    exportedAt: new Date().toISOString(),
    userCount: allUsers.length,
    s3Key: exportKey,
  };
};
