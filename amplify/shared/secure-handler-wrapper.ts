import type { AppSyncResolverEvent, AppSyncResolverHandler, Context } from 'aws-lambda';

type HandlerFn<TArgs = Record<string, unknown>, TResult = unknown> = (
  event: AppSyncResolverEvent<TArgs>,
  permULID: string,
) => Promise<TResult>;

/**
 * Wraps an AppSync resolver handler with Cognito JWT auth extraction.
 * Throws Unauthorized if custom:permULID is absent from the identity claims.
 */
export function withGraphQLHandler<TArgs = Record<string, unknown>, TResult = unknown>(
  fn: HandlerFn<TArgs, TResult>,
): AppSyncResolverHandler<TArgs, TResult> {
  return async (event: AppSyncResolverEvent<TArgs>, _context?: Context) => {
    const claims = (event.identity as any)?.claims as Record<string, string> | undefined;
    const permULID = claims?.['custom:permULID'];
    if (!permULID) {
      throw new Error('Unauthorized: missing custom:permULID in token claims');
    }
    return fn(event, permULID);
  };
}

/** Alias — use withGraphQLHandler for AppSync resolvers. */
export const withSecureHandler = withGraphQLHandler;

/**
 * Wraps a REST Lambda handler (API Gateway proxy event) with Cognito JWT auth.
 * Reads permULID from the authorizer context injected by API Gateway.
 */
export function withRestHandler(
  fn: (event: AWSLambda.APIGatewayProxyEvent, permULID: string) => Promise<AWSLambda.APIGatewayProxyResult>,
): AWSLambda.Handler {
  return async (event: AWSLambda.APIGatewayProxyEvent) => {
    const permULID =
      event.requestContext?.authorizer?.claims?.['custom:permULID'] as string | undefined;
    if (!permULID) {
      return { statusCode: 401, body: JSON.stringify({ error: 'Unauthorized' }) };
    }
    return fn(event, permULID);
  };
}
