---
name: amplify_gen2_architect
description: Canonical Amplify Gen 2 architectural rules sourced from official AWS docs + hard-won production learnings from BeboCard. Prevents circular dependency errors, deployment locks, and CloudFormation synthesis failures.
---

# Amplify Gen 2 Architecture SOP

> **Source**: [AWS Amplify Gen 2 Troubleshooting — Circular Dependencies](https://docs.amplify.aws/react/build-a-backend/troubleshooting/circular-dependency/) + BeboCard production experience  
> **AI Tools**: Available via [AWS MCP Server](https://docs.amplify.aws/react/start/mcp-server/set-up-mcp/) — use `retrieve_agent_sop` with `amplify-backend-implementation`

---

## The Error

```
The CloudFormation deployment failed due to circular dependency
```

Two variants:
1. **Between nested stacks** — `[data1234ABCD, function6789XYZ]`
2. **Within a single stack** — `[resource1, resource2, ...]` (all in one list)

---

## Fix 1 — Function ↔ Stack Circular Dep: `resourceGroupName` (OFFICIAL)

**Root cause**: A Lambda is deployed as a function but also creates a back-reference to the `data` or `auth` stack (e.g., as a GraphQL handler that also reads from DynamoDB, or a DynamoDB stream processor).

**Fix**: Co-locate the function with the stack it references using `resourceGroupName` in `defineFunction`.

```typescript
// If used as a GraphQL resolver OR DynamoDB stream processor:
export const myResolver = defineFunction({
  name: 'my-resolver',
  resourceGroupName: 'data',  // places it in the data nested stack
});

// If used as a Cognito trigger (pre-signup, post-confirmation, etc.):
export const preSignUpTrigger = defineFunction({
  name: 'pre-sign-up',
  resourceGroupName: 'auth',  // places it in the auth nested stack
});
```

**Quick reference table:**

| Function type | `resourceGroupName` |
| :--- | :--- |
| GraphQL resolver (`.handler()`) | `'data'` |
| DynamoDB Stream processor | `'data'` |
| SQS consumer (queue in data stack) | `'data'` |
| Cognito trigger (any) | `'auth'` |
| S3 trigger (Amplify-managed bucket) | `'storage'` |
| Standalone business logic (no back-ref) | *(omit — default `functions` stack)* |

---

## Fix 2 — Custom CDK Resource ↔ Amplify Stack: Use `backend.<resource>.stack` (OFFICIAL)

**Root cause**: Creating a CDK resource in a standalone `createStack()` that references resources in another Amplify-managed stack (e.g., an SQS queue that references the S3 bucket from `defineStorage`).

**Error looks like**: `[storage1234ABCD, auth5678XYZ, MyCustomStack0123AB]`

**Fix**: Create the custom resource *inside the same stack as the Amplify resource it interacts with*, using `backend.<resource>.stack` as the CDK scope:

```typescript
// CORRECT — SQS queue goes in the storage stack since it interacts with S3
const queue = new sqs.Queue(backend.storage.stack, 'MyCustomQueue');

// WRONG — standalone custom stack creates cross-stack cycle
const myStack = backend.createStack('MyCustomResources');
const queue = new sqs.Queue(myStack, 'MyCustomQueue'); // breaks if queue references S3
```

**Stack accessor references:**
- `backend.auth.resources.userPool.stack` — Cognito stack
- `backend.data.resources.tables['TableName'].stack` — Data/AppSync stack
- `backend.storage.stack` — S3 storage stack
- `backend.<functionName>.resources.lambda.stack` — A function's stack

---

## Fix 3 — Auxiliary CDK Resources: Use `Stack.of(resource)` (BEBOCARD PATTERN)

**Root cause**: Large backends with many Lambdas often place CloudWatch Alarms, Lambda Aliases, and CodeDeploy DeploymentGroups in a shared/wrong stack, creating cross-stack cycles.

**Fix**: Always scope auxiliary resources to the same stack as the resource they wrap, using `Stack.of(resource)`:

```typescript
// CORRECT — Alias, DeploymentGroup, and Alarm all follow the function's stack
const fnStack = Stack.of(scanLambda);

const liveAlias = new lambda.Alias(fnStack, 'ScanLiveAlias', {
  aliasName: 'live',
  version: scanLambda.currentVersion,
});

const errorAlarm = new cloudwatch.Alarm(fnStack, 'ScanCanaryErrorAlarm', {
  metric: liveAlias.metricErrors({ period: Duration.minutes(1) }),
  threshold: 1,
  evaluationPeriods: 3,
});

new codedeploy.LambdaDeploymentGroup(fnStack, 'ScanDeploymentGroup', {
  alias: liveAlias,
  deploymentConfig: codedeploy.LambdaDeploymentConfig.CANARY_10PERCENT_5MINUTES,
  alarms: [errorAlarm],
});

// CORRECT — DLQ alarm follows the queue's stack
const dlqAlarm = new cloudwatch.Alarm(Stack.of(myDLQ), `${name}DLQAlarm`, { ... });
```

---

## Fix 4 — IAM & Env Var Decoupling (BEBOCARD PATTERN)

**Root cause**: Using CDK Token values like `table.tableArn` or `table.tableName` in cross-stack contexts creates synthesis-time links that CloudFormation cannot resolve.

**Fix (IAM)**: Use string-based ARN templates instead of CDK Token outputs:

```typescript
// SAFE — string interpolation, no CDK Token dependency
fn.addToRolePolicy(new iam.PolicyStatement({
  actions: ['dynamodb:GetItem', 'dynamodb:Query'],
  resources: [
    `arn:aws:dynamodb:${stack.region}:${stack.account}:table/UserDataEvent-*`,
    `arn:aws:dynamodb:${stack.region}:${stack.account}:table/UserDataEvent-*/index/*`,
  ],
}));
```

**Fix (Env Vars)**: For functions that do NOT co-locate with the `data` stack, pass table names via SSM parameters resolved at runtime, not at synthesis time. Avoid `addEnvironment('TABLE', table.tableName)` across stack boundaries.

---

## Fix 5 — "Within a single stack" Variant (BEBOCARD PATTERN)

When the error lists dozens of resources all in one stack (not two nested stacks), the problem is within the `data` nested stack itself — too many Lambdas with DynamoDB grants, stream sources, and aliases all creating a dependency web.

**Fix**: Move business-logic Lambdas (those not triggered by streams) out of the data stack entirely:

```typescript
// amplify/functions/scan-handler/resource.ts — REST API handler, no stream
export const scanHandlerFn = defineFunction({
  resourceGroupName: 'functions', // isolated from data stack
  name: 'bebo-scan-handler',
});

// amplify/functions/segment-processor/resource.ts — DynamoDB stream processor
export const segmentProcessorFn = defineFunction({
  resourceGroupName: 'data', // must stay (stream trigger)
  name: 'bebo-segment-processor',
});
```

Keep **only** these in `resourceGroupName: 'data'`:
- DynamoDB Stream processors (`EventSourceMapping` ties them to the table stack)
- GraphQL resolvers (`.handler()`)
- SQS consumers whose queue lives inside the data stack

---

## Troubleshooting Checklist

- [ ] **Error: `[dataNNN, functionNNN]`?** — Add `resourceGroupName: 'data'` to the function.
- [ ] **Error: `[storageNNN, authNNN, MyCustomStackNNN]`?** — Replace `createStack()` with `backend.<resource>.stack`.
- [ ] **Error: `[50+ resources in one list]`?** — Move non-stream Lambdas to `resourceGroupName: 'functions'`.
- [ ] **Aliases/DeploymentGroups in error list?** — Use `Stack.of(fn)` as constructor scope.
- [ ] **Stack stuck in `UPDATE_ROLLBACK_FAILED`?** — `aws cloudformation continue-update-rollback --stack-name <name> --resources-to-skip <LogicalID>`.
- [ ] **Cognito custom attribute mutability error?** — `custom:` attributes are immutable after pool creation. Delete and recreate (dev only) or accept.

---

## AI Tooling for Amplify Gen 2

**AWS MCP Server** (official, recommended): Provides pre-built Amplify agent SOPs inside your AI assistant.
Setup: [https://docs.amplify.aws/react/start/mcp-server/set-up-mcp/](https://docs.amplify.aws/react/start/mcp-server/set-up-mcp/)

Once configured, trigger with:
- `retrieve_agent_sop("amplify-backend-implementation")` — auth, data, storage, functions, AI
- `retrieve_agent_sop("amplify-frontend-integration")` — frontend library wiring
- `retrieve_agent_sop("amplify-deployment-guide")` — CI/CD, pipeline-deploy

**AWS Agent Plugin** (for Claude Code / Kiro):
```bash
COMPOUND_PLUGIN_GITHUB_SOURCE=https://github.com/awslabs/agent-plugins \
bunx @every-env/compound-plugin install aws-amplify --to claude-code
```
Activates on prompts like "build Amplify app", "add auth to Amplify", "deploy Amplify".
