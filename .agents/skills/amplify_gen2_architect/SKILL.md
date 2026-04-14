---
name: amplify_gen2_architect
description: Architectural rules to prevent circular dependencies and deployment locks in Amplify Gen 2
---

# Amplify Gen 2 Architecture SOP

This skill provides mandatory guidelines for managing dependencies between nested stacks in Amplify Gen 2 projects, specifically to avoid `CloudformationResourceCircularDependencyError` and deployment locks.

## The Core Problem
Amplify Gen 2 partitions resources into nested stacks. Complexity arises when auxiliary resources (API Gateways, Alarms, S3 Buckets) and business logic (functions) create tight webs of dependencies between the `data`, `auth`, and `storage` stacks.

## Mandatory Rules

### 1. Shared Infrastructure Stack (Stack Partitioning)
For large-scale projects, do not crowd the `data` stack with auxiliary plumbing. Use `backend.createStack('SharedInfrastructure')` to isolate:
- API Gateways
- Global S3 Buckets
- SNS Topics / Alarms
- SSM Parameters

### 2. Fine-Grained Function Grouping
Break loops by moving functions into logical stacks. Use `resourceGroupName` in `defineFunction`.

| Function Type | Stack Group | Rationale |
| :--- | :--- | :--- |
| **Stream Processor** | `'data'` | Co-located with Table Streams to avoid cross-stack EventSourceMapping loops. |
| **GraphQL Resolver** | `'data'` | Default for AppSync proximity. |
| **Cognito Trigger** | `'auth'` | Co-located with UserPool. |
| **Business Logic** | `'functions'` | Isolated from data schema changes; reduces `data` stack complexity. |

### 3. Localization of Auxiliary CDK Resources
Auxiliary resources like **Alarms**, **Aliases**, and **LambdaDeploymentGroups** must use the same scope as the resource they monitor/wrap. Use `Stack.of(resource)`:

```typescript
// SAFE: Alias and DeploymentGroup stay with the function stack
const fnStack = Stack.of(scanLambda);
const liveAlias = new lambda.Alias(fnStack, 'LiveAlias', { version: scanLambda.currentVersion, aliasName: 'live' });
new codedeploy.LambdaDeploymentGroup(fnStack, 'DG', { alias: liveAlias });
```

### 4. IAM & Environment Variable Decoupling
- **IAM**: Avoid CDK Tokens (like `table.tableArn`) for cross-stack grants. Use string-based ARN templates (e.g., `arn:aws:dynamodb:${region}:${account}:table/MyTable-*`).
- **Env Vars**: For non-stream functions, pass Table Names or API URLs via SSM parameters or hardcoded config to break the synthesis-time link back to the `data` stack.

## Troubleshooting Checklist
- [ ] **Circular Dependency?** Split business logic out of `data` stack and move Alarms/Aliases to their respective function stacks.
- [ ] **Large Resource Count?** Offload API Gateways and S3 Buckets to a `SharedInfrastructure` stack.
- [ ] **Stack Stuck?** Use `aws cloudformation continue-update-rollback --resources-to-skip [LogicalID]` for persistent locks.

