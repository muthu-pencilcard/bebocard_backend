---
name: amplify_gen2_architect
description: Architectural rules to prevent circular dependencies and deployment locks in Amplify Gen 2
---

# Amplify Gen 2 Architecture SOP

This skill provides mandatory guidelines for managing dependencies between nested stacks in Amplify Gen 2 projects, specifically to avoid the `CloudformationStackCircularDependencyError`.

## The Core Problem
Amplify Gen 2 partitions resources into nested stacks: `auth`, `data`, `storage`, and `functions`.
A loop occurs when:
1. **Stack A (Data)** depends on **Stack B (Function)** (e.g., as a GraphQL handler).
2. **Stack B (Function)** depends on **Stack A (Data)** (e.g., referencing a DynamoDB Table Name for an environment variable).

## Mandatory Rules

### 1. Resource Grouping (The Co-location Rule)
Break loops by moving the function into the stack it depends on. Use `resourceGroupName` in `defineFunction`.

| If the Lambda is a... | Use `resourceGroupName` |
| :--- | :--- |
| GraphQL Resolver (`.handler()`) | `'data'` |
| DynamoDB Stream Processor | `'data'` |
| SQS Consumer (Queue in Data stack) | `'data'` |
| API Gateway Integration | `'data'` |
| Cognito Trigger (Pre-signup, etc.) | `'auth'` |
| S3 Trigger (Bucket in Storage stack) | `'storage'` |

### 2. IAM Decoupling (String-based ARNs)
Avoid using CDK Tokens (like `table.tableArn`) for cross-stack grants. Tokens create implicit stack dependencies.
**In `amplify/backend.ts`**, use string-based ARN construction:

```typescript
// SAFE: No hard dependency on the table's construct output
lambda.addToRolePolicy(new iam.PolicyStatement({
  actions: ['dynamodb:GetItem', 'dynamodb:Query'],
  resources: [`arn:aws:dynamodb:${stack.region}:${stack.account}:table/${tableName}`],
}));
```

### 3. Environment Variable Decoupling
Do not use `table.tableName` in `addEnvironment` for functions in the default group if you have many cross-stack targets. Use SSM Parameters or hardcoded patterns where possible.

## Troubleshooting Checklist
- [ ] Is the stack in `UPDATE_ROLLBACK_FAILED`? Use `aws cloudformation continue-update-rollback` to force a skip of the stuck resource.
- [ ] Did you check all `events.Rule` targets? Cron jobs must also be co-located if they touch tables.
- [ ] Verify `custom:attributes` in Cognito. Mutability cannot be changed after creation.
