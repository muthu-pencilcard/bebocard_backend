# BeboCard Disaster Recovery Plan (P0-6)

**Last Updated:** 2026-04-12
**Version:** 2.0
**Classification:** Internal — SRE + Engineering Leads only
**Owner:** Platform Engineering
**Review cadence:** Annual (after each drill)

---

## 1. RPO and RTO Targets

| Metric | Target | Basis |
|---|---|---|
| **RPO (Recovery Point Objective)** | 1 hour | DynamoDB PITR granularity |
| **RTO (Recovery Time Objective)** | 4 hours | Measured restore + SSM update + smoke test |

These values are committed in the MSA (Section 3.1). Do not alter without updating the MSA and notifying all active brand tenants with signed SLAs.

---

## 2. Scope

This plan covers catastrophic loss or corruption of any of the following:

| System | Backup mechanism | Retention |
|---|---|---|
| `UserDataEvent` DynamoDB table | PITR | 35 days |
| `RefDataEvent` DynamoDB table | PITR | 35 days |
| `AdminDataEvent` DynamoDB table | PITR | 35 days |
| `ReportDataEvent` DynamoDB table | PITR | 35 days |
| Cognito user pool | Weekly S3 export (`cognito-export` Lambda) | 90 days |
| S3 analytics lake (Iceberg/Parquet) | S3 versioning | 90 days |
| Lambda function code | Versioned Lambda + CodeDeploy aliases | Indefinite |

Out of scope: AWS management plane outages, Cognito service outages (no workaround — document in §8).

---

## 3. Recovery Runbooks

### 3.1 DynamoDB Table Restore (primary path)

**Trigger:** Table data corruption, accidental deletion, or ransomware event.

**Step 1 — Identify recovery point**
```bash
# List available PITR windows for the affected table
aws dynamodb describe-continuous-backups \
  --table-name UserDataEvent-<prod-suffix> \
  --query 'ContinuousBackupsDescription.PointInTimeRecoveryDescription'
```

**Step 2 — Restore to a new table**
```bash
# Restore all three tables. Run in parallel (three terminal tabs).
aws dynamodb restore-table-to-point-in-time \
  --source-table-name UserDataEvent-<prod-suffix> \
  --target-table-name UserDataEvent-RESTORED-$(date +%Y%m%d%H%M) \
  --restore-date-time "<ISO-8601 timestamp of recovery point>"

aws dynamodb restore-table-to-point-in-time \
  --source-table-name RefDataEvent-<prod-suffix> \
  --target-table-name RefDataEvent-RESTORED-$(date +%Y%m%d%H%M) \
  --restore-date-time "<ISO-8601 timestamp>"

aws dynamodb restore-table-to-point-in-time \
  --source-table-name AdminDataEvent-<prod-suffix> \
  --target-table-name AdminDataEvent-RESTORED-$(date +%Y%m%d%H%M) \
  --restore-date-time "<ISO-8601 timestamp>"
```

**Step 3 — Enable PITR on restored tables immediately**
```bash
aws dynamodb update-continuous-backups \
  --table-name UserDataEvent-RESTORED-<timestamp> \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
# Repeat for RefDataEvent and AdminDataEvent restored tables
```

**Step 4 — Update SSM parameters** (Lambdas read table names from SSM — no redeployment required)
```bash
AMPLIFY_APP_ID="<app-id>"
AMPLIFY_BRANCH="prod"

aws ssm put-parameter \
  --name "/bebocard/${AMPLIFY_APP_ID}/${AMPLIFY_BRANCH}/USER_TABLE" \
  --value "UserDataEvent-RESTORED-<timestamp>" \
  --type String --overwrite

aws ssm put-parameter \
  --name "/bebocard/${AMPLIFY_APP_ID}/${AMPLIFY_BRANCH}/ADMIN_TABLE" \
  --value "AdminDataEvent-RESTORED-<timestamp>" \
  --type String --overwrite
```

**Step 5 — Update Lambda environment variables** for Lambdas that read table names directly from env (not SSM):
```bash
# scan-handler, receipt-processor, segment-processor, receipt-iceberg-writer
for fn in bebo-scan-handler bebo-receipt-processor bebo-segment-processor bebo-receipt-iceberg-writer; do
  aws lambda update-function-configuration \
    --function-name "$fn" \
    --environment "Variables={USER_TABLE=UserDataEvent-RESTORED-<timestamp>,REFDATA_TABLE=RefDataEvent-RESTORED-<timestamp>,ADMIN_TABLE=AdminDataEvent-RESTORED-<timestamp>}"
done
```

**Step 6 — Smoke test sequence**
```bash
# 1. Health check
curl -s https://api.bebocard.com/v1/health | jq '.status'
# Expected: "operational"

# 2. Synthetic scan (use sandbox test ULID — never a real user ULID in DR test)
curl -s -X POST https://api.bebocard.com/v1/scan \
  -H "X-Api-Key: bebo_sandbox_test" \
  -H "Content-Type: application/json" \
  -d '{"secondaryULID": "01DRTEST00000000000000000"}' | jq '.hasLoyaltyCard'

# 3. Verify PITR is active on all restored tables
aws dynamodb describe-continuous-backups --table-name UserDataEvent-RESTORED-<timestamp> \
  | jq '.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus'
# Expected: "ENABLED"
```

**Step 7 — CloudFront cache invalidation** (clears any stale API responses cached at edge)
```bash
DISTRIBUTION_ID=$(aws cloudfront list-distributions \
  --query "DistributionList.Items[?Comment=='bebocard-api'].Id" --output text)
aws cloudfront create-invalidation \
  --distribution-id "$DISTRIBUTION_ID" \
  --paths "/*"
```

**Step 8 — Update status page**
- Set `status.bebocard.com` Scan API component to "Operational" once smoke test passes.
- Post incident update: "Service restored. Data recovered to [timestamp]. See post-mortem [link]."

---

### 3.2 Cognito User Pool Recovery

**Trigger:** Cognito pool accidental deletion or AWS-side pool corruption.

> Cognito has no native PITR. The `cognito-export` Lambda exports the pool weekly to `s3://bebocard-cognito-exports-<account>/pool-exports/`. Users can authenticate with exported data restored to a new pool, but **passwords are hashed by Cognito's internal KMS and cannot be migrated**. All users must reset passwords on first login after recovery.

**Step 1 — Identify latest export**
```bash
aws s3 ls s3://bebocard-cognito-exports-<account>/pool-exports/ --recursive | sort | tail -5
```

**Step 2 — Create new Cognito user pool** with identical attributes (same schema as current pool — documented in `amplify/auth/resource.ts`).

**Step 3 — Restore users from export**
```bash
# Export format: JSON array of Cognito user objects
# Use AWS CLI admin-create-user for each user, preserving custom:permULID, custom:userId attributes
# Users are created with FORCE_CHANGE_PASSWORD status — they reset on next login
node scripts/cognito-restore.js \
  --pool-id <new-pool-id> \
  --export-file <path-to-export.json>
```

**Step 4 — Update Amplify app to point to new pool**
```bash
aws ssm put-parameter \
  --name "/bebocard/<app-id>/<branch>/COGNITO_USER_POOL_ID" \
  --value "<new-pool-id>" --type String --overwrite
```

**Communicate to users:** "We've restored your account. Please reset your password using the 'Forgot Password' link. Your loyalty cards, receipts, and preferences are fully intact."

---

### 3.3 S3 Analytics Lake Recovery

**Trigger:** Corrupted or accidentally deleted Parquet files in `s3://bebocard-analytics-<account>/`.

**Step 1 — Identify corrupted objects**
```bash
aws s3api list-object-versions \
  --bucket bebocard-analytics-<account> \
  --prefix "<tenantId>/receipts/" \
  --query 'Versions[?IsLatest==`false`]' | head -20
```

**Step 2 — Restore previous version via S3 Batch Operations**
```bash
# Create a manifest of objects to restore, then submit a Batch Operations job
aws s3control create-job \
  --account-id <account-id> \
  --operation '{"S3CopyObject": {"TargetResource": "arn:aws:s3:::bebocard-analytics-<account>"}}' \
  --manifest '{"Spec": {"Format": "S3BatchOperations_CSV_20180820"}, "Location": {"ObjectArn": "arn:aws:s3:::bebocard-manifest/restore-manifest.csv", "ETag": "<etag>"}}'
```

**Step 3 — Re-run Glue crawlers** to synchronize catalog metadata after restore.
```bash
aws glue start-crawler --name bebocard-receipts-crawler
```

---

### 3.4 Full Regional Failover (catastrophic ap-southeast-2 outage)

> This scenario assumes the entire `ap-southeast-2` region is unavailable. Multi-region active-active (P3-6) is not yet deployed. The mitigation is communication + queue retention.

1. All SQS queues retain messages for 14 days — no data loss for receipt, webhook, or analytics events queued during the outage.
2. Update `status.bebocard.com` to "Major Outage — Scan API unavailable. Receipts will process automatically when service resumes."
3. Notify all Intelligence/Enterprise tier brand contacts via email (contact list in `bebocard_compliance/ops/enterprise-contacts.csv`).
4. Once `ap-southeast-2` recovers, SQS queues drain automatically — no manual intervention needed for queued events.
5. Post post-mortem within 5 business days.

---

## 4. Composite Failure Detection

A CloudWatch composite alarm (`BebocardMultiTableFailure`) fires when 3+ DynamoDB tables simultaneously enter elevated error state. This is the early-warning indicator for a partial infrastructure failure.

**Alarm ARN:** Defined in `backend.ts` as `BebocardDRCompositeAlarm`.
**Action:** SNS → `bebocard-oncall-topic` → PagerDuty P1 escalation.

---

## 5. Communication Templates

### Internal (Slack #incidents)
```
🔴 [P1 INCIDENT] BeboCard <component> is down.
Declared: <timestamp UTC>
Impact: <description>
IC (Incident Commander): <name>
Bridge: <link>
Status page: status.bebocard.com — set to Major Outage
```

### External (status.bebocard.com + Enterprise email)
```
Subject: [BeboCard Status] Service disruption — <date>

We are currently experiencing disruption to <component>.
Impact: <brands affected / API affected>
We are actively working to restore service. Our target RTO is 4 hours from incident declaration.
Next update: <timestamp>

Enterprise accounts: your CSM will contact you directly within 1 hour.
```

### Recovery notification
```
Subject: [BeboCard Status] Service restored — <date>

Service has been fully restored as of <timestamp UTC>.
Duration: <X hours Y minutes>
Data recovered to: <recovery point timestamp>
Post-mortem: Will be shared within 5 business days.
```

---

## 6. Annual Drill Schedule

| Year | Date | Scope | Outcome |
|---|---|---|---|
| 2026 | First week of August | Full `UserDataEvent` PITR restore in staging; smoke test sequence | To be documented |

**Drill procedure:**
1. Announce drill window to engineering team (1 week advance notice).
2. In staging environment: delete `UserDataEvent-staging`, restore from PITR to `UserDataEvent-staging-DR`.
3. Update SSM staging parameters to point to restored table.
4. Run full smoke test sequence (§3.1 Step 6).
5. Measure elapsed time from "table deleted" to "smoke test passes" — this is the measured RTO.
6. Document result in `bebocard_backend/chaos-reports/dr-drill-<year>.md`.
7. If measured RTO > 4 hours: update runbook to optimize the slow steps; update MSA if committed RTO needs adjustment.

---

## 7. Contacts and Escalation

| Role | Contact | Escalation trigger |
|---|---|---|
| On-call engineer | PagerDuty rotation | Any P1 alarm |
| Engineering Lead | Direct message | RTO > 2 hours |
| CEO | Direct message | Full regional outage or data breach |
| Legal counsel | Documented in `bebocard_compliance/legal/contacts.md` | Data breach (72-hour GDPR notification clock starts) |

---

## 8. Known Limitations

| Limitation | Impact | Mitigation |
|---|---|---|
| Cognito has no PITR | Pool deletion requires user password resets | Weekly export to S3; users retain all data, only passwords need reset |
| Single-region architecture | `ap-southeast-2` outage = full scan path outage | P3-6 multi-region deferred until first enterprise contract requires it; SQS retention covers queued events |
| PITR restores to new table name | Lambda env vars require update | SSM pattern minimises blast radius — most Lambdas only need SSM update |
| CloudFront caches API responses | Stale responses possible post-restore | Cache invalidation in §3.1 Step 7 |
