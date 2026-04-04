#!/usr/bin/env bash
set -e

# BeboCard White-Label Infrastructure Provisioning Script
# This script provisions an isolated Amplify Gen 2 backend for an enterprise licensee.

if [ -z "$1" ]; then
  echo "Usage: ./deploy-tenant.sh <tenant-id>"
  echo "Example: ./deploy-tenant.sh enterprise-woolworths"
  exit 1
fi

TENANT_ID=$1
echo "🚀 Provisioning BeboCard Infrastructure for Tenant: $TENANT_ID"

# 1. AWS CLI Profile Check
if [ -z "$AWS_PROFILE" ]; then
  echo "⚠️  WARNING: No AWS_PROFILE set. Defaulting to standard credentials."
fi

# 2. Deploy Isolated Branch
# In Amplify Gen 2, deploying to a specific branch name guarantees 
# completely isolated Cognito, DynamoDB, API Gateway, and S3 resources.
export BEBO_TENANT_ID=$TENANT_ID

npx ampx sandbox --name "tenant-$TENANT_ID" --outputs-out-dir "../../bebocard_business/tenant_configs/$TENANT_ID" --yes

echo "✅ Deployment successful. Client configuration saved."
echo "Provide the amplify_outputs.json file generated above to the licensee's mobile team."
