#!/usr/bin/env bash
# Deploy the S3 landing zone with the AWS CLI (CloudFormation). Usage:
#   aws/deploy.sh                       # normal
#   SIMULATE_OUTAGE=true aws/deploy.sh  # point the Lambda at an unreachable host (dead-letter test)
set -euo pipefail
cd "$(dirname "$0")/.."
AWS="$HOME/.local/bin/aws"; PROFILE="${AWS_PROFILE_NAME:-keelstone-aws}"; REGION="${AWS_REGION_NAME:-us-east-2}"
STACK=keelstone-landing
ACCOUNT=$("$AWS" sts get-caller-identity --query Account --output text --profile "$PROFILE")
ART="keelstone-artifacts-$ACCOUNT-$REGION"
.venv/bin/python aws/build_lambda_config.py
if ! "$AWS" s3api head-bucket --bucket "$ART" --profile "$PROFILE" --region "$REGION" 2>/dev/null; then
  LOC=(); [ "$REGION" != "us-east-1" ] && LOC=(--create-bucket-configuration "LocationConstraint=$REGION")
  "$AWS" s3api create-bucket --bucket "$ART" "${LOC[@]}" --profile "$PROFILE" --region "$REGION" >/dev/null
  "$AWS" s3api put-public-access-block --bucket "$ART" --profile "$PROFILE" --region "$REGION" \
    --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  "$AWS" s3api put-bucket-encryption --bucket "$ART" --profile "$PROFILE" --region "$REGION" \
    --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
  "$AWS" s3api put-bucket-lifecycle-configuration --bucket "$ART" --profile "$PROFILE" --region "$REGION" \
    --lifecycle-configuration '{"Rules":[{"ID":"delete-after-7-days","Status":"Enabled","Filter":{},"Expiration":{"Days":7}}]}'
fi
"$AWS" cloudformation package --template-file aws/template.yaml --s3-bucket "$ART" \
  --output-template-file aws/.packaged.yaml --profile "$PROFILE" --region "$REGION" >/dev/null
HOST=$(.venv/bin/python -c "import configparser,os;c=configparser.ConfigParser();c.read(os.path.expanduser('~/.databrickscfg'));print(c['keelstone']['host'])")
"$AWS" cloudformation deploy --template-file aws/.packaged.yaml --stack-name "$STACK" --capabilities CAPABILITY_IAM \
  --parameter-overrides "DatabricksHost=$HOST" "SimulateOutage=${SIMULATE_OUTAGE:-false}" \
  --profile "$PROFILE" --region "$REGION" --no-fail-on-empty-changeset
"$AWS" cloudformation describe-stacks --stack-name "$STACK" --profile "$PROFILE" --region "$REGION" \
  --query "Stacks[0].Outputs" --output table
