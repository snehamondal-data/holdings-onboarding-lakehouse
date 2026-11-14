#!/usr/bin/env bash
# Delete everything the landing zone created: the stack (bucket, Lambda, queue, role, log group),
# the artifacts bucket, the SSM parameter, and the Databricks token and service principal.
set -uo pipefail
cd "$(dirname "$0")/.."
AWS="$HOME/.local/bin/aws"; PROFILE="${AWS_PROFILE_NAME:-keelstone-aws}"; REGION="${AWS_REGION_NAME:-us-east-2}"
STACK=keelstone-landing
ACCOUNT=$("$AWS" sts get-caller-identity --query Account --output text --profile "$PROFILE")
for B in "keelstone-landing-$ACCOUNT-$REGION" "keelstone-artifacts-$ACCOUNT-$REGION"; do
  "$AWS" s3 rm "s3://$B" --recursive --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1 || true
done
"$AWS" cloudformation delete-stack --stack-name "$STACK" --profile "$PROFILE" --region "$REGION"
"$AWS" cloudformation wait stack-delete-complete --stack-name "$STACK" --profile "$PROFILE" --region "$REGION" \
  && echo "stack $STACK deleted"
"$AWS" s3api delete-bucket --bucket "keelstone-artifacts-$ACCOUNT-$REGION" --profile "$PROFILE" --region "$REGION" \
  2>/dev/null && echo "artifacts bucket deleted"
.venv/bin/python aws/databricks_identity.py revoke --aws-profile "$PROFILE" --region "$REGION"
echo "--- left over (should all be empty):"
"$AWS" cloudformation describe-stacks --stack-name "$STACK" --profile "$PROFILE" --region "$REGION" 2>&1 | grep -c "does not exist" | sed 's/^/stack gone: /'
"$AWS" s3api list-buckets --query "Buckets[?starts_with(Name, 'keelstone-')].Name" --output text --profile "$PROFILE"
"$AWS" ssm describe-parameters --parameter-filters "Key=Name,Values=/keelstone/" --query "Parameters[].Name" --output text --profile "$PROFILE" --region "$REGION"
