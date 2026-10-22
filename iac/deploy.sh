#!/usr/bin/env bash
# One-command deploy / teardown for the Kestrion CDC pipeline (6 CloudFormation stacks).
#
#   iac/deploy.sh deploy     build everything from scratch (about 25-40 minutes, mostly RDS, DMS, Redshift)
#   iac/deploy.sh smoke      start one pipeline execution and wait for the result
#   iac/deploy.sh status     show stack states and key outputs
#   iac/deploy.sh teardown   stop DMS, empty the bucket, delete all stacks (reverse order)
#
# Required for deploy (export them, never commit them):
#   ALERT_EMAIL         where SNS notifications go
#   DB_PASSWORD         source database master password   (letters and digits, 8+ chars)
#   REDSHIFT_PASSWORD   warehouse admin password          (8-64 letters/digits with upper, lower and a digit)
# Optional: PROJECT (default kestrion-iac), AWS_DEFAULT_REGION (default us-east-1), DB_NAME, RS_DB,
#           VPC_ID, SUBNET_IDS (comma separated), MY_IP_CIDR, SCHEDULE_STATE (DISABLED|ENABLED), FORCE=1
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT="${PROJECT:-kestrion-iac}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
DB_NAME="${DB_NAME:-kestriondb}"
RS_DB="${RS_DB:-dev}"
SCHEDULE_STATE="${SCHEDULE_STATE:-DISABLED}"

S_FOUNDATION="$PROJECT-foundation"
S_SOURCE="$PROJECT-source"
S_INGEST="$PROJECT-ingest"
S_GLUE="$PROJECT-glue"
S_WAREHOUSE="$PROJECT-warehouse"
S_ORCH="$PROJECT-orchestration"

log() { printf '\n==> %s\n' "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "$1 is required but not installed"; }

# out <stack> <OutputKey>  -> value, or empty when the stack or output does not exist
out() {
  local v
  v="$(aws cloudformation describe-stacks --stack-name "$1" \
        --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue | [0]" --output text 2>/dev/null || true)"
  [ "$v" = "None" ] && v=""
  printf '%s' "$v"
}

# cfn <stack> <template> [Key=Value ...]
cfn() {
  local stack="$1" tpl="$2"
  shift 2
  log "Deploying $stack"
  aws cloudformation deploy \
    --stack-name "$stack" \
    --template-file "$ROOT/iac/$tpl" \
    --capabilities CAPABILITY_NAMED_IAM \
    --no-fail-on-empty-changeset \
    --tags Project=kestrion \
    --parameter-overrides "ProjectName=$PROJECT" "$@"
}

discover_network() {
  VPC_ID="${VPC_ID:-$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true \
        --query 'Vpcs[0].VpcId' --output text)}"
  { [ -n "$VPC_ID" ] && [ "$VPC_ID" != "None" ]; } || die "no default VPC found: set VPC_ID and SUBNET_IDS"
  SUBNET_IDS="${SUBNET_IDS:-$(aws ec2 describe-subnets \
        --filters "Name=vpc-id,Values=$VPC_ID" Name=default-for-az,Values=true \
        --query 'Subnets[].SubnetId' --output text | tr '\t' ',')}"
  [ -n "$SUBNET_IDS" ] || die "no subnets found: set SUBNET_IDS"
  MY_IP_CIDR="${MY_IP_CIDR:-$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]')/32}"
  echo "VPC $VPC_ID | subnets $SUBNET_IDS | operator IP $MY_IP_CIDR"
}

# DMS needs this account-level role once per account. It is bootstrapped here and never deleted by teardown.
ensure_dms_vpc_role() {
  if aws iam get-role --role-name dms-vpc-role >/dev/null 2>&1; then return 0; fi
  log "Creating the account-level role dms-vpc-role (needed once per account)"
  aws iam create-role --role-name dms-vpc-role \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"dms.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  aws iam attach-role-policy --role-name dms-vpc-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole
  sleep 15
}

upload_artifacts() {
  log "Uploading Glue scripts and the state machine definition to s3://$1/scripts/"
  aws s3 cp "$ROOT/glue/cdc_transform.py" "s3://$1/scripts/cdc_transform.py" --only-show-errors
  aws s3 cp "$ROOT/glue/dq_gate.py" "s3://$1/scripts/dq_gate.py" --only-show-errors
  aws s3 cp "$ROOT/stepfunctions/pipeline.asl.json" "s3://$1/scripts/pipeline.asl.json" --only-show-errors
}

dms_status() {
  aws dms describe-replication-tasks --filters "Name=replication-task-arn,Values=$1" \
    --query 'ReplicationTasks[0].Status' --output text 2>/dev/null || echo unknown
}

start_dms_task() {
  local arn="$1" status progress i
  status="$(dms_status "$arn")"
  if [ "$status" != "running" ]; then
    local start_type="start-replication"
    case "$status" in failed | stopped) start_type="reload-target" ;; esac
    log "Starting the DMS task ($start_type: full load, then ongoing replication)"
    aws dms start-replication-task --replication-task-arn "$arn" \
      --start-replication-task-type "$start_type" >/dev/null
  fi
  for i in $(seq 1 90); do
    status="$(dms_status "$arn")"
    progress="$(aws dms describe-replication-tasks --filters "Name=replication-task-arn,Values=$arn" \
        --query 'ReplicationTasks[0].ReplicationTaskStats.FullLoadProgressPercent' --output text 2>/dev/null || echo 0)"
    echo "  DMS task: $status, full load ${progress}%"
    case "$status" in
      running) [ "$progress" = "100" ] && return 0 ;;
      failed | stopped) die "DMS task is $status. Check the task in the DMS console (Last failure message)" ;;
    esac
    sleep 20
  done
  die "DMS full load did not finish in 30 minutes"
}

cmd_deploy() {
  need aws; need psql; need python3; need curl
  python3 -c 'import boto3' 2>/dev/null || die "python3 needs boto3 (pip install boto3)"
  : "${ALERT_EMAIL:?export ALERT_EMAIL}" "${DB_PASSWORD:?export DB_PASSWORD}" "${REDSHIFT_PASSWORD:?export REDSHIFT_PASSWORD}"
  aws sts get-caller-identity >/dev/null || die "AWS credentials are not configured"

  discover_network
  ensure_dms_vpc_role

  cfn "$S_FOUNDATION" 01-foundation.yaml "AlertEmail=$ALERT_EMAIL"
  local bucket topic glue_role dms_role rs_role sfn_role sfn_role_name events_role
  bucket="$(out "$S_FOUNDATION" BucketName)"
  topic="$(out "$S_FOUNDATION" TopicArn)"
  glue_role="$(out "$S_FOUNDATION" GlueRoleArn)"
  dms_role="$(out "$S_FOUNDATION" DmsS3RoleArn)"
  rs_role="$(out "$S_FOUNDATION" RedshiftRoleArn)"
  sfn_role="$(out "$S_FOUNDATION" SfnRoleArn)"
  sfn_role_name="$(out "$S_FOUNDATION" SfnRoleName)"
  events_role="$(out "$S_FOUNDATION" EventsRoleArn)"
  upload_artifacts "$bucket"

  cfn "$S_SOURCE" 02-source.yaml "VpcId=$VPC_ID" "SubnetIds=$SUBNET_IDS" "MyIpCidr=$MY_IP_CIDR" \
      "DbPassword=$DB_PASSWORD" "DbName=$DB_NAME"
  local db_host dms_sg
  db_host="$(out "$S_SOURCE" DbEndpoint)"
  dms_sg="$(out "$S_SOURCE" DmsSecurityGroupId)"

  log "Seeding the source database"
  PGHOST="$db_host" PGPASSWORD="$DB_PASSWORD" PGDATABASE="$DB_NAME" "$ROOT/scripts/seed_source.sh"

  cfn "$S_INGEST" 03-ingest.yaml "SubnetIds=$SUBNET_IDS" "DmsSecurityGroupId=$dms_sg" "DbEndpoint=$db_host" \
      "DbName=$DB_NAME" "DbPassword=$DB_PASSWORD" "DataBucketName=$bucket" "DmsS3RoleArn=$dms_role"
  local task_arn task_id
  task_arn="$(out "$S_INGEST" ReplicationTaskArn)"
  task_id="$(out "$S_INGEST" ReplicationTaskId)"
  if [ "$(dms_status "$task_arn")" != "running" ]; then drop_stale_slot "$db_host"; fi
  start_dms_task "$task_arn"

  cfn "$S_GLUE" 04-glue.yaml "DataBucketName=$bucket" "GlueRoleArn=$glue_role"
  local transform_job gate_job
  transform_job="$(out "$S_GLUE" TransformJobName)"
  gate_job="$(out "$S_GLUE" QualityGateJobName)"

  cfn "$S_WAREHOUSE" 05-warehouse.yaml "VpcId=$VPC_ID" "SubnetIds=$SUBNET_IDS" "RedshiftRoleArn=$rs_role" \
      "DatabaseName=$RS_DB" "AdminPassword=$REDSHIFT_PASSWORD"
  local workgroup secret_arn
  workgroup="$(out "$S_WAREHOUSE" WorkgroupName)"
  secret_arn="$(out "$S_WAREHOUSE" AdminSecretArn)"

  log "Creating the warehouse tables and granting the state machine access"
  python3 "$ROOT/scripts/setup_warehouse.py" --workgroup "$workgroup" --database "$RS_DB" \
      --secret-arn "$secret_arn" --sfn-role-name "$sfn_role_name" \
      --ddl "$ROOT/sql/redshift_ddl.sql" --grants "$ROOT/sql/grants.sql"

  cfn "$S_ORCH" 06-orchestration.yaml "DataBucketName=$bucket" "TopicArn=$topic" "SfnRoleArn=$sfn_role" \
      "EventsRoleArn=$events_role" "WorkgroupName=$workgroup" "DatabaseName=$RS_DB" "DmsTaskId=$task_id" \
      "TransformJobName=$transform_job" "QualityGateJobName=$gate_job" "ScheduleState=$SCHEDULE_STATE"

  log "Deployed in $((SECONDS / 60)) min $((SECONDS % 60)) s"
  cat <<EOF

Next:
  1. Confirm the SNS subscription email sent to $ALERT_EMAIL (no emails arrive until you do).
  2. Run one end-to-end execution:  iac/deploy.sh smoke
  3. The EventBridge rule is $SCHEDULE_STATE. Enable it only while demoing:
       aws events enable-rule  --name $PROJECT-pipeline-schedule
       aws events disable-rule --name $PROJECT-pipeline-schedule
  4. Generate changes:  python3 source/cdc_change_generator.py --host $db_host --user postgres \\
       --password '<DB_PASSWORD>' --database $DB_NAME --engine postgres --duration-minutes 15
EOF
}

cmd_smoke() {
  local arn input exec_arn status
  arn="$(out "$S_ORCH" StateMachineArn)"
  input="$(out "$S_ORCH" StateMachineInput)"
  [ -n "$arn" ] || die "state machine not found: deploy first"
  exec_arn="$(aws stepfunctions start-execution --state-machine-arn "$arn" \
      --name "smoke-$(date +%Y%m%d-%H%M%S)" --input "$input" --query executionArn --output text)"
  echo "Started $exec_arn"
  while :; do
    status="$(aws stepfunctions describe-execution --execution-arn "$exec_arn" --query status --output text)"
    echo "  status: $status"
    [ "$status" != "RUNNING" ] && break
    sleep 20
  done
  [ "$status" = "SUCCEEDED" ] || die "execution ended as $status (open it in the Step Functions console)"
  echo "Pipeline run succeeded"
}

# Drop a stale, unused replication slot so DMS can create a fresh one. Safe no-op when absent or active.
drop_stale_slot() {
  PGHOST="$1" PGPASSWORD="$DB_PASSWORD" PGDATABASE="$DB_NAME" PGUSER=postgres PGSSLMODE=require \
    psql -qtA -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'dms_kestrion' AND NOT active;" \
    >/dev/null 2>&1 || true
}

cmd_status() {
  local s
  for s in "$S_FOUNDATION" "$S_SOURCE" "$S_INGEST" "$S_GLUE" "$S_WAREHOUSE" "$S_ORCH"; do
    printf '%-40s %s\n' "$s" \
      "$(aws cloudformation describe-stacks --stack-name "$s" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo 'not deployed')"
  done
  echo "DMS task: $(dms_status "$(out "$S_INGEST" ReplicationTaskArn)")"
}

delete_stack() {
  if aws cloudformation describe-stacks --stack-name "$1" >/dev/null 2>&1; then
    log "Deleting $1"
    aws cloudformation delete-stack --stack-name "$1"
    aws cloudformation wait stack-delete-complete --stack-name "$1"
  fi
}

cmd_teardown() {
  need aws
  if [ "${FORCE:-0}" != "1" ]; then
    read -r -p "This deletes every $PROJECT resource, including the data bucket. Type the project name to continue: " ans
    [ "$ans" = "$PROJECT" ] || die "aborted"
  fi

  local task_arn status bucket i
  task_arn="$(out "$S_INGEST" ReplicationTaskArn)"
  if [ -n "$task_arn" ]; then
    status="$(dms_status "$task_arn")"
    if [ "$status" = "running" ] || [ "$status" = "starting" ]; then
      log "Stopping the DMS task"
      aws dms stop-replication-task --replication-task-arn "$task_arn" >/dev/null || true
      for i in $(seq 1 30); do
        [ "$(dms_status "$task_arn")" = "stopped" ] && break
        sleep 10
      done
    fi
  fi

  bucket="$(out "$S_FOUNDATION" BucketName)"
  if [ -n "$bucket" ]; then
    log "Emptying s3://$bucket"
    aws s3 rm "s3://$bucket" --recursive --only-show-errors || true
  fi

  delete_stack "$S_ORCH"
  delete_stack "$S_WAREHOUSE"
  delete_stack "$S_GLUE"
  delete_stack "$S_INGEST"
  delete_stack "$S_SOURCE"
  delete_stack "$S_FOUNDATION"
  log "Teardown finished in $((SECONDS / 60)) min $((SECONDS % 60)) s"
}

case "${1:-}" in
  deploy) cmd_deploy ;;
  smoke) cmd_smoke ;;
  status) cmd_status ;;
  teardown) cmd_teardown ;;
  *) sed -n '2,15p' "${BASH_SOURCE[0]}"; exit 1 ;;
esac
