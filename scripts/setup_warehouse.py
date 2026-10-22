#!/usr/bin/env python3
"""One-off Redshift Serverless setup: create the star-schema tables, grant the state machine's database
user access, and set a daily RPU-hour usage limit. Uses the Redshift Data API with the admin secret
created by the warehouse stack. Idempotent: safe to run again."""
import argparse
import re
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def statements(path, replacements=None):
    text = Path(path).read_text()
    for old, new in (replacements or {}).items():
        text = text.replace(old, new)
    text = re.sub(r"--[^\n]*", "", text)  # strip comments before splitting on ;
    return [s.strip() for s in text.split(";") if s.strip()]


def run(client, args, sql, ignore=()):
    last = None
    for attempt in range(8):  # the workgroup can need a moment before it accepts the first connection
        try:
            sid = client.execute_statement(
                WorkgroupName=args.workgroup, Database=args.database, SecretArn=args.secret_arn, Sql=sql
            )["Id"]
            break
        except ClientError as exc:
            last = exc
            print(f"  waiting for the workgroup ({exc.response['Error']['Code']})...")
            time.sleep(15)
    else:
        raise SystemExit(f"Could not submit statement: {last}")

    while True:
        d = client.describe_statement(Id=sid)
        if d["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(1)
    if d["Status"] != "FINISHED":
        err = d.get("Error", "")
        if any(token in err for token in ignore):
            print(f"  skipped (already done): {err}")
            return
        raise SystemExit(f"FAILED: {sql[:90]!r}\n  {err}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--workgroup", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--secret-arn", required=True)
    p.add_argument("--sfn-role-name", required=True)
    p.add_argument("--ddl", required=True)
    p.add_argument("--grants", required=True)
    p.add_argument("--daily-rpu-hours", type=int, default=20)
    args = p.parse_args()

    rsd = boto3.client("redshift-data")

    print("Creating tables...")
    for sql in statements(args.ddl):
        run(rsd, args, sql)

    print(f"Granting access to IAMR:{args.sfn_role_name} ...")
    for sql in statements(args.grants, {"__SFN_ROLE__": args.sfn_role_name}):
        run(rsd, args, sql, ignore=("already exists",))

    print("Setting the daily usage limit (best effort)...")
    try:
        rs = boto3.client("redshift-serverless")
        arn = rs.get_workgroup(workgroupName=args.workgroup)["workgroup"]["workgroupArn"]
        rs.create_usage_limit(
            resourceArn=arn,
            usageType="serverless-compute",
            amount=args.daily_rpu_hours,
            period="daily",
            breachAction="log",
        )
        print(f"  limit: {args.daily_rpu_hours} RPU-hours per day (action: log)")
    except Exception as exc:  # noqa: BLE001 - a missing limit must not fail the deploy
        print(f"  could not create the usage limit: {exc}")

    print("Warehouse setup OK")


if __name__ == "__main__":
    sys.exit(main())
