import sys, json
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "RUN_ID"])
sc = SparkContext.getOrCreate()
spark = GlueContext(sc).spark_session
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

B, RUN = args["BUCKET"], args["RUN_ID"]
CUR = f"s3://{B}/curated/current"
QUAR = f"s3://{B}/quarantine/{RUN}"

t = {n: spark.read.parquet(f"{CUR}/{n}/").cache()
     for n in ["customers", "inventory", "orders", "order_lines"]}
total = {n: d.count() for n, d in t.items()}
results = []

def record(check, table, bad):
    n_bad = bad.count()
    if n_bad > 0:
        bad.write.mode("overwrite").parquet(f"{QUAR}/{check}/")
    results.append({"check": check, "table": table, "bad_rows": n_bad,
                    "total_rows": total[table], "passed": n_bad == 0})

def not_null(table, cols):
    cond = " OR ".join(f"{c} IS NULL" for c in cols)
    record(f"{table}_not_null", table, t[table].filter(cond))

def unique(table, pk):
    record(f"{table}_pk_unique", table,
           t[table].groupBy(pk).count().filter(F.col("count") > 1))

def ref(child, col, parent, pcol):
    parent_keys = t[parent].select(F.col(pcol).alias(col)).distinct()
    record(f"{child}_{col}_in_{parent}", child,
           t[child].join(parent_keys, col, "left_anti"))

# completeness
not_null("customers", ["customer_id", "customer_name"])
not_null("inventory", ["product_id", "sku"])
not_null("orders", ["order_id", "customer_id", "order_date", "status"])
not_null("order_lines", ["line_id", "order_id", "product_id", "quantity", "unit_price"])
# uniqueness
unique("customers", "customer_id"); unique("inventory", "product_id")
unique("orders", "order_id");       unique("order_lines", "line_id")
# referential integrity
ref("orders", "customer_id", "customers", "customer_id")
ref("order_lines", "order_id", "orders", "order_id")
ref("order_lines", "product_id", "inventory", "product_id")
# validity
record("orders_valid_status", "orders",
       t["orders"].filter(~F.col("status").isin("placed", "shipped", "delivered", "cancelled")))
record("order_lines_quantity_positive", "order_lines", t["order_lines"].filter("quantity <= 0"))
record("order_lines_price_positive", "order_lines", t["order_lines"].filter("unit_price <= 0"))
# volume
for n in ["orders", "order_lines"]:
    results.append({"check": f"{n}_not_empty", "table": n,
                    "bad_rows": 0 if total[n] > 0 else 1, "total_rows": total[n],
                    "passed": total[n] > 0})

failed = [r for r in results if not r["passed"]]
summary = {"run_id": RUN, "passed": len(failed) == 0, "checks_run": len(results),
           "failed_checks": failed,
           "quarantine_path": f"s3://{B}/quarantine/{RUN}/" if failed else None}
boto3.client("s3").put_object(Bucket=B, Key=f"dq/results/{RUN}.json",
                              Body=json.dumps(summary).encode(),
                              ContentType="application/json")
print(json.dumps(summary, indent=2))
