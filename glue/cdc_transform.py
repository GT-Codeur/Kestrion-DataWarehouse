import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
sc = SparkContext.getOrCreate()
spark = GlueContext(sc).spark_session
# no _SUCCESS marker files, so Redshift COPY sees only Parquet files
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

B = args["BUCKET"]
RAW, CUR, STAR = (f"s3://{B}/raw/public", f"s3://{B}/curated/current", f"s3://{B}/curated/star")

TABLES = {
    "customers":   {"pk": ["customer_id"], "cols": {"customer_id": "int", "customer_name": "string",
                    "city": "string", "category": "string", "onboarded_date": "date"}},
    "inventory":   {"pk": ["product_id"], "cols": {"product_id": "int", "sku": "string",
                    "product_name": "string", "category": "string",
                    "unit_cost": "decimal(10,2)", "qty_on_hand": "int"}},
    "orders":      {"pk": ["order_id"], "cols": {"order_id": "int", "customer_id": "int",
                    "order_date": "date", "status": "string"}},
    "order_lines": {"pk": ["line_id"], "cols": {"line_id": "int", "order_id": "int",
                    "product_id": "int", "quantity": "int", "unit_price": "decimal(10,2)"}},
}

def resolve(table, spec):
    df = (spark.read.option("recursiveFileLookup", "true").option("mergeSchema", "true")
          .parquet(f"{RAW}/{table}/"))
    if "Op" not in df.columns:
        df = df.withColumn("Op", F.lit("I"))
    df = df.withColumn("Op", F.coalesce(F.col("Op"), F.lit("I")))
    # delete wins ties: an UPDATE and the DELETE right after it can share a millisecond timestamp
    rank = F.when(F.col("Op") == "D", 2).when(F.col("Op") == "U", 1).otherwise(0)
    w = Window.partitionBy(*spec["pk"]).orderBy(F.col("ts").desc(), rank.desc())
    latest = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
    current = latest.filter(F.col("Op") != "D")
    out = current.select(*[F.col(c).cast(t).alias(c) for c, t in spec["cols"].items()])
    print(f"{table}: change_rows={df.count()} keys={latest.count()} "
          f"deleted={latest.filter(F.col('Op') == 'D').count()} current={out.count()}")
    return out

# 1) CDC resolution -> curated/current
for t, spec in TABLES.items():
    resolve(t, spec).write.mode("overwrite").parquet(f"{CUR}/{t}/")

# 2) Star schema -> curated/star (column ORDER must match the Redshift DDL)
c = {t: spark.read.parquet(f"{CUR}/{t}/") for t in TABLES}
o, ol = c["orders"], c["order_lines"]

dim_customer = c["customers"]
dim_product = c["inventory"]

fact = (ol.join(o.select("order_id", "customer_id", "order_date",
                         F.col("status").alias("order_status")), "order_id", "inner")
          .select("line_id", "order_id", "customer_id", "product_id",
                  F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
                  "order_status", "quantity", "unit_price",
                  (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)").alias("line_amount")))

b = o.agg(F.min("order_date").alias("mn"), F.max("order_date").alias("mx")).first()
dim_date = (spark.range(1)
    .select(F.explode(F.sequence(F.lit(b["mn"]), F.lit(b["mx"]), F.expr("interval 1 day"))).alias("full_date"))
    .select(F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"), "full_date",
            F.year("full_date").cast("smallint").alias("cal_year"),
            F.quarter("full_date").cast("smallint").alias("cal_quarter"),
            F.month("full_date").cast("smallint").alias("cal_month"),
            F.dayofmonth("full_date").cast("smallint").alias("cal_day"),
            F.dayofweek("full_date").cast("smallint").alias("day_of_week")))

for name, d in [("dim_customer", dim_customer), ("dim_product", dim_product),
                ("dim_date", dim_date), ("fact_order_lines", fact)]:
    d.coalesce(1).write.mode("overwrite").parquet(f"{STAR}/{name}/")
print("fact rows:", fact.count())
