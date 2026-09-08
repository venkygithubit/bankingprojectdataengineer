# Databricks notebook source
df_dim_cust = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_customer/")

# Write Delta
(df_dim_cust.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save("/Volumes/project/bank/banks/Gold/dim_customer/"))

cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/dim_customer/").count()
print(f" Delta → {cnt} rows")
df_dim_cust.write.format("delta").mode("overwrite").saveAsTable("project.bank.dim_customer")

# COMMAND ----------

df_dim_acc = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_accounts/")

(df_dim_acc.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save("/Volumes/project/bank/banks/Gold/dim_accounts/"))

cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/dim_accounts/").count()
print(f" Delta → {cnt} rows")
df_dim_acc.write.format("delta").mode("overwrite").saveAsTable("project.bank.dim_accounts")


# COMMAND ----------

# STEP 3 — dim_date (365 rows for 2026)
print("\n" + "─"*60)
print("STEP 3 — dim_date (full year 2026)")
print("─"*60)
from pyspark.sql.functions import *
df_dates = spark.sql("""
    SELECT explode(sequence(
        to_date('2026-01-01'),
        to_date('2026-12-31'),
        interval 1 day
    )) AS full_date
""")

df_dim_date = (
    df_dates
    .withColumn("date_key",   date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("year",       year(col("full_date")))
    .withColumn("month",      month(col("full_date")))
    .withColumn("day",        dayofmonth(col("full_date")))
    .withColumn("quarter",    quarter(col("full_date")))
    .withColumn("day_name",   date_format(col("full_date"), "EEEE"))
    .withColumn("month_name", date_format(col("full_date"), "MMMM"))
    .withColumn("is_weekend",
        when(dayofweek(col("full_date")).isin([1, 7]), lit(1))
        .otherwise(lit(0))
    )
)

(df_dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save("/Volumes/project/bank/banks/Gold/dim_date/"))

cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/dim_date/").count()
print(f"  Delta → {cnt} rows  (expected 365)")
df_dim_date.write.format("delta").mode("overwrite").saveAsTable("project.bank.dim_date")


# COMMAND ----------


# STEP 4 — fact_transactions
print("\n" + "─"*60)
print("STEP 4 — fact_transactions")
print("─"*60)

df_fact_txn = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_transactions/")
print(f"  Rows : {df_fact_txn.count()}")

(df_fact_txn.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .partitionBy("txn_date")
    .save("/Volumes/project/bank/banks/Gold/fact_transactions/"))

cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/fact_transactions/").count()
print(f"  Delta → {cnt} rows")
df_fact_txn.write.format("delta").mode("overwrite").saveAsTable("project.bank.fact_transactions")

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import IntegerType
print("STEP 4 — dim_location")
LOCATION_COUNTRY_MAP   = {
    "Mumbai":"India","Delhi":"India",
    "London":"UK","New York":"USA","Toronto":"Canada"
}
loc_map = F.create_map(*[v for kv in LOCATION_COUNTRY_MAP.items()
                         for v in (F.lit(kv[0]), F.lit(kv[1]))])

df_dim_location = (
    df_fact_txn.select("location").distinct()
    .filter(F.col("location").isNotNull())
    .withColumn("country",
        F.coalesce(loc_map[F.col("location")], F.lit("Unknown")))
    .withColumn("region",
        F.when(F.col("country") == "India",             F.lit("Asia"))
         .when(F.col("country") == "UK",                F.lit("Europe"))
         .when(F.col("country").isin(["USA","Canada"]), F.lit("Americas"))
         .otherwise(                                     F.lit("Other")))
    .withColumn("location_key",
        F.row_number().over(Window.orderBy("location")).cast(IntegerType()))
    .select("location_key", "location", "country", "region")
)

(df_dim_location.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save("/Volumes/project/bank/banks/Gold/dim_location/"))

cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/dim_location/").count()
print(f"Delta → {cnt} rows")
df_dim_location.write.format("delta").mode("overwrite").saveAsTable("project.bank.dim_location")

# COMMAND ----------

print("\n" + "─"*60)
print("STEP 5 — fact_fraud_risk")
print("─"*60)

df_fraud = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/fact_fraud/")
cnt = df_fraud.count()
print(f"  Already written by job_4 → {cnt} rows")


# COMMAND ----------

# STEP 6 — fact_credit_risk
print("\n" + "─"*60)
print("STEP 6 — fact_credit_risk")
print("─"*60)

df_credit = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/fact_credit/")
cnt = df_credit.count()
print(f" Already written by job_5 → {cnt} rows")


# COMMAND ----------

# STEP 7 — fact_customer_churn
print("\n" + "─"*60)
print("STEP 7 — fact_customer_churn")
print("─"*60)

df_acc = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_accounts/")

df_churn = df_acc.withColumn(
    "churn_risk",
    when(col("status") == "Closed",   lit("Churned"))
    .when(col("status") == "Dormant", lit("High"))
    .otherwise(lit("Low"))
)

(df_churn.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save("/Volumes/project/bank/banks/Gold/churn/"))

cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/churn/").count()
print(f" Delta → {cnt} rows")

print("  Churn distribution:")
df_churn.groupBy("churn_risk").count().show()
df_churn.write.format("delta").saveAsTable("project.bank.churn")

# COMMAND ----------

# STEP 8 — kpi_daily_fraud
print("\n" + "─"*60)
print("STEP 8 — kpi_daily_fraud")
print("─"*60)

df_kpi = (
    df_fraud
    .groupBy("txn_date", "fraud_risk_category")
    .agg(
        count("transaction_id").alias("transaction_count"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("fraud_score"),  4).alias("avg_fraud_score")
    )
    .orderBy("txn_date", "fraud_risk_category")
)

(df_kpi.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save("/Volumes/project/bank/banks/Gold/KPI_daily_fraud/"))

cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/KPI_daily_fraud/").count()
print(f"  Delta → {cnt} rows")

print("  Sample kpi_daily_fraud:")
spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/KPI_daily_fraud/").show(8)
df_kpi.write.format("delta").saveAsTable("project.bank.KPI_daily_fraud")