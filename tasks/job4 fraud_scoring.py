# Databricks notebook source
from pyspark.sql.functions import (
    col, round as spark_round,
    count, sum as spark_sum,
    avg, desc
)

# COMMAND ----------

df_txn = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_transactions/")
total  = df_txn.count()
print(f"  Rows read : {total}")
df_txn.printSchema()

# COMMAND ----------

# STEP 2 — Compute fraud_score
#          Formula from document:
#          (high_amount × 0.4) +
#          (geo_mismatch × 0.3) +
#          (rapid_txn × 0.3)

FRAUD_WEIGHT_HIGH_AMOUNT  = 0.4
FRAUD_WEIGHT_GEO_MISMATCH = 0.3
FRAUD_WEIGHT_RAPID_TXN    = 0.3

print("\n" + "─"*60)
print("STEP 2 — Compute fraud_score")
print("─"*60)

df_scored = df_txn.withColumn(
    "fraud_score",
    spark_round(
        (col("high_amount_flag")  * FRAUD_WEIGHT_HIGH_AMOUNT)  +
        (col("geo_mismatch_flag") * FRAUD_WEIGHT_GEO_MISMATCH) +
        (col("rapid_txn_flag")    * FRAUD_WEIGHT_RAPID_TXN),
        2
    )
)

print(f"  Weights used:")
print(f"    high_amount_flag  × {FRAUD_WEIGHT_HIGH_AMOUNT}")
print(f"    geo_mismatch_flag × {FRAUD_WEIGHT_GEO_MISMATCH}")
print(f"    rapid_txn_flag    × {FRAUD_WEIGHT_RAPID_TXN}")
print(f"  Score range : 0.0 to 1.0")

df_scored.select(
    "transaction_id", "high_amount_flag",
    "geo_mismatch_flag", "rapid_txn_flag",
    "fraud_score"
).show(5, truncate=False)

# COMMAND ----------

# STEP 3 — Classify risk
#          score < 0.3  → Low
#          score < 0.6  → Medium
#          score >= 0.6 → High
from pyspark.sql.functions import when,lit
RISK_LOW_THRESHOLD     = 0.3
RISK_MEDIUM_THRESHOLD  = 0.6
print("\n" + "─"*60)
print("STEP 3 — Classify fraud_risk_category")
print("─"*60)
def classify_risk(score_col):
    return (
        when(score_col < RISK_LOW_THRESHOLD,     lit("Low"))
        .when(score_col < RISK_MEDIUM_THRESHOLD, lit("Medium"))
        .otherwise(lit("High"))
    )
df_classified = df_scored.withColumn(
    "fraud_risk_category",
    classify_risk(col("fraud_score"))
)

# Show distribution
print("  Risk distribution:")
df_classified.groupBy("fraud_risk_category").count().show()

# COMMAND ----------

# STEP 4 — Select final columns for fact table
print("\n" + "─"*60)
print("STEP 4 — Select final columns")
print("─"*60)

df_fact_fraud = df_classified.select(
    "transaction_id",
    "account_id",
    "customer_id",
    "amount",
    "currency",
    "transaction_type",
    "merchant",
    "location",
    "txn_timestamp",
    "transaction_hour",
    "txn_date",
    "high_amount_flag",
    "rapid_txn_flag",
    "geo_mismatch_flag",
    "fraud_score",
    "fraud_risk_category"
)

print(f"  Final columns : {len(df_fact_fraud.columns)}")
print(f"  Final rows    : {df_fact_fraud.count()}")

# COMMAND ----------

# STEP 5 — Write fact_fraud_risk
#          OVERWRITE mode
#          Partitioned by txn_date
print("\n" + "─"*60)
print("STEP 5 — Write fact_fraud_risk to Gold")
print("─"*60)

(df_fact_fraud.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema",     "true")
    
    .partitionBy("txn_date")
    .save("/Volumes/project/bank/banks/Gold/fact_fraud/"))

cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/fact_fraud/").count()
print(f"  fact_fraud_risk written → {cnt} rows")


(df_fact_fraud.write.format("delta").mode("overwrite").saveAsTable("project.bank.fact_fraud_risk"))
print(f"  fact_fraud_risk CSV written for Power BI")


# COMMAND ----------

# STEP 6 — Alert Summary
#          Count High risk transactions
#          Top 10 accounts by high risk count
print("\n" + "─"*60)
print("STEP 6 — Alert Summary")
print("─"*60)

df_gold = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/fact_fraud/")

high_risk_count = df_gold.filter(
    col("fraud_risk_category") == "High"
).count()

print(f"\n  🚨 HIGH RISK TRANSACTIONS : {high_risk_count}")
print()

print("  Top 10 accounts by High Risk transaction count:")
(df_gold
    .filter(col("fraud_risk_category") == "High")
    .groupBy("account_id")
    .agg(
        count("transaction_id").alias("high_risk_txn_count"),
        spark_sum("amount").alias("total_amount"),
        avg("fraud_score").alias("avg_fraud_score")
    )
    .orderBy(desc("high_risk_txn_count"))
    .limit(10)
    .show(truncate=False)
)