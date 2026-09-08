# Databricks notebook source
from pyspark.sql.functions import col
df_credit = spark.read.format("delta").load("/Volumes/project/bank/banks/bronze/credit_history_bronze/")
df_customers = (spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_customer/")
    .filter(col("is_current") == True))
print(f"  bronze_credit_history : {df_credit.count()} rows")
print(f"  silver_customers      : {df_customers.count()} rows (current)")

# COMMAND ----------

# STEP 2 — Join
print("\nSTEP 2 — Join credit + customers")
df_joined = df_credit.join(
    df_customers.select("customer_id","name","country","customer_type","risk_category"),
    on="customer_id", how="inner"
)
print(f"  Rows after join : {df_joined.count()}")

# COMMAND ----------

from pyspark.sql.functions import *
df_defaults = df_joined.withColumn(
    "recent_defaults",
    when(col("last_default_date").isNotNull(), lit(1)).otherwise(lit(0))
)
print(f"  Customers with defaults : {df_defaults.filter(col('recent_defaults')==1).count()}")

# COMMAND ----------

# STEP 4 — Normalize open_loans
print("\nSTEP 4 — Normalize open_loans (÷10, cap 1.0)")
from pyspark.sql.functions import round
CREDIT_MAX_LOANS_NORM  = 10.0

df_norm = df_defaults.withColumn(
    "open_loans_norm",
    when(col("open_loans") / CREDIT_MAX_LOANS_NORM > 1.0, lit(1.0))
    .otherwise(round(col("open_loans") / CREDIT_MAX_LOANS_NORM, 2))
)

# COMMAND ----------

# STEP 5 — Compute credit_risk_score
print("\nSTEP 5 — Compute credit_risk_score")
CREDIT_WEIGHT_SCORE    = 0.6
CREDIT_WEIGHT_LOANS    = 0.2
CREDIT_WEIGHT_DEFAULTS = 0.2
df_scored = df_norm.withColumn(
    "credit_risk_score",
    round(
        ((1 - col("credit_score") / 900) * CREDIT_WEIGHT_SCORE) +
        (col("open_loans_norm")           * CREDIT_WEIGHT_LOANS) +
        (col("recent_defaults")           * CREDIT_WEIGHT_DEFAULTS),
        2
    )
)
print(f" credit_risk_score computed")

# COMMAND ----------

# DBTITLE 1,Cell 6
# STEP 6 — Classify
print("\nSTEP 6 — Classify credit_risk_category")
df_classified = df_scored.withColumn(
    "credit_risk_category",
    when(col("credit_risk_score") < 0.3, "Low")
    .when((col("credit_risk_score") >= 0.3) & (col("credit_risk_score") < 0.6), "Medium")
    .when(col("credit_risk_score") >= 0.6, "High")
    .otherwise("Unknown")
)
df_classified.groupBy("credit_risk_category").count().show()

# COMMAND ----------

# DBTITLE 1,Cell 7
# STEP 7 — Write — NO overwriteSchema!
print("\nSTEP 7 — Write fact_credit_risk")

df_fact = df_classified.select(
    "customer_id","name","country","customer_type",
    "risk_category","credit_score","open_loans",
    "open_loans_norm","last_default_date",
    "recent_defaults","credit_risk_score","credit_risk_category"
)

(df_fact.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save("/Volumes/project/bank/banks/Gold/fact_credit/"))

cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/Gold/fact_credit/").count()
print(f"fact_credit_risk → {cnt} rows")

(df_fact.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("project.bank.fact_credit_risk"))
print(f"CSV written for Power BI")
