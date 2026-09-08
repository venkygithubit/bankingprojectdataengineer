# Databricks notebook source
#HIGH_AMOUNT_THRESHOLD    = 50_000
#RAPID_TXN_WINDOW_SECONDS = 300
#RAPID_TXN_COUNT_LIMIT    = 3
#FRAUD_WEIGHT_HIGH_AMOUNT  = 0.4
#FRAUD_WEIGHT_GEO_MISMATCH = 0.3
#FRAUD_WEIGHT_RAPID_TXN    = 0.3
#CREDIT_WEIGHT_SCORE    = 0.6
#CREDIT_WEIGHT_LOANS    = 0.2
#CREDIT_WEIGHT_DEFAULTS = 0.2
#CREDIT_MAX_LOANS_NORM  = 10.0
#RISK_LOW_THRESHOLD     = 0.3
#RISK_MEDIUM_THRESHOLD  = 0.6

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, lit, to_timestamp,
    hour, to_date, count, create_map,
    coalesce
)
from pyspark.sql.types import(
    StructType,StructField,StringType,DoubleType,IntegerType,ArrayType
)
from pyspark.sql.window import Window

# COMMAND ----------

# STEP 1 — Read bronze_transactions
#          Filter NULL transaction_id
#          Filter amount <= 0
df_raw = spark.read.format("delta").load("/Volumes/project/bank/banks/bronze/transaction_bronze/")
raw_count = df_raw.count()
print(f"  Raw rows from bronze : {raw_count}")
df_raw.printSchema()
df_raw.show(2,truncate=False)

columns = df_raw.columns
print(f"Columns found : {columns}")

# ==== 1) Flatten metadata if exists ====
if "metadata" in columns:
    print("Found metadata column — flattening...")
    df_flat = (
        df_raw
            .withColumn("device",     col("metadata.device"))
            .withColumn("ip_address", col("metadata.ip_address"))
            .withColumn("channel",    col("metadata.channel"))
            .drop("metadata")
    )
else:
    print("No nested metadata found.")
    df_flat = df_raw

df_flat.printSchema()
df_flat.show(2, truncate=False)

# ==== 2) Filter bad records ====
df_clean = df_flat.filter(
    col("transaction_id").isNotNull() &
    (col("amount") > 0)
)

clean_count  = df_clean.count()
dropped_count = raw_count - clean_count

print(f"  Rows after cleaning  : {clean_count}")
print(f"  Rows dropped         : {dropped_count}")
print("  Dropped reason       : NULL transaction_id OR amount <= 0")

# COMMAND ----------


#          Extract txn_date and transaction_hour
print("\n" + "─"*60)
print("STEP 2 — Parse timestamp → txn_date + transaction_hour")
print("─"*60)

df_parsed = (
    df_clean
    .withColumn(
        "txn_timestamp",
        to_timestamp(col("timestamp"))
    )
    .withColumn(
        "transaction_hour",
        hour(col("txn_timestamp"))
    )
    .withColumn(
        "txn_date",
        to_date(col("txn_timestamp"))
    )
)

print(f"  txn_timestamp parsed")
print(f"  transaction_hour extracted (0-23)")
print(f"  txn_date extracted")
df_parsed.select(
    "transaction_id", "timestamp",
    "txn_timestamp", "transaction_hour", "txn_date"
).show(3, truncate=False)

# COMMAND ----------

# STEP 3 — Add high_amount_flag
#          1 if amount > 50,000 else 0
print("\n" + "─"*60)
print("STEP 3 — Add high_amount_flag")
print("─"*60)

df_flagged = df_parsed.withColumn(
    "high_amount_flag",
    when(col("amount") > 50000, lit(1))
    .otherwise(lit(0))
)

high_amt_count = df_flagged.filter(col("high_amount_flag") == 1).count()
print(f"  HIGH_AMOUNT_THRESHOLD : {50000}")
print(f"  Rows with flag = 1    : {high_amt_count}")
print(f"  Rows with flag = 0    : {clean_count - high_amt_count}")

# COMMAND ----------

# STEP 4 — Add rapid_txn_flag
#          5-minute rolling window per account
#          If count > 3 → flag = 1
from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp

# Convert txn_timestamp to unix seconds for window
df_unix = df_flagged.withColumn(
    "txn_unix",
    unix_timestamp(col("txn_timestamp"))
)

# Window: per account_id, ordered by time
window_spec = Window.partitionBy("account_id").orderBy("txn_unix").rangeBetween(
    -300,   # 5 minutes back
    0
)

# Count transactions in 5-min window
df_rapid = df_unix.withColumn(
    "txn_count_5min",
    count("transaction_id").over(window_spec)
)

# Flag if count > 3
df_rapid = df_rapid.withColumn(
    "rapid_txn_flag",
    when(col("txn_count_5min") > 3, lit(1))
    .otherwise(lit(0))
)

rapid_count = df_rapid.filter(col("rapid_txn_flag") == 1).count()
print(f"  Window     : {300} seconds (5 minutes)")
print(f"  Threshold  : > {3} transactions")
print(f"  Rows flagged as rapid : {rapid_count}")

# COMMAND ----------

# STEP 5 — Join with silver_accounts
#          Get customer_id for each transaction
print("\n" + "─"*60)
print("STEP 5 — Join with silver_accounts → get customer_id")
print("─"*60)

df_accounts = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_accounts/")
print(f"  silver_accounts rows : {df_accounts.count()}")

# Join transactions with accounts on account_id
df_joined_acc = df_rapid.join(
    df_accounts.select("account_id", "customer_id"),
    on="account_id",
    how="left"
)

matched = df_joined_acc.filter(col("customer_id").isNotNull()).count()
unmatched = df_joined_acc.filter(col("customer_id").isNull()).count()
print(f"  Rows matched    : {matched}")
print(f"  Rows unmatched  : {unmatched}  (no account found)")

# COMMAND ----------

# STEP 6 — Join with silver_customers
#          Get home country for each customer
print("\n" + "─"*60)
print("STEP 6 — Join with silver_customers → get home country")
print("─"*60)

df_customers = (
    spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_customer/")
    .filter(col("is_current") == True)
    .select("customer_id", "country")
    .withColumnRenamed("country", "home_country")
)
print(f"  silver_customers current rows : {df_customers.count()}")

# Join on customer_id
df_joined_cust = df_joined_acc.join(
    df_customers,
    on="customer_id",
    how="left"
)

print(f"  Rows after join : {df_joined_cust.count()}")

# COMMAND ----------

# STEP 7 — Add geo_mismatch_flag
#          Map transaction location → country
#          Compare with customer home country
#          1 if different → fraud signal
print("\n" + "─"*60)
print("STEP 7 — Add geo_mismatch_flag")
print("─"*60)
LOCATION_COUNTRY_MAP   = {
    "Mumbai":"India","Delhi":"India",
    "London":"UK","New York": "USA","Toronto":"Canada"
}
print(f"  Location → Country mapping:")
for city, country in LOCATION_COUNTRY_MAP.items():
    print(f"    {city:<12} → {country}")

# Map transaction location to country
# Using when/otherwise chain from LOCATION_COUNTRY_MAP
location_mapping = None
for city, country in LOCATION_COUNTRY_MAP.items():
    if location_mapping is None:
        location_mapping = when(col("location") == city, lit(country))
    else:
        location_mapping = location_mapping.when(col("location") == city, lit(country))

location_mapping = location_mapping.otherwise(lit("Unknown"))

df_geo = df_joined_cust.withColumn(
    "txn_country",
    location_mapping
)

# Add geo_mismatch_flag
# 1 if transaction country != home country
df_geo = df_geo.withColumn(
    "geo_mismatch_flag",
    when(
        (col("txn_country") != col("home_country")) &
        (col("txn_country") != "Unknown") &
        (col("home_country").isNotNull()),
        lit(1)
    ).otherwise(lit(0))
)

geo_count = df_geo.filter(col("geo_mismatch_flag") == 1).count()
print(f"\n  Rows with geo_mismatch = 1 : {geo_count}")
print(f"  Rows with geo_mismatch = 0 : {clean_count - geo_count}")

df_geo.select(
    "transaction_id", "location",
    "txn_country", "home_country", "geo_mismatch_flag"
).show(5, truncate=False)

# COMMAND ----------

 #STEP 8 — Select final columns
#          Write to silver_transactions
#          APPEND partitioned by txn_date
print("\n" + "─"*60)
print("STEP 8 — Write silver_transactions")
print("─"*60)

df_silver_txn = df_geo.select(
    "transaction_id",
    "account_id",
    "customer_id",
    "amount",
    "currency",
    "transaction_type",
    "merchant",
    "location",
    "txn_country",
    "home_country",
    "txn_timestamp",
    "transaction_hour",
    "txn_date",
    "high_amount_flag",
    "rapid_txn_flag",
    "geo_mismatch_flag",
    "created_timestamp",
    "source_table"
)

print(f"  Final columns    : {len(df_silver_txn.columns)}")
print(f"  Final row count  : {df_silver_txn.count()}")
print(f"  Write mode       : APPEND")
print(f"  Partition by     : txn_date")

# Write silver_transactions — APPEND
(df_silver_txn.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("txn_date")
    .save("/Volumes/project/bank/banks/silver/silver_transactions/"))

cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_transactions/").count()
print(f"  silver_transactions written → {cnt} rows")