# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from delta.tables import DeltaTable

df_staging = spark.read.format("delta").load("/Volumes/project/bank/banks/bronze/customers_bronze/")
print(f"  Staging rows : {df_staging.count()}")
#we are preparing scd type2 columns
#effective_start-when record becomes active
#effective_end-when record expires
#is_current-current active record
df_first = (
    df_staging
    .withColumn("effective_start", current_timestamp())
    .withColumn("effective_end",   lit(None).cast("timestamp"))
    .withColumn("is_current",      lit(True))
)
#Checks if Silver Delta table already exists at path
if not DeltaTable.isDeltaTable(spark, "/Volumes/project/bank/banks/silver/silver_customer/"):
#CASE 1: First Load (Initial Load)
    print("\nStep 3A — Initial load...")
    (df_first.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("/Volumes/project/bank/banks/silver/silver_customer/"))
    print("Initial load complete")
#CASE 2: Incremental Load (CDC Logic)
else:
    print("\nStep 3B — Incremental CDC MERGE...")

    silver_table = DeltaTable.forPath(spark,"/Volumes/project/bank/banks/silver/silver_customer")  

    # Expire changed rows
    print("\nMerge Step A — Expiring changed rows...")

    (silver_table.alias("silver")
        .merge(
            df_staging.alias("staging"),
            "silver.customer_id = staging.customer_id AND silver.is_current = true"
        )#Expire Changed Rows (SCD Type 2)
        .whenMatchedUpdate(
            condition="silver.customer_type <> staging.customer_type",
            set={
                "is_current": lit(False),
                "effective_end": current_timestamp()
            }
        )
        .execute()
    )

    print("Expired changed rows")

    # ✅ STEP B — Insert new rows

    df_silver_current = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_customer/")

    df_expired = (
        df_silver_current
        .filter(col("is_current") == False)
        .select("customer_id")
    )
#Finds records that were just expired
    df_changed = df_staging.join(df_expired, "customer_id", "inner")

    if df_changed.limit(1).count() > 0:
  #Gets updated/new versions of those customers
        df_new_rows = (
            df_changed
            .withColumn("effective_start", current_timestamp())
            .withColumn("effective_end", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
        )

        (df_new_rows.write
            .format("delta")
            .mode("append")
            .save("/Volumes/project/bank/banks/silver/silver_customer/"))

        print(f"Inserted {df_new_rows.count()} new rows")

    else:
        print("No changes detected")


# COMMAND ----------

# STEP 4 — Verify
print("\nStep 4 — Verifying silver_customers...")

df_silver = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_customer/")
total   = df_silver.count()
current = df_silver.filter(col("is_current") == True).count()
expired = df_silver.filter(col("is_current") == False).count()

print(f"  Total rows    : {total}")
print(f"  Current rows  : {current}  ← should equal 100")
print(f"  Expired rows  : {expired}")

df_silver.filter(col("is_current") == True).show(3, truncate=False)



# COMMAND ----------

df_accounts = spark.read.format("delta").load("/Volumes/project/bank/banks/bronze/accounts_bronze/")
print(f"  Staging rows : {df_accounts.count()}")

# COMMAND ----------

# STEP 2 — Check exists
print("\nStep 2 — Checking if silver_accounts exists...")
silver_acc_exists = DeltaTable.isDeltaTable(spark, "/Volumes/project/bank/banks/silver/silver_accounts/")
print(f"  Exists : {silver_acc_exists}")

# COMMAND ----------

df_accounts = df_accounts.dropDuplicates(["account_id"])

# COMMAND ----------

# STEP 3 — FIRST LOAD
if not silver_acc_exists:
    print("\nStep 3 — First load!")

    (df_accounts.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save("/Volumes/project/bank/banks/silver/silver_accounts/"))
    cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_accounts/").count()
    print(f" First load → {cnt} rows")
    # STEP 3 — INCREMENTAL (SCD Type 1 MERGE)
else:
    print("\nStep 3 — SCD Type 1 MERGE...")

    silver_acc = DeltaTable.forPath(spark, "/Volumes/project/bank/banks/silver/silver_accounts/")

    # SCD Type 1:
    # condition inside whenMatchedUpdateAll NOT needed
    # WHEN MATCHED   → UPDATE ALL fields
    # WHEN NOT MATCHED → INSERT
    (silver_acc.alias("silver")
        .merge(
            df_accounts.alias("staging"),
            "silver.account_id = staging.account_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    cnt = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_accounts/").count()
    print(f" MERGE complete → {cnt} rows")

# COMMAND ----------

# STEP 4 — Verify
print("\nStep 4 — Verifying silver_accounts...")
df_silver_acc = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_accounts/")
print(f"  Total rows : {df_silver_acc.count()}  ← should equal 150")
df_silver_acc.show(3, truncate=False)


# COMMAND ----------

print("[6]  OPTIMIZE + ZORDER Silver Tables")
 
for path, zcols in [
    ("/Volumes/project/bank/banks/silver/silver_customer/" ,      "customer_id, is_current"),
    ("/Volumes/project/bank/banks/silver/silver_accounts/",       "account_id, customer_id")
]:
    try:
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({zcols})")
        print(f"  ✓  OPTIMIZE  →  {path.split('/')[-1]}  (ZORDER: {zcols})")
    except Exception as e:
        print(f"  ⚠  {path.split('/')[-1]}  :  {e}")

# COMMAND ----------

# FINAL SUMMARY
from pyspark.sql.functions import *
print("="*70)
print("  JOB-2 SILVER LAYER — FINAL SUMMARY")
print("="*70)

df_sc = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_customer/")
df_sa = spark.read.format("delta").load("/Volumes/project/bank/banks/silver/silver_accounts/")

total_sc   = df_sc.count()
current_sc = df_sc.filter(col("is_current") == True).count()
expired_sc = df_sc.filter(col("is_current") == False).count()
total_sa   = df_sa.count()

print(f"  {'Table':<20} {'Total':>6}  {'Current':>8}  {'Expired':>8}  Type")
print(f"  {'─'*55}")
print(f"  {'silver_customers':<20} {total_sc:>6}  {current_sc:>8}  {expired_sc:>8}  SCD2")
print(f"  {'silver_accounts':<20} {total_sa:>6}  {'N/A':>8}  {'N/A':>8}  SCD1")
print()

if current_sc == 100:
    print("  silver_customers current rows = 100  PASS")
else:
    print(f" silver_customers current rows = {current_sc}  (expected 100)")

if total_sa == 150:
    print(" silver_accounts total rows    = 150  PASS")
else:
    print(f" silver_accounts total rows    = {total_sa}  (expected 150)")
