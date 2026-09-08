# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit

#Define function
def add_audit_columns(df, table_name):
    return df \
        .withColumn("created_timestamp", current_timestamp())\
        .withColumn("source_table", lit("/Volumes/project/bank/banks/source_data/customers.csv"))

#Read CSV
df_customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/project/bank/banks/source_data/customers.csv")

print(f"Rows read: {df_customers.count()}")

#Add audit columns
df_customers = add_audit_columns(df_customers, "customers.csv")
display(df_customers)
#Write to Delta
df_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/Volumes/project/bank/banks/bronze/customers_bronze/")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

#Define function
def add_audit_columns1(df, table_name):
    return df \
        .withColumn("created_timestamp", current_timestamp())\
        .withColumn("source_table", lit("/Volumes/project/bank/banks/source_data/accounts.csv"))

df_accounts=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/project/bank/banks/source_data/accounts.csv")
print(f"  Rows read from SQL : {df_accounts.count()}")
df_accounts.printSchema()

#Add audit columns
df_accounts = add_audit_columns1(df_accounts, "accounts.csv")
display(df_accounts)
df_accounts.write.format("delta").mode("overwrite")\
    .option("overwriteSchema","true")\
.save("/Volumes/project/bank/banks/bronze/accounts_bronze/")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

#Define function
def add_audit_columns2(df, table_name):
    return df \
        .withColumn("created_timestamp", current_timestamp())\
        .withColumn("source_table", lit("/Volumes/project/bank/banks/source_data/complaints.csv"))

df_complaints=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/project/bank/banks/source_data/complaints.csv")
print(f"  Rows read from SQL : {df_complaints.count()}")
df_complaints.printSchema()
df_complaints = add_audit_columns2(df_complaints, "complaints.csv")
display(df_complaints)
df_complaints.write.format("delta").mode("overwrite").option("overwriteSchema","true").save("/Volumes/project/bank/banks/bronze/complaints_bronze/")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

#Define function
def add_audit_columns3(df, table_name):
    return df \
        .withColumn("created_timestamp", current_timestamp())\
        .withColumn("source_table", lit("/Volumes/project/bank/banks/source_data/credit_history.csv"))
df_credit_history=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/project/bank/banks/source_data/credit_history.csv")
print(f"  Rows read from SQL : {df_credit_history.count()}")
df_credit_history.printSchema()
df_credit_history = add_audit_columns3(df_credit_history, "credit_history.csv")
display(df_credit_history)
df_credit_history.write.format("delta").mode("overwrite").option("overwriteSchema","true").save("/Volumes/project/bank/banks/bronze/credit_history_bronze/")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

#Define function
def add_audit_columns4(df, table_name):
    return df \
        .withColumn("created_timestamp", current_timestamp())\
        .withColumn("source_table", lit("/Volumes/project/bank/banks/source_data/transactions.json"))
df_transaction=spark.read.format("json").option("multiline","true").load("/Volumes/project/bank/banks/source_data/transactions.json")

print(f"  Rows read from SQL : {df_transaction.count()}")
df_transaction.printSchema()
df_transaction= add_audit_columns4(df_transaction, "transaction.csv")
display(df_transaction)
df_transaction.write.format("delta").mode("overwrite").option("overwriteSchema","true").save("/Volumes/project/bank/banks/bronze/transaction_bronze/")