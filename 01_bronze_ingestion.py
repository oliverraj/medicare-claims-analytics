# Databricks notebook source
display(dbutils.fs.ls("/Volumes/workspace/default/medicare_raw/")) 

# COMMAND ----------

beneficiary_path = (
    "/Volumes/workspace/default/medicare_raw/"
    "DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"
)

beneficiary_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(beneficiary_path)
)

display(beneficiary_raw.limit(5))

# COMMAND ----------

print("Total rows:", beneficiary_raw.count())
print("Total columns:", len(beneficiary_raw.columns))

beneficiary_raw.printSchema()
print("Total rows:", beneficiary_raw.count())
print("Total columns:", len(beneficiary_raw.columns))

beneficiary_raw.printSchema()

from pyspark.sql import functions as F

beneficiary_bronze = (
    beneficiary_raw
    .withColumn("source_file", F.lit(beneficiary_path))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("data_year", F.lit(2008))
)

# COMMAND ----------

beneficiary_bronze.createOrReplaceTempView("beneficiary_bronze_staging")

spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.bronze_beneficiary_2008
    USING DELTA
    AS SELECT * FROM beneficiary_bronze_staging
""")

# COMMAND ----------

saved_beneficiary = spark.table(
    "workspace.default.bronze_beneficiary_2008"
)

print("Saved rows:", saved_beneficiary.count())
display(saved_beneficiary.limit(5))

# COMMAND ----------

inpatient_path = (
    "/Volumes/workspace/default/medicare_raw/"
    "DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv"
)

inpatient_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(inpatient_path)
)

print("Total rows:", inpatient_raw.count())
print("Total columns:", len(inpatient_raw.columns))

display(inpatient_raw.limit(5))

# COMMAND ----------

inpatient_bronze = (
    inpatient_raw
    .withColumn("source_file", F.lit(inpatient_path))
    .withColumn("ingestion_timestamp", F.current_timestamp())
)

inpatient_bronze.createOrReplaceTempView("inpatient_bronze_staging")

spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.bronze_inpatient
    USING DELTA
    AS SELECT * FROM inpatient_bronze_staging
""")

print(
    "Saved rows:",
    spark.table("workspace.default.bronze_inpatient").count()
)

# COMMAND ----------

saved_beneficiary.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("DESYNPUF_ID").alias("unique_beneficiaries"),
    F.sum(
        F.when(
            F.col("DESYNPUF_ID").isNull()
            | (F.trim(F.col("DESYNPUF_ID")) == ""),
            1
        ).otherwise(0)
    ).alias("missing_ids")
).show()

# COMMAND ----------

inpatient_saved = spark.table("workspace.default.bronze_inpatient")

unmatched = inpatient_saved.join(
    saved_beneficiary.select("DESYNPUF_ID"),
    on="DESYNPUF_ID",
    how="left_anti"
)

print("Inpatient rows without a matching beneficiary:", unmatched.count())

# COMMAND ----------

inpatient_saved.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("CLM_ID").alias("distinct_claim_ids"),
    F.countDistinct(
        "DESYNPUF_ID", "CLM_ID", "SEGMENT"
    ).alias("distinct_member_claim_segments")
).show()

# COMMAND ----------

print("Source CSV rows:", inpatient_raw.count())

print(
    "Saved Bronze rows:",
    spark.table("workspace.default.bronze_inpatient").count()
)

print("DataFrame rows:", inpatient_saved.count())

# COMMAND ----------

(
    inpatient_saved
    .groupBy("DESYNPUF_ID", "CLM_ID")
    .agg(
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("SEGMENT")).alias("segments")
    )
    .filter(F.col("row_count") > 1)
    .show(10, truncate=False)
)

# COMMAND ----------

(
    inpatient_saved
    .groupBy("SEGMENT")
    .count()
    .orderBy("SEGMENT")
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze ingestion checkpoint
# MAGIC
# MAGIC - Beneficiary table: `workspace.default.bronze_beneficiary_2008`
# MAGIC - Inpatient table: `workspace.default.bronze_inpatient`
# MAGIC - Beneficiary rows: 116,352
# MAGIC - Beneficiary IDs: all unique; none missing
# MAGIC - Inpatient rows: 66,773
# MAGIC - Distinct inpatient claim IDs: 66,705
# MAGIC - Segment 1 rows: 66,705
# MAGIC - Segment 2 rows: 68
# MAGIC - Beneficiary + claim + segment combinations: unique
# MAGIC - Inpatient rows without a matching beneficiary: 0
# MAGIC
# MAGIC Raw columns were preserved as strings, with source-file and ingestion metadata.
# MAGIC Claim segments were retained. Payment aggregation rules still require review.
# MAGIC All records are synthetic CMS data.

# COMMAND ----------

