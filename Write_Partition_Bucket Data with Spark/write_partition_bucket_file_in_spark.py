# Databricks notebook source
# Read csv data with spark

df = spark.read.format("csv")\
            .option("header", "true")\
            .option("inferschema", "true")\
            .option("mode","PERMISSIVE")\
            .load("/FileStore/tables/employee_write_data.csv")

# COMMAND ----------

# Write csv data with spark

df.write.format("csv")\
        .option("header", "true")\
        .option("mode", "overwrite")\
        .option("path", "/FileStore/tables/csv_write/")\
        .save()

# COMMAND ----------

# Check stored file location
dbutils.fs.ls("/FileStore/tables/csv_write/part-00000-tid-1497463552793362417-6a94b08a-6c46-4b7c-9c8d-5fde5bc2fd06-10-1-c000.csv")

# COMMAND ----------

# Check stored file data
spark.read.format("csv").load("/FileStore/tables/csv_write/part-00000-tid-1497463552793362417-6a94b08a-6c46-4b7c-9c8d-5fde5bc2fd06-10-1-c000.csv").show()

# COMMAND ----------

# Make repartition of file data
df.repartition(3).write.format("csv")\
                        .option("header", "true")\
                        .option("mode", "overwrite")\
                        .option("path", "/FileStore/tables/csv_write_repartition/")\
                        .save()

# COMMAND ----------

# Check stored file location with repartition
dbutils.fs.ls("/FileStore/tables/csv_write_repartition/")

# COMMAND ----------


# Check stored file data with repartition
spark.read.format("csv").option("header", "true").load("/FileStore/tables/csv_write_repartition/part-00000-tid-6119069401746916541-83058fc0-eb34-43c6-b41c-24aa3c4b0fa1-14-1-c000.csv").show()

spark.read.format("csv").option("header", "true").load("/FileStore/tables/csv_write_repartition/part-00001-tid-6119069401746916541-83058fc0-eb34-43c6-b41c-24aa3c4b0fa1-15-1-c000.csv").show()

spark.read.format("csv").option("header", "true").load("/FileStore/tables/csv_write_repartition/part-00002-tid-6119069401746916541-83058fc0-eb34-43c6-b41c-24aa3c4b0fa1-16-1-c000.csv").show()

# COMMAND ----------

# Check the file schema
df.printSchema()

# COMMAND ----------

# Code for removing white space from the corrupted file

from pyspark.sql.functions import trim
from pyspark.sql.functions import regexp_replace

# Remove spaces from column names
for col in df.columns:
    df = df.withColumnRenamed(col, col.replace(" ", ""))

# to clean up whitespace at the start/end of values
for col in df.columns:
    df = df.withColumn(col, trim(df[col]))

# Remove all spaces from column values (not just leading/trailing)
for col in df.columns:
    df = df.withColumn(col, regexp_replace(df[col], " ", ""))

# COMMAND ----------



# Write csv data with spark

df.write.format("csv")\
        .option("header", "true")\
        .option("mode", "overwrite")\
        .option("path", "/FileStore/tables/cleaned_csv_write/")\
        .save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/cleaned_csv_write/")

# COMMAND ----------

# Reading cleaned csv data

df_cleaned = spark.read.format("csv")\
            .option("header", "true")\
            .option("mode", "PERMISSIVE")\
            .option("inferschema", "true")\
            .load("/FileStore/tables/cleaned_csv_write/part-00000-tid-7450304239423440684-22332eb0-9566-4b6b-b080-792f41d43164-8-1-c000.csv").show()

# COMMAND ----------

# Write csv data with spark partitioned by Address

df.write.format("csv")\
        .option("header", "true")\
        .option("mode", "overwrite")\
        .partitionBy("address")\
        .option("path", "/FileStore/tables/partition_by_address/")\
        .save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/partition_by_address/")

# COMMAND ----------

# Write csv data with spark partitioned by Address & Gender

df.write.format("csv")\
        .option("header", "true")\
        .option("mode", "overwrite")\
        .option("path", "/FileStore/tables/partition_by_address_gender/")\
        .partitionBy("address", "gender")\
        .save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/partition_by_address_gender/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/partition_by_address_gender/address=INDIA/")

# COMMAND ----------

# Write csv data with spark bucket by id

df.write.format("csv")\
        .option("header", "true")\
        .option("mode", "overwrite")\
        .option("path", "/FileStore/tables/bucket_by_id/")\
        .bucketBy(3, "id")\
        .saveAsTable("bucket_by_id_table")


# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/bucket_by_id/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/bucket_by_id/part-00000-tid-3420780607978864999-356aeb41-8b9e-4b37-a62c-ea21127628bc-18-1_00000.c000.csv")

# COMMAND ----------

# Printing all the bucket data

df_bucket = spark.read.format("csv")\
            .option("header", "true")\
            .option("mode", "PERMISSIVE")\
            .option("inferschema", "true")\
            .load("/FileStore/tables/bucket_by_id/part-00000-tid-3420780607978864999-356aeb41-8b9e-4b37-a62c-ea21127628bc-18-1_00000.c000.csv").show()

df_bucket = spark.read.format("csv")\
            .option("header", "true")\
            .option("mode", "PERMISSIVE")\
            .option("inferschema", "true")\
            .load("/FileStore/tables/bucket_by_id/part-00000-tid-3420780607978864999-356aeb41-8b9e-4b37-a62c-ea21127628bc-18-2_00001.c000.csv").show()


df_bucket = spark.read.format("csv")\
            .option("header", "true")\
            .option("mode", "PERMISSIVE")\
            .option("inferschema", "true")\
            .load("/FileStore/tables/bucket_by_id/part-00000-tid-3420780607978864999-356aeb41-8b9e-4b37-a62c-ea21127628bc-18-3_00002.c000.csv").show()

# COMMAND ----------

