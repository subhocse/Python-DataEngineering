# Databricks notebook source
from pyspark.sql.types import StringType,StructField,StructType,IntegerType

# COMMAND ----------

emp_schema = StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                        StructField("age", IntegerType(), True),
                        StructField("Salary", IntegerType(), True),
                        StructField("address", StringType(), True),
                        StructField("nominee", StringType(), True),
                        StructField("_corrupt_record", StringType(), True)
                        ])

# COMMAND ----------

emp_df1 = spark.read.format("csv")\
                        .option("header", "true")\
                        .option("inferschema","true")\
                        .option("mode", "PERMISSIVE")\
                        .schema(emp_schema)\
                        .load("/FileStore/tables/emp.csv")
emp_df1.show(truncate=False)

# COMMAND ----------

emp_df1 = spark.read.format("csv")\
                        .option("header", "TRUE")\
                        .option("inferschema","true")\
                        .schema(emp_schema)\
                        .option("badRecordsPath","/FileStore/tables/bad_records")\
                        .load("/FileStore/tables/emp.csv")
emp_df1.show(truncate=False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/bad_records/20250521T160017/bad_records/part-00000-46720f4a-55d2-401f-b79e-aaa2c94dacee

# COMMAND ----------

bad_data_df = spark.read.format("json").load("/FileStore/tables/bad_records/20250521T160017/bad_records/part-00000-46720f4a-55d2-401f-b79e-aaa2c94dacee")

bad_data_df.show(truncate=False)

# COMMAND ----------

