# Databricks notebook source
# MAGIC %md
# MAGIC # instructions (Assignment Part 1 - Clean data)
# MAGIC
# MAGIC
# MAGIC ### upload data to azure storage account
# MAGIC
# MAGIC Upload \InputData\DailyMeterData.dat in the repo to your Azure storage account.
# MAGIC
# MAGIC Write the PySpark code using the Databricks notebook named 'CleanMeterData.py' that will take the input data and transform it to a cleaned result data with the following characteristics:
# MAGIC
# MAGIC ### Conversion from wide format to long format
# MAGIC
# MAGIC - Each long format row will contain the columns not associated with the meter readings (all columns up to QC#1), a single hour (integer from 1-24), and associated QC code and value. 
# MAGIC
# MAGIC ### names of columns
# MAGIC - The names of the new columns should be 'IntervalHour', 'QCCode', and 'IntervalValue'. To be clear, each input row of 24 hours should be converted to `24 separate rows`.
# MAGIC
# MAGIC ### what to keep:
# MAGIC - Retain readings with "Data Type" values of "KWH", "UNITS", "Signed Net in Watts", and "Fwd Consumption in Watts". 
# MAGIC
# MAGIC ### what to remove:
# MAGIC - Other data types should be `removed`. Note - don't read too much into what these mean; all retained readings should be forward energy measured in kWh.
# MAGIC
# MAGIC - All bad readings (a QC code other that '3') should be deleted. This should include empty values.
# MAGIC
# MAGIC - Any duplicate readings should be deleted.
# MAGIC
# MAGIC ### Ordering
# MAGIC - Data should be sorted in ascending customer, meter, datatype, date, and interval hour (note - interval hour should be an integer and the other columns should be strings).
# MAGIC
# MAGIC ### output
# MAGIC - The long form cleaned file should be saved in `two formats`:
# MAGIC
# MAGIC - A `single CSV` file (coaleased) in `/output/CSV` in the storage account.
# MAGIC
# MAGIC - A `single Parquet` file in `/Output/Parquet` in the storage account.
# MAGIC
# MAGIC
# MAGIC ### where to upload
# MAGIC The customer data CSV file should be uploaded from the repo to Azure storage targeting the path `/Output/CustomerData`. You will need this in Part 2.
# MAGIC
# MAGIC
# MAGIC ## Submissions = csv file + Parquet file + Screenshot the storage account + the notebook
# MAGIC HandIn - Download the `CSV` and `Parquet` files and save in the `\CleanedData` folder of this repo.
# MAGIC HandIn - `Screenshot the storage account` with the output data files and save in the `\CleanedData` folder of this repo.
# MAGIC HandIn - Save the `notebook` in the `\Src folder of this repo.

# COMMAND ----------

storage_end_point = "assign1storebekalue.dfs.core.windows.net" 
my_scope = "MarchMadnessScope"
my_key = "march-madstore-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

uri = "abfss://assign1@assign1storebekalue.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the Data
# MAGIC
# MAGIC - DailyMeterData.dat (first data)
# MAGIC - CustMeter.csv (2nd data)

# COMMAND ----------

# read data
# Reading the data from the Azure Blob storage
## instructions (Assignment Part 1 - Clean data)
meter_readings_df = spark.read.csv(uri + "InputData/DailyMeterData.dat", header=True, inferSchema=True)


display(meter_readings_df)

# COMMAND ----------

# columns
all_columns = meter_readings_df.columns

print("\nAll columns", all_columns)


print("\nTotal columns:  ", len(meter_readings_df.columns))
print("\nTotal rows:  ", meter_readings_df.count()) 


# data types
print("\nData Types", meter_readings_df.dtypes)

# COMMAND ----------

custMeter_df = spark.read.csv(uri + "InputData/CustMeter.csv", header=True, inferSchema=True)


display(custMeter_df)

# COMMAND ----------

# columns
all_columns = custMeter_df.columns

print("\nAll columns", all_columns)


print("\nTotal columns:  ", len(custMeter_df.columns))
print("\nTotal rows:  ", custMeter_df.count()) 


# data types
print("\nData Types", custMeter_df.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F

# Define the schema for your data
schema = StructType([
    StructField("DT", StringType(), True),
    StructField("Meter Number", IntegerType(), True),
    StructField("Customer Account Number", StringType(), True),
    StructField("Serial Number", StringType(), True),
    StructField("Port", IntegerType(), True),
    StructField("Channel", IntegerType(), True),
    StructField("Conversion Factor", DoubleType(), True),
    StructField("Data Type", StringType(), True),
    StructField("Start Date", StringType(), True)
] + [
    StructField(f"QC#{i}", DoubleType(), True) for i in range(1, 25)  # Dynamically add QC#1 to QC#24
])

# Read the data into a DataFrame with headers and schema
meter_readings_df = spark.read.options(delimiter='|', header=True).schema(schema).csv(uri + "InputData/DailyMeterData.dat")

# Extract column labels
column_labels = meter_readings_df.columns

# Create a separate DataFrame for column labels
labels_df = spark.createDataFrame(column_labels, StringType()).toDF("ColumnLabels")

# Read records into a new DataFrame
records_df = meter_readings_df

# Display the DataFrames for verification
print("Column Labels DataFrame:")
display(labels_df)

print("Records DataFrame:")
display(records_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Join the two data sets
# MAGIC
# MAGIC - what type of join is appropriate

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## subset dataset

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Create new column ... may be

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter, Aggregate

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Smelting / povoting 
# MAGIC
# MAGIC - convert into long format

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Save the data
# MAGIC
# MAGIC - coalease
# MAGIC - parquet and csv

# COMMAND ----------


