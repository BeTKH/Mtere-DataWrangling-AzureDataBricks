# Databricks notebook source
# MAGIC %md
# MAGIC # CleanMeterData
# MAGIC
# MAGIC Use this notebook to write the code required to clean the data as described in the ReadMe.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instructions (Assignment Part 1 - Clean data)
# MAGIC
# MAGIC
# MAGIC **upload data to azure storage account**
# MAGIC
# MAGIC Upload \InputData\DailyMeterData.dat in the repo to your Azure storage account.
# MAGIC
# MAGIC Write the PySpark code using the Databricks notebook named 'CleanMeterData.py' that will take the input data and transform it to a cleaned result data with the following characteristics:
# MAGIC
# MAGIC **Conversion from wide format to long format**
# MAGIC
# MAGIC - Each long format row will contain the columns not associated with the meter readings (all columns up to QC#1), 
# MAGIC - a single hour (integer from 1-24), and associated QC code and value. 
# MAGIC
# MAGIC **names of columns**
# MAGIC - The names of the new columns should be 'IntervalHour', 'QCCode', and 'IntervalValue'. 
# MAGIC - To be clear, each input row of 24 hours should be converted to `24 separate rows`.
# MAGIC
# MAGIC **what to keep:**
# MAGIC - Retain readings with "Data Type" values of "KWH", "UNITS", "Signed Net in Watts", and "Fwd Consumption in Watts". 
# MAGIC
# MAGIC **what to remove:**
# MAGIC - Other data types should be `removed`. 
# MAGIC - Note - don't read too much into what these mean; all retained readings should be forward energy measured in kWh.
# MAGIC - All bad readings (a QC code other that '3') should be deleted. This should include empty values.
# MAGIC - Any duplicate readings should be deleted.
# MAGIC
# MAGIC **Sorting / Ordering:**
# MAGIC - Data should be sorted in ascending customer, meter, datatype, date, and interval hour 
# MAGIC - (note - interval hour should be an integer and the other columns should be strings).
# MAGIC
# MAGIC **Output:**
# MAGIC - The long form cleaned file should be saved in `two formats`:
# MAGIC - A `single CSV` file (coaleased) in `/output/CSV` in the storage account.
# MAGIC - A `single Parquet` file in `/Output/Parquet` in the storage account.
# MAGIC
# MAGIC
# MAGIC **Where to upload:**
# MAGIC The customer data CSV file should be uploaded from the repo to Azure storage targeting the path `/Output/CustomerData`. You will need this in Part 2.
# MAGIC
# MAGIC
# MAGIC **Submissions:** = csv file + Parquet file + Screenshot the storage account + the notebook
# MAGIC
# MAGIC
# MAGIC
# MAGIC - HandIn - Download the `CSV` and `Parquet` files and save in the `\CleanedData` folder of this repo.
# MAGIC - HandIn - `Screenshot the storage account` with the output data files and save in the `\CleanedData` folder of this repo.
# MAGIC - HandIn - Save the `notebook` in the `\Src folder of this repo.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Reading the Data
# MAGIC
# MAGIC - DailyMeterData.dat (first data)
# MAGIC - CustMeter.csv (2nd data)

# COMMAND ----------

storage_end_point = "assign1storebekalue.dfs.core.windows.net" 
my_scope = "MarchMadnessScope"
my_key = "march-madstore-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

uri = "abfss://assign1@assign1storebekalue.dfs.core.windows.net/"


# read data from the Azure Blob storage
meter_readings_df = spark.read.csv(uri + "InputData/DailyMeterData.dat", 
                                   header=True, inferSchema=False)


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

# MAGIC %md
# MAGIC %md
# MAGIC ### Insights:
# MAGIC - All columns (DT, Meter Number, etc.) are read as a single column string field => data in a df has just one column.
# MAGIC - data type of the entire field is string =>  each row in a column is treated as one long string
# MAGIC
# MAGIC ### steps to fix those:
# MAGIC - separate columns using a delemiter `|`
# MAGIC - define schema to handle data types, proper labeling to missing / empty cells in the table
# MAGIC - data type string: 
# MAGIC
# MAGIC   -  for any field which we may not perform calculation e.g. Meter Number, Customer Account, Serial Number
# MAGIC   -  data type ( measurement unit) e.g. KwH is also string 
# MAGIC
# MAGIC - double data type:
# MAGIC   - values that are numerical and need more precison
# MAGIC   - if we use int, we will lose readings
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="../Schema.png" alt="Image Description" width="500" height="500">

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql import functions as F

# Define schema for meter readings with dynamic QC and Interval fields
schema = StructType([
    StructField("DT", StringType(), True),
    StructField("Meter Number", StringType(), True),
    StructField("Customer Account Number", StringType(), True),
    StructField("Serial Number", StringType(), True),
    StructField("Port", IntegerType(), True),
    StructField("Channel", IntegerType(), True),
    StructField("Conversion Factor", DoubleType(), True),
    StructField("Data Type", StringType(), True),
    StructField("Start Date", DateType(), True),
    StructField("Start Time", IntegerType(), True)  # Start Time as IntegerType
] + [
    StructField(f"QC#{i}", DoubleType(), True) for i in range(1, 25)
] + [
    StructField(f"Interval#{i}", DoubleType(), True) for i in range(1, 25)
])

# Load the CSV file into a DataFrame with the schema
meter_readings_df = spark.read.options(delimiter='|', header=True).schema(schema).csv(uri + "InputData/DailyMeterData.dat")



# Display the cleaned DataFrame
display(meter_readings_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Melting 

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, explode, lit, struct
from typing import Iterable

def melt_meter_readings(
    df: DataFrame,
    id_vars: Iterable[str],  # Columns to keep
    interval_hour_col: str,  # Name for the new IntervalHour column
    interval_value_col: str,  # Name for the new IntervalValue column
    qc_code_col: str,        # Name for the new QCCode column
    qcc_value_col: str       # Name for the new QCC_Value column
) -> DataFrame:
    """Melt the DataFrame from wide to long format based on specified columns."""
    
    # Prepare the columns for QC and Interval
    qc_columns = [f"QC#{i}" for i in range(1, 25)]
    interval_columns = [f"Interval#{i}" for i in range(1, 25)]

    # Create an array of structs
    #  each struct contains  hour, QC code, interval value, and QCC value
    stacked_cols = array(*(
        struct(
            col("Start Time").alias(interval_hour_col),  # Use Start Time as IntervalHour
            col(f"Interval#{i}").alias(interval_value_col),  # Interval value for that interval
            lit(f"QC#{i}").alias(qc_code_col),               # QC code (QC#1 to QC#24)
            col(f"QC#{i}").cast(DoubleType()).alias(qcc_value_col)  # QCC value for that QC code
        )
        for i in range(1, 25)  # For intervals 1 to 24
    ))

    # Add the array of structs as a new column and then explode it to create multiple rows
    exploded_df = df.withColumn("exploded_data", explode(stacked_cols))
    
    # Select the original columns + exploded columns for IntervalHour, IntervalValue, QCCode, and QCC_Value
    melted_df = exploded_df.select(
        *id_vars,                                   # Retain the specified original columns
        col("exploded_data." + interval_hour_col),  # Extract interval hour
        col("exploded_data." + interval_value_col),  # Extract interval value
        col("exploded_data." + qc_code_col),          # Extract QC code
        col("exploded_data." + qcc_value_col)         # Extract QCC value
    )
    
    return melted_df

# Columns to keep
retained_columns = [
    "DT",
    "Meter Number",
    "Customer Account Number",
    "Serial Number",
    "Port",
    "Channel",
    "Conversion Factor",
    "Data Type",
    "Start Date"
]

# Apply the melting function to the DataFrame
long_format_df = melt_meter_readings(
    meter_readings_df,
    retained_columns,
    "IntervalHour",     # New column for interval hour
    "IntervalValue",    # New column for interval value
    "QCCode",           # New column for QC code
    "QCC_Value"         # New column for QCC Value
)


display(long_format_df)

# COMMAND ----------

# total number of rows in df
total_rows = long_format_df.count()

print(f"Total number of rows: {total_rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insight
# MAGIC - now the data is in long format 
# MAGIC - we went from 4,644 rows to 111,456 which seems correct because 4,644  * 24 = 111,456
# MAGIC
# MAGIC
# MAGIC ### Subset specific Data Types 
# MAGIC
# MAGIC subset DataTypes of: 
# MAGIC
# MAGIC - `KWH`, 
# MAGIC - `UNITS`, 
# MAGIC - `Signed Net in Watts`, and 
# MAGIC - `Fwd Consumption in Watts` from the list

# COMMAND ----------

from pyspark.sql import functions as F

# filter list
data_types_to_filter = ["KWH", "UNITS", "Signed Net in Watts", "Fwd Consumption in Watts"]

# Filter the df using filter
filtered_df_kws = long_format_df.filter(F.col("Data Type").isin(data_types_to_filter))

display(filtered_df_kws)


# COMMAND ----------

# total number of rows in the df
total_rows_kwh_filtered = filtered_df_kws.count()

print(f"Total number of rows: {total_rows_kwh_filtered}")

# COMMAND ----------

# MAGIC %md
# MAGIC - after susbetting those specicific data types, we have `10,2984` records. 
# MAGIC - Ignored `8,472` records of other data types

# COMMAND ----------

from pyspark.sql import functions as F


def number_of_rows(df_, data_type_filter):

    filtered_df = long_format_df.filter(F.col("Data Type").isin(data_types_to_filter))

    print(f"\nTotal number of rows with data Type {data_type_filter}: {filtered_df.count()}")



number_of_rows(long_format_df, ["KWH"])
number_of_rows(long_format_df, ["UNITS"])
number_of_rows(long_format_df, ["Signed Net in Watts"])
number_of_rows(long_format_df, ["Fwd Consumption in Watts"])

# COMMAND ----------

# MAGIC %md
# MAGIC Filtering data units in either of "KWH", "UNITS", "Signed Net in Watts", "Fwd Consumption in Watts" would give us the same data. Those filters must be the same. 

# COMMAND ----------

# unique Data Type values
unique_data_types = long_format_df.select("Data Type").distinct()

unique_data_types_list = [row["Data Type"] for row in unique_data_types.collect()]

print("\nnumber of unique units:", len(unique_data_types_list), "\n")
# unique Data Type units
for i in unique_data_types_list:
  print(i)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove some records further
# MAGIC
# MAGIC - all bad readings havd   QC != 3, should be removed ( including zero)
# MAGIC - any duplicate reading should be deleted. 
# MAGIC
# MAGIC #### Remove Duplicates & bad reads  with `QC` != `QC#3`

# COMMAND ----------

# Remove records where QCCode != "QC#3"

goodreadings_df = long_format_df.filter(filtered_df_kws.QCCode == "QC#3")

goodreadings_dedup_df = long_format_df.filter(filtered_df_kws.QCCode == "QC#3").dropDuplicates()


goodreadings_df_rows_count = goodreadings_df.count()
print(f"\nTotal number of rows: {goodreadings_df_rows_count} \n")



display(goodreadings_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insights
# MAGIC
# MAGIC - after ignoring all bad readings which are not QC#3, we are down to `4,644` records.
# MAGIC - after removing duplicates, we have now `4,630`. ( there were 14 duplicate records)
# MAGIC - removed `9,8340` records. 
# MAGIC - seems like we are back to the original data length but smaller width
# MAGIC
# MAGIC
# MAGIC ## Ordering the Records 
# MAGIC
# MAGIC - Data should be sorted in ascending customer, meter, datatype, date, and interval hour 
# MAGIC - (note - interval hour should be an integer and the other columns should be strings).

# COMMAND ----------

# Sort by specified columns

order_by_ = ["Customer Account Number", "Meter Number", "Data Type", "Start Date", "IntervalHour"]
ascending_ = [True, True, True, True, True]

sorted_clean_df = goodreadings_df.orderBy(order_by_, ascending=ascending_)
display(sorted_clean_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save the data into storage account
# MAGIC
# MAGIC - two output formats: `csv` and `pqrquet`
# MAGIC - coaleased into single file for each
# MAGIC - output directories `/OutPut/CSV` and `OutPut/Parquet`

# COMMAND ----------

# Save sorted_clean_df as CSV into "output/CleanMeterData/CSV" 
#sorted_clean_df.coalesce(1).write.option('header', True).mode('overwrite').csv(uri + "output/CleanMeterData/CSV")

# Save sorted_clean_df as Parquet into "output/CleanMeterData/Parquet"
#sorted_clean_df.coalesce(1).write.mode('overwrite').parquet(uri + "output/CleanMeterData/Parquet")
