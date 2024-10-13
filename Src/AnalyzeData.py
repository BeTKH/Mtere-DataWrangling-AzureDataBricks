# Databricks notebook source
# MAGIC %md
# MAGIC # AnalyzeData
# MAGIC
# MAGIC Use the cleaned dataset to answer the following questions.
# MAGIC
# MAGIC ## Read data
# MAGIC
# MAGIC - Data set1: Cleaned meter data ( from step-1)
# MAGIC - Data set2: `CustMeter.csv`
# MAGIC   - contains more information about each customer and meter. 
# MAGIC   - A given Customer Account Number can have multiple Meter Numbers. 
# MAGIC   - Each Meter Number may have multiple Data Types.
# MAGIC
# MAGIC ### CSV vs Parquet ?
# MAGIC
# MAGIC - Parquet format: 
# MAGIC   - preserves meta-data (`schema information`)
# MAGIC   - binary data
# MAGIC   - used for data analysis   
# MAGIC
# MAGIC
# MAGIC - csv format: 
# MAGIC   - does not retain schema
# MAGIC   - all data types will be downcasted to text type ( shown below)
# MAGIC
# MAGIC
# MAGIC ### Explore datasets:
# MAGIC

# COMMAND ----------

# -------------------------- SET UP -------------------------------------------

storage_end_point = "assign1storebekalue.dfs.core.windows.net" 
my_scope = "MarchMadnessScope"
my_key = "march-madstore-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

uri = "abfss://assign1@assign1storebekalue.dfs.core.windows.net/"

# -----------------------------------------------------------------------------



# customer data
custMeter_df = spark.read.csv(uri + "InputData/CustMeter.csv", header=True, inferSchema=True)



# columns
all_columns = custMeter_df.columns


print("CustMeter.csv : ")
print(f"Total rows:  {custMeter_df.count():->24}") 
for col, dType in custMeter_df.dtypes:

  print(f"{col:25}: {dType:->10}")

print("\n")

display(custMeter_df.limit(5))





# cleaned meter data
meter_parquet = spark.read.parquet(uri + "output/CleanMeterData/Parquet/part-00000-tid-4767130156803915849-ffc53a3e-9e39-485c-9640-5d3d06a24781-186-1.c000.snappy.parquet")



meter_csv = spark.read.csv(uri + "output/CleanMeterData/CSV/part-00000-tid-2212775626371479390-e97d2afe-de1e-483e-b327-c8a671ce6365-182-1-c000.csv")

print("\ncsv data - no schema:")
display(meter_csv.limit(2))
print("\nparquet data - with schema:")
display(meter_parquet.limit(2))

# COMMAND ----------

storage_end_point = "assign1storebekalue.dfs.core.windows.net" 
my_scope = "MarchMadnessScope"
my_key = "march-madstore-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

uri = "abfss://assign1@assign1storebekalue.dfs.core.windows.net/"

# COMMAND ----------

# Generate data frame for the questions and answers.
from pyspark.sql import Row
from pyspark.sql.functions import lit, when, sum

# Specify each row with custom values
rows = [
    Row(Number=1, Question="Rows  in your cleaned dataset: ", Answer=0.0, Points=2),
    Row(Number=2, Question="Total electrical usage for the day: ", Answer=0.0, Points=4),
    Row(Number=3, Question="Total electrical usage for Residental customers for the day: ", Answer=0.0, Points=4),
    Row(Number=4, Question="Total electrical usage in hour 7 of the day: ", Answer=0.0, Points=4),
    Row(Number=5, Question="Top meter in terms of usage: ", Answer=0.0, Points=1),
    Row(Number=6, Question="Usage for top meter: ", Answer=0.0, Points=1),
    Row(Number=7, Question="Second highest meter in terms of usage: ", Answer=0.0, Points=1),
    Row(Number=8, Question="Usage for second highest meter 7", Answer=0.0, Points=1),
    Row(Number=9, Question="Top hour in terms of usage: ", Answer=0.0, Points=2),
    Row(Number=10, Question="Usage in top hour: ", Answer=0.0, Points=2),
    Row(Number=11, Question="Number of meters with no valid readings after cleaning: ", Answer=0.0, Points=4),
    Row(Number=12, Question="Number of Customer Account Number / Meter Number / Data Type combos with some data but not all data after cleaning: ", Answer=0.0, Points=4)
]

# Create a dataframe from the list of rows
answer_df = spark.createDataFrame(rows)

display(answer_df)

# COMMAND ----------

# Question 1 - How many rows are in your cleaned dataset.

# Your code



# read csv 
# meter_csv = spark.read.csv(uri + "output/CleanMeterData/CSV/part-00000-tid-2212775626371479390-e97d2afe-de1e-483e-b327-c8a671ce6365-182-1-c000.csv", header=True, inferSchema=True)

# read parquet 
meter_parquet = spark.read.parquet(uri + "output/CleanMeterData/Parquet/part-00000-tid-4767130156803915849-ffc53a3e-9e39-485c-9640-5d3d06a24781-186-1.c000.snappy.parquet")


#the dataset
print("The dataset - read from parquet - retain schema: ")
display(meter_parquet)

row_count = meter_parquet.count()


print(f"\n\nTotal number of rows: {row_count}")

your_answer = row_count
# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 1, lit(your_answer)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 2 - What's the total electrical usage for the day?



# Your code

from pyspark.sql import functions as F

# Sum the 'IntervalValue' column to get the total usage for the day
total_usage_for_day = meter_parquet.filter(F.col("IntervalValue").isNotNull()) \
                                   .agg(F.sum("IntervalValue").alias("TotalUsage")) \
                                   .collect()[0][0]

# Display the total usage
print(f"Total electrical usage for the day: {total_usage_for_day}")


your_answer = total_usage_for_day

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 2, lit(your_answer)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 3 - What's the total electrical usage for 'Residental' customers for the day?



# Your code

# solution : Inner join 
# Inner join meter_parquet_df and customer_info_df on Customer Account Number and Meter Number


# customer data
custMeter_df = spark.read.csv(uri + "InputData/CustMeter.csv", header=True, inferSchema=True)

# cleaned meter data
meter_parquet = spark.read.parquet(uri + "output/CleanMeterData/Parquet/part-00000-tid-4767130156803915849-ffc53a3e-9e39-485c-9640-5d3d06a24781-186-1.c000.snappy.parquet")




joined_df = meter_parquet.join(
    custMeter_df, 
    (meter_parquet["Customer Account Number"] == custMeter_df["Customer Account Number"]) & 
    (meter_parquet["Meter Number"] == custMeter_df["Meter Number"]),
    "inner"
)

# Filter to include only 'Residential' customers
residential_df = joined_df.filter(joined_df["ServiceType"] == "Residential")

# Calculate the total electrical usage for Residential customers
total_residential_usage_df = residential_df \
    .filter(F.col("IntervalValue").isNotNull()) \
    .agg(F.sum("IntervalValue").alias("TotalResidentialUsage"))


total_residential_usage = total_residential_usage_df.collect()[0][0]

print(f"\n\nTotal Residential usage for the day: {total_residential_usage}")





your_answer = total_residential_usage

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 3, lit(your_answer)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 4 - What's the total electrical usage for hour 7 of the day?




# Your code
# solution - # Aggregate the total usage for hour using IntervalValue == 7 

from pyspark.sql import functions as F


total_usage_hour_7 = meter_parquet \
    .filter(F.col("IntervalHour") == 7) \
    .agg(F.sum("IntervalValue")).collect()[0][0]

# Print the result
print(f"Total usage for hour 7: {total_usage_hour_7}")


your_answer = total_usage_hour_7

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 4, lit(your_answer)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 5-8 - What are the top 2 meters in terms of usage for the day and how much power did they use?

# what is the top meter (meter number)
# usage for the top meter

# what is second highest meter in terms of usage
# usage for the second highet meter



# Your code

from pyspark.sql import functions as F

# Aggregate total usage by 'Meter Number' and get top 2 meters
top_10_meters = (
    meter_parquet
    .groupBy("Meter Number")
    .agg(F.sum("IntervalValue").alias("TotalUsage"))
    .orderBy(F.desc("TotalUsage"))
    .limit(10)
)

display(top_10_meters)

# Collect results for top and second top meters
top_meter = top_10_meters.collect()[0]
second_meter = top_10_meters.collect()[1]

# Output results
print(f"Top meter: {top_meter['Meter Number']} with usage: {top_meter['TotalUsage']}")
print(f"Second highest meter: {second_meter['Meter Number']} with usage: {second_meter['TotalUsage']}")



# a look at the top meter
topMeter_num = "17047518"
topMeter_rec = meter_parquet.filter(F.col("Meter Number") == topMeter_num)
display(topMeter_rec)



meter1 = top_meter['Meter Number']   
meter1_usage = top_meter['TotalUsage']
meter2 = second_meter['Meter Number']
meter2_usage = second_meter['TotalUsage']

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 5, lit(meter1)).otherwise(answer_df.Answer))
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 6, lit(meter1_usage)).otherwise(answer_df.Answer))
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 7, lit(meter2)).otherwise(answer_df.Answer))
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 8, lit(meter2_usage)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 9-10 - Which hour had the most usage for the day and what was the total electrical usage?



# Your code
from pyspark.sql import functions as F

# Aggregate total usage by 'IntervalHour'
hourly_usage_df = (
    meter_parquet
    .groupBy("IntervalHour")
    .agg(F.sum("IntervalValue").alias("TotalUsage"))
    .orderBy(F.desc("TotalUsage"))
)

# Get the hour with the most usage
most_usage_hour = hourly_usage_df.first()

# Extract the hour and total usage
hour_with_most_usage = most_usage_hour['IntervalHour']
total_usage_for_hour = most_usage_hour['TotalUsage']

# Output results
print(f"Hour with the most usage: {hour_with_most_usage} with total electrical usage: {total_usage_for_hour}")





hour = hour_with_most_usage
hour_usage = total_usage_for_hour

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 9, lit(hour)).otherwise(answer_df.Answer))
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 10, lit(hour_usage)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 11 - How many meters are in CustMeter.csv dataset that didn't have any valid readings for the day after cleaning the data?

# Your code


custMeter_df = spark.read.csv(uri + "InputData/CustMeter.csv", header=True, inferSchema=True)
meter_parquet = spark.read.parquet(uri + "output/CleanMeterData/Parquet/part-00000-tid-4767130156803915849-ffc53a3e-9e39-485c-9640-5d3d06a24781-186-1.c000.snappy.parquet")


# Get distinct meter numbers with valid readings 
valid_meters = meter_parquet.select("Meter Number").distinct()

# Get total distinct meters in CustMeter 
total_meters = custMeter_df.select("Meter Number").distinct()

# Find meters without valid readings
meters_without_valid_readings = total_meters.subtract(valid_meters)

# Count number of meters without valid readings
count_meters_without_valid_readings = meters_without_valid_readings.count()

print(f"\n\nNumber of meters without valid readings for the day: {count_meters_without_valid_readings}")
print("\n\nThe list of meters with no valid readings:")


display(meters_without_valid_readings)


total_meters = count_meters_without_valid_readings

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 11, lit(total_meters)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 12 - How many Custmer Account Number / Meter Number / Data Type combinations have some data in the cleaned file but not all?




# Your code


from pyspark.sql import functions as F

# Group by 'Customer Account Number', 'Meter Number', and 'Data Type' and count distinct readings
data_counts_df = (
    meter_parquet
    .groupBy("Customer Account Number", "Meter Number", "Data Type")
    .agg(F.count("IntervalValue").alias("DataCount"))
)


expected_total_readings = 4277  # from the number of rows in the cleaned meter data

# Filter combinations with some but not all readings
incomplete_combinations = data_counts_df.filter((F.col("DataCount") > 0) & (F.col("DataCount") < expected_total_readings))

# Count the unique combinations
count_incomplete_combinations = incomplete_combinations.count()

# Output the result
print(f"\n\nNumber of incomplete combinations: {count_incomplete_combinations}")


display(incomplete_combinations)





total_combos = count_incomplete_combinations

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 12, lit(total_combos)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Save your file.  Specify your container and storage account path in the uri variable.
# answer_df.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"Analysis/Answers.csv")
