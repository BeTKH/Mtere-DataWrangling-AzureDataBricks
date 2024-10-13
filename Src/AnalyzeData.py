# Databricks notebook source
# MAGIC %md
# MAGIC # AnalyzeData
# MAGIC
# MAGIC Use the cleaned dataset to answer the following questions.

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
# meter_csv = spark.read.csv(uri + "output/CleanMeterData/CSV/part-00000-tid-8348721999135757182-bed5e7a6-8e5a-4f20-bd5a-1036b3b0717b-108-1-c000.csv", header=True, inferSchema=True)

# read parquet 
meter_parquet = spark.read.parquet(uri + "output/CleanMeterData/Parquet/part-00000-tid-8124918960389894414-f0768c86-e91b-4064-931d-a06ea3bcfd48-111-1.c000.snappy.parquet")


print("\nparquet data - with schema:")
display(meter_parquet)


print(f"Total number of rows: {meter_parquet.count()}")

your_answer = meter_parquet.count()
# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 1, lit(your_answer)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 2 - What's the total electrical usage for the day?



# Your code
from pyspark.sql import functions as F



# total electric usage for the day
total_usage_df = meter_parquet.filter(F.col("IntervalValue").isNotNull()).groupBy("Meter Number", "Customer Account Number", "Start Date").agg(F.sum("IntervalValue").alias("TotalUsage"))

# Display the result
display(total_usage_df)


your_answer = 2.0

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 2, lit(your_answer)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 3 - What's the total electrical usage for 'Residental' customers for the day?
# Your code
your_answer = 3.0

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 3, lit(your_answer)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 4 - What's the total electrical usage for hour 7 of the day?
# Your code
your_answer = 4.0

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 4, lit(your_answer)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 5-8 - What are the top 2 meters in terms of usage for the day and how much power did they use?

# top meter interms of usage
# usage for the top meter
# second highest meter in terms of usage
# usage for the second highet meter



# Your code
meter1 = 12345   # May need to convert the meter number from a string
meter1_usage = 6.0
meter2 = 12345
meter2_usage = 8.0

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 5, lit(meter1)).otherwise(answer_df.Answer))
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 6, lit(meter1_usage)).otherwise(answer_df.Answer))
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 7, lit(meter2)).otherwise(answer_df.Answer))
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 8, lit(meter2_usage)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 9-10 - Which hour had the most usage for the day and what was the total electrical usage?
# Your code
hour = 9
hour_usage = 10.0

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 9, lit(hour)).otherwise(answer_df.Answer))
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 10, lit(hour_usage)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 11 - How many meters are in CustMeter.csv dataset that didn't have any valid readings for the day after cleaning the data?

# Your code
total_meters = 11

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 11, lit(total_meters)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 12 - How many Custmer Account Number / Meter Number / Data Type combinations have some data in the cleaned file but not all?
# Your code
total_combos = 12

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 12, lit(total_combos)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data
# MAGIC
# MAGIC - Data set1: Cleaned meter data ( from step-1)
# MAGIC - Data set2: `CustMeter.csv`
# MAGIC   - contains more information about each customer and meter. 
# MAGIC   - A given Customer Account Number can have multiple Meter Numbers. 
# MAGIC   - Each Meter Number may have multiple Data Types.

# COMMAND ----------

# MAGIC %md
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

# COMMAND ----------

storage_end_point = "assign1storebekalue.dfs.core.windows.net" 
my_scope = "MarchMadnessScope"
my_key = "march-madstore-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

uri = "abfss://assign1@assign1storebekalue.dfs.core.windows.net/"


# read csv 
meter_csv = spark.read.csv(uri + "output/CleanMeterData/CSV/part-00000-tid-8348721999135757182-bed5e7a6-8e5a-4f20-bd5a-1036b3b0717b-108-1-c000.csv", 
                                   header=True, inferSchema=True)

# read parquet 
meter_parquet = spark.read.parquet(uri + "output/CleanMeterData/Parquet/part-00000-tid-8124918960389894414-f0768c86-e91b-4064-931d-a06ea3bcfd48-111-1.c000.snappy.parquet")

print("\ncsv data - no schema:")
display(meter_csv.limit(2))
print("\nparquet data - with schema:")
display(meter_parquet.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore dataset2: `CustMeter.csv`

# COMMAND ----------


custMeter_df = spark.read.csv(uri + "InputData/CustMeter.csv", header=True, inferSchema=True)

# columns
all_columns = custMeter_df.columns

print(f"All columns: {all_columns}" )
print(f"Total columns:  {len(custMeter_df.columns):->21}")
print(f"Total rows:  {custMeter_df.count():->24}") 

# data types
print("\nData Types : ")
for col, dType in custMeter_df.dtypes:

  print(f"{col:25}: {dType:->10}")

print("\n")

display(custMeter_df.limit(5))

# COMMAND ----------

# Save your file.  Specify your container and storage account path in the uri variable.
# answer_df.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"Analysis/Answers.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis 
# MAGIC
# MAGIC ### Question 2 - What's the total electrical usage for the day?

# COMMAND ----------

display(meter_parquet.limit(2))

# COMMAND ----------

# how many unique customer accounts
unique_customer_count = meter_parquet.select("Customer Account Number").distinct().count()

# Display the result
print(f"Number of unique customer accounts: {unique_customer_count}")



# how many unique meter  numbers
unique_customer_count = meter_parquet.select("Meter Number").distinct().count()

# Display the result
print(f"Number of unique Meter Numbers: {unique_customer_count}")

print(f"Total number of rows: {meter_parquet.count()}")


# COMMAND ----------

from pyspark.sql import functions as F
# look for speicif customer
specific_meter_number = "11517331"

# Filter the DataFrame by the specific customer Number
subset_df = meter_parquet.filter(F.col("Meter Number") == specific_meter_number)

# Display the first 2 rows of the filtered DataFrame
display(subset_df)


# COMMAND ----------

from pyspark.sql import functions as F



# total electric usage for the day

total_usage_df = meter_parquet.filter(F.col("IntervalValue").isNotNull()).groupBy("Meter Number", "Customer Account Number", "Start Date").agg(F.sum("IntervalValue").alias("TotalUsage"))

# Display the result
display(total_usage_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 3 - What's the total electrical usage for 'Residental' customers for the day?
# MAGIC
# MAGIC - Inner Join 

# COMMAND ----------

# Perform the join between meter_parquet_df and customer_info_df on Customer Account Number and Meter Number
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

# Display the result
display(total_residential_usage_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 4 - What's the total electrical usage for hour 7 of the day?

# COMMAND ----------

from pyspark.sql import functions as F

# Filter for hour 7 and sum the IntervalValue
total_usage_hour_7_df = meter_parquet \
    .filter(F.col("IntervalHour") == 7) \
    .agg(F.sum("IntervalValue").alias("TotalUsageHour7"))

# Display the result
display(total_usage_hour_7_df)


# COMMAND ----------

## aggregate usage for each hour

from pyspark.sql import functions as F

# Group by IntervalHour and aggregate total usage for each hour
total_usage_by_hour_df = meter_parquet.filter(F.col("IntervalValue").isNotNull()) \
    .groupBy("IntervalHour") \
    .agg(F.sum("IntervalValue").alias("TotalUsage"))

# Display the result
display(total_usage_by_hour_df.orderBy("IntervalHour"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 5-8 - What are the top 2 meters in terms of usage for the day and how much power did they use?

# COMMAND ----------

# MAGIC %md
# MAGIC ### aggregate use by meter

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F


# top meters interms of usage (ranked list of top usage meters)
# usage for the top meter (in kw?)
# what is the second highest meter in terms of usage ( meter number)
# What is usage for the second highet meter ( uasge amount in KW?)


# Group by 'Meter Number' and aggregate total usage (sum of 'IntervalValue')
meter_usage_df = meter_parquet.groupBy("Meter Number") \
    .agg(F.sum("IntervalValue").alias("TotalUsage")) \
    .orderBy(F.desc("TotalUsage"))

# Get top 2 meters in terms of usage
top_meters = meter_usage_df.limit(2)

# Rank meters based on total usage
window_spec = Window.orderBy(F.desc("TotalUsage"))
ranked_meter_usage_df = meter_usage_df.withColumn("Rank", F.rank().over(window_spec))

# 5. Top meters in terms of usage - aggregate by eter and rank
print("\n\n\nTop meters interms of usage:")
display(ranked_meter_usage_df)

# 6, 7: Usage for the top meter and second highest meter
top_meter_usage = top_meters.collect()[0]['TotalUsage']
second_meter = top_meters.collect()[1]['Meter Number']
second_meter_usage = top_meters.collect()[1]['TotalUsage']


# Usage for second highest meter 7

# Output results
print(f"Usage for the top meter: {top_meter_usage}")
print(f"Second highest meter: {second_meter}")
print(f"Usage for the second highest meter: {second_meter_usage}")


# COMMAND ----------

# look at that speicific meter
specific_meter_number = "11517331"

# Filter by meter num
subset_df = meter_parquet.filter(F.col("Meter Number") == specific_meter_number)

display(subset_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 9. Which hour had the most usage for the day and what was the total electrical usage?
# MAGIC

# COMMAND ----------

# 9 Which hour had the most usage for the day 
# 10 what was the total electrical usage?

# COMMAND ----------

# MAGIC %md
# MAGIC #### 11. How many meters are in CustMeter.csv dataset that didn't have any valid readings for the day after cleaning the data?
# MAGIC
# MAGIC
# MAGIC -outre join

# COMMAND ----------

# Question 11 - How many meters are in CustMeter.csv dataset that didn't have any valid readings for the day after cleaning the data?

from pyspark.sql import functions as F

# Load CustMeter.csv dataset
cust_meter_df = spark.read.csv(uri + "InputData/CustMeter.csv", header=True, inferSchema=True)

# Clean the data by removing invalid readings (e.g., null or zero IntervalValue)
cleaned_df = meter_parquet.filter(F.col("IntervalValue").isNotNull() & (F.col("IntervalValue") > 0))

# Identify meters that didn't have any valid readings by using an anti-join
meters_with_no_valid_readings = cust_meter_df.alias("cm").join(
    cleaned_df.alias("mp"),
    on=cust_meter_df["Meter Number"] == cleaned_df["Meter Number"],
    how="leftanti"  # This gets all the meters that don't have any matching valid readings
)

# Count the number of meters with no valid readings
num_meters_no_valid_readings = meters_with_no_valid_readings.count()

# Output the result
print(f"Number of meters with no valid readings: {num_meters_no_valid_readings}")
print("The record:")
display(meters_with_no_valid_readings)




# COMMAND ----------

# MAGIC %md
# MAGIC #### 12- How many Custmer Account Number / Meter Number / Data Type combinations have some data in the cleaned file but not all?

# COMMAND ----------

# Question 12 - How many Custmer Account Number / Meter Number / Data Type combinations have some data in the cleaned file but not all?


from pyspark.sql import functions as F

# Load the cleaned meter data (this assumes you've already cleaned your data)
# cleaned_df = ... (your existing cleaned DataFrame)

# Group by Customer Account Number, Meter Number, and Data Type, and count valid readings
grouped_df = cleaned_df.groupBy(
    "Customer Account Number",
    "Meter Number",
    "Data Type"
).agg(F.count("IntervalValue").alias("ValidReadingsCount"))

# Count the total expected readings (assuming you expect 24 hourly readings for the day)
total_expected_readings = 24

# Filter combinations with some but not all valid readings
partial_data_combinations = grouped_df.filter(
    (F.col("ValidReadingsCount") > 0) & (F.col("ValidReadingsCount") < total_expected_readings)
)

# Count the number of such combinations
num_partial_data_combinations = partial_data_combinations.count()

# Output the result
print(f"Number of combinations with some but not all data: {num_partial_data_combinations}")


display(partial_data_combinations)

