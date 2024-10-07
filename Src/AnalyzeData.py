# Databricks notebook source
# MAGIC %md
# MAGIC # AnalyzeData
# MAGIC
# MAGIC Use the cleaned dataset to answer the following questions.

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
your_answer = 4644.0

# Add answer to the answer data frame.  
answer_df = answer_df.withColumn("Answer", when(answer_df.Number == 1, lit(your_answer)).otherwise(answer_df.Answer))
display(answer_df)

# COMMAND ----------

# Question 2 - What's the total electrical usage for the day?
# Your code
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
