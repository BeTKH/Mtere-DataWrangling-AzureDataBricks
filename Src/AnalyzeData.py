# Databricks notebook source
# MAGIC %md
# MAGIC # AnalyzeData
# MAGIC
# MAGIC Use the cleaned dataset to answer the following questions.

# COMMAND ----------

# Read your data.

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
your_answer = 1.0 

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

# Save your file.  Specify your container and storage account path in the uri variable.
answer_df.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"Analysis/Answers.csv")
