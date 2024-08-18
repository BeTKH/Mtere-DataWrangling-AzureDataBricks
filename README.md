# csci422-datawrangling-assgnmt (60 points total)

This assignment uses PySpark in Azure Databricks to clean a mock up of a real world data format.  
After cleaning, several analysis questions will need to be answered by querying the cleaned dataset.

## About the data
The input dataset is a mockup of data sourced from an electrical meter reading system.  Each row of the data represents a full day of hourly samples for a given meter. In addition to the value of electrical usage in kilowatt-hours for each hour, there is a QC code indicating if the reading is a good read or not. A QC code of '3' represents a good reading; any other value is a bad reading.

The input data format is an ASCII formatted file. The delimiter for each column is '|'. Each row contains information about the customer, the meter, and 48 columns representing the QC code and value for 24 hours of data.

In addition to the meter reading file, there's a data file containing more information about each customer and meter.  This is called 'CustMeter.csv'.

A given Customer Account Number can have multiple Meter Numbers.  Each Meter Number may have multiple Data Types.  

The data is located in this repo in the \InputData folder.

This assignment should use the Azure Databricks environment set up in the previous assignment.  You will need to clone this repo into the Databricks workspace.

## Assignment Part 1 - Clean data (30 points)
Upload \InputData\DailyMeterData.dat in the repo to your Azure storage account.

Write the PySpark code using the Databricks notebook named 'CleanMeterData.py' that will take the input data and transform it to a cleaned result data with the following characteristics:
- Conversion from wide format to long format. Each long format row will contain the columns not associated with the meter readings (all columns up to QC#1), a single hour (integer from 1-24), and associated QC code and value.  The names of the new columns should be 'IntervalHour', 'QCCode', and 'IntervalValue'.  To be clear, each input row of 24 hours should be converted to 24 separate rows.
- Retain readings with "Data Type" values of "KWH", "UNITS", "Signed Net in Watts", and "Fwd Consumption in Watts".  Other data types should be removed.  Note - don't read too much into what these mean; all retained readings should be forward energy measured in kWh.
- All bad readings (a QC code other that '3') should be deleted.  This should include empty values.
- Any duplicate readings should be deleted.
- Data should be sorted in ascending customer, meter, datatype, date, and interval hour (note - interval hour should be an integer and the other columns should be strings).
- The long form cleaned file should be saved in two formats:
    - A single CSV file in /output/CSV in the storage account.
    - A single Parquet file in /Output/Parquet in the storage account.
- The customer data CSV file should be uploaded from the repo to Azure storage targeting the path /Output/CustomerData.  You will need this in Part 2.
- HandIn - Download the CSV and Parquet files and save in the \CleanedData folder of this repo.
- HandIn - Screenshot the storage account with the output data files and save in the \CleanedData folder of this repo.
- HandIn - Save the notebook in the \Src folder of this repo.

## Assignment Part 2 - Analysis (30 points)
In the PySpark notebook named 'AnalyzeData.py' in Src, write the code to answer the questions in the notebook. As you will see, your answers will need to be put into a standardized data frame in the notebook and outputted to a CSV file.  Read the Parquet version of the cleaned data for analysis. Partial credit will be provided for the correct code but a wrong answer due to improper inputs.
- HandIn - Save the notebook in the \Src folder of this repo.
- HandIn - Download the Answers.CSV file that is saved in your storage account and copy it to the Analysis directory.

Remember to commit all repo changes and push the repo!





