# Project 2

## Introduction 
This project focuses on data validation and analysis using PySpark. It is divided into two main parts.
In Part I, I implement a custom class, `SparkDataCheck`, to perform data quality checks and summarization on Spark DataFrames.
In Part II, I analyze NFL quarterback data using both pandas-on-Spark and Spark SQL DataFrame API to compare their workflows and results.

## Part I: Testing SparkDataCheck Class  
   1. Load Library and Class
   2. Preliminary Check: Test Class
   3. Example of Part1 Class
      A. `check_numeric_range()`
      B. `check_string_levels()`
      C. `check_missing()`
      D. `summarize_min_max`
      E. `summarize_counts()`
   4. Read that same data set in using `Pandas`
   5. Example method

## Part II: NFL Data Analysis 
   ### pandas-on-Spark
       1. Read NFL data
       2. Check out the DataFrame
       3. Report all of the column names
       4. QB stats for the seasons 05-23

   ### Spark SQL DataFrame
       1. Read Data
       2. Check column names
       3. QB stats for the seasons 05-23
       4. Comparison between pandas-on-Spark and Spark SQL DataFrames 

 ## Conclusion 
