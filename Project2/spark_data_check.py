######.py file: Define Class####################
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck: #Define class
    def __init__(self, dataframe): ##create an init function
        self.df = dataframe #create a .df(dataframe) attribute

    ######Add 2 Class method######        
    @classmethod
    def from_csv(cls, spark, path): #creates an instance while reading in a csv file
        df = spark.read.load(path, format="csv", header=True, inferSchema=True) #inferSchema: prevent reading as string
        return cls(df) #class return
    
    @classmethod
    def from_pandas(cls, spark, pandas_df): #creates an instance from a pandas dataframe
        df = spark.createDataFrame(pandas_df)
        return cls(df) #class retrun
    
    ########Create validation method#####
    def check_numeric_range(self, column, lower=None, upper=None): #create a method; supply a single column and a lower and upper value (if not provided, don't check that side)
        dtype_dict = dict(self.df.dtypes)
        numeric_type = ["float", "int", "longint", "bigint", "double", "integer"] #numeric column data type
        
        if column not in dtype_dict:    #if user input non-exist column; prevent error
            print(f"Column '{column}' does not exist.") #print message
            return self      
        
        if dtype_dict[column] not in numeric_type:   #if user supply a non-numeric column
            print(f"Column '{column}' is not numeric.")
            return self   #return the df without modification
        
        ###Check that at least one of lower or upper is provided
        if lower is None and upper is None:
            print("At least one of lower or upper must be provided.")
            return self
        
        new_col_name = f"{column}_in_range" #retruns the dataframe with an appended column of Boolean values
        
        #If not provided, don't check that side
        #for any NULL values, return NULL
        if lower is not None and upper is not None:
            condition = F.when(F.col(column).isNull(), F.lit(None)).otherwise(F.col(column).between(lower, upper)) #between(lower, upper) is inclusive
        elif lower is not None:
            condition = F.when(F.col(column).isNull(), F.lit(None)).otherwise(F.col(column) >= lower)
        else:
            condition = F.when(F.col(column).isNull(), F.lit(None)).otherwise(F.col(column) <= upper)
        
        self.df = self.df.withColumn(new_col_name, condition) #withColumn(); for append new Boolean column
        return self
    
    ###Create a method that checks if a each value in a column is missing
    def check_missing(self, column):
        dtype_dict = dict(self.df.dtypes)
            
        if column not in dtype_dict:
            print(f"Column '{column}' does not exist.")
            return self
            
        new_col_name = f"{column}_is_null"
            
        self.df = self.df.withColumn(new_col_name, F.col(column).isNull())
            
        return self
    
    #####Create a couple of summarization method####
    def summarize_min_max(self, column = None, groupby = None):
        dtype_dict = dict(self.df.dtypes)
        numeric_types = ["float", "int", "longint", "bigint", "double", "integer"] #numeric column data type
        
        # Case 1: specific column
        if column is not None:  #numeric column supplied by the user
            if column not in dtype_dict: 
                print(f"Column '{column}' does not exist.")
                return None
            
            if dtype_dict[column] not in numeric_types: #check numeric column
                print(f"Column '{column}' is not numeric.") #print message
                return None 
            
            if groupby: #if there are not given column, rouped if appropriate
                result = self.df.groupBy(groupby).agg(F.min(column).alias(f"{column}_min"), F.max(column).alias(f"{column}_max")) #return min and max of every column
            
            else:
                result = self.df.agg(F.min(column).alias(f"{column}_min"), F.max(column).alias(f"{column}_max"))
            return result.toPandas() #return as pandas dataframe
        
        # Case 2: all numeric columns
        else:
            numeric_cols = [col for col, dtype in dtype_dict.items() if dtype in numeric_types]
            # calcuate min/max of each column
            dfs = []
            
            for col in numeric_cols:
                if groupby:
                    temp = self.df.groupBy(groupby).agg(F.min(column).alias(f"{column}_min"), F.max(column).alias(f"{column}_max"))
                else:
                    temp = self.df.agg(F.min(column).alias(f"{column}_min"), F.max(column).alias(f"{column}_max"))
                    
                dfs.append(temp.toPandas())
            #merge
            final_df = reduce(lambda left, right: pd.merge(left, right, how="outer"), dfs)
            
            return final_df
        
        # Create a method to report the counts associated with one or two string columns.
    def summarize_counts(self, column1, column2 = None): #Have the function take in two separate arguments for columns, with the second being optional.
        dtype_dict = dict(self.df.dtypes) 
        
        if column1 not in dtype_dict: #prevent error
            print(f"Column '{column1}' does not exist.")
            return None
        
        if dtype_dict[column1] != "string": #check if the columns are string
            print(f"Column '{column1}' is not a string column.")
            return None
        
        if column2 is not None and column2 not in dtype_dict: #Check the column2 is exsit
            print(f"Column '{column2}' does not exist.")
            return None
        
        if column2 is not None and dtype_dict[column2] != "string": #Check the column2 is string
            print(f"Column '{column2}' is not a string column.")
            return None
        
        if column2 is None:
            result = self.df.groupBy(column1).count() #report count if there is one column
            
        else:
            result = self.df.groupBy(column1, column2).count() #report the counts for the combinations of levels of each variable
            
        return result.toPandas() #return pandas dataframe
    