######.py file: Define Class####################
#Instruction: At the top of the file, include the following
from pyspark.sql import DataFrame          
from pyspark.sql import functions as F     
from functools import reduce                
from pyspark.sql.types import *             
import pandas as pd

#Instruction: Start a class called SparkDataCheck
class SparkDataCheck:                
    def __init__(self, dataframe):   #Instruction: Create an __init__ function that takes in `self` and a dataframe argument
        self.df = dataframe          #Instruction: Within this, create `.df` attribute that is the dataframe

    #Instruction: Add two `@classmethods`        
    @classmethod
    def from_csv(cls, spark, path):                                             #Instruction: The method should have arguments for the class, the spark session, and the path to the file
        df = spark.read.load(path, format="csv", header=True, inferSchema=True) #Instruction: You should use the `spark.read.load()` function as we did in our `pyspark` notebook.
        return cls(df)                                                          #Instruction: Create an object of our class that is returned
    
    #Instruction: One that creates an instance from a pandas dataframe (standard pandas)
    @classmethod
    def from_pandas(cls, spark, pandas_df):      #Instruction: The method should have arguments for the class, the spark session, and the pandas dataframe
        df = spark.createDataFrame(pandas_df)    #Instruction: You should use the `spark.CreateDataFrame()` function as we did in our `pyspark` notebook.
        return cls(df)                           #Instruction: Create an object of our class that is returned
    
    
    #Instruction: Now we're going to create a couple of validation methods. >>> it will be use to be "my_object.df.show()"
    def check_numeric_range(self, column, lower=None, upper=None):                #Instruction: The function should allow the user to supply a single column and a lower and upper value.
        
        if lower is None and upper is None:                             #Instruction: Check that at least one of lower or upper is provided (if not provided, don’t check that side).
            print("At least one of lower or upper must be provided.")
            return self
        
        dtype_dict = dict(self.df.dtypes)                                         #Instruction: Check out the .dtypes attribute of the data frame >>> `self.df.dtypes` gives column type info
        numeric_type = ["float", "int", "longint", "bigint", "double", "integer"] #Instruction: numeric column (float, int, longint, bigint, double, or integer)
        
        if column not in dtype_dict:                    #if user input non-exist column; prevent error
            print(f"Column '{column}' does not exist.") #print message
            return self      
        
        if dtype_dict[column] not in numeric_type:        #Instruction: If the user supplies a non-numeric column,
            print(f"Column '{column}' is not numeric.")   #Instruction: print a message
            return self                                   #Instruction: return the df without modification
        
        new_col_name = f"{column}_in_range" #retruns the dataframe with an appended column of Boolean values
        
        if lower is not None and upper is not None:
            condition = F.when(F.col(column).isNull(), F.lit(None)).otherwise(F.col(column).between(lower, upper)) #Instruction: On a column, you can use the `.between()` method
        elif lower is not None:                                
            condition = F.when(F.col(column).isNull(), F.lit(None)).otherwise(F.col(column) >= lower)              #Instruction: For any NULL values, return NULL
        else:
            condition = F.when(F.col(column).isNull(), F.lit(None)).otherwise(F.col(column) <= upper)
        
        self.df = self.df.withColumn(new_col_name, condition) #withColumn(); for append new Boolean column
        return self

############################# Checkpoint: Test Class #1 - `check_numeric_range()` method ######################################################     

    #Instruction: Create a method that checks if each value in a string column falls within a user specified set of levels and returns the dataframe with an appended column of Boolean values.
    def check_string_levels(self, column, levels):                                       
        dtype_dict = dict(self.df.dtypes)
            
        if column not in dtype_dict:                     
            print(f"Column '{column}' does not exist.")
            return self
        
        if dtype_dict[column] != "string":                          #Instruction: If the user supplies a non-string column 
            print(f"Column '{column}' is not a string column.")     #Instruction: print a message and 
            return self                                             #Instruction: return the df without modification
            
        new_col_name = f"{column}_valid"                                                              #Instruction: For any NULL vlaues, return NULL
        
        condition = F.when(F.col(column).isNull(), F.lit(None)).otherwise(F.col(column).isin(levels)) #Instruction: Hint: The .isin() method on a column is useful!
        
        self.df = self.df.withColumn(new_col_name, condition) #retruns the dataframe with an appended column of Boolean values
        return self
    
############################# Checkpoint: Test Class #1 - `check_string_levels()` method ######################################################      

    #Instruction: Create a method that checks if a each value in a column is missing (NULL specifically) and returns the dataframe with an appended column of Boolean values.
    def check_missing(self, column):                                       #Instruction: create a method
        dtype_dict = dict(self.df.dtypes)                                  
        
        if column not in dtype_dict:                                       #Instruction: checks if a each value in a column is missing (NULL specifically)
            print(f"Column '{column}' does not exist.")
            return self                                                    #Instruction: returns the dataframe with an appended column of Boolean values.
        
        new_col_name = f"{column}_is_null"
        
        self.df = self.df.withColumn(new_col_name, F.col(column).isNull()) #Instruction: Hint: The .isNULL() method on a column is useful!
        
        return self

############################# Checkpoint: Test Class #1 - `check_missing()` method ###################################################### 

    #Instruction: We’re going to also create a couple of summarization methods. Each summarization method will return the summarizations of the data as a pandas data frame.
    #Instruction: Create a method to report the min and max of a numeric column supplied by the user. Add an optional grouping variable.
    def summarize_min_max(self, column = None, groupby = None):
        dtype_dict = dict(self.df.dtypes)
        numeric_types = ["float", "int", "longint", "bigint", "double", "integer"] #Instruction: The method should check if the column is numeric.
        
        if column is not None:                                
            if column not in dtype_dict: 
                print(f"Column '{column}' does not exist.")
                return None
            
            if dtype_dict[column] not in numeric_types:        #Instruction: If not, a message should be printed that the column isn’t numeric and 
                print(f"Column '{column}' is not numeric.") 
                return None                                    #Instruction: None should be returned
                
            if groupby:                                        #Instruction: If so, it should report the min and max of the column (grouped if appropriate).
                result = self.df.groupBy(groupby).agg(F.min(column).alias(f"{column}_min"), F.max(column).alias(f"{column}_max")) #groupBy().agg() is min/max of each group
            
            else:
                result = self.df.agg(F.min(column).alias(f"{column}_min"), F.max(column).alias(f"{column}_max")) #.agg() is entie min/max
            return result.toPandas()                                                                             #Instruction: will return the summarizations of the data as a pandas data frame
        
        #Instruction: If no column is supplied, the method should report the min and max of any numeric columns (and produce no messages otherwise)
        else:
            numeric_cols = [col for col, dtype in dtype_dict.items() if dtype in numeric_types]
            dfs = []                      #Instruction: grouped if appropriate
            
            for col in numeric_cols:      #Instruction: For the grouped option with all numeric columns 
                if groupby:
                    temp = self.df.groupBy(groupby).agg(F.min(column).alias(f"{column}_min"), F.max(column).alias(f"{column}_max"))
                else:
                    temp = self.df.agg(F.min(column).alias(f"{column}_min"), F.max(column).alias(f"{column}_max"))
                    
                dfs.append(temp.toPandas())

            final_df = reduce(lambda left, right: pd.merge(left, right, how="outer"), dfs) #Instruction: I used reduce() from functools with pd.merge()
            
            return final_df
        
############################# Checkpoint: Test Class #1 - `summarize_min_max()` method ###################################################### 

    #Instruction: Create a method to report the counts associated with one or two string columns.
    def summarize_counts(self, column1, column2 = None): #Instruction: Have the function take in two separate arguments for columns, with the second being optional and the first required.
        dtype_dict = dict(self.df.dtypes) 
        
        if column1 not in dtype_dict: 
            print(f"Column '{column1}' does not exist.")
            return None
        
        if dtype_dict[column1] != "string":                       #Instruction: The method should check if the column(s) are strings.
            print(f"Column '{column1}' is numeric.")              #Instruction: If not, a message should be printed that the column is numeric.
            return None
        
        if column2 is not None:
            if column2 not in dtype_dict: 
                print(f"Column '{column2}' does not exist.")
                return None
            if dtype_dict[column2] != "string":
                print(f"Column '{column2}' is numeric.")
                return None
        
        if column2 is None:                           #Instruction: If so, it should report the counts for the combinations of levels of each variable or of the single variable.
            result = self.df.groupBy(column1).count() #report count if there is one column
            
        else:
            result = self.df.groupBy(column1, column2).count() #report the counts for the combinations of levels of each variable
            
        return result.toPandas()                               #return pandas dataframe
