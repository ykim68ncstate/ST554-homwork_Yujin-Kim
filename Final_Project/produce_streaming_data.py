#Produce Data

#import library
import pandas as pd
import time
import os



#read the streaming source data
streaming_data = pd.read_csv("power_streaming_data.csv")

#folder whatch
output_folder = "stream_folder"

#Make sure the output folder exists
os.makedirs(output_folder, exist_ok = True)


#Instruction: Writes a loop (say 20 iterations) to:
for i in range(20):
    #Instruction: Randomly sample five rows and output those to a .csv file in the folder you are watching with your stream.
    sample_df = streaming_data.sample(n = 5)
    
    output_path = os.path.join(output_folder, f"stream_batch_{i+1}.csv")
    
    #Instruction: Be sure not to write out the indices. You can leave the column names as long as you handle that on your stream appropriately
    sample_df.to_csv(output_path, index = False)
    
    print(f"Wrote {output_path}")
    
    #Pause for 10 seconds in between outputting of data sets
    time.sleep(10)