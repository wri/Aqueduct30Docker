
# coding: utf-8

# # Add upstream, downstream and basin information to the dataframe
# 
# * Purpose of script: add contextual data to the datafram. 
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170915

# In[2]:

import time
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
print(dateString,timeString)


# In[ ]:

S3_INPUT_PATH  = "s3://wri-projects/Aqueduct30/processData/Y2017M09D14_RH_merge_EE_results_V01/output/"

EC2_INPUT_PATH = "/volumes/data/Y2017M09D15_RH_Add_Basin_Data_V01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M09D15_RH_Add_Basin_Data_V01/output"

