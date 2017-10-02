
# coding: utf-8

# # Calculate Water Stress from dataframe
# 
# * Purpose of script: calculate total demand (Dom, IrrLinear, Liv, Ind) and Reduced Runoff and water stress.
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171002

# In[2]:

import time
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
print(dateString,timeString)


# In[3]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M09D15_RH_Add_Basin_Data_V01/output/"

EC2_INPUT_PATH = "/volumes/data/Y2017M10D02_RH_Calculate_Water_Stress_V01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M10D02_RH_Calculate_Water_Stress_V01/output"

INPUT_FILENAME


# In[ ]:



