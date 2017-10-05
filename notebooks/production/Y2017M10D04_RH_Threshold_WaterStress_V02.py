
# coding: utf-8

# # Thresholds WaterStress 
# 
# * Purpose of script: calculate total demand (Dom, IrrLinear, Liv, Ind) and Reduced Runoff and water stress.
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171004

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

S3_INPUT_PATH =  "s3://wri-projects/Aqueduct30/processData/Y2017M09D15_RH_Add_Basin_Data_V01/output/"

INPUT_FILE_NAME = "Y2017M09D15_RH_Add_Basin_Data_V02"

EC2_INPUT_PATH = "/volumes/data/Y2017M10D04_RH_Threshold_WaterStress_V02/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M10D04_RH_Threshold_WaterStress_V02/output"



# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH} ')
get_ipython().system('rm -r {EC2_OUTPUT_PATH} ')


# In[4]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH} ')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH} ')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[ ]:




# In[6]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

