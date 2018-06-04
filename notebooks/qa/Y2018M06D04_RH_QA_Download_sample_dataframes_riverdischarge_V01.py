
# coding: utf-8

# In[1]:

""" Download pickled dataframes and convert to csv of combined riverdischarge. 
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180604
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

SCRIPT_NAME = "Y2018M06D04_RH_QA_Download_sample_dataframes_riverdischarge_V01"
OUTPUT_VERSION = 1
OVERWRITE =1 

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M05D16_RH_Final_Riverdischarge_30sPfaf06_V01/output_V06 "

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
s3_output_path = "s3://wri-projects/Aqueduct30/qaData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input S3: " + S3_INPUT_PATH,
      "\nOutput s3: " + s3_output_path)


# In[2]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import pandas as pd
import os
pd.set_option('display.max_columns', 500)


# In[4]:

if OVERWRITE:
    get_ipython().system('rm -r {ec2_input_path}')
    get_ipython().system('rm -r {ec2_output_path}')
    get_ipython().system('mkdir -p {ec2_input_path}')
    get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

temporal_resolution = "month"
year = 1970
month = 1


# In[6]:

file_name = "global_historical_combinedriverdischarge_{}_millionm3_30sPfaf06_1960_2014_I*Y{:04.0f}M{:02.0f}.pkl".format(temporal_resolution,year,month)


# In[7]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --exclude "*" --include {file_name}')


# In[8]:

file_names = os.listdir(ec2_input_path)


# In[9]:

for file_name in file_names:
    df = pd.read_pickle(ec2_input_path + "/" + file_name)
    df.to_csv(ec2_output_path+"/"+file_name + ".csv")    


# In[10]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

