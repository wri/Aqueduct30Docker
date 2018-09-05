
# coding: utf-8

# In[1]:

""" This notebook will download the data from S3 to the EC2 instance 
-------------------------------------------------------------------------------

Groundwater stress, Groundwater table declining trend tabular data will be 
ingested to Google BigQuery.

Author: Rutger Hofste
Date: 20180903
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    INPUT_VERSION (integer) : input version, see readme and output number
                              of previous script.
    OUTPUT_VERSION (integer) : output version for ec2 and s3.
    
    
Returns:

Result:
    Table on Google Bigquery.

"""

SCRIPT_NAME = "Y2018M09D03_RH_GS_Tables_To_BQ_V01"
OUTPUT_VERSION = 1


s3_input_path = "s3://wri-projects/Aqueduct30/rawData/Deltares/groundwater/Final_Oct_2017/data/tables"
ec2_input_path = "/volumes/data/{}/input_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("s3_input_path",s3_input_path,
      "\nec2_input_path",ec2_input_path)



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import os
import pandas as pd


# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_input_path}')


# In[5]:

get_ipython().system('aws s3 cp {s3_input_path} {ec2_input_path} --recursive')


# In[16]:

input_file_names = ["aquifer_table_sorted.txt",
               "hybas_table_sorted.txt",
               "state_table_sorted.txt"]


# In[18]:

for input_file_name in input_file_names:
    input_file_path = os.path.join(ec2_input_path,input_file_name)
    input_file_base_name,input_file_ext  = input_file_name.split(".")
    df = pd.read_csv(input_path,delimiter=";")
    


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



