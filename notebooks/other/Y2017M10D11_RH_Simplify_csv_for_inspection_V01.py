
# coding: utf-8

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

INPUT_VERSION = 5
OUTPUT_VERSION = 2

S3_INPUT_PATH =  "s3://wri-projects/Aqueduct30/processData/Y2017M10D04_RH_Threshold_WaterStress_V02/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M10D11_RH_Simplify_csv_for_inspection_V01/output/"

INPUT_FILE_NAME = "Y2017M10D04_RH_Threshold_WaterStress_V%0.2d" %(INPUT_VERSION)
OUTPUT_FILE_NAME = "Y2017M10D11_RH_Simplify_csv_for_inspection_V%0.2d" %(OUTPUT_VERSION)

EC2_INPUT_PATH = "/volumes/data/Y2017M10D11_RH_Simplify_csv_for_inspection_V01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M10D11_RH_Simplify_csv_for_inspection_V01/output"

YEAR = 2014


# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH} ')
get_ipython().system('rm -r {EC2_OUTPUT_PATH} ')


# In[4]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH} ')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH} ')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[6]:

import pandas as pd
import numpy as np
import os
import math


# In[7]:

df = pd.read_pickle(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME+".pkl"))


# In[8]:

dfSimple = df.filter(regex=(".runoff.*|.Supply.*|.supply.*"))


# In[9]:

dfSimple.head()


# In[11]:

dfSimple.to_csv(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+".csv"))


# In[12]:

dfSimple.to_pickle(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+".pkl" ))


# In[13]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[ ]:



