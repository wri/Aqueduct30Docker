
# coding: utf-8

# In[1]:

""" Create rasterized zones at 30s and 5min resolution.  
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180605
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

SCRIPT_NAME = "Y2018M06D05_RH_QA_Sample_Raster_Hydrobasins_V01"
OUTPUT_VERSION = 1
OVERWRITE =1 

# Nile Delta
XMIN = 28
YMIN = 27
XMAX = 33
YMAX = 32

S3_INPUT_PATH =  "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V02/output_V04"

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

if OVERWRITE:
    get_ipython().system('rm -r {ec2_input_path}')
    get_ipython().system('rm -r {ec2_output_path}')
    get_ipython().system('mkdir -p {ec2_input_path}')
    get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive')


# In[5]:

import subprocess


# In[6]:

file_names = ["hybas_lev06_v1c_merged_fiona_30s_V04.tif",
              "hybas_lev06_v1c_merged_fiona_5min_V04.tif"]


# In[7]:

# Uint will only work for level 6 but not with level 00


# In[ ]:




# In[8]:

for file_name in file_names:
    command = "/opt/anaconda3/envs/python35/bin/gdalwarp -te {} {} {} {} -ot Int32 {}/{} {}/{}".format(XMIN,YMIN,XMAX,YMAX,ec2_input_path,file_name,ec2_output_path,"qa_" + file_name) 
    print(command)
    result = subprocess.check_output(command,shell=True)


# In[9]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[10]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 
# 
