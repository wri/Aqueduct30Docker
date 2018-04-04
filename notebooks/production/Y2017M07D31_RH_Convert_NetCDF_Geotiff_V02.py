
# coding: utf-8

# In[2]:

""" convert netCDF4 to Geotiff.
-------------------------------------------------------------------------------

Convert individual images from a netCDF to geotiffs. Output is stored in 
Amazon S3 folder and on EC2 


Author: Rutger Hofste
Date: 20180327
Kernel: python36
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    EC2_INPUT_PATH (string) : path to output of previous script. See Readme 
                              for more details. 
    PRINT_METADATA (boolean) : Print out metadata in Jupyter Notebook


Returns:

"""

# Input Parameters

SCRIPT_NAME = "Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02"

EC2_INPUT_PATH = "/volumes/data/Y2017M07D31_RH_download_PCRGlobWB_data_V02/output/"

PRINT_METADATA = False

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160

# Output Parameters


# In[3]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[4]:

# Imports
import aqueduct3

import os
import datetime
import subprocess
import numpy as np
import warnings


# In[5]:

# ETL

ec2_output_path = "/volumes/data/{}/output/".format(SCRIPT_NAME)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output/".format(SCRIPT_NAME)


# In[6]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[7]:

# Assume uniform dimensions specified in input dimensions. 

default_geotransform, default_geoprojection = aqueduct3.get_global_georeference(np.ones([Y_DIMENSION_5MIN,X_DIMENSION_5MIN]))

for root, dirs, file_names in os.walk(EC2_INPUT_PATH):
    for file_name in file_names:
        if file_name.endswith(".nc4") or file_name.endswith(".nc"):
            print(file_name)
            input_path = os.path.join(root, file_name) 
            Z = aqueduct3.netCDF4_to_geotiff(file_name,input_path,ec2_output_path, default_geotransform, default_geoprojection)
           


# In[8]:

files = os.listdir(ec2_output_path)
print("Number of files: " + str(len(files)))


# Some files from Utrecht contain double years, removing the erroneous ones (used Panoply/Qgis to inspect those files):
# 
# global_historical_PDomWN_year_millionm3_5min_1960_2014I055Y1960M01.tif
# global_historical_PDomWN_month_millionm3_5min_1960_2014I660Y1960M01.tif
# global_historical_PDomWN_month_millionm3_5min_1960_2014I661Y1960M01.tif
# 
# 
# 

# In[9]:

get_ipython().system('mkdir /volumes/data/trash')


# In[12]:

get_ipython().system('mv /volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02/output/global_historical_PDomWN_year_millionm3_5min_1960_2014I055Y1960M01.tif /volumes/data/trash/global_historical_PDomWN_year_millionm3_5min_1960_2014I055Y1960M01.tif')
get_ipython().system('mv /volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02/output/global_historical_PDomWN_month_millionm3_5min_1960_2014I660Y1960M01.tif /volumes/data/trash/global_historical_PDomWN_month_millionm3_5min_1960_2014I660Y1960M01.tif')
get_ipython().system('mv /volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02/output/global_historical_PDomWN_month_millionm3_5min_1960_2014I661Y1960M01.tif /volumes/data/trash/global_historical_PDomWN_month_millionm3_5min_1960_2014I661Y1960M01.tif')


# In[11]:

files = os.listdir(ec2_output_path)
print("Number of files: " + str(len(files)))


# In[8]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

