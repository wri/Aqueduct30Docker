
# coding: utf-8

# In[1]:

""" convert netCDF4 to Geotiff.
-------------------------------------------------------------------------------

Convert individual images from a netCDF on EC2 to geotiffs. Output is stored in 
Amazon S3 folder and on EC2 / GCS. 


Author: Rutger Hofste
Date: 20180731
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    PRINT_METADATA (boolean) : Print out metadata in Jupyter Notebook.
    SCRIPT_NAME (string) : Script name.
    PREVIOUS_SCRIPT_NAME (string) : Previous script name used to identify input files.    
    INPUT_VERSION (integer) : Input version.
    OUTPUT_VERSION (integer) : Output version.     
    X_DIMENSION_5MIN (integer) : horizontal or longitudinal dimension of 
                                 raster.
    Y_DIMENSION_5MIN (integer) : vertical or latitudinal dimension of 
                                 raster.
    
    
Returns:

"""


# Input Parameters
PRINT_METADATA = False
SCRIPT_NAME = "Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02"
PREVIOUS_SCRIPT_NAME = "Y2017M07D31_RH_download_PCRGlobWB_data_V02"
INPUT_VERSION = 1
OUTPUT_VERSION = 1
X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160


# ETL
ec2_input_path = "/volumes/data/{}/output_V{:02.0f}/".format(PREVIOUS_SCRIPT_NAME,INPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input ec2: " + ec2_input_path +
      "\nOutput ec2: " + ec2_output_path +
      "\nOutput S3: " + s3_output_path +
      "\nOutput GCS: " +  gcs_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

# Imports
import aqueduct3
import os
import subprocess
import numpy as np
import warnings
import logging


# In[4]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

"""

This cell loops over the images in a netCDF. There are a couple of PCRGlobWB specific properties so
be careful when using with other netCDFs. PCRGLOBWB specific properties include datatype (float32), 
nodata value, time format, minmax value etc. 

"""

default_geotransform, default_geoprojection = aqueduct3.get_global_georeference(np.ones([Y_DIMENSION_5MIN,X_DIMENSION_5MIN]))

for root, dirs, file_names in os.walk(ec2_input_path):
    for file_name in file_names:
        if file_name.endswith(".nc4") or file_name.endswith(".nc"):
            print(file_name)
            input_path = os.path.join(root, file_name) 
            output_path = aqueduct3.netCDF4_to_geotiff(file_name,input_path,ec2_output_path, default_geotransform, default_geoprojection)


# In[ ]:

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

# In[ ]:

get_ipython().system('mkdir /volumes/data/trash')


# In[ ]:

get_ipython().system('mv /volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02/output/global_historical_PDomWN_year_millionm3_5min_1960_2014I055Y1960M01.tif /volumes/data/trash/global_historical_PDomWN_year_millionm3_5min_1960_2014I055Y1960M01.tif')
get_ipython().system('mv /volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02/output/global_historical_PDomWN_month_millionm3_5min_1960_2014I660Y1960M01.tif /volumes/data/trash/global_historical_PDomWN_month_millionm3_5min_1960_2014I660Y1960M01.tif')
get_ipython().system('mv /volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02/output/global_historical_PDomWN_month_millionm3_5min_1960_2014I661Y1960M01.tif /volumes/data/trash/global_historical_PDomWN_month_millionm3_5min_1960_2014I661Y1960M01.tif')


# In[ ]:

files = os.listdir(ec2_output_path)
print("Number of files: " + str(len(files)))


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

get_ipython().system('gsutil -m cp {ec2_output_path}*.tif {gcs_output_path}')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:

Previous runs:    

