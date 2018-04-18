
# coding: utf-8

# In[1]:

""" Convert Indicators from ASCII to Geotiff
-------------------------------------------------------------------------------
A couple of indicators are shared in ASCII format. Renaming to a uniform naming
convention. Converting to geotiff and upload to GCS and AWS.


Author: Rutger Hofste
Date: 20170808
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name.
    PREVIOUS_SCRIPT_NAME (string) : Previous script name. 
    INPUT_VERSION (integer) : Input version.
    OUTPUT_VERSION (integer) : Output version.  

    X_DIMENSION_5MIN (integer) : horizontal or longitudinal dimension of 
                                 raster.
    Y_DIMENSION_5MIN (integer) : vertical or latitudinal dimension of 
                                 raster.
    RENAME_DICT (dictionary) : Old and new names.

Returns:


"""

# Input Parameters

SCRIPT_NAME = "Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V02"
PREVIOUS_SCRIPT_NAME = "Y2017M07D31_RH_copy_S3raw_s3process_V02"

INPUT_VERSION = 3
OUTPUT_VERSION = 7

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160

RENAME_DICT = {
 "global_droughtseveritystandardisedsoilmoisture_5min_1960-2014.tif":
 "global_historical_droughtseveritystandardisedsoilmoisture_reduced_dimensionless_5min_1960_2014.tif",
 "global_droughtseveritystandardisedstreamflow_5min_1960-2014.tif":
 "global_historical_droughtseveritystandardisedstreamflow_reduced_dimensionless_5min_1960_2014.tif",
 "global_environmentalflows_5min_1960-2014.tif":
 "global_historical_environmentalflows_reduced_m3second_5min_1960_2014.tif",
 "global_interannualvariabilitywatersupply_5min_1960-2014.tif":
 "global_historical_interannualvariabilitywatersupply_reduced_dimensionless_5min_1960_2014.tif",
 "global_q1seasonalvariabilitywatersupply_5min_1960-2014.tif":
 "global_historical_q1seasonalvariabilitywatersupply_reduced_dimensionless_5min_1960-2014.tif",
 "global_q2seasonalvariabilitywatersupply_5min_1960-2014.tif":
 "global_historical_q2seasonalvariabilitywatersupply_reduced_dimensionless_5min_1960-2014.tif",
 "global_q3seasonalvariabilitywatersupply_5min_1960-2014.tif":
 "global_historical_q3seasonalvariabilitywatersupply_reduced_dimensionless_5min_1960-2014.tif",
 "global_q4seasonalvariabilitywatersupply_5min_1960-2014.tif":
 "global_historical_q4seasonalvariabilitywatersupply_reduced_dimensionless_5min_1960-2014.tif"
}


# ETL
ec2_input_path = "/volumes/data/{}/input_V{:02.0f}/".format(SCRIPT_NAME,INPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_input_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(PREVIOUS_SCRIPT_NAME,INPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input s3: " + s3_input_path +
      "\nInput ec2: " + ec2_input_path +
      "\nOutput ec2: " + ec2_output_path +
      "\nOutput s3: " + s3_output_path +
      "\nOutput gcs: " + gcs_output_path )


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

# imports
import os
import numpy as np
from osgeo import gdal
import aqueduct3


# In[4]:

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

get_ipython().system('aws s3 cp {s3_input_path} {ec2_input_path} --recursive --exclude="*" --include="*.asc"')


# In[6]:

get_ipython().system('ls {ec2_input_path}')


# In[7]:

# Convert the ascii files in the ec2_input_directory into geotiffs in the ec2_output_directory

default_geotransform, default_geoprojection = aqueduct3.get_global_georeference(np.ones([Y_DIMENSION_5MIN,X_DIMENSION_5MIN]))
for root, dirs, file_names in os.walk(ec2_input_path):
    for file_name in file_names:
        print(file_name)
        base , extension = file_name.split(".")
        output_path = ec2_output_path  + base + ".tif"
        input_path = os.path.join(root, file_name)     
        xsize,ysize,geotransform,geoproj,Z = aqueduct3.read_gdal_file(input_path)
        Z[Z<-9990]= -9999
        Z[Z>1e19] = -9999
        aqueduct3.write_geotiff(output_path,default_geotransform,default_geoprojection,Z,nodata_value=-9999,datatype=gdal.GDT_Float32)         



# In[8]:

for old_name, new_name in RENAME_DICT.items():
    get_ipython().system('mv {ec2_output_path}{old_name} {ec2_output_path}{new_name}')
    assert len(new_name)<100, "new key should not exceed 100 characters"


# In[9]:

get_ipython().system('gsutil -m cp {ec2_output_path}/*.tif {gcs_output_path}')


# In[10]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:04:07.188513 (manual)  
# 0:01:02.868362 

# In[ ]:



