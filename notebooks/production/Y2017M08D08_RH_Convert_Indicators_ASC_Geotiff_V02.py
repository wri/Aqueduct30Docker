
# coding: utf-8

# In[33]:

""" Convert Indicators from ASCII to Geotiff
-------------------------------------------------------------------------------
A couple of indicators are shared in ASCII format. Converting to geotiff and
upload to GCS and AWS.


Author: Rutger Hofste
Date: 20170808
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    S3_INPUT_PATH (string) : S3 input path containing the ascii rasters.  
    GCS_OUTPUT (string) : Google Cloud Storage output namespace.

    X_DIMENSION_5MIN (integer) : horizontal or longitudinal dimension of 
                                 raster.
    Y_DIMENSION_5MIN (integer) : vertical or latitudinal dimension of 
                                 raster.
    



Returns:


"""

# Input Parameters

SCRIPT_NAME = "Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V02"


INPUT_VERSION = 2
S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M07D31_RH_copy_S3raw_s3process_V{:02.0f}/output/".format(INPUT_VERSION)


GCS_OUTPUT = "gs://aqueduct30_v01/{}/".format(SCRIPT_NAME)


X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160

# Output Parameters


# In[4]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[31]:

# imports
import os
import numpy as np
from osgeo import gdal
import aqueduct3


# In[ ]:

# ETL


# In[35]:

ec2_input_path =  "/volumes/data/{}/input/".format(SCRIPT_NAME)
ec2_output_path = "/volumes/data/{}/output/".format(SCRIPT_NAME)

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output/".format(SCRIPT_NAME)


# In[7]:

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[11]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --exclude="*" --include="*.asc"')


# In[32]:

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



# Upload to GCS

# In[34]:

get_ipython().system('gsutil -m cp {ec2_output_path}/*.tif {GCS_OUTPUT}')


# In[36]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[39]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

