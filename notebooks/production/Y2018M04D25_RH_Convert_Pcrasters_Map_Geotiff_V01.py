
# coding: utf-8

# In[1]:

""" Convert pcrasters to geotiffs.
-------------------------------------------------------------------------------
Convert pcraster created maps to geotiffs including:
    - streamorder


Author: Rutger Hofste
Date: 20180425
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    SCRIPT_NAME (string) : Script Name.
    S3_INPUT_PATH (string) : Amazon S3 input location.
    INPUT_VERSION (integer) : Input version.
    OUTPUT_VERSION (integer) : Output version.
    X_DIMENSION_5MIN (integer) : horizontal or longitudinal dimension of 
        raster.
    Y_DIMENSION_5MIN (integer) : vertical or latitudinal dimension of 
        raster.
    RENAME_DICT (dictionary) : Old and new names.    
"""

SCRIPT_NAME = "Y2018M04D25_RH_Convert_Pcrasters_Map_Geotiff_V01"
PREVIOUS_SCRIPT_NAME = "Y2018M04D24_RH_Create_Additional_Pcrasters_V01"
INPUT_VERSION = 3
OUTPUT_VERSION = 2

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160

# ETL
ec2_input_path = "/volumes/data/{}/input_V{:02.0f}/".format(SCRIPT_NAME,INPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_input_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(PREVIOUS_SCRIPT_NAME,INPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)


print("Input ec2: " + ec2_input_path +
      "\nOutput ec2: " + ec2_output_path +
      "\nInput S3: " + s3_input_path +
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

# imports
import os
import numpy as np
from osgeo import gdal
import aqueduct3


# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

get_ipython().system('aws s3 cp {s3_input_path} {ec2_input_path} --recursive --exclude="*" --include="*.map"')


# In[6]:

get_ipython().system('ls {ec2_input_path}')


# In[7]:

default_geotransform, default_geoprojection = aqueduct3.get_global_georeference(np.ones([Y_DIMENSION_5MIN,X_DIMENSION_5MIN]))
for root, dirs, file_names in os.walk(ec2_input_path):
    for file_name in file_names:
        print(file_name)
        base , extension = file_name.split(".")
        output_path = ec2_output_path  + base + "_V{:02.0f}.tif".format(OUTPUT_VERSION)
        input_path = os.path.join(root, file_name)     
        xsize,ysize,geotransform,geoproj,Z = aqueduct3.read_gdal_file(input_path)
        Z[Z<-9990]= -9999
        Z[Z>1e19] = -9999
        
        if file_name == "global_streamorder_dimensionless_05min.map":
            nodata_value= -9999
            datatype=gdal.GDT_Int32 # Could probably use byte type as well.
        else:            
            nodata_value=-9999
            datatype=gdal.GDT_Float32
        aqueduct3.write_geotiff(output_path,default_geotransform,default_geoprojection,Z,nodata_value,datatype)


# In[8]:

get_ipython().system('gsutil -m cp {ec2_output_path}/*.tif {gcs_output_path}')


# In[9]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[10]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:19.294955
# 
# 

# In[ ]:



