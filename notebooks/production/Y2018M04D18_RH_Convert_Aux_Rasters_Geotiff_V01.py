
# coding: utf-8

# In[1]:

""" Convert auxiliary files like DEM, LDD etc. to geotiff. Store on GCS.
-------------------------------------------------------------------------------

In addition to the time-series and non-time-series data from PCRGLOBWB 
auxilirary data files were shared that contain crucial information such as 
digital elevation model (DEM), local drainage direction (ldd) etc.

The files are renamed, converted to geotiffs and uploaded to GCS.


Author: Rutger Hofste
Date: 20180418
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

SCRIPT_NAME = "Y2018M04D18_RH_Convert_Aux_Rasters_Geotiff_V01"
S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/Utrecht/additionalFiles/flowNetwork/topo_pcrglobwb_05min"
INPUT_VERSION = 1
OUTPUT_VERSION = 6

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160

RENAME_DICT = {
    "accumulated_drainage_area_05min_sqkm.map":
    "global_accumulateddrainagearea_km2_05min.map",
    "cellsize05min.correct.map":
    "global_cellsize_m2_05min.map",
    "gtopo05min.map":
    "global_gtopo_m_05min.map",
    "lddsound_05min.map":
    "global_lddsound_numpad_05min.map",
    "outletendorheicbasins_05min.map":
    "global_outletendorheicbasins_boolean_05min.map"
}

# ETL
ec2_input_path = "/volumes/data/{}/input_V{:02.0f}/".format(SCRIPT_NAME,INPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input s3: " + S3_INPUT_PATH +
      "\nInput ec2: " + ec2_input_path +
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

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --exclude="*" --include="*.map"')


# renaming the files with a structured name using schema: `<geographic_range>_<indicator>_<spatial_resolution>_<unit>.map`
# 
# copy from readme.txt file on S3
# 
# 
# Data received from Rens van Beek on Feb 24 2017. Rutger Hofste
# topo_pcrglobwb_05min.zip  
# 
# |Archive Length |   Date |   Time |    Name | Units | newName | 
# |---:|---:|---:|---:| ---:| ---:|
# |37325056|  02-24-2017| 15:46 |  accumulated_drainage_area_05min_sqkm.map| $$km^2$$ |global_accumulateddrainagearea_km2_05min.map |
# |37325056|  02-24-2017 |15:45 |  cellsize05min.correct.map| $$m^2$$ |global_cellsize_m2_05min.map |
# |37325056| 02-24-2017| 15:44 |  gtopo05min.map| $$m$$ |global_gtopo_km2_05min.map|
# |9331456|  02-24-2017| 15:45 |   lddsound_05min.map| numpad |global_lddsound_numpad_05min.map |
# | -9999 | 05-04-2018 | 13:29 | outletendorheicbasins_05min.map | boolean | global_outletendorheicbasins_boolean_05min.map|
# |121306624| | |               4 files| | |
# 
# All files are 5 arc minute maps in PCRaster format and WGS84 projection (implicit).
# The gtopo05min.map is the DEM from the gtopo30 dataset that we use for downscaling meteo data occasionally. This is consistent with the CRU climate data sets and the hydro1k drainage dataset. Elevation is in **metres**. The cellsize05.correct.map is the surface area of a geographic cell in **m2** per 5 arc minute cell. The lddsound_05min.map is identical to the LDD we sent you earlier with the **8-point pour algorithm**.(numpad e.g. 7 is NW 6 is E etc.) The accumulated_drainage_area_05min_sqkm.map is the accumulated drainage area in **km2** per cell along the LDD.
# 

# In[6]:

get_ipython().system('ls {ec2_input_path}')


# In[7]:

for old_name, new_name in RENAME_DICT.items():
    get_ipython().system('mv {ec2_input_path}{old_name} {ec2_input_path}{new_name}')
    assert len(new_name)<100, "new key should not exceed 100 characters"


# In[8]:

get_ipython().system('ls {ec2_input_path}')


# In[9]:

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
        
        if file_name == "global_lddsound_numpad_05min.map" or file_name == "global_outletendorheicbasins_boolean_05min.map":
            nodata_value= 255
            datatype=gdal.GDT_Int32 # Could probably use byte type as well.
            
        else:            
            nodata_value=-9999
            datatype=gdal.GDT_Float32
        
        
        aqueduct3.write_geotiff(output_path,default_geotransform,default_geoprojection,Z,nodata_value,datatype)         



# In[10]:

get_ipython().system('gsutil -m cp {ec2_output_path}/*.tif {gcs_output_path}')


# In[11]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[12]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:39.741528
