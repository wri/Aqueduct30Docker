
# coding: utf-8

# In[1]:

""" Merge WWF's HydroBasins
-------------------------------------------------------------------------------
Copy the relevant files from S3 raw to S3 process.
Merge the shapefiles of level 6 and level 0 using Fiona.
Rasterize shapefiles using gdal_rasterize (CLI)
Uploads to S3 and GCS.

Author: Rutger Hofste
Date: 20170802
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    SCRIPT_NAME (string) : Script name
    S3_INPUT_PATH (string) : Name of script used as input. 
    INPUT_VERSION (integer) : Input version.
    OUTPUT_VERSION (integer) : Output version.
    S3_INPUT_PATH (string) : S3 input path. Hardcoded since most upstream.
    GDAL_RASTERIZE_PATH (string) : GDAL version used.
    X_DIMENSION_5MIN (integer) : horizontal or longitudinal dimension of 
        raster at 5 arcminutes resolution.
    Y_DIMENSION_5MIN (integer) : vertical or latitudinal dimension of 
        raster at 5 arcminutes resolution.
    X_DIMENSION_30S (integer) : horizontal or longitudinal dimension of 
        raster at 30 arcseconds resolution.
    Y_DIMENSION_30S (integer) : vertical or latitudinal dimension of 
        raster at 30 arcseconds resolution.
    SPATIAL_RESOLUTIONS (list) : Spatial Resolutions used for rasterization.
        Supported are '5min' and '30s'. List of strings.
    PFAF_LEVELS (list) : Pfafstetter code used for rasterization.
        Supported are '06' and '00'. List of Strings.
    
"""

# Input Parameters

SCRIPT_NAME = "Y2017M08D02_RH_Merge_HydroBasins_V02"
INPUT_VERSION = 1
OUTPUT_VERSION = 4
S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/WWF/HydroSheds30sComplete/"
EE_OUTPUT_VERSION = 9 
GDAL_RASTERIZE_PATH = "/opt/anaconda3/envs/python35/bin/gdal_rasterize"

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160
X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600

SPATIAL_RESOLUTIONS =  ["5min","30s"]
PFAF_LEVELS = ["06","00"]

# ETL
ec2_input_path = "/volumes/data/{}/input_V{:02.0f}/".format(SCRIPT_NAME,INPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ee_output_path = "projects/WRI-Aquaduct/PCRGlobWB20V{:02.0f}".format(EE_OUTPUT_VERSION)

print("Input s3: " + S3_INPUT_PATH +
      "\nInput ec2: " + ec2_input_path +
      "\nOutput ec2: " + ec2_output_path +
      "\nOutput S3: " + s3_output_path +
      "\nOutput gcs: " +  gcs_output_path+
      "\nOutput ee: " + ee_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# ## Preparation
# 
# make sure you are authorized to use AWS S3, gcs, gdal.
# 
# ## Origin of the WWF data
# 
# The Hydrosheds data has been downloaded from the [WWF Website](http://www.hydrosheds.org/download). A login is required for larger datasets. For Aqueduct we used the Standard version without lakes. Since the download is limited to 5GB we split up the download in two batches:  
# 
# 1. Africa, North American Arctic, Central & South-east Asia, Australia & Oceania, Europe & Middle East
# 1. Greenland, North America & Caribbean, South America, Siberia
# 
# Download URLs (no longer valid)  
# [link1](http://www.hydrosheds.org/tempdownloads/hydrosheds-3926b3742a77b18974ca.zip)  
# [link2](http://www.hydrosheds.org/tempdownloads/hydrosheds-a69872e3f4059aea2434.zip)
# 
# 
# The data was downloaded earlier but replicated here so the latest download data would be 2017/08/03 
# 
# The folders contain all levels but for this phase of Aqueduct we decided to use level 6. More information regarding this decision will be in the methodology document. 
# 
# 
# 

# In[3]:

import os
import fiona
import subprocess
import pandas as pd
import re
import time
from datetime import timedelta


# In[4]:

# functions
def etl():
    """ Downloads and unzips files from S3 to ec2
    """
    get_ipython().system('rm -r {ec2_input_path}')
    get_ipython().system('rm -r {ec2_output_path}')
    get_ipython().system('mkdir -p {ec2_input_path}')
    get_ipython().system('mkdir -p {ec2_output_path}')
    get_ipython().system('aws s3 cp {S3_INPUT_PATH}HydrobasinsStandardAfr-Eu.zip {ec2_input_path}')
    get_ipython().system('aws s3 cp {S3_INPUT_PATH}HydrobasinsStandardGR-SI.zip {ec2_input_path}')
    os.chdir(ec2_input_path)
    get_ipython().system("find . -name '*.zip' -exec unzip {} \\;")

def merge_shapefiles(input_meta_path,input_directory_path,search_string,output_file_path):
    """ Merge shapefiles in directory matching regex to output.
    -------------------------------------------------------------------------------

    Args:
        input_meta_path (string) : input path for sample shapefile with metadata.
            The attribute table and layer name of this shapefile will be used for 
            the result.
        input_directory_path (string) : input path of directory containing the 
            other shapefiles.
        search_string (regex) : end of file matching this string are included in 
            the merged shapefile. 
        output_file_path (string) : Output file path for merged shapefile.

    Returns:


    """
    files = os.listdir(input_directory_path)
    meta = fiona.open(input_meta_path,encoding='UTF-8').meta
    with fiona.open(output_file_path, 'w', **meta) as output:
        for one_file in files:
            if re.search(search_string,one_file):
                print(one_file)
                for features in fiona.open(one_file,encoding='UTF-8'):
                    output.write(features)  

                
def rasterize_shapefiles(pfaf_level,spatial_resolution,ec2_input_path,ec2_output_path,output_version):
    """Rasterize shapefile using GDAL.
    -------------------------------------------------------------------------------
    Geotiffs are stored in the same path as input shapefile
    
    Args:
        pfaf_level (string) : Pfafstetter level. Supported '06' and '00'.
        spatial_resolution (string) : Spatial resolution. Supported '5min' and '30s'
        ec2_input_path (string) : Path with the merged shapefiles. 
        ec2_output_path (string) : Path where geotiffs are stored.
        output_version (integer) : Output version. 
        
    Returns:
        None
    
    """

    print("Rasterizing pfaf_level: {}, spatial resolution: {}".format(pfaf_level,spatial_resolution))
    layer_name = "hybas_lev{}_v1c_merged_fiona_V{:02.0f}".format(pfaf_level,OUTPUT_VERSION)
    input_path = "{}hybas_lev{}_v1c_merged_fiona_V{:02.0f}.shp".format(ec2_output_path,pfaf_level,OUTPUT_VERSION)
    output_path = "{}hybas_lev{}_v1c_merged_fiona_{}_V{:02.0f}.tif".format(ec2_output_path,pfaf_level,spatial_resolution,OUTPUT_VERSION)

    if spatial_resolution == "5min":
        x_dimension = X_DIMENSION_5MIN
        y_dimension = Y_DIMENSION_5MIN
    elif spatial_resolution == "30s":
        x_dimension = X_DIMENSION_30S
        y_dimension = Y_DIMENSION_30S
    else: 
        raise("spatial resolution not accepted")

    if pfaf_level == "06":
        field = "PFAF_ID"
    elif pfaf_level == "00":
        field = "PFAF_12"
    else:
        raise("Pfaf_level not accepted")

    command = "{} -a {} -ot Integer64 -of GTiff -te -180 -90 180 90 -ts {} {} -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6 -l {} -a_nodata -9999 {} {}".format(GDAL_RASTERIZE_PATH,field,x_dimension,y_dimension,layer_name,input_path,output_path)
    print(command)
    response = subprocess.check_output(command,shell=True)


# In[5]:

# ETL
etl()


# In[6]:

# merging shapefiles
for pfaf_level in PFAF_LEVELS:
    command = "find / -name '*lev{}_v1c.zip' -exec unzip -o {{}} \;".format(pfaf_level)
    print(command)
    response = subprocess.check_output(command,shell=True)
    
    input_directory_path = ec2_input_path
    input_meta_path = '{}hybas_ar_lev{}_v1c.shp'.format(ec2_input_path,pfaf_level)
    output_file_path = "{}/hybas_lev{}_v1c_merged_fiona_V{:02.0f}.shp".format(ec2_output_path,pfaf_level,OUTPUT_VERSION)
    search_string = "lev{}_v1c.shp$".format(pfaf_level)
    merge_shapefiles(input_meta_path,input_directory_path,search_string,output_file_path)


# We also like to have rasterized versions of the shapefiles at 5min and 30s resolution (0.0833333 degrees and 0.00833333 degrees).
# Rasterizing on PFAF_ID and PFAF_12
# Layer name hybas_lev00_v1c_merged_fiona_V01
# 

# In[7]:

#Rasterizing Shapefiles
for spatial_resolution in SPATIAL_RESOLUTIONS:
    for pfaf_level in PFAF_LEVELS:
        rasterize_shapefiles(pfaf_level,spatial_resolution,ec2_output_path,ec2_output_path,OUTPUT_VERSION)
        


# In[17]:

get_ipython().system('gsutil -m cp {ec2_output_path}/* {gcs_output_path}')


# In[9]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[10]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:43:42.915611

# In[ ]:



