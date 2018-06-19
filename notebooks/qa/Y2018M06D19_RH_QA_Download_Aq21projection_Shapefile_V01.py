
# coding: utf-8

# In[6]:

""" Download Aqueduct 2.1 projection shapefile and upload to S3 and GCS.
-------------------------------------------------------------------------------

Determine area using geopandas and the Eckert IV projection. Stores output on 
Amazon S3 and Google GCS.

Author: Rutger Hofste
Date: 20180605
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    TESTING (boolean) : Testing mode. Uses a smaller geography if enabled.
    
    SCRIPT_NAME (string) : Script name.
    EE_INPUT_ZONES_PATH (string) : earthengine input path for zones.
    EE_INPUT_VALUES_PATH (string) : earthengine input path for value images.
    INPUT_VERSION_ZONES (integer) : input version for zones images.
    INPUT_VERSION_VALUES (integer) : input version for value images.
    OUTPUT_VERSION (integer) : output version. 
    EXTRA_PROPERTIES (dictionary) : Extra properties to store in the resulting
        pandas dataframe. 
    

Returns:



"""

TESTING = 0
SCRIPT_NAME = "Y2018M06D19_RH_QA_Download_Aq21projection_Shapefile_V01"
OUTPUT_VERSION = 1
OVERWRITE =1 

AQUEDUCT21PROJ_URL = "http://data.wri.org/Aqueduct/web/aqueduct_projections_20150309_shp.zip"
FILE_NAME = "aqueduct21projection"
DETAILED_FILE_NAME = "aqueduct_projections_20150309.shp"
OUTPUT_FILE_NAME = "aqueduct21projection_flux.shp"

ECKERT_IV_PROJ4_STRING = "+proj=eck4 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"

s3_output_path = "s3://wri-projects/Aqueduct30/qaData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 


print("Input : " + AQUEDUCT21PROJ_URL +
      "\nOutput s3: " + s3_output_path + 
      "\nOutput gcp: " + gcs_output_path)


# In[4]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[7]:

import subprocess
import geopandas as gpd


# In[8]:

if OVERWRITE:
    get_ipython().system('rm -r {ec2_input_path}')
    get_ipython().system('rm -r {ec2_output_path}')
    get_ipython().system('mkdir -p {ec2_input_path}')
    get_ipython().system('mkdir -p {ec2_output_path}')


# In[10]:

command = "wget -O {}/{}.zip {}".format(ec2_input_path,FILE_NAME,AQUEDUCT21PROJ_URL)
subprocess.check_output(command,shell=True)


# In[11]:

command = "unzip {}/{} -d {}".format(ec2_input_path,FILE_NAME,ec2_input_path)
subprocess.check_output(command,shell=True)


# In[12]:

input_file_path = "{}/{}".format(ec2_input_path,DETAILED_FILE_NAME)


# In[13]:

gdf = gpd.read_file(input_file_path)


# In[14]:

gdf.crs


# In[15]:

gdf_eckert4 = gdf.to_crs(ECKERT_IV_PROJ4_STRING)


# In[16]:

gdf["area_m2"] = gdf_eckert4.geometry.area


# In[17]:

output_file_path = "{}/{}".format(ec2_output_path,OUTPUT_FILE_NAME)


# In[18]:

gdf.to_file(driver = 'ESRI Shapefile',filename=output_file_path)


# In[19]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[21]:

get_ipython().system('gsutil -m cp {ec2_output_path}/* {gcs_output_path}')


# In[20]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:04:53.090698
# 

# In[ ]:



