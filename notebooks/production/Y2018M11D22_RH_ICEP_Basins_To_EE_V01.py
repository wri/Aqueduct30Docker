
# coding: utf-8

# In[1]:

""" Rasterize and store shape and raste ICEP data in earthengine and S3.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181122
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_
    NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.     

"""

OVERWRITE_OUTPUT = 1

SCRIPT_NAME = "Y2018M11D22_RH_ICEP_Basins_To_EE_V01"
OUTPUT_VERSION = 1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/ICEP"


GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}/".format(SCRIPT_NAME) 

X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600


ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nInput ec2: " + ec2_input_path,
      "\nInput s3 : " + S3_INPUT_PATH,
      "\nOutput S3: " + s3_output_path +
      "\nee_output_path : " + ee_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')
get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --quiet')
get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_output_path} --recursive --quiet')


# In[4]:

import os
import subprocess


# In[5]:

files = os.listdir(ec2_input_path)


# In[6]:

# Rasterize ICEPBasinid's


# In[7]:

files


# In[8]:

source = "{}/icep_results.shp".format(ec2_input_path)


# In[9]:

destination = "{}/icep_icepraw_30s.tif".format(ec2_output_path)


# In[10]:

command = "/opt/anaconda3/envs/python35/bin/gdal_rasterize -a ICEP_raw -ot Float64 -of GTiff -te -180 -90 180 90 -ts {} {} -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6 -l icep_results -a_nodata -9999 {} {}".format(X_DIMENSION_30S,Y_DIMENSION_30S,source,destination)




# In[11]:

subprocess.check_output(command,shell=True)


# In[12]:

get_ipython().system('gsutil -m -q cp -r {ec2_output_path} {GCS_OUTPUT_PATH}')


# In[13]:

# Upload Shapefile to EE


# In[14]:

source = "{}output_V01/icep_results.shp".format(GCS_OUTPUT_PATH)


# In[15]:

destination = "{}/icep_results".format(ee_output_path)


# In[16]:

command = "/opt/anaconda3/bin/earthengine upload table --asset_id={} {}".format(destination,source)


# In[17]:

command


# In[18]:

subprocess.check_output(command,shell=True)


# In[19]:

# Upload geotiff to EE


# In[20]:

source = "{}output_V01/icep_icepraw_30s.tif".format(GCS_OUTPUT_PATH)


# In[21]:

destination = "{}/icep_icepraw_30s".format(ee_output_path)


# In[22]:

command = "/opt/anaconda3/bin/earthengine upload image --asset_id={} {} --nodata_value=-9999 -p script_used={}".format(destination,source,SCRIPT_NAME)


# In[23]:

command


# In[24]:

subprocess.check_output(command,shell=True)


# In[25]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive ')


# In[26]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:40.396891
# 
# 

# In[ ]:



