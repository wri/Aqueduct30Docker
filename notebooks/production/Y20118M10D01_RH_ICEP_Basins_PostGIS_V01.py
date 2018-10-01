
# coding: utf-8

# In[1]:

""" Store ICEP data in PostGIS Database.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181001
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
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

SCRIPT_NAME = "Y20118M10D01_RH_ICEP_Basins_PostGIS_V01"
OUTPUT_VERSION = 1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/ICEP"

OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()


ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


print("\nInput ec2: " + ec2_input_path,
      "\nInput s3 : " + S3_INPUT_PATH,
      "\nOutput postGIS table : " + OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[ ]:

if OVERWRITE_INPUT:
    get_ipython().system('rm -r {ec2_input_path}')
    get_ipython().system('mkdir -p {ec2_input_path}')
    get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive')

if OVERWRITE_OUTPUT:
    get_ipython().system('rm -r {ec2_output_path}')
    get_ipython().system('mkdir -p {ec2_output_path}')
    

