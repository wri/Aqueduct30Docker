
# coding: utf-8

# In[2]:

""" Ingest aqueduct 2.1 shapefile with fluxes into earthengine.
-------------------------------------------------------------------------------

Manual step since earthengine does not allow fully automatic table
upload (yet).

Input data: wri-projects/Aqueduct2x/Aqueduct21Data/demand
Algorithm used: ArcMap batch copy raster


Author: Rutger Hofste
Date: 20180611
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M06D11_RH_QA_Ingest_Aq21_Flux_Shapefile_v01'
OUTPUT_VERSION = 1

EXTRA_PROPERTIES = {"ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION}

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/qaData/Y2018M06D05_RH_QA_Aqueduct21_Flux_Shapefile_V01/output_V01"


ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input s3: " +  S3_INPUT_PATH+
      "\nOutput ee: "+ ee_output_path)


# In[3]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[4]:

import aqueduct3


# In[5]:

result = aqueduct3.earthengine.create_ee_folder_recursive(ee_output_path,OVERWRITE_OUTPUT)


# In[ ]:



