
# coding: utf-8

# In[1]:

""" Ingest rasterized indicators in earthengine. 
-------------------------------------------------------------------------------

Ingest rasterized aqueduct 30 indicators to earthengine.

Author: Rutger Hofste
Date: 20190111
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0
SCRIPT_NAME = "Y2019M01D10_RH_GA_Rasterize_Indicators_EE_V01"
OUTPUT_VERSION = 1

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2019M01D10_RH_GA_Rasterize_Indicators_GDAL_V01/output_V02/"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("GCS_INPUT_PATH: " + GCS_INPUT_PATH +
      "\nee_output_path: " + ee_output_path )


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

#imports
import subprocess
import datetime
import os
import time
import re
import pandas as pd
from datetime import timedelta
import aqueduct3


# In[4]:

command = "earthengine create folder projects/WRI-Aquaduct/{}".format(SCRIPT_NAME)
response = subprocess.check_output(command,shell=True)


# In[5]:

command = "earthengine create folder projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
response = subprocess.check_output(command,shell=True)


# In[6]:

indicators = ["bws","bwd","iav","sev","gtd","drr","rfr","cfr","ucw","cep","udw","usa","rri"]


# In[7]:

for indicator in indicators:
    print(indicator)
    source_path = "{}{}.tif".format(GCS_INPUT_PATH,indicator)
    destination_path = "{}/{}".format(ee_output_path,indicator)
    command = "/opt/anaconda3/envs/python35/bin/earthengine upload image --asset_id={} {} --nodata_value=-9999".format(destination_path,source_path)
    response = subprocess.check_output(command,shell=True)
    


# In[8]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:09.489050
# 
# 

# In[ ]:



