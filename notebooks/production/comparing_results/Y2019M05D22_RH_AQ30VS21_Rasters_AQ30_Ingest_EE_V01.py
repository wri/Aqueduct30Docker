
# coding: utf-8

# In[1]:

""" Ingest rasterized AQ30 indicators to earthengine.
-------------------------------------------------------------------------------

Ingest the rasterized tiffs of selected indicators to earthengine.

Author: Rutger Hofste
Date: 20190522
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""
TESTING = 0

SCRIPT_NAME = "Y2019M05D22_RH_AQ30VS21_Rasters_AQ30_Ingest_EE_V01"
OUTPUT_VERSION = 4

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2019M05D21_RH_AQ30VS21_Rasterize_AQ30_EE_V01/output_V07/"

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


# In[4]:

command = "earthengine create folder projects/WRI-Aquaduct/{}".format(SCRIPT_NAME)
response = subprocess.check_output(command,shell=True)


# In[5]:

command = "earthengine create folder projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
response = subprocess.check_output(command,shell=True)


# In[6]:

indicators = ["owr_score",
              "owr_wf",
              "bws_score",
              "bws_cat",
              "bwd_score",
              "bwd_cat",
              "iav_score",
              "iav_cat",
              "sev_score",
              "sev_cat",
              "gtd_score",
              "gtd_cat",
              "rfr_score",
              "rfr_cat",
              "cfr_score",
              "cfr_cat",
              "drr_score",
              "drr_cat",
              "ucw_score",
              "ucw_cat",
              "cep_score",
              "cep_cat",
              "udw_score",
              "udw_cat",
              "usa_score",
              "usa_cat",
              "rri_score",
              "rri_cat"]


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


# Previous run:
# 0:59:39.278812
