
# coding: utf-8

# In[1]:

""" Copy drought severity geotiffs to S3.
-------------------------------------------------------------------------------

Drought Severity Soil Moisture was provided in .asc format. The files are
converted to geotiff in the main aqueduct branch. See :
https://github.com/wri/Aqueduct30Docker/blob/master/notebooks/production/readme.ipynb
for the full processing steps. 

The file for drought severity soil moisture is:
"global_historical_droughtseveritystandardisedsoilmoisture_reduced_dimensionless_5min_1960_2014.tif"


Author: Rutger Hofste
Date: 20180502
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04


"""

SCRIPT_NAME = "Y2018M06D08_RH_QA_Copy_Drought_Severity_Data_V01"
OUTPUT_VERSION = 1
OVERWRITE =1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V02/output_V07"
s3_output_path = "s3://wri-projects/Aqueduct30/qaData/aqueduct_food/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input s3 : " + S3_INPUT_PATH +
      "\nOutput s3: " + s3_output_path)


# In[2]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {s3_output_path} --recursive ')


# In[4]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

