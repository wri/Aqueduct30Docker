
# coding: utf-8

# In[ ]:

""" Create rasters for area per pixel at 5min and 30s resolution. 
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180606
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

SCRIPT_NAME = "Y2018M06D06_RH_QA_Sample_Raster_Area_V01"
OUTPUT_VERSION = 1
OVERWRITE =1 

# Nile Delta
XMIN = 28
YMIN = 27
XMAX = 33
YMAX = 32

S3_INPUT_PATH =  "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V02/output_V04"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
s3_output_path = "s3://wri-projects/Aqueduct30/qaData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


