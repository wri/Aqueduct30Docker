
# coding: utf-8

# In[1]:

""" Merge the difference geotiffs into one.
-------------------------------------------------------------------------------

When using 30 arc seconds in earthengine, the results are exported into 
multiple chunks. This script will merge then back.

When the previous script exports to 5 arc minute (for printing), 
you can ignore this script.

Author: Rutger Hofste
Date: 20190530
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0

SCRIPT_NAME = "Y2019M05D30_RH_AQ30VS21_Merge_Dif_Geotiff_V01"
OUTPUT_VERSION = 1

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2019M05D28_RH_AQ30VS21_Export_Dif_Geotiff_EE_V01/output_V06"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
s3_output_path = "s3://wri-projects/Aqueduct30/Aq30vs21/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

target_filenames = ["owr_score_minus_DEFAULT",
                    "bws_score_minus_BWS_s",
                    "sev_score_minus_SV_s",
                    "iav_score_minus_WSV_s"]


print("Input GCS : " + GCS_INPUT_PATH +
      "\nInput ec2: " + ec2_input_path + 
      "\nOutput ec2: " + ec2_output_path +
      "\nOutput s3: " + ec2_output_path)


# In[2]:

import time, datetime, sys, logging
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


# In[4]:

get_ipython().system('gsutil -m cp {GCS_INPUT_PATH}/* {ec2_input_path}')


# In[5]:

import os
import re
import rasterio
import numpy as np
from rasterio import merge


# In[6]:

files = os.listdir(ec2_input_path)


# In[7]:

files


# In[8]:

def merge_selected_files(selected_files, output_filename):
    """
    Merges the files into one geotiff
    
    Args:
        selected_files(list): List of paths
        output_filename(string): Output path including extension (.tif)
    Returns:
        None
    
    """
    
    datasets = []
    for selected_file in selected_files:
        datasets.append(rasterio.open("{}/{}".format(ec2_input_path,selected_file)))
        Z, out_transform = rasterio.merge.merge(datasets=datasets)
        Z = Z[0,:,:]
        Z = np.float32(Z)

        # Write geotiff
        with rasterio.open(
            output_filename,
            'w',
            driver='GTiff',
            height=Z.shape[0],
            width=Z.shape[1],
            count=1,
            dtype=Z.dtype,
            crs='+proj=latlong',
            transform=out_transform,
            compress='lzw'
        ) as dst:
            dst.write(Z, 1)   


# In[9]:

for target_filename in target_filenames:
    print("merging: " , target_filename)
    regex = re.compile(r"{}".format(target_filename))
    selected_files = list(filter(regex.search, files))
    print(selected_files)
    output_filename = "{}/{}.tif".format(ec2_output_path,target_filename)
    merge_selected_files(selected_files, output_filename)
    print(output_filename)


# In[10]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# previous run:  
# 0:07:49.738804
# 

# In[ ]:



