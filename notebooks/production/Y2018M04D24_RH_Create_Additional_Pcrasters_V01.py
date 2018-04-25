
# coding: utf-8

# In[1]:

""" Use pcraster to create additional rasters such as streamorder.
-------------------------------------------------------------------------------
Use pcraster to create:
- a stream order map for a local drainage network.



Author: Rutger Hofste
Date: 20180425
Kernel: python27
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    SCRIPT_NAME (string) : Script name
    OUTPUT_VERSION (integer) : Output version.
    OUTPUT_FILE_NAME (string) : Output file name.

Returns:

"""

SCRIPT_NAME = "Y2018M04D24_RH_Create_Additional_Pcrasters_V01"
PREVIOUS_SCRIPT_NAME = "Y2018M04D18_RH_Convert_Aux_Rasters_Geotiff_V01"
INPUT_VERSION = 1
INPUT_FILE_NAME = "global_lddsound_numpad_05min.map"

OUTPUT_VERSION = 3
OUTPUT_FILE_NAME = "global_streamorder_dimensionless_05min.map"

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,INPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_input_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(PREVIOUS_SCRIPT_NAME,INPUT_VERSION)


print("Input s3: " + s3_input_path +
      "\nInput ec2: " + ec2_input_path +
      "\nOutput ec2: " + ec2_output_path +
      "\nOutput S3: " + s3_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import sys
sys.path.append("/opt/pcraster-4.1.0_x86-64/python")
import pcraster


# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

get_ipython().system('aws s3 cp {s3_input_path} {ec2_input_path} --recursive')


# In[6]:

def main():
    s3_input_path
    input_file_path = "/volumes/data/Y2018M04D18_RH_Convert_Aux_Rasters_Geotiff_V01/input_V01/global_lddsound_numpad_05min.map"
    ldd_map = pcraster.readmap(input_file_path)
    streamorder_map = pcraster.streamorder(ldd_map)
    output_file_path = "{}/{}".format(ec2_output_path,OUTPUT_FILE_NAME)
    pcraster.report(streamorder_map,output_file_path)
    get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')

if __name__ == "__main__":
    main()


# In[7]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

