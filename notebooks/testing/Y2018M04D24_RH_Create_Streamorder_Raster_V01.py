
# coding: utf-8

# In[1]:

import sys
sys.path.append("/opt/pcraster-4.1.0_x86-64/python")
import pcraster


# In[2]:

SCRIPT_NAME = "Y2018M04D24_RH_Create_Streamorder_Raster_V01"
OUTPUT_VERSION = 1

OUTPUT_FILE_NAME = "streamorder_v01.map"


# In[3]:

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)


# In[4]:

input_file_path = "/volumes/data/Y2018M04D18_RH_Convert_Aux_Rasters_Geotiff_V01/input_V01/global_lddsound_numpad_05min.map"


# In[9]:

get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

Ldd = pcraster.readmap(input_file_path)


# In[6]:

Result = pcraster.streamorder(Ldd)


# In[7]:

output_file_path = "{}/{}".format(ec2_output_path,OUTPUT_FILE_NAME)


# In[10]:

pcraster.report(Result,output_file_path)


# In[11]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:



