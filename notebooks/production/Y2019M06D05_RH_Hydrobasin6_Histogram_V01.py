
# coding: utf-8

# In[1]:

""" Generate histogram data for in technical note.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20190605
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04


"""

SCRIPT_NAME = 'Y2019M06D05_RH_Hydrobasin6_Histogram_V01'
OUTPUT_VERSION = 1


S3_INPUT_PATH= "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V02/output_V04"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("S3_INPUT_PATH: ", S3_INPUT_PATH,
      "\ns3_output_path: ", s3_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version
get_ipython().magic('matplotlib inline')


# In[3]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive')


# In[5]:

import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt


# In[6]:

input_filename = "hybas_lev06_v1c_merged_fiona_V04.shp"


# In[7]:

input_path = "{}/{}".format(ec2_input_path,input_filename)


# In[8]:

gdf = gpd.read_file(filename=input_path)


# In[9]:

gdf.head()


# In[10]:

# SUB_AREA is Area of the individual polygon (i.e. sub-basin),  in square kilometers.


# In[11]:

values = gdf["SUB_AREA"].values


# In[12]:

median = np.median(values)


# In[13]:

median


# In[14]:

bin_edges = np.logspace(start=0,stop=6,num=13,base=10)


# In[15]:

bin_edges


# In[16]:

hist, bin_edges = np.histogram(values, bins=bin_edges)


# In[17]:

plt.bar(np.arange(len(hist))/2,hist)


# In[18]:

output_filename = "histogram_values.csv"


# In[19]:

output_path = "{}/{}".format(ec2_output_path,output_filename)


# In[20]:

output_path


# In[21]:

hist.tofile(output_path,sep=",")


# In[22]:

output_filename_edges =  "histogram_binedges.csv"


# In[23]:

output_path_edges = "{}/{}".format(ec2_output_path,output_filename_edges)


# In[24]:

bin_edges.tofile(output_path_edges,sep=",")


# In[25]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[26]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 
