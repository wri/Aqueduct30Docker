
# coding: utf-8

# In[5]:

""" Union of hydrobasin and GADM 36 level 1 merge results. 
-------------------------------------------------------------------------------

Step 1:
Create polygons (10x10 degree, 648).

Step 2:
Clip all geodataframes with polygons (intersect).

Step 3:
Peform union per polygon.

Step 4: 
Merge results into large geodataframe.

Step 5:
Dissolve on unique identifier.

Step 6:
Save output

Author: Rutger Hofste
Date: 20181128
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""


TESTING = 1
SCRIPT_NAME = "Y2018M11D29_RH_Hybas6_U_GADM36L01_GPD_PP_Merge_V01"
OUTPUT_VERSION = 1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M11D29_RH_Hybas6_U_GADM36L01_GPD_PP_V01/output_V04/"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nS3_INPUT_PATH:", S3_INPUT_PATH,
      "\nec2_input_path:", ec2_output_path,
      "\nec2_output_path:", ec2_output_path,
      "\ns3_output_path: ", s3_output_path)


# In[7]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[8]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[9]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive')


# In[10]:

import os
import pandas as pd
import geopandas as gpd


# In[11]:

files = os.listdir(ec2_input_path)


# In[ ]:

gdfs = []
for one_file in files:
    
    input_path = "{}/{}".format(ec2_input_path,one_file)
    gdfs.append(pd.read_pickle(input_path))


# In[ ]:

gdf = pd.concat(gdfs, ignore_index=True)


# In[ ]:

get_ipython().magic('matplotlib inline')


# In[ ]:

gdf.head()


# In[ ]:

gdf["composite_index"] = gdf["gid_1"] + gdf["pfaf_id"].map(str)


# In[ ]:

gdf.head()


# In[ ]:

gdf.shape


# In[ ]:

gdf_dissolved = gdf.dissolve(by="composite_index")


# In[ ]:

gdf_dissolved.shape


# In[ ]:

output_file_path = "{}/{}.gpkg".format(ec2_output_path,SCRIPT_NAME)


# In[ ]:

gdf_dissolved.to_file(filename=output_file_path,driver="GPKG")


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:45:12.187817
