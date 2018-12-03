
# coding: utf-8

# In[1]:

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
OUTPUT_VERSION = 6

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M11D29_RH_Hybas6_U_GADM36L01_GPD_PP_V01/output_V19/"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nS3_INPUT_PATH:", S3_INPUT_PATH,
      "\nec2_input_path:", ec2_output_path,
      "\nec2_output_path:", ec2_output_path,
      "\ns3_output_path: ", s3_output_path)


# In[2]:

import time, datetime, sys
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

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --exclude *.csv')


# In[5]:

import os
import numpy as np
import pandas as pd
import geopandas as gpd
import multiprocessing


# In[6]:

files = os.listdir(ec2_input_path)


# In[7]:

len(files)


# In[8]:

gdfs = []
for one_file in files:    
    input_path = "{}/{}".format(ec2_input_path,one_file)
    gdfs.append(pd.read_pickle(input_path))


# In[9]:

gdf = pd.concat(gdfs, ignore_index=True)


# In[10]:

gdf.shape


# In[11]:

gdf.head()


# In[12]:

gdf["separator"] = "-"


# In[13]:

gdf["composite_index"] = gdf["gid_1"] + gdf["separator"] + gdf["pfaf_id"].map(str)


# In[14]:

gdf.drop(columns=["separator"],inplace=True)


# In[15]:

gdf_list = [gdf.loc[gdf['composite_index']==val, :] for val in gdf["composite_index"].unique()]


# In[16]:

len(gdf_list)


# In[17]:

def process_small_gdf(gdf_small):
    """ Process a small geodataframe. Dissolves into df with 1 row
    
    Args:
        gdf_small(GeoDataFrame): Geodataframe with unique identifiers
    
    Returns:
        gdf_small_out(GeoDataFrame): GeoDataFrame with dissolved geometries.
    
    """
    if gdf_small.shape[0] > 0:
        composite_index = gdf.iloc[0]["composite_index"]
        destination_path = "{}/gdf_dissolved_{}.pkl".format(ec2_output_path,composite_index)
        gdf_small_dissolved = gdf_small.dissolve(by="composite_index")
        #gdf_small_dissolved.to_pickle(path=destination_path)
    
    else:
        print("invalid input dataframe")
        gdf_small_dissolved = None
    return gdf_small_dissolved
    
    


# In[18]:

cpu_count = multiprocessing.cpu_count()
cpu_count = cpu_count -2 #Avoid freeze
print("Power to the maxxx:", cpu_count)


# In[19]:

p= multiprocessing.Pool(processes=cpu_count)
df_out_list = p.map(process_small_gdf,gdf_list)
p.close()
p.join()


# In[20]:

gdf_out = pd.concat(df_out_list, ignore_index=True)


# In[21]:

gdf_out.shape


# In[22]:

gdf.head()


# In[23]:

output_file_path = "{}/{}.shp".format(ec2_output_path,SCRIPT_NAME)


# In[24]:

gdf_out.to_file(filename=output_file_path,driver="ESRI Shapefile")


# In[25]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[26]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:45:12.187817  
# 0:08:00.518025  
# 

# In[ ]:



