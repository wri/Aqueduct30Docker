
# coding: utf-8

# In[18]:

SCRIPT_NAME = "Y2018M06D21_RH_Test_Geopandas_Int64_SHP_V01"
OUTPUT_VERSION = 1

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 


# In[19]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[12]:

import geopandas as gpd
import numpy as np


# In[13]:

gdf = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))


# In[14]:

gdf["valid"] = np.int64(2147483647)


# In[15]:

gdf["invalid"] = np.int64(2147483647+1)


# In[16]:

gdf.head()


# In[20]:

output_file_path = ec2_output_path +"/" + "test.shp"


# In[21]:

gdf.to_file(driver="ESRI Shapefile",filename=output_file_path)


# In[22]:

gdf_from_shp = gpd.read_file(output_file_path)


# In[23]:

gdf_from_shp.dtypes


# In[ ]:



