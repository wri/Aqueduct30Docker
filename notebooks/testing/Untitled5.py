
# coding: utf-8

# In[9]:

from osgeo import ogr
import geopandas as gpd


# In[6]:

shape_file_path = "/volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V02/output_V04/hybas_lev06_v1c_merged_fiona_V04.shp"


# In[8]:

info = ogr.info(shape_file_path)


# In[10]:

gdf = gpd.read_file(shape_file_path)


# In[11]:

gdf


# In[ ]:



