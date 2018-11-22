
# coding: utf-8

# In[1]:

# Creates 
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon, box
from matplotlib import pyplot as plt
factor = 2 # in native crs (m?)


# In[2]:

get_ipython().magic('matplotlib inline')


# In[3]:

input_path = "./"
output_path = "./"


# In[4]:

gdf = gpd.GeoDataFrame.from_file(os.path.join(input_path,"TestSet_2.geojson"))


# In[5]:

gdf.shape


# In[6]:

gdf_valid = gdf[gdf.geometry.notna()]


# In[7]:

gdf_valid.shape


# In[8]:

crs = gdf.crs


# In[9]:

gdf_bounds = gdf_valid.geometry.bounds


# In[10]:

gdf_total = pd.concat([gdf_valid,gdf_bounds],axis=1)


# In[11]:

def create_bbox(minx,miny,maxx,maxy):
    bbox = box(minx,miny,maxx,maxy)
    return bbox


# In[12]:

def create_bbox_polygon(minx,miny,maxx,maxy):
    bbox = Polygon((0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.))
    return bbox


# In[13]:

gdf_total['bbox'] = gdf_total.apply(lambda row: Polygon([(row.minx,row.miny),
                                                        (row.maxx,row.miny),
                                                        (row.maxx,row.maxy),
                                                        (row.minx,row.maxy)])
                                                        ,axis=1)


# In[14]:

gdf_total['bbox_big'] = gdf_total.apply(lambda row: Polygon([(row.minx-factor,row.miny-factor),
                                                             (row.maxx+factor,row.miny-factor),
                                                             (row.maxx+factor,row.maxy+factor),
                                                             (row.minx-factor,row.maxy+factor)])
                                                             ,axis=1)




# In[15]:

gdf_total.head()


# In[18]:

destination_originalvalid = os.path.join(output_path,"testdata2_originalvalid2.geojson")


# In[19]:

destination_originalvalid


# In[21]:

gdf_valid = gdf_valid.drop(["building"],axis=1)


# In[25]:




# In[26]:




# In[22]:

gdf_valid.to_file(destination_originalvalid,driver="GeoJSON")


# In[ ]:

gdf_bbox = gdf_total.drop(columns=["minx","miny","maxx","maxy","geometry","bbox_big"])


# In[ ]:

gdf_bbox = gdf_bbox.set_geometry(col="bbox")


# In[ ]:

gdf_bbox.head()


# In[ ]:

destination_bbox = os.path.join(output_path,"testdata1_bbox.geojson")


# In[ ]:

gdf_bbox.to_file(destination_bbox,driver="GeoJSON")


# In[ ]:

gdf_bboxbig = gdf_total.drop(columns=["minx","miny","maxx","maxy","geometry","bbox"])


# In[ ]:

gdf_bboxbig = gdf_bboxbig.set_geometry(col="bbox_big")


# In[ ]:

destination_bboxbig = os.path.join(output_path,"testdata1_bboxbig.geojson")


# In[ ]:

gdf_bboxbig.to_file(destination_bboxbig,driver="GeoJSON")


# In[ ]:



