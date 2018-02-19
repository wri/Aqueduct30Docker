
# coding: utf-8

# In[6]:

import multiprocessing


# In[8]:

cpu_count = multiprocessing.cpu_count()


# In[3]:

def f(x):
    return x*x


# In[12]:

get_ipython().run_cell_magic('time', '', 'with Pool(cpu_count) as p:\n    print(p.map(f, [1, 2, 3,8888]))')


# In[ ]:

file_name_streams = "/volumes/data/Y2018M02D16_RH_Number_Streams_Per_Basin_V01/input/GDBD_streams_EPSG4326_V06.shp"


# In[ ]:

gdf_streams = gpd.GeoDataFrame.from_file(file_name_streams)


# In[ ]:



