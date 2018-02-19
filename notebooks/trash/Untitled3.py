
# coding: utf-8

# In[40]:

import numpy as np
import geopandas as gpd
import pandas as pd
import multiprocessing
from multiprocessing import Pool


# In[41]:

file_name_streams = "/volumes/data/Y2018M02D16_RH_Number_Streams_Per_Basin_V01/input/GDBD_streams_EPSG4326_V06.shp"


# In[42]:

gdf_streams = gpd.GeoDataFrame.from_file(file_name_streams)


# In[43]:

cpu_count = multiprocessing.cpu_count()
print(cpu_count)


# In[44]:

df_split = np.array_split(gdf_streams, cpu_count)


# In[45]:

gdf_streams.head()


# In[46]:

def rutger(df):
    return(df)


# In[47]:

with Pool(cpu_count) as p:
        results = p.map(rutger,df_split)


# In[52]:


def post_process_results(results):
    df_out = pd.DataFrame()
    for result in results:
        df_out = df_out.append(result)
    return df_out
    
    


# In[53]:

df_out = post_process_results(results)


# In[54]:




# In[ ]:




# In[ ]:



