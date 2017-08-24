
# coding: utf-8

# In[26]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/test/testGpd/"
EC2_INPUT_PATH = "/volumes/data/temp/"


# In[27]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')


# In[28]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[14]:

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
get_ipython().magic('matplotlib notebook')
import os
import folium


# In[3]:

from shapely.wkt import loads
from shapely.geometry import Point


# In[4]:

data = {'name': ['a', 'b', 'c'],
        'lat': [45, 46, 47.5],
        'lon': [-120, -121.2, -122.9]}


# In[5]:

geometry = [Point(xy) for xy in zip(data['lon'], data['lat'])]
geometry


# In[6]:

df = pd.DataFrame(data)
df


# In[9]:

geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
gdfCities = gpd.GeoDataFrame(df, geometry=geometry)


# In[10]:

gdfCities


# In[17]:

gdfCities.crs = {'init': 'epsg:4326'}


# In[18]:

gdfWorld = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
gdfWorld.head(2)


# In[24]:

gdfJoined = gpd.sjoin(gdfWorld,gdfCities, how="right", op='intersects')


# In[25]:

gdfJoined


# In[ ]:

m = folium.Map(location=[47.8, -122.5], zoom_start=7,tiles='Stamen Toner')


# In[ ]:

geo_str = world.to_json()


# In[ ]:

m.choropleth(geo_str=geo_str)


# In[ ]:

m


# In[ ]:



