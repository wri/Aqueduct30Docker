
# coding: utf-8

# Goal is to create read / write functions for FeatureCollections to AWS RDS PostGreSQL 

# In[1]:

get_ipython().magic('matplotlib inline')


# In[2]:

import ee
import geopandas as gpd


# In[3]:

ee.Initialize()


# In[4]:

fc = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017");


# In[5]:

fcEu = fc.filter(ee.Filter.eq("wld_rgn","Europe"))


# In[11]:

feature = ee.Feature(fcEu.filter(ee.Filter.eq("country_na","Netherlands")).first())


# In[13]:

print(feature.get("country_na").getInfo())


# In[ ]:

geom = feature.geometry().getInfo()


# In[ ]:

coords = geom["coordinates"]


# In[ ]:

from shapely.geometry.multipolygon import MultiPolygon


# In[ ]:

from shapely.geometry import shape


# In[ ]:

geom2 = shape(geom)


# In[ ]:

geoSeries = gpd.GeoSeries(geom2)


# In[ ]:

geoSeries.plot()


# In[ ]:



