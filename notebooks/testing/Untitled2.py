
# coding: utf-8

# Testing if I can upload shapefile to a postGIS server

# https://gis.stackexchange.com/questions/239198/geopandas-dataframe-to-postgis-table-help

# In[8]:

from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import pandas as pd
import geopandas as gpd


# In[9]:

engine = create_engine('postgresql://rutgerhofste:nopassword@aqueduct30v02.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com:5432/database01')


# In[14]:

engine.connect()


# In[11]:




# In[13]:

print(engine.table_names())


# In[ ]:


