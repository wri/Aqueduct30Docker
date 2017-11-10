
# coding: utf-8

# Testing if I can upload shapefile to a postGIS server

# https://gis.stackexchange.com/questions/239198/geopandas-dataframe-to-postgis-table-help
# 
# http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ConnectToPostgreSQLInstance.html
# 
# http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html#Appendix.PostgreSQL.CommonDBATasks.PostGIS

# In[8]:

from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import pandas as pd
import geopandas as gpd


# In[9]:

engine = create_engine('postgresql://rutgerhofste:nopassword@aqueduct30v02.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com:5432/database01')


# In[16]:

connection = engine.connect()


# In[22]:

cmd = 'select * from table01'


# In[29]:

employees = connection.execute(text(cmd))


# In[30]:

for item in employees:
    print(item)


# In[ ]:



