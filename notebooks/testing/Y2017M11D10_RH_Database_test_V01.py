
# coding: utf-8

# Testing if I can upload shapefile to a postGIS server

# https://gis.stackexchange.com/questions/239198/geopandas-dataframe-to-postgis-table-help
# 
# http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ConnectToPostgreSQLInstance.html
# 
# http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html#Appendix.PostgreSQL.CommonDBATasks.PostGIS

# In[31]:

SCRIPT_NAME = "Y2017M11D10_RH_Database_test_V01"

S3_INPUT_PATH_SHAPE = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"
INPUT_VERSION_SHAPE = 1

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

INPUT_FILE_NAME_SHAPE = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V%0.2d" %(INPUT_VERSION_SHAPE)


# In[32]:

get_ipython().system('rm -r {EC2_INPUT_PATH} ')
get_ipython().system('rm -r {EC2_OUTPUT_PATH} ')


# In[33]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH} ')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH} ')


# In[34]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_SHAPE} {EC2_INPUT_PATH} --recursive')


# In[36]:

from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import pandas as pd
import geopandas as gpd
import os


# In[68]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME_SHAPE+".shp"))


# In[69]:

gdf = gdf[0:1000]


# In[80]:

gdf2 = gdf.copy()


# In[81]:

gdf2["type"] = gdf.geometry.geom_type


# In[82]:

gdf2 = gdf.loc[gdf2["type"]=="Polygon"]


# In[83]:

gdf2['geom'] = gdf2['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))


# In[85]:

gdf2.drop('geometry', 1, inplace=True)


# In[86]:

engine = create_engine('postgresql://rutgerhofste:nopassword@aqueduct30v02.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com:5432/database01')


# In[91]:

gdf2.to_sql("table03", engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4326)})


# In[92]:

connection = engine.connect()


# In[93]:

sql = "select * from table03"


# In[94]:

gdfAWS=gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' )


# In[103]:

get_ipython().magic('matplotlib inline')


# In[100]:

test = gdfAWS.head()


# In[104]:

test.plot()


# In[ ]:



