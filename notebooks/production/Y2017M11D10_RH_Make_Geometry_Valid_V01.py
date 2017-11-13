
# coding: utf-8

# ### Merge Upstream Downstream with FAO names 
# 
# * Purpose of script: Create a shapefile and csv file with both the upstream / downstream relation and the FAO basin names
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170829

# This script requires some additional steps that are not automated yet. The objective is to set up a PosGIS enabled PostgreSQL AWS RDS instance. 
# 
# https://gis.stackexchange.com/questions/239198/geopandas-dataframe-to-postgis-table-help
# 
# http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ConnectToPostgreSQLInstance.html
# 
# http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html#Appendix.PostgreSQL.CommonDBATasks.PostGIS
# 
# database is not protected by deafault. 

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[23]:

SCRIPT_NAME = "Y2017M11D10_RH_Make_Geometry_Valid_V01"

INPUT_VERSION = 1
OUTPUT_VERSION = 1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)

INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V%0.2d" %(INPUT_VERSION)

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

OUTPUT_FILE_NAME = "Y2017M11D10_RH_Make_Geometry_Valid_V%0.2d" %(OUTPUT_VERSION)

S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"

# Database settings
TABLE_NAME = "hybasvalid03"




# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive ')


# In[5]:

from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import pandas as pd
import geopandas as gpd
import os
from shapely.geometry.multipolygon import MultiPolygon


# In[6]:

get_ipython().magic('matplotlib inline')


# In[7]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".shp"))


# In[8]:

gdf = gdf.set_index("PFAF_ID", drop=False)


# In[9]:

gdf.head()


# In[10]:

gdf.shape


# In[11]:

gdf2 = gdf.copy()


# In[12]:

gdf2.geometry = gdf['geometry'].apply(lambda x: MultiPolygon([x]))


# In[13]:

gdf3 = gdf2.copy()


# In[14]:

gdf3['geom'] = gdf2['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))


# In[15]:

gdf3.drop("geometry",1, inplace=True)


# In[20]:

gdf3.shape


# The following command will connect to a temporary free tier AWS RDS instance

# In[21]:

engine = create_engine('postgresql://rutgerhofste:nopassword@aqueduct30v02.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com:5432/database01')


# In[26]:

gdf3.to_sql(TABLE_NAME, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('MULTIPOLYGON', srid= 4326)})


# In[27]:

connection = engine.connect()


# In[28]:

sql = "update %s set geom = st_makevalid(geom)" %(TABLE_NAME)


# In[29]:

result = connection.execute(sql)


# Check if operation succesful 

# In[30]:

sql = "select * from %s" %(TABLE_NAME)


# In[31]:

gdfAWS=gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' ).set_index("PFAF_ID", drop=False)


# In[32]:

connection.close()


# In[33]:

gdfAWS.head()


# In[ ]:

gdfAWS.to_file(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+".shp"))


# In[ ]:




# In[ ]:



