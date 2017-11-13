
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


# In[2]:

SCRIPT_NAME = "Y2017M11D10_RH_Make_Geometry_Valid_V01"

INPUT_VERSION = 1
OUTPUT_VERSION = 1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)

INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V%0.2d" %(INPUT_VERSION)

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

OUTPUT_FILE_NAME = "Y2017M11D10_RH_Make_Geometry_Valid_V%0.2d" %(OUTPUT_VERSION)

S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)

# Database settings
TABLE_NAME = "hybasvalid05"


# In[ ]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[ ]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive ')


# In[3]:

from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import pandas as pd
import geopandas as gpd
import os
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon


# In[4]:

get_ipython().magic('matplotlib inline')


# In[5]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".shp"))


# In[6]:

gdf = gdf.set_index("PFAF_ID", drop=False)


# In[7]:

gdf.head()


# In[8]:

gdf.shape


# In[9]:

gdf2 = gdf.copy()


# In[ ]:

"""
def explode(indf):
    outdf = gpd.GeoDataFrame(columns=indf.columns)
    for idx, row in indf.iterrows():
        if type(row.geometry) == Polygon:
            outdf = outdf.append(row,ignore_index=True)
        if type(row.geometry) == MultiPolygon:
            multdf = gpd.GeoDataFrame(columns=indf.columns)
            recs = len(row.geometry)
            multdf = multdf.append([row]*recs,ignore_index=True)
            for geom in range(recs):
                multdf.loc[geom,'geometry'] = row.geometry[geom]
            outdf = outdf.append(multdf,ignore_index=True)
    return outdf
"""


# In[ ]:




# In[27]:

gdf2["type"] = gdf2.geometry.geom_type


# In[28]:

gdfPolygon = gdf2.loc[gdf2["type"]=="Polygon"]
gdfMultiPolygon = gdf2.loc[gdf2["type"]=="MultiPolygon"]


# In[29]:

gdfPolygon2 = gdfPolygon.copy()
gdfMultiPolygon2 = gdfMultiPolygon.copy()


# In[30]:

gdfPolygon2['geom'] = gdfPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))


# In[31]:

gdfMultiPolygon2['geom'] = gdfMultiPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))


# In[32]:

gdfPolygon2.drop("geometry",1, inplace=True)
gdfMultiPolygon2.drop("geometry",1, inplace=True)


# The following command will connect to a temporary free tier AWS RDS instance

# In[34]:

engine = create_engine('postgresql://rutgerhofste:nopassword@aqueduct30v02.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com:5432/database01')


# In[43]:

tableNamePolygon = TABLE_NAME+"polygon"
tableNameMultiPolygon = TABLE_NAME+"multipolygon"


# In[44]:

gdfPolygon2.to_sql(tableNamePolygon, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4326)})


# In[45]:

gdfMultiPolygon2.to_sql(tableNameMultiPolygon, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('MULTIPOLYGON', srid= 4326)})


# In[46]:

connection = engine.connect()


# In[47]:

sql = "update %s set geom = st_makevalid(geom)" %(tableNamePolygon)


# In[48]:

result = connection.execute(sql)


# In[49]:

sql = "update %s set geom = st_makevalid(geom)" %(tableNameMultiPolygon)


# In[50]:

result = connection.execute(sql)


# Check if operation succesful 

# In[51]:

sql = "select * from %s" %(tableNamePolygon)


# In[52]:

gdfAWSPolygon=gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' ).set_index("PFAF_ID", drop=False)


# In[53]:

sql = "select * from %s" %(tableNameMultiPolygon)


# In[54]:

gdfAWSMultiPolygon=gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' ).set_index("PFAF_ID", drop=False)


# In[61]:

gdfAWSPolygon.crs = {'init' :'epsg:4326'}
gdfAWSMultiPolygon.crs = {'init' :'epsg:4326'}


# In[68]:

gdfAWS = gdfAWSPolygon.append(gdfAWSMultiPolygon)


# In[70]:

gdfAWS.to_file(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+".shp"))


# In[71]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[72]:

connection.close()


# In[ ]:



