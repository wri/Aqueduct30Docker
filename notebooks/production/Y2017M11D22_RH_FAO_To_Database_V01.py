
# coding: utf-8

# # Store FAO related files on PostgreSQL RDS database
# 
# * Purpose of script: This script will process the hydrobasin related data into multiple tables according to the database ERD
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171122 
# 
# The script requires a file called .password to be stored in the current working directory with the password to the database.

# In[1]:

get_ipython().magic('matplotlib inline')
import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2017M11D22_RH_FAO_To_Database_V01"

INPUT_VERSION = 1
INPUT_VERSION_LINK = 4
OUTPUT_VERSION = 5

INPUT_FILE_NAME = "hydrobasins_fao_fiona_merged_v%0.2d" %(INPUT_VERSION)
INPUT_FILE_NAME_LINK = "hybas_lev06_v1c_merged_fiona_withFAO_V%0.2d_link" %(INPUT_VERSION_LINK)

EC2_INPUT_PATH = "/volumes/data/%s/input/" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output/" %(SCRIPT_NAME)

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Merge_FAONames_V01/output/"
S3_INPUT_PATH_LINK = "s3://wri-projects/Aqueduct30/processData/Y2017M08D25_RH_spatial_join_FAONames_V01/output/"

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v01"
DATABASE_NAME = "database01"
TABLE_NAME = str.lower(SCRIPT_NAME)

TABLE_NAME_FAO_MAJOR = "fao_major_v%0.2d" %(OUTPUT_VERSION)
TABLE_NAME_FAO_MINOR = "fao_minor_v%0.2d" %(OUTPUT_VERSION)
TABLE_NAME_FAO_LINK = "fao_link_v%0.2d" %(OUTPUT_VERSION)


# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_LINK} {EC2_INPUT_PATH} --recursive')


# In[6]:

import os
import pandas as pd
import geopandas as gpd
from ast import literal_eval
import boto3
import botocore
from sqlalchemy import *
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry.multipolygon import MultiPolygon


# In[42]:

# RDS Connection
def rdsConnect(database_identifier,database_name):
    rds = boto3.client('rds')
    F = open(".password","r")
    password = F.read().splitlines()[0]
    F.close()
    response = rds.describe_db_instances(DBInstanceIdentifier="%s"%(database_identifier))
    status = response["DBInstances"][0]["DBInstanceStatus"]
    print("Status:",status)
    endpoint = response["DBInstances"][0]["Endpoint"]["Address"]
    print("Endpoint:",endpoint)
    engine = create_engine('postgresql://rutgerhofste:%s@%s:5432/%s' %(password,endpoint,database_name))
    connection = engine.connect()
    return engine, connection

def uploadGDFtoPostGIS(gdf,tableName):
    # this function uploads a shapefile to table in AWS RDS. 
    # It handles combined polygon/multipolygon geometry and stores it in multipolygon
    gdf2 = gdf.copy()
    gdf2["type"] = gdf2.geometry.geom_type
    gdfPolygon = gdf2.loc[gdf2["type"]=="Polygon"]         
    gdfMultiPolygon = gdf2.loc[gdf2["type"]=="MultiPolygon"]
    gdfPolygon2 = gdfPolygon.copy()
    gdfMultiPolygon2 = gdfMultiPolygon.copy()
    gdfPolygon2['geom'] = gdfPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
    gdfMultiPolygon2['geom'] = gdfMultiPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
    gdfPolygon2.drop("geometry",1, inplace=True)
    gdfMultiPolygon2.drop("geometry",1, inplace=True)
    gdfPolygon2.drop("type",1, inplace=True)
    gdfMultiPolygon2.drop("type",1, inplace=True)
    
    gdfPolygon2.to_sql("temppolygon", engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4326)})
    
    gdfMultiPolygon2.to_sql("tempmultipolygon", engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('MULTIPOLYGON', srid= 4326)})
    
    sql = "ALTER TABLE temppolygon ALTER COLUMN geom type geometry(MultiPolygon, 4326) using ST_Multi(geom);" 
    result = connection.execute(sql)
    sql = "CREATE TABLE temp_minor AS (SELECT * FROM temppolygon UNION SELECT * FROM tempmultipolygon);" 
    result = connection.execute(sql)
    
    # dissolve based on fao_id and left join results
    sql = "CREATE TABLE %s AS SELECT fao_id, ST_Multi(ST_Union(f.geom)) as geom FROM temp_minor As f GROUP BY fao_id"
    
    
    
    
    
    sql = "DROP TABLE temppolygon,tempmultipolygon"
    result = connection.execute(sql)
    sql = "select * from %s" %(tableName)
    gdfFromSQL =gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' )   
    """
    return gdfMultiPolygon#gdfFromSQL


# In[8]:

engine, connection = rdsConnect(DATABASE_IDENTIFIER,DATABASE_NAME)


# In[ ]:




# In[9]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME+".shp"))


# In[10]:

gdf.columns = map(str.lower, gdf.columns)


# In[11]:

gdf.head()


# The idea is to store the data in two tables: major basin and minor basin together with the geometry. There is no unique identifier for the minor basins so we will use a composite key    
#     

# In[12]:

def compositeKey(maj_bas,sub_bas):
    key = 'MAJ_BAS_%0.4d_SUB_BAS_%0.7d' %(maj_bas,sub_bas)
    return key


# In[13]:

gdf["fao_id"]= gdf.apply(lambda x: compositeKey(x["maj_bas"],x["sub_bas"]),1)


# In[14]:

gdf.head()


# In[16]:

gdf = gdf.set_index("fao_id",drop=False)


# ## Major River Basins

# In[17]:

dfMajorFull = gdf[["maj_bas","maj_name","maj_area","legend"]]
gdfMinor = gdf[["sub_bas","to_bas","maj_bas","sub_name","sub_area","geometry","fao_id"]]


# In[18]:

dfMajorFull.head()


# In[19]:

dfMajor = dfMajorFull.groupby("maj_bas").first()


# In[20]:

dfMajor.head()


# In[21]:

dfMajor.to_sql(
    name = TABLE_NAME_FAO_MAJOR,
    con = connection,
    if_exists="replace",
    index= True)


# ## Minor River Basins

# In[22]:

gdfMinor.head()


# Geometry consists of polygon and multipolygon type. Upload both to postGIS and set polygon to multipolygon and join. 

# In[ ]:




# In[45]:

gdfFromSQL = uploadGDFtoPostGIS(gdfMinor,TABLE_NAME_FAO_MINOR)


# In[44]:

gdfFromSQL.head()


# ### Link Table
# 
# this information comes from a spatial join and is stored in a different table. 
# 
# 

# In[26]:

df_link = pd.read_pickle(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME_LINK+".pkl"))


# In[27]:

df_link = df_link.reset_index()


# In[28]:

df_link.drop("index",1,inplace=True)


# In[29]:

df_link.index.names = ['id']


# In[30]:

#df_link.columns = map(str.lower, df_link.columns)


# In[31]:

df_link.columns = ["pfaf_id","fao_id"]


# In[32]:

df_link.to_sql(
    name = TABLE_NAME_FAO_LINK,
    con = connection,
    if_exists="replace",
    index= True)


# In[ ]:




# In[ ]:



