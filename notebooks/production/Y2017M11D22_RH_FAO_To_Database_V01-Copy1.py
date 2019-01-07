
# coding: utf-8

# # Store FAO related files on PostgreSQL RDS database
# 
# * Purpose of script: This script will process the hydrobasin related data into multiple tables according to the database ERD
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171122 
# 
# The script requires a file called .password to be stored in the current working directory with the password to the database. Basic functionality
# 
# 

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
OUTPUT_VERSION = 7

INPUT_FILE_NAME = "hydrobasins_fao_fiona_merged_v%0.2d" %(INPUT_VERSION)
INPUT_FILE_NAME_LINK = "hybas_lev06_v1c_merged_fiona_withFAO_V%0.2d_link" %(INPUT_VERSION_LINK)

EC2_INPUT_PATH = "/volumes/data/%s/input/" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output/" %(SCRIPT_NAME)

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Merge_FAONames_V01/output/"
S3_INPUT_PATH_LINK = "s3://wri-projects/Aqueduct30/processData/Y2017M08D25_RH_spatial_join_FAONames_V01/output/"

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v02"
DATABASE_NAME = "database01"

TABLE_NAME_FAO_MAJOR = "fao_major_v%0.2d" %(OUTPUT_VERSION)
TABLE_NAME_FAO_MINOR = "fao_minor_v%0.2d" %(OUTPUT_VERSION)
TABLE_NAME_FAO_MINOR_TEMP = "fao_minor_temp_v%0.2d" %(OUTPUT_VERSION)
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


# In[7]:

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

def uploadGDFtoPostGIS(gdf,tableName,saveIndex):
    # this function uploads a polygon shapefile to table in AWS RDS. 
    # It handles combined polygon/multipolygon geometry and stores it in valid multipolygon in epsg 4326.
    
    # gdf = input geoDataframe
    # tableName = postGIS table name (string)
    # saveIndex = save index column in separate column in postgresql, otherwise discarded. (Boolean)
    
    
    gdf["type"] = gdf.geometry.geom_type    
    geomTypes = ["Polygon","MultiPolygon"]
    
    for geomType in geomTypes:
        gdfType = gdf.loc[gdf["type"]== geomType]
        geomTypeLower = str.lower(geomType)
        gdfType['geom'] = gdfType['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
        gdfType.drop(["geometry","type"],1, inplace=True)      
        print("Create table temp%s" %(geomTypeLower)) 
        gdfType.to_sql(
            name = "temp%s" %(geomTypeLower),
            con = engine,
            if_exists='replace',
            index= saveIndex, 
            dtype={'geom': Geometry(str.upper(geomType), srid= 4326)}
        )
        
    # Merge both tables and make valid
    sql = []
    sql.append("DROP TABLE IF EXISTS %s"  %(tableName))
    sql.append("ALTER TABLE temppolygon ALTER COLUMN geom type geometry(MultiPolygon, 4326) using ST_Multi(geom);")
    sql.append("CREATE TABLE %s AS (SELECT * FROM temppolygon UNION SELECT * FROM tempmultipolygon);" %(tableName))
    sql.append("UPDATE %s SET geom = st_makevalid(geom);" %(tableName))
    sql.append("DROP TABLE temppolygon,tempmultipolygon")

    for statement in sql:
        print(statement)
        result = connection.execute(statement)    
    gdfFromSQL =gpd.GeoDataFrame.from_postgis("select * from %s" %(tableName),connection,geom_col='geom' )
    return gdfFromSQL


def postGISDissolveFirst(sourceTableName,targetTableName,by):
    #dissolve polygons and polygon related attributes (area)
    #take first attribute of other
    
    sql =   ("CREATE TABLE %s AS SELECT MIN(%s) as %s,MIN(sub_bas) as sub_bas, " 
            "MIN(to_bas) as to_bas, "
            "MIN(maj_bas) as maj_bas, "
            "MIN(sub_name) as sub_name, "
            "MIN(sub_area) as sub_area, "
            "ST_Multi(ST_Union(t.geom)) as geom "
            "FROM %s As t GROUP BY %s") %(targetTableName,by, by, sourceTableName,by)
    connection.execute(sql)
    gdfFromSQL =gpd.GeoDataFrame.from_postgis("select * from %s" %(targetTableName),connection,geom_col='geom' )
    print(sql)
    return gdfFromSQL



# In[8]:

engine, connection = rdsConnect(DATABASE_IDENTIFIER,DATABASE_NAME)


# In[ ]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME+".shp"))


# In[ ]:

gdf.columns = map(str.lower, gdf.columns)


# In[ ]:

gdf.head()


# The idea is to store the data in two tables: major basin and minor basin together with the geometry. There is no unique identifier for the minor basins so we will use a composite key    
#     

# In[ ]:

def compositeKey(maj_bas,sub_bas):
    key = 'MAJ_BAS_%0.4d_SUB_BAS_%0.7d' %(maj_bas,sub_bas)
    return key


# In[ ]:

gdf["fao_id"]= gdf.apply(lambda x: compositeKey(x["maj_bas"],x["sub_bas"]),1)


# In[ ]:

gdf.head()


# In[ ]:

gdf = gdf.set_index("fao_id",drop=False)


# In[ ]:

df = gdf.drop("geometry",1)


# In[ ]:

dfGrouped = df.groupby(df["fao_id"]).first()


# In[ ]:

dfGrouped.shape


# ## Major River Basins

# In[ ]:

dfMajorFull = gdf[["maj_bas","maj_name","maj_area","legend"]]
gdfMinor = gdf[["sub_bas","to_bas","maj_bas","sub_name","sub_area","geometry","fao_id"]]


# In[ ]:

dfMajorFull.head()


# In[ ]:

dfMajor = dfMajorFull.groupby("maj_bas").first()


# In[ ]:

dfMajor.head()


# In[ ]:

dfMajor.to_sql(
    name = TABLE_NAME_FAO_MAJOR,
    con = connection,
    if_exists="replace",
    index= True)


# ## Minor River Basins

# In[ ]:

gdfMinor.head()


# Geometry consists of polygon and multipolygon type. Upload both to postGIS and set polygon to multipolygon and join. 

# In[ ]:

gdfFromSQL = uploadGDFtoPostGIS(gdfMinor,TABLE_NAME_FAO_MINOR_TEMP,False)


# In[ ]:

gdfFromSQL.head()


# In[ ]:

test = gdfFromSQL.duplicated(subset="fao_id")


# In[ ]:

test.head()


# In[ ]:

gdfFromSQL2 = postGISDissolveFirst(TABLE_NAME_FAO_MINOR_TEMP,TABLE_NAME_FAO_MINOR,"fao_id")


# Drop temporary table

# In[ ]:

sql = "DROP TABLE IF EXISTS %s" %(TABLE_NAME_FAO_MINOR_TEMP)


# In[ ]:

connection.execute(sql)


# Check dimensions :

# In[ ]:

gdfFromSQL2.shape[0]


# In[ ]:

dfGrouped.shape[0]


# ### Link Table
# 
# this information comes from a spatial join and is stored in a different table. 
# 
# 

# In[ ]:

df_link = pd.read_pickle(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME_LINK+".pkl"))


# In[ ]:

df_link = df_link.reset_index()


# In[ ]:

df_link.drop("index",1,inplace=True)


# In[ ]:

df_link.index.names = ['id']


# In[ ]:

#df_link.columns = map(str.lower, df_link.columns)


# In[ ]:

df_link.columns = ["pfaf_id","fao_id"]


# In[ ]:

df_link.to_sql(
    name = TABLE_NAME_FAO_LINK,
    con = connection,
    if_exists="replace",
    index= True)


# In[ ]:

connection.close()


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

