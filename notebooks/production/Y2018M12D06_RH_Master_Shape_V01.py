
# coding: utf-8

# In[1]:

""" Process master shapefile and store in multiple formats.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181206
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2018M12D06_RH_Master_Shape_V01"
OUTPUT_VERSION = 2

NODATA_VALUE = -9999

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M12D06_RH_Master_Shape_Dissolve_01/output"
INPUT_FILE_NAME = "Y2018M12D06_RH_Master_Shape_Dissolved_V01.shp"

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"

OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("S3_INPUT_PATH: ",S3_INPUT_PATH,
      "\nec2_input_path: ",ec2_input_path,
      "\nec2_output_path: ",ec2_output_path,
      "\nBQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
      "\nOUTPUT_TABLE_NAME: ",OUTPUT_TABLE_NAME,
      "\ns3_output_path: ", s3_output_path
      )


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive ')


# In[5]:

import os
import sqlalchemy
import multiprocessing
import pandas as pd
import geopandas as gpd
import numpy as np
from google.cloud import bigquery
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement

pd.set_option('display.max_columns', 500)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[6]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))
connection = engine.connect()


# In[7]:

input_path = "{}/{}".format(ec2_input_path,INPUT_FILE_NAME)


# In[8]:

gdf = gpd.read_file(input_path)


# In[9]:

gdf.head()


# In[10]:

gdf.shape


# In[11]:

gdf.dtypes


# In[12]:

gdf[['pfaf_id','gid_1','aqid']] = gdf.string_id.str.split('-', expand=True)


# In[13]:

gdf.replace("None",str(NODATA_VALUE),inplace=True)


# In[14]:

gdf["pfaf_id"] = pd.to_numeric(gdf["pfaf_id"])
gdf["aqid"] = pd.to_numeric(gdf["aqid"])


# In[15]:

gdf = gdf.sort_values("string_id")


# In[16]:

gdf["aq30_id"] = gdf.index


# In[17]:

gdf = gdf.reindex(sorted(gdf.columns), axis=1)


# In[18]:

gdf.head()


# In[19]:

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


# In[20]:

gdf.shape


# In[ ]:

gdfFromSQL = uploadGDFtoPostGIS(gdf,OUTPUT_TABLE_NAME,False)


# In[ ]:

gdfFromSQL.shape


# In[ ]:

gdfFromSQL.head()


# In[ ]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,OUTPUT_TABLE_NAME)


# In[ ]:

gdfFromSQL.to_gbq(destination_table=destination_table,
                  project_id=BQ_PROJECT_ID,
                  chunksize=1000,
                  if_exists="replace")


# In[ ]:

output_file_path = "{}/{}".format(ec2_output_path,SCRIPT_NAME)


# In[ ]:

gdf.to_pickle(output_file_path + ".pkl")


# In[ ]:

gdf.to_file(output_file_path + ".shp",driver="ESRI Shapefile")


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 0:01:12.245867  
# 0:48:09.273757

# In[ ]:



