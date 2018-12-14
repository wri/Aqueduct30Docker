
# coding: utf-8

# In[1]:

""" Create horizontal table for readability. 
-------------------------------------------------------------------------------

Data is strored vertically in bigquery which means each indicator has its 
own row. This script puts the verious indicators as columns in a new,
horizontal table. Additional useful attributes are added. 

gadm metadata:
https://gadm.org/metadata.html
 

Author: Rutger Hofste
Date: 20181214
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = 'Y2018M12D14_RH_Master_Horizontal_GPD_V01'
OUTPUT_VERSION = 1


# AWS RDS PostGIS
DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

POSTGIS_INPUT_TABLE_NAME = "y2018m12d06_rh_master_shape_v01_v02"

# BigQuery 
BQ_IN = {}
# gadm
BQ_IN["GADM36L01"] = "y2018m11d12_rh_gadm36_level1_rds_to_bq_v01_v01"
# Area 
BQ_IN["area"] = 'y2018m12d07_rh_process_area_bq_v01_v01'

# too slow, using s3 instead
BQ_IN["indicators"] = 'y2018m12d11_rh_master_weights_gpd_v02_v01'
S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M12D11_RH_Master_Weights_GPD_V02/output_V03"

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

ec2_input_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("PostGIS table name: ", POSTGIS_INPUT_TABLE_NAME,
      "\nBQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
      "\nBQ_OUTPUT_TABLE_NAME: ", BQ_OUTPUT_TABLE_NAME,
      "\ns3_output_path: ", s3_output_path,
      "\nec2_output_path:" , ec2_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive')


# In[5]:

import os
import pandas as pd
import geopandas as gpd
import numpy as np
import sqlalchemy
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)

get_ipython().magic('matplotlib inline')
pd.set_option('display.max_columns', 500)


# In[6]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))


# In[ ]:

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


# In[7]:

sql = """
SELECT
    aq30_id,
    string_id,
    pfaf_id,
    gid_1, 
    aqid,
    geom
FROM {}
""".format(POSTGIS_INPUT_TABLE_NAME)


# In[8]:

gdf =gpd.GeoDataFrame.from_postgis(sql,engine,geom_col='geom')


# In[9]:

gdf.head()


# In[10]:

gdf.shape


# In[11]:

gdf_master = gdf


# In[12]:

sql_gadm = """
SELECT
  gid_1,
  name_1,
  gid_0,
  name_0,
  varname_1,
  nl_name_1,
  type_1,
  engtype_1,
  cc_1,
  hasc_1
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["GADM36L01"])


# In[13]:

df_gadm = pd.read_gbq(query=sql_gadm,dialect="standard")


# In[14]:

gdf_master = pd.merge(left=gdf_master,
                      right=df_gadm,
                      left_on ="gid_1",
                      right_on = "gid_1",
                      how = "left")


# In[15]:

sql_area = """
SELECT
  string_id,
  area_km2
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["area"])
df_area = pd.read_gbq(query=sql_area,dialect="standard")


# In[16]:

gdf_master = pd.merge(left=gdf_master,
                      right=df_area,
                      left_on ="string_id",
                      right_on = "string_id",
                      how = "left")


# In[17]:

sql_in = """
SELECT
  string_id,
  indicator,
  group_short,
  industry_short,
  raw,
  score,
  cat,
  label,
  weight_fraction,
  weighted_score
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["indicators"])
#df_in = pd.read_gbq(query=sql_in,dialect="standard") # Takes too long, reverting to pickled file instead


# In[18]:

source_path = "{}/Y2018M12D11_RH_Master_Weights_GPD_V02.pkl".format(ec2_input_path)


# In[19]:

df_in = pd.read_pickle(source_path)


# In[20]:

df_in.head()


# # Append (horizontally) all indicators

# In[21]:

indicators = list(df_in["indicator"].unique())
indicators.remove('awr')
for indicator in indicators:
    print(indicator)
    df_sel = df_in.loc[(df_in["industry_short"] == "def") &(df_in["indicator"] == indicator)]
    df_out = df_sel[["string_id","raw","score","cat","label"]]
    df_out.columns = ["string_id",
                      indicator + "_raw",
                      indicator +"_score",
                      indicator +"_cat",
                      indicator +"_label"]
    gdf_master = pd.merge(left=gdf_master,
                          right=df_out,
                          left_on ="string_id",
                          right_on = "string_id",
                          how="left")


# In[22]:

gdf_master.loc[gdf_master["string_id"] == "253001-SJM.2_1-89"]


# # Append (horizontally) all aggregated water risk scores

# In[24]:

indicator = "awr"
industries = list(df_in["industry_short"].unique())
groups = list(df_in["group_short"].unique())

for industry in industries:
    for group in groups:
        print(industry,group)
        df_sel = df_in.loc[(df_in["industry_short"] == industry) &(df_in["group_short"] == group) &(df_in["indicator"] == indicator)]
        df_out = df_sel[["string_id","raw","score","cat","label"]]

        df_out.columns = ["string_id",
                          "w_{}_{}_{}_raw".format(indicator,industry,group),
                          "w_{}_{}_{}_score".format(indicator,industry,group),
                          "w_{}_{}_{}_cat".format(indicator,industry,group),
                          "w_{}_{}_{}_label".format(indicator,industry,group)]
        gdf_master = pd.merge(left=gdf_master,
                              right=df_out,
                              left_on ="string_id",
                              right_on = "string_id",
                              how="left")


# In[25]:

gdf_master.head()


# In[26]:

gdf_master.sort_index(axis=1,inplace=True)


# In[27]:

gdf_master.shape


# # Save in multiple formats:
# 
# 1. Geopackage
# 1. CSV (no geom)
# 1. Pickle 
# 1. Bigquery 
# 1. PostGIS
# 
# 

# In[36]:

df_master =gdf_master.drop("geom",axis=1)


# In[42]:

destination_path_gpkg = "{}/{}.gpkg".format(ec2_output_path,SCRIPT_NAME)
destination_path_csv = "{}/{}.csv".format(ec2_output_path,SCRIPT_NAME)
destination_path_pkl = "{}/{}.pkl".format(ec2_output_path,SCRIPT_NAME)
output_table_name = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()


# In[ ]:

gdf_master.to_file(filename=destination_path_gpkg,driver="GPKG",encoding ='utf-8')


# In[31]:

gdf_master.to_pickle(destination_path_pkl)


# In[39]:

df_master.to_csv(destination_path_csv, encoding='utf-8')


# In[32]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

gdfFromSQL = uploadGDFtoPostGIS(gdf_master,output_table_name,False)


# In[ ]:

gdfFromSQL.to_gbq(destination_table=destination_table,
                  project_id=BQ_PROJECT_ID,
                  chunksize=1000,
                  if_exists="replace")


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 
