
# coding: utf-8

# In[1]:

""" Rasterize indicators at 30s as input for aggregation.
-------------------------------------------------------------------------------

rasterize indicators at 30s resolution. Note that a simplified version of the
geometry is used to speed up calculations. 

Added -at (all touch) to the rasterize command to avoid gaps.

rasterizing is intesive and takes 20 minutes per indicator. 

Author: Rutger Hofste
Date: 20190108
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

"""

TESTING = 0
SCRIPT_NAME = "Y2019M01D10_RH_GA_Rasterize_Indicators_GDAL_V01"
OUTPUT_VERSION = 3

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160

X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
RDS_INPUT_TABLE ="y2018m12d06_rh_master_shape_v01_v02"

BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME = "y2018m12d11_rh_master_weights_gpd_v02_v10"

GDAL_RASTERIZE_PATH = "/opt/anaconda3/envs/python35/bin/gdal_rasterize"

GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}".format(SCRIPT_NAME)

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("RDS_INPUT_TABLE: " + RDS_INPUT_TABLE +
      "\nBQ_INPUT_TABLE_NAME: " + BQ_INPUT_TABLE_NAME +
      "\ns3_output_path: " + s3_output_path,
      "\nGCS_OUTPUT_PATH:"+ GCS_OUTPUT_PATH)


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

import os
import subprocess
import sqlalchemy
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[5]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))
connection = engine.connect()


# In[6]:

sql = """
SELECT
  string_id,
  pfaf_id,
  gid_1,
  aqid,
  ST_SimplifyPreserveTopology(geom,0.00833333) as geom
FROM
  {}
""".format(RDS_INPUT_TABLE)


# In[7]:

gdf = gpd.read_postgis(sql=sql,
                       con=engine)


# In[8]:

gdf.shape


# In[9]:

gdf.head()


# In[10]:

indicators = ["bws","bwd","iav","sev","gtd","drr","rfr","cfr","ucw","cep","udw","usa","rri"]


# In[11]:

def rasterize_indicator(indicator):
    """ Rasterize Indicator Score at 30 arc seconds
    resolution.
    
    """
    bq_sql = """
    SELECT
      string_id,
      score
    FROM
      `{}.{}.{}`
    WHERE
      industry_short ='def'
      AND indicator = '{}'
    """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE_NAME,indicator)
    df = pd.read_gbq(query=bq_sql,
                 dialect="standard")
    gdf_merged = gdf.merge(df,on="string_id")
    destination_path_shp = "{}{}.shp".format(ec2_input_path,indicator)
    destination_path_tif = "{}{}.tif".format(ec2_output_path,indicator)
    gdf_merged.to_file(filename=destination_path_shp,
                   driver="ESRI Shapefile")
    command = "{} -a {} -at -ot Integer64 -of GTiff -te -180 -90 180 90 -ts {} {} -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6 -l {} -a_nodata -9999 {} {}".format(GDAL_RASTERIZE_PATH,"score",X_DIMENSION_30S,Y_DIMENSION_30S,indicator,destination_path_shp,destination_path_tif)
    response = subprocess.check_output(command,shell=True)


# In[ ]:

for indicator in indicators:
    print(indicator)
    rasterize_indicator(indicator)


# In[ ]:

get_ipython().system('gsutil -m cp -r {ec2_output_path} {GCS_OUTPUT_PATH}')


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# \> 2 uur
# 

# In[ ]:



