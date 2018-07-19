
# coding: utf-8

# In[1]:

""" Compare weighted and unweighted annual results for all basins.
-------------------------------------------------------------------------------

Issue: Mapbox GL does not (yet) support tooltips with multiple items when the 
data is joined locally thereby limiting performance for larger tables.
This script will therefore use the cartoframes option. Note that this repo is 
in active developement so this code will likely break in the future.

Steps:

1. Query postGIS table from RDS
1. Query result table from Bigquery
1. Upload result data to Carto
1. Join data in Carto
1. Define tooltip
1. Define plotting
1. Plot. 


Author: Rutger Hofste
Date: 20180718
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M07D18_RH_QA_Annual_Weighted_Unweighted_OneBasin_V01'
OUTPUT_VERSION = 1

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"

BQ_PROJECT_ID = "aqueduct30"
BQ_INPUT_TABLE_NAME = "Y2018M07D17_RH_RDS_To_S3_V01"
BQ_INPUT_DATASET_NAME = "aqueduct30v01"

CARTO_INPUT_TABLE_NAME_LEFT = "y2018m07d18_rh_upload_hydrobasin_carto_v01_v01"

YEAR_OF_INTEREST = 2014
MONTH_OF_INTEREST = 12
TEMPORAL_RESOLUTION_OF_INTEREST = 'year' #If year, then use month 12

COLUMNS_OF_INTEREST = ["pfafid_30spfaf06",
                       "temporal_resolution",
                       "year",
                       "month",
                       "waterstress_label_dimensionless_30spfaf06",
                       "waterstress_category_dimensionless_30spfaf06",
                       "waterstress_score_dimensionless_30spfaf06",
                       "waterstress_raw_dimensionless_30spfaf06",
                       "avg1y_ols_ols10_weighted_waterstress_dimensionless_30spfaf06",
                       "avg1y_ols_ols10_waterstress_dimensionless_30spfaf06",
                       "ols_ols10_waterstress_dimensionless_30spfaf06",
                       "ols_ols10_ptotww_m_30spfaf06"]

carto_output_table_name = "{}_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("carto_output_table_name: ",carto_output_table_name)




# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().magic('matplotlib inline')
import os
import json
import mapboxgl
import sqlalchemy
import pandas as pd
import geopandas as gpd

import cartoframes
from cartoframes.contrib import vector

#from cartoframes import CartoContext, Credentials


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"


# In[4]:

F = open("/.carto","r")
carto_api_key = F.read().splitlines()[0]
F.close()


# In[5]:

creds = cartoframes.Credentials(key=carto_api_key, 
                    username='wri-01')
cc = cartoframes.CartoContext(creds=creds)


# In[6]:

# Query postGIS table from RDS (already on Carto)


# In[7]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))


# In[8]:

# Query result table from Bigquery


# In[9]:

sql = "SELECT"
for column_of_interest in COLUMNS_OF_INTEREST:
    sql += " {},".format(column_of_interest)
sql = sql[:-1]
sql += " FROM {}.{}".format(BQ_INPUT_DATASET_NAME,BQ_INPUT_TABLE_NAME)
sql += " WHERE year = 2014"
sql += " AND month = {}".format(MONTH_OF_INTEREST)
sql += " AND temporal_resolution = '{}'".format(TEMPORAL_RESOLUTION_OF_INTEREST)


# In[10]:

print(sql)


# In[12]:

df = pd.read_gbq(query=sql,
                 project_id=BQ_PROJECT_ID,
                 dialect="standard")


# In[14]:

# Upload result data to Carto
cc.write(df=df,
         table_name=carto_output_table_name,
         overwrite=True,
         privacy="link")


# In[15]:

# There are now two tables on carto. One with the geometries, one with the results from BigQuery. Combining both
columns_to_keep_left = ["pfaf_id",
                        "the_geom",
                        "the_geom_webmercator", #This column is a reprojection of the 'the_geom' column.
                        "cartodb_id"]

columns_to_keep_right = COLUMNS_OF_INTEREST # Same as for BigQuery

left_on = "pfaf_id"
right_on = "pfafid_30spfaf06"


# In[18]:

sql= "SELECT" 
for column_to_keep_left in columns_to_keep_left:
    sql += " l.{},".format(column_to_keep_left)
for column_to_keep_right in columns_to_keep_right:
    sql += " r.{},".format(column_to_keep_right)
sql = sql[:-1]    
sql+= " FROM {} l, {} r".format(CARTO_INPUT_TABLE_NAME_LEFT,carto_output_table_name)
sql+= " WHERE l.{} = r.{}".format(left_on,right_on)




# In[19]:

print(sql)


# In[31]:

int_dict = {"event":'click',
            "cols":['pfaf_id'] +
                    COLUMNS_OF_INTEREST}


# In[32]:

int_dict


# In[33]:

vl_01 = vector.QueryLayer(query=sql,
                          color="#9e9e9e",
                          size=None,
                          time=None,
                          strokeColor="#000000",
                          strokeWidth=None,
                          interactivity=int_dict)


# In[34]:

vector.vmap([vl_01],context=cc)


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

carto_sql = "SELECT * FROM y2018m07d18_rh_upload_hydrobasin_carto_v01_v01"


# In[ ]:

v0 = vector.QueryLayer(query=carto_sql)


# In[ ]:

vector.vmap([v0 ], context=cc)


# In[ ]:

cc.map()


# In[ ]:

from cartoframes.examples import read_taxi


# In[ ]:

cc.write(
    read_taxi(),
    'taxi_50k',
    lnglat=('pickup_longitude', 'pickup_latitude')
)


# In[ ]:

cc.map(
    QueryLayer('''
        SELECT
            ST_Transform(the_geom, 3857) AS the_geom_webmercator,
            the_geom,
            cartodb_id,
            ST_Length(the_geom::geography) AS distance
        FROM (
            SELECT
                ST_MakeLine(
                    CDB_LatLng(pickup_latitude, pickup_longitude),
                    CDB_LatLng(dropoff_latitude, dropoff_longitude)
                ) AS the_geom,
                cartodb_id
            FROM taxi_50k
            WHERE pickup_latitude <> 0 AND dropoff_latitude <> 0
        ) AS _w
        ORDER BY 4 DESC''',
        color='distance'),
    zoom=11, lng=-73.9442, lat=40.7473,
    interactive=False)


# In[ ]:




# In[ ]:

gdf.shape[0]-df.shape[0]


# In[ ]:

gdf2 = gdf.merge(df,
                 how= "left",
                 left_on="pfaf_id",
                 right_on="pfafid_30spfaf06")


# In[ ]:

test = gdf2.loc[pd.isna(gdf2["pfafid_30spfaf06"])]


# In[ ]:

test


# In[ ]:

# 1. Upload to Carto


# In[ ]:

gdf2_small = gdf2[0:1000]


# In[ ]:

cc.write(gdf2,
         encode_geom=True,
         table_name='cartoframes_geopandas',
         overwrite=True)


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

F = open("/.mapbox_public","r")
token = F.read().splitlines()[0]
F.close()
os.environ["MAPBOX_ACCESS_TOKEN"] = token


# In[ ]:

df.head()


# In[ ]:

df["diff_weighted_nonweighted"] = df["avg1y_ols_ols10_weighted_waterstress_dimensionless_30spfaf06"] - df["avg1y_ols_ols10_waterstress_dimensionless_30spfaf06"]


# In[ ]:

data = json.loads(df.to_json(orient='records'))


# In[ ]:

color_stops_raw = [[-0.0001,'rgb(241,12,249)'],
                  #[-0.001,'rgb(255,0,84)'], 
                   [0,'rgb(255,255,153)'], # low
                   [0.1,'rgb(255,230,0)'], # low to medium
                   [0.2,'rgb(255,153,0)'], # Medium to High
                   [0.4,'rgb(255,25,0)'], # High
                   [0.8,'rgb(153,0,0)']]  # Extremely High


# In[ ]:

viz = mapboxgl.viz.ChoroplethViz(data = data, 
                                 vector_url='mapbox://rutgerhofste.hybas06_v04_V04',
                                 vector_layer_name='hybas06_v04', # Warning should match name on mapbox.
                                 vector_join_property='pfaf_id',
                                 data_join_property= "pfafid_30spfaf06",
                                 color_property= "avg1y_ols_ols10_waterstress_dimensionless_30spfaf06",
                                 color_stops= color_stops_raw,
                                 line_color = 'rgba(0,0,0,0.05)',
                                 line_width = 0.5,
                                 opacity=0.7,
                                 center=(5, 52),
                                 zoom=4,
                                 below_layer='waterway-label'
                                 )


# In[ ]:

viz.show()


# In[ ]:

viz2 = mapboxgl.viz.ChoroplethViz(data = data, 
                                 vector_url='mapbox://rutgerhofste.hybas06_v04_V04',
                                 vector_layer_name='hybas06_v04', # Warning should match name on mapbox.
                                 vector_join_property='pfaf_id',
                                 data_join_property= "pfafid_30spfaf06",
                                 color_property= "avg1y_ols_ols10_weighted_waterstress_dimensionless_30spfaf06",
                                 color_stops= color_stops_raw,
                                 line_color = 'rgba(0,0,0,0.05)',
                                 line_width = 0.5,
                                 opacity=0.7,
                                 center=(5, 52),
                                 zoom=4,
                                 below_layer='waterway-label'
                                 )


# In[ ]:

viz2.show()


# In[ ]:

color_stops_dif = [[-1,'rgb(255,0,0)'],
                   [0,'rgb(255,255,255)'], 
                   [1,'rgb(0,255,0)']]


# In[ ]:

viz3 = mapboxgl.viz.ChoroplethViz(data = data, 
                                 vector_url='mapbox://rutgerhofste.hybas06_v04_V04',
                                 vector_layer_name='hybas06_v04', # Warning should match name on mapbox.
                                 vector_join_property='pfaf_id',
                                 data_join_property= "pfafid_30spfaf06",
                                 color_property= "diff_weighted_nonweighted",
                                 color_stops= color_stops_dif,
                                 line_color = 'rgba(0,0,0,0.05)',
                                 line_width = 0.5,
                                 opacity=0.7,
                                 center=(5, 52),
                                 zoom=4,
                                 below_layer='waterway-label'
                                 )


# In[ ]:

viz3.show()


# In[ ]:



