
# coding: utf-8

# In[ ]:

""" Compare weighted and unweighted annual results for all basins.
-------------------------------------------------------------------------------

Issue: Mapbox GL does not support tooltips with multiple items when the data 
is joined locally yet. Exploring the cartoframes option. For cartoframes 
I need to send the data to carto before visualizing. 

Steps:

1. Query postGIS table from RDS
1. Query result table from Bigquery
1. Join results
1. Upload to Carto
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

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

PROJECT_ID = "aqueduct30"
INPUT_TABLE_NAME = "Y2018M07D17_RH_RDS_To_S3_V01"
INPUT_DATASET_NAME = "aqueduct30v01"

POSTGIS_INPUT_TABLE_NAME = "hybas06_v04"

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


# In[ ]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[ ]:

get_ipython().magic('matplotlib inline')
import os
import json
import mapboxgl
import sqlalchemy
import pandas as pd
import geopandas as gpd
from cartoframes import CartoContext, Credentials
from cartoframes.contrib import vector

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"


# In[ ]:

F = open("/.carto","r")
carto_api_key = F.read().splitlines()[0]
F.close()


# In[ ]:

creds = Credentials(key=carto_api_key, 
                    username='wri-01')
cc = CartoContext(creds=creds)


# In[ ]:

# Query postGIS table from RDS


# In[ ]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))


# In[ ]:




# In[ ]:

sql = "SELECT pfaf_id, sub_area, geom FROM {}".format(POSTGIS_INPUT_TABLE_NAME)


# In[ ]:

gdf =gpd.GeoDataFrame.from_postgis(sql,engine,geom_col='geom' )


# In[ ]:

# Query result table from Bigquery


# In[ ]:

sql = "SELECT"
for column_of_interest in COLUMNS_OF_INTEREST:
    sql += " {},".format(column_of_interest)
sql = sql[:-1]
sql += " FROM {}.{}".format(INPUT_DATASET_NAME,INPUT_TABLE_NAME)
sql += " WHERE year = 2014"
sql += " AND month = {}".format(MONTH_OF_INTEREST)
sql += " AND temporal_resolution = '{}'".format(TEMPORAL_RESOLUTION_OF_INTEREST)


# In[ ]:

print(sql)


# In[ ]:

df = pd.read_gbq(query=sql,
                 project_id=PROJECT_ID,
                 dialect="standard")


# In[ ]:

# Join results (11 tiny basins have noData)


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



