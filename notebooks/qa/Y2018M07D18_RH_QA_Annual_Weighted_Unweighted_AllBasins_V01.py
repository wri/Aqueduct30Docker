
# coding: utf-8

# In[22]:

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

Unfortunately the plot area is very small and the tooltip's font size is huge.
Visualizing using the webtool might be an alternative. 

Expected result: A Carto Map with 13 layers. An Annual layer and 12 months, 
stylized.

using carto: Reaching account limits within notime. Probably easiest to switch 
back to shapefile plus csv files method. pff 


Author: Rutger Hofste
Date: 20180718
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M07D18_RH_QA_Annual_Weighted_Unweighted_AllBasins_V01'
OUTPUT_VERSION = 2

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"

BQ_PROJECT_ID = "aqueduct30"
BQ_INPUT_TABLE_NAME = "Y2018M07D17_RH_RDS_To_S3_V01"
BQ_INPUT_DATASET_NAME = "aqueduct30v01"

CARTO_INPUT_TABLE_NAME_LEFT = "y2018m07d18_rh_upload_hydrobasin_carto_v01_v02"

YEAR_OF_INTEREST = 2014

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

# tooltips are not scrollable so pick a limited number of items to visualize.
COLUMNS_TO_VISUALIZE = ["pfafid_30spfaf06",
                        "waterstress_score_dimensionless_30spfaf06",
                        "avg1y_ols_ols10_weighted_waterstress_dimensionless_30spfaf06",
                        "avg1y_ols_ols10_waterstress_dimensionless_30spfaf06"]

COLOR_COLUMN = "waterstress_score_dimensionless_30spfaf06"


carto_output_table_name = "{}_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("carto_output_table_name: ",carto_output_table_name)




# In[23]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[24]:

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


# In[25]:

F = open("/.carto_builder","r")
carto_api_key = F.read().splitlines()[0]
F.close()


# In[26]:

creds = cartoframes.Credentials(key=carto_api_key, 
                    username='wri-playground')
cc = cartoframes.CartoContext(creds=creds)


# In[27]:

# Query postGIS table from RDS (already on Carto)


# In[28]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))


# In[29]:

# Query result table from Bigquery


# In[30]:

sql = "SELECT"
for column_of_interest in COLUMNS_OF_INTEREST:
    sql += " {},".format(column_of_interest)
sql = sql[:-1]
sql += " FROM {}.{}".format(BQ_INPUT_DATASET_NAME,BQ_INPUT_TABLE_NAME)
sql += " WHERE year = 2014"
# sql += " AND month = {}".format(MONTH_OF_INTEREST)
# sql += " AND temporal_resolution = '{}'".format(TEMPORAL_RESOLUTION_OF_INTEREST)


# In[31]:

print(sql)


# In[32]:

df = pd.read_gbq(query=sql,
                 project_id=BQ_PROJECT_ID,
                 dialect="standard")


# In[33]:

df.shape


# In[34]:

# Upload result data to Carto
cc.write(df=df,
         table_name=carto_output_table_name,
         overwrite=True,
         privacy="link")


# In[35]:

index_columns = ["pfafid_30spfaf06","year","month","temporal_resolution"]


# In[38]:

# Create indices

for index_column in index_columns:
    sql_index = "CREATE INDEX idx_allbasin_v{:02.0f}_{} ON {} ({})".format(OUTPUT_VERSION,index_column,carto_output_table_name,index_column)
    print(sql_index )
    cc.query(sql_index)


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

# There are now two tables on carto. One with the geometries, one with the results from BigQuery. Combining both
columns_to_keep_left = ["pfaf_id",
                        "the_geom",
                        "the_geom_webmercator", #This column is a reprojection of the 'the_geom' column.
                        "cartodb_id"]

columns_to_keep_right = COLUMNS_OF_INTEREST # Same as for BigQuery

left_on = "pfaf_id"
right_on = "pfafid_30spfaf06"


# In[ ]:

def create_query(temporal_resolution,year,month):
    sql= "SELECT" 
    for column_to_keep_left in columns_to_keep_left:
        sql += " l.{},".format(column_to_keep_left)
    for column_to_keep_right in columns_to_keep_right:
        sql += " r.{},".format(column_to_keep_right)
    sql = sql[:-1]    
    sql+= " FROM {} l, {} r".format(CARTO_INPUT_TABLE_NAME_LEFT,carto_output_table_name)
    sql+= " WHERE l.{} = r.{}".format(left_on,right_on)
    sql+= " AND r.year = {}".format(year)
    sql+= " AND r.month ={}".format(month)
    sql+= " AND r.temporal_resolution = '{}'".format(temporal_resolution)
    
    return sql



# In[ ]:

temporal_resolutions = ["year","month"]
year = YEAR_OF_INTEREST


# In[ ]:

for temporal_resolution in temporal_resolutions:
    if temporal_resolution == "year":
        month = 12
        
        sql = create_query(temporal_resolution,year,month)     
        table_name = "temp_table_{}_y{}m{}".format(temporal_resolution,year,month)
        print(temporal_resolution,year,month,table_name)
        cc.query(query=sql,
                 table_name=table_name)
        
        
    else:
        for month in range(1,12+1):
            sql = create_query(temporal_resolution,year,month)
            table_name = "temp_table_{}_y{}m{}".format(temporal_resolution,year,month)
            print(temporal_resolution,year,month,table_name)
            cc.query(query=sql,
                     table_name=table_name)
            
            


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



