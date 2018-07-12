
# coding: utf-8

# In[1]:

""" Inspect waters stress after applying the aridlowwateruse once mask.
-------------------------------------------------------------------------------

15% of the basins is arid and lowwateruse. 

Warning: Aqueduct Categories rank from 1 - 5

max(0,min(5,((np.log(r)-np.log(0.1))/np.log(2))+1))

[0-1) Low
[1-2) Low to Medium
[2-3) Medium to High
[3-4) High 
[4-5] Extremely High

raw values are converted to scores and can result to scores in range [0-5] 
including start and endpoint. To go from scores to categories (binned) 

from score to category
0.0 need to become 1
5.0 needs to become 5



Author: Rutger Hofste
Date: 201807010
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M0710_RH_QA_Water_Stress_Masked_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = 'y2018m07d10_rh_update_waterstress_aridlowonce_postgis_v01_v02'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

OUTPUT_SCHEMA = "qa"

print("Input table: " + INPUT_TABLE_NAME,
      "\nOutput table: " + OUTPUT_SCHEMA +"."+ OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

# imports
import os
import re
import os
import json
import getpass
import geojson
import mapboxgl
import aqueduct3
import sqlalchemy
import numpy as np
import pandas as pd
import geopandas as gpd
from bokeh.plotting import figure 
from bokeh.io import output_notebook, show
from bokeh.models import HoverTool
from datetime import timedelta
get_ipython().magic('matplotlib inline')
pd.set_option('display.max_columns', 500)


# In[4]:

F = open("/.mapbox_public","r")
token = F.read().splitlines()[0]
F.close()
os.environ["MAPBOX_ACCESS_TOKEN"] = token

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()


# In[5]:

columns = ["pfafid_30spfaf06",
           "temporal_resolution",
           "year",
           "month",
           "ols_ols10_aridandlowwateruse_boolean_30spfaf06",
           "waterstress_raw_dimensionless_30spfaf06",
           "waterstress_score_dimensionless_30spfaf06"]


# In[6]:

sql = "SELECT"
for column in columns:
    sql += " {},".format(column)
sql = sql[:-1]
sql += " FROM {}".format(INPUT_TABLE_NAME)
sql += " WHERE year= 2014 AND temporal_resolution = 'year'"


# In[7]:

sql


# In[8]:

df = pd.read_sql(sql,engine)


# In[9]:

df_small = df[0:10000]


# In[10]:

df_small.head()


# In[ ]:




# In[11]:

# Mapbox Settings
color_stops_raw = [[-1,'rgb(128,128,128)'],
                  #[-0.001,'rgb(255,0,84)'], 
                   [0,'rgb(255,255,153)'], # low
                   [0.2,'rgb(255,230,0)'], # low to medium
                   [0.4,'rgb(255,153,0)'], # Medium to High
                   [0.8,'rgb(255,25,0)'], # High
                   [1,'rgb(153,0,0)']]  # Extremely High

color_stops_scores = [[-1,'rgb(128,128,128)'],
                  #[-0.001,'rgb(255,0,84)'], 
                   [1,'rgb(255,255,153)'], # low
                   [2,'rgb(255,230,0)'], # low to medium
                   [3,'rgb(255,153,0)'], # Medium to High
                   [4,'rgb(255,25,0)'], # High
                   [5,'rgb(153,0,0)']]  # Extremely High

# Pandas Settings:
labels_raw = ["low [0 - 0.1)",
              "low-medium [0.1 - 0.2)",
              "medium-high [0.2 - 0.4)",
              "high [0.4 - 0.8)",
              "very high [0.8 - inf]"]

labels_scores = ["low [0 - 1)",
                 "low-medium [1 - 2)",
                 "medium-high [2 - 3)",
                 "high [3 - 4)",
                 "very high [4 - 5]"]

bins_raw=[0,0.1,0.2,0.4,0.8,9999]
bins_scores = [-1,1,2,3,4,5]





# In[12]:




# In[ ]:


          


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

def create_viz(case):
    viz = mapboxgl.viz.ChoroplethViz(data = case["json"], 
                                      vector_url='mapbox://rutgerhofste.hybas06_v04_V04',
                                      vector_layer_name='hybas06_v04', # Warning should match name on mapbox.
                                      vector_join_property='pfaf_id',
                                      data_join_property=case["dimension"],
                                      color_property=case["measure"],
                                      color_function_type='match',
                                      color_stops= case["color_stops"],
                                      line_color = 'rgba(0,0,0,0.05)',
                                      line_width = 0.5,
                                      opacity=0.7,
                                      center=(5, 52),
                                      zoom=4,
                                      below_layer='waterway-label'
                                      )
    return viz


# In[ ]:

cases = {}


# In[ ]:

case = {}
case["description"] = "Arid"
case["id"] = "annual_aridandlowateruse"
case["df"] = df.copy()
case["measure"] = "ols_ols10_aridandlowwateruse_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)']]
case["viz"] = create_viz(case)

cases[case["id"]] = case


# In[ ]:

viz = cases["annual_aridandlowateruse"]["viz"]
viz.show()


# In[ ]:

def ws_r_to_s(r):
    # Convert raw water stress value to score; Equation from Aqueduct 2.1.
    return max(0,min(5,((np.log(r)-np.log(0.1))/np.log(2))+1))


# ws categories 
# 
# low [0 - 0.1)  
# low-medium [0.1 - 0.2)  
# medium-high [0.2 - 0.4)  
# high [0.4 - 0.8)  
# very high [0.8 - inf]  
#     
# 
# 

# In[ ]:

labels_raw = ["low [0 - 0.1)",
          "low-medium [0.1 - 0.2)",
          "medium-high [0.2 - 0.4)",
          "high [0.4 - 0.8)",
          "very high [0.8 - inf]"]

labels_scores = ["low [0 - 1)",
                  "low-medium [1 - 2)",
                  "medium-high [2 - 3)",
                  "high [3 - 4)",
                  "very high [4 - 5]"]


bins_raw=[0,0.1,0.2,0.4,0.8,9999]
bins_scores = [0,1,2,3,4,5]
          


# In[ ]:




# In[ ]:

df["ws_cat"] = pd.cut(df["waterstress_score_dimensionless_30spfaf06"],bins=bins_scores,right=False,labels=labels_scores)
df["ws_cat"] = np.where(df["ols_ols10_aridandlowwateruse_boolean_30spfaf06"] == 1, "arid and lowwateruse", df["ws_cat"])
    


# In[ ]:

df.head()


# In[ ]:

case = {}
case["description"] = "water stress"
case["id"] = "annual_waterstress"
case["df"] = df.copy()
case["measure"] = "ws_cat"
case["dimension"] = "pfafid_30spfaf06"

#df_temp = case["df"][[case["dimension"],case["measure"]]]
df_temp = case["df"][[case["dimension"],case["measure"],'year']]

case["json"] = json.loads(df_temp.to_json(orient='records'))


case["color_stops"] = color_stops = [[-1,'rgb(128,128,128)'],
                                     #[-0.001,'rgb(255,0,84)'], 
                                     [0,'rgb(255,255,153)'], # low
                                     [0.2,'rgb(255,230,0)'], # low to medium
                                     [0.4,'rgb(255,153,0)'], # Medium to High
                                     [0.8,'rgb(255,25,0)'], # High
                                     [1,'rgb(153,0,0)']]  # Extremely High

case["color_stops"] = color_stops = [["arid and lowwateruse",'rgb(128,128,128)'],
                                     ["low [0 - 0.1)",'rgb(255,255,153)'], # low
                                     ["low-medium [0.1 - 0.2)",'rgb(255,230,0)'], # low to medium
                                     ["medium-high [0.2 - 0.4)",'rgb(255,153,0)'], # Medium to High
                                     ["high [0.4 - 0.8)",'rgb(255,25,0)'], # High
                                     ["very high [0.8 - inf]",'rgb(153,0,0)']]  # Extremely High


case["viz"] = create_viz(case)

cases[case["id"]] = case


# In[ ]:

case["json"]


# In[ ]:

safename = json.loads(df_temp.to_json(orient='records'))


# In[ ]:

viz = mapboxgl.viz.ChoroplethViz(data = safename, 
                                  vector_url='mapbox://rutgerhofste.hybas06_v04_V04',
                                  vector_layer_name='hybas06_v04', # Warning should match name on mapbox.
                                  vector_join_property='pfaf_id',
                                  data_join_property=case["dimension"],
                                  color_property=case["measure"],
                                  color_function_type='match',
                                  color_stops= case["color_stops"],
                                  line_color = 'rgba(0,0,0,0.05)',
                                  line_width = 0.5,
                                  opacity=0.7,
                                  center=(5, 52),
                                  zoom=4,
                                  below_layer='waterway-label'
                                  )


# In[ ]:

viz.show()


# In[15]:




# In[ ]:



