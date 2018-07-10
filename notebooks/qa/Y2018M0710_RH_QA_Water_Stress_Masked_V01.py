
# coding: utf-8

# In[4]:

""" Inspect waters stress after applying the aridlowwateruse once mask.
-------------------------------------------------------------------------------

15% of the basins is arid and lowwateruse. 

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

INPUT_TABLE_NAME = 'y2018m07d10_rh_update_waterstress_aridlowonce_postgis_v01_v01'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

OUTPUT_SCHEMA = "qa"

print("Input table: " + INPUT_TABLE_NAME,
      "\nOutput table: " + OUTPUT_SCHEMA +"."+ OUTPUT_TABLE_NAME)


# In[5]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[6]:

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


# In[12]:

F = open("/.mapbox_public","r")
token = F.read().splitlines()[0]
F.close()
os.environ["MAPBOX_ACCESS_TOKEN"] = token

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()


# In[43]:

columns = ["pfafid_30spfaf06",
           "temporal_resolution",
           "year",
           "month",
           "ols_ols10_aridandlowwateruse_boolean_30spfaf06",
           "waterstress_masked_dimensionless_30spfaf06"]


# In[28]:

sql = "SELECT"
for column in columns:
    sql += " {},".format(column)
sql = sql[:-1]
sql += " FROM {}".format(INPUT_TABLE_NAME)
sql += " WHERE year= 2014 AND temporal_resolution = 'year'"


# In[29]:

sql


# In[30]:

df = pd.read_sql(sql,engine)


# In[31]:

df.shape


# In[109]:

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


# In[33]:

df["ols_ols10_aridandlowwateruse_boolean_30spfaf06"].sum()


# In[42]:

df.head()


# In[35]:

cases = {}


# In[40]:

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


# In[41]:

viz = cases["annual_aridandlowateruse"]["viz"]
viz.show()


# In[130]:

case = {}
case["description"] = "Arid"
case["id"] = "ws_r"
case["df"] = df.copy()
case["measure"] = "waterstress_masked_dimensionless_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)']]
case["viz"] = create_viz(case)

cases[case["id"]] = case


# In[132]:

viz = cases["ws_r"]["viz"]
viz.show()


# In[ ]:




# In[ ]:




# In[ ]:




# In[60]:

df["ws_r"] = np.where(df["ols_ols10_aridandlowwateruse_boolean_30spfaf06"] == 1, -1 , df["waterstress_masked_dimensionless_30spfaf06"])


# In[93]:

df["ws_s"] = df["ws_r"].apply(ws_r_to_s)


# In[108]:

df.head()


# In[ ]:




# In[89]:

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

# In[116]:

labels_raw = ["low [0 - 0.1)",
          "low-medium [0.1 - 0.2)",
          "medium-high [0.2 - 0.4)",
          "high [0.4 - 0.8)",
          "very high [0.8 - inf]"]

labels_scores = [["low [0 - 1)",
                  "low-medium [1 - 2)",
                  "medium-high [2 - 3)",
                  "high [3 - 4)",
                  "very high [4 - 5]"]


bins_raw=[0,0.1,0.2,0.4,0.8,9999]
bins_scores = [0,1,2,3,4,5]
          


# In[117]:

df["ws_cat"] = pd.cut(df["ws_s"],bins=bins_categories,right=False,labels=labels)
df["ws_cat"] = np.where(df["ols_ols10_aridandlowwateruse_boolean_30spfaf06"] == 1, "arid and lowwateruse", df["ws_cat"])
    


# In[125]:

df.head()


# In[128]:

case = {}
case["description"] = "water stress"
case["id"] = "annual_waterstress"
case["df"] = df.copy()
case["measure"] = "ws_cat"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"],"ws_r","ws_s"]]

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


# In[129]:

viz = cases["annual_waterstress"]["viz"]
viz.show()


# In[59]:




# In[ ]:



