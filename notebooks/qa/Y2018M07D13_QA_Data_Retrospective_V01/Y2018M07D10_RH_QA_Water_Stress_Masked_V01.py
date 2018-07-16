
# coding: utf-8

# In[32]:

""" Inspect waters stress after applying the aridlowwateruse once mask.
-------------------------------------------------------------------------------

15% of the basins is arid and lowwateruse. 

Warning: Aqueduct Categories rank from 1 - 5

max(0,min(5,((np.log(r)-np.log(0.1))/np.log(2))+1))

[-9999] NoData 
[-1] Arid and Low Wateruse
[0-1) Low
[1-2) Low to Medium
[2-3) Medium to High
[3-4) High 
[4-5] Extremely High

raw values are converted to scores and can result to scores in range [0-5] 
including start and endpoint. To go from scores to categories (binned) 


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

INPUT_TABLE_NAME = 'y2018m07d12_rh_ws_categorization_label_postgis_v01_v03'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

OUTPUT_SCHEMA = "qa"

print("Input table: " + INPUT_TABLE_NAME,
      "\nOutput table: " + OUTPUT_SCHEMA +"."+ OUTPUT_TABLE_NAME)


# In[33]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[34]:

# imports
import os
import re
import os
import json
import getpass
import geojson
import mapboxgl
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


# In[35]:

F = open("/.mapbox_public","r")
token = F.read().splitlines()[0]
F.close()
os.environ["MAPBOX_ACCESS_TOKEN"] = token

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()


# In[94]:

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
                                      center=(0, 0),
                                      zoom=4,
                                      below_layer='waterway-label'
                                      )
    return viz


# In[119]:

columns = ["pfafid_30spfaf06",
           "temporal_resolution",
           "year",
           "month",
           "ols10_waterstress_dimensionless_30spfaf06",
           "ols_ols10_aridandlowwateruse_boolean_30spfaf06",
           "ols_ols10_waterstress_dimensionless_30spfaf06",
           "avg1y_ols_ols10_waterstress_dimensionless_30spfaf06",
           "waterstress_raw_dimensionless_30spfaf06",
           "waterstress_score_dimensionless_30spfaf06",
           "waterstress_category_dimensionless_30spfaf06",
           "waterstress_label_dimensionless_30spfaf06"]


# In[38]:

# Inspecting Annual results


# In[39]:

sql = "SELECT"
for column in columns:
    sql += " {},".format(column)
sql = sql[:-1]
sql += " FROM {}".format(INPUT_TABLE_NAME)
sql += " WHERE pfafid_30spfaf06 = 161180"

#sql += " WHERE year= 2014 AND temporal_resolution = 'year'"


# In[40]:

sql


# In[41]:

df = pd.read_sql(sql,engine)


# In[66]:

color_stops_category = [[-9999,'rgb(241,12,249)'],
                        [-1,'rgb(128,128,128)'],
                        #[-0.001,'rgb(255,0,84)'], 
                        [0,'rgb(255,255,153)'], # low
                        [1,'rgb(255,230,0)'], # low to medium
                        [2,'rgb(255,153,0)'], # Medium to High
                        [3,'rgb(255,25,0)'], # High
                        [4,'rgb(153,0,0)']]  # Extremely High


color_stops_raw = [[-0.0001,'rgb(241,12,249)'],
                  #[-0.001,'rgb(255,0,84)'], 
                   [0,'rgb(255,255,153)'], # low
                   [0.1,'rgb(255,230,0)'], # low to medium
                   [0.2,'rgb(255,153,0)'], # Medium to High
                   [0.4,'rgb(255,25,0)'], # High
                   [0.8,'rgb(153,0,0)']]  # Extremely High


# In[43]:

cases = {}


# ## Visualize Annual Results

# In[44]:

sql = "SELECT"
for column in columns:
    sql += " {},".format(column)
sql = sql[:-1]
sql += " FROM {}".format(INPUT_TABLE_NAME)
sql += " WHERE temporal_resolution = 'year' AND year = 2014"


# In[45]:

df_annual = pd.read_sql(sql,engine)


# In[46]:

df_annual.shape


# In[47]:

data_annual = json.loads(df_annual.to_json(orient='records'))


# ## Annual Category

# In[96]:

case = {}
case["description"] = "Arid"
case["id"] = "annual_waterstress_cat"
case["df"] = df.copy()
case["measure"] = "waterstress_category_dimensionless_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

case["json"] = data_annual

case["color_stops"] = color_stops_category
case["viz"] = create_viz(case)

cases[case["id"]] = case


# In[97]:

viz = cases["annual_waterstress_cat"]["viz"]
viz.show()


# ## Annual Raw

# In[69]:

case = {}
case["description"] = "Arid"
case["id"] = "annual_waterstress_ols_ols10"
case["df"] = df.copy()
case["measure"] = "ols_ols10_waterstress_dimensionless_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

case["json"] = data

case["color_stops"] = color_stops_raw
case["viz"] = create_viz(case)

cases[case["id"]] = case


# In[70]:

viz = cases["annual_waterstress_ols_ols10"]["viz"]
viz.show()


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[71]:

# How many basins have a negative waterstress score and are not arid?  

df_annual_negative = df_annual.loc[df_annual["ols_ols10_waterstress_dimensionless_30spfaf06"] < 0]


# In[72]:

df_annual_negative.shape


# In[80]:

df_annual_negative.describe()


# In[110]:

df_annual_negative_non_arid = df_annual_negative.loc[df_annual_negative["ols_ols10_aridandlowwateruse_boolean_30spfaf06"] == 0]


# In[111]:

df_annual_negative_non_arid.shape


# In[ ]:

# conclusion: Arid thresholds do not mask subbasins with negative waterstress scores. 


# In[ ]:




# In[ ]:




# In[ ]:




# ## Visualize Monthly Results
#           
#           
# The annual results are the average of the monthly (ols_ols10) water values. 

# In[103]:

sql = "SELECT"
for column in columns:
    sql += " {},".format(column)
sql = sql[:-1]
sql += " FROM {}".format(INPUT_TABLE_NAME)
sql += " WHERE temporal_resolution = 'month' AND year = 2014 AND month = 11"


# In[104]:

df_month = pd.read_sql(sql,engine)


# In[105]:

data_month = json.loads(df_month.to_json(orient='records'))


# In[106]:

case = {}
case["description"] = "Arid"
case["id"] = "monthly_waterstress_cat"
case["df"] = df.copy()
case["measure"] = "waterstress_category_dimensionless_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = data_month

case["color_stops"] = color_stops_category
case["viz"] = create_viz(case)

cases[case["id"]] = case


# In[107]:

viz = cases["monthly_waterstress_cat"]["viz"]
viz.show()


# # subbasin deep dive

# In[121]:

sql = "SELECT"
for column in columns:
    sql += " {},".format(column)
sql = sql[:-1]
sql += " FROM {}".format(INPUT_TABLE_NAME)
sql += " WHERE pfafid_30spfaf06 = 172111"


# In[122]:

df_basin = pd.read_sql(sql,engine)


# In[123]:

df_basin_selection = df_basin.loc[df_basin["temporal_resolution"] == 'year']


# In[ ]:




# In[125]:

output_notebook()
p = figure(width=900, height=600)
p.line(x = df_basin_selection["year"], y = df_basin_selection["ols10_waterstress_dimensionless_30spfaf06"],color="red",legend= "ols10_aridandlowwateruse_boolean_30spfaf06")
p.line(x = df_basin_selection["year"], y = df_basin_selection["ols_ols10_waterstress_dimensionless_30spfaf06"],color="black",legend= "aridandlowwateruse_boolean_30spfaf06")
p.line(x = df_basin_selection["year"], y = df_basin_selection["waterstress_raw_dimensionless_30spfaf06"],color="blue",legend= "aridandlowwateruse_boolean_30spfaf06")


p.legend.location = "top_left"
p.legend.click_policy="hide"
hover = HoverTool(tooltips = [('year', '@x'),
                             ('value',  '@y')])
p.add_tools(hover)

show(p)


# In[ ]:




# In[ ]:




# In[ ]:

# Mapbox Settings


color_stops_scores = [[-1,'rgb(128,128,128)'],
                  #[-0.001,'rgb(255,0,84)'], 
                   [0,'rgb(255,255,153)'], # low
                   [1,'rgb(255,230,0)'], # low to medium
                   [2,'rgb(255,153,0)'], # Medium to High
                   [3,'rgb(255,25,0)'], # High
                   [4,'rgb(153,0,0)']]  # Extremely High

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

