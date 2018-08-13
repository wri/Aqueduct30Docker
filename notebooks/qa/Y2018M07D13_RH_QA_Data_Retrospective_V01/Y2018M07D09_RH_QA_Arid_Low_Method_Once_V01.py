
# coding: utf-8

# In[1]:

""" Evaluate performance of setting arid low thresholds once.
-------------------------------------------------------------------------------
Compare several methods for determining arid and lowwateruse per subbasins.

This notebook evaluates Method 2:
Use ols_ols10_xxx for use and withdrawal to set categorization upfront.


Key Takeaways:

Patterns very similar to Aqueduct 2. 

Difference. Noth America less arid


Author: Rutger Hofste
Date: 201807006
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M07D09_RH_QA_Arid_Low_Method_Once_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = 'y2018m07d09_rh_arid_lowwateruse_full_ols_postgis_v01_v03'

print("Input table: " + INPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[4]:

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


# In[5]:

F = open("/.mapbox_public","r")
token = F.read().splitlines()[0]
F.close()
os.environ["MAPBOX_ACCESS_TOKEN"] = token

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()


# In[6]:

def create_viz(case):
    viz = mapboxgl.viz.ChoroplethViz(data = case["json"], 
                                      vector_url='mapbox://rutgerhofste.hybas06_v04_V04',
                                      vector_layer_name='hybas06_v04', # Warning should match name on mapbox.
                                      vector_join_property='pfaf_id',
                                      data_join_property=case["dimension"],
                                      color_property=case["measure"],
                                      color_stops= case["color_stops"],
                                      line_color = 'rgba(0,0,0,0.05)',
                                      line_width = 0.5,
                                      opacity=0.7,
                                      center=(5, 52),
                                      zoom=4,
                                      below_layer='waterway-label'
                                      )
    return viz


# In[7]:

sql = "SELECT * FROM {}".format(INPUT_TABLE_NAME)
sql += " WHERE year= 2014"


# In[8]:

sql


# In[9]:

df = pd.read_sql(sql,engine)


# In[26]:

df.head()


# In[10]:

cases = {}


# In[13]:

case = {}
case["description"] = "Annual Arid"
case["id"] = "annual_arid"
case["df"] = df.copy()
case["measure"] = "ols_ols10_arid_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)']]
case["viz"] = create_viz(case)

cases[case["id"]] = case


# In[14]:

# Subbasins qualified as arid based on full range ols of ol10

viz = cases["annual_arid"]["viz"]
viz.show()


# In[17]:

case = {}
case["description"] = "Annual Lowwateruse"
case["id"] = "annual_lowwateruse"
case["df"] = df.copy()
case["measure"] = "ols_ols10_lowwateruse_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)']]
case["viz"] = create_viz(case)

cases[case["id"]] = case


# In[18]:

# Subbasins qualified as lowwateruse based on full range ols of ol10

viz = cases["annual_lowwateruse"]["viz"]
viz.show()


# In[19]:

case = {}
case["description"] = "Annual aridandlowwateruse"
case["id"] = "annual_aridandlowwateruse"
case["df"] = df.copy()
case["measure"] = "ols_ols10_aridandlowwateruse_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)']]
case["viz"] = create_viz(case)

cases[case["id"]] = case


# In[21]:

# Subbasins qualified as arid and lowwateruse based on full range ols of ol10

viz = cases["annual_aridandlowwateruse"]["viz"]
viz.show()


# In[ ]:



