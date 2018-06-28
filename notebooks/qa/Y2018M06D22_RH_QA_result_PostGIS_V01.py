
# coding: utf-8

# In[1]:

""" Create table with result, raw ma10 and ols with geometries.
-------------------------------------------------------------------------------

Create postGIS table for selected basins with all ma_10 indicators

Author: Rutger Hofste
Date: 20180622
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M06D22_RH_QA_result_PostGIS_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = 'y2018m06d04_rh_water_stress_postgis_30spfaf06_v02_v04'
GEOM_TABLE = 'hybas06_v04'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)
OUTPUT_SCHEMA = "test"


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[18]:

# imports
get_ipython().magic('matplotlib inline')
import re
import os
import random
import numpy as np
import pandas as pd
import bokeh.palettes
from datetime import timedelta
from sqlalchemy import *
from bokeh.plotting import figure 
from bokeh.io import output_notebook, show
from bokeh.models import HoverTool

d.set_option('display.max_columns', 500)


# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))


# In[58]:

# What to compare

sql = "SELECT *"
sql +=" FROM {}".format(INPUT_TABLE_NAME)
sql +=" WHERE pfafid_30spfaf06 = 431666"
sql +=" AND temporal_resolution = 'year'"
sql +=" LIMIT 100000"
print(sql)
df = pd.read_sql(sql,engine)



# In[59]:

df.head()


# In[60]:

ax1 = df.plot.scatter("year","waterstress_dimensionless_30spfaf06")
ax1.set_ylim(df["waterstress_dimensionless_30spfaf06"].min(),df["waterstress_dimensionless_30spfaf06"].max())


# In[61]:

palette = bokeh.palettes.Category20


# In[62]:

output_notebook()


# In[63]:

p = figure(width=900, height=800)
p.line(x = df["year"], y = df["waterstress_dimensionless_30spfaf06"],color="black",legend= "10_waterstress_dimensionless_30spfaf06")
p.line(x = df["year"], y = df["ma10_waterstress_dimensionless_30spfaf06"],color="blue",legend= "ma10_waterstress_dimensionless_30spfaf06")
p.line(x = df["year"], y = df["ols10_waterstress_dimensionless_30spfaf06"],color="red",legend= "ols10_waterstress_dimensionless_30spfaf06")

p.legend.location = "top_left"
p.legend.click_policy="hide"
hover = HoverTool(tooltips = [('year', '@x'),
                             ('value',  '@y')])
p.add_tools(hover)

show(p)


# In[ ]:



