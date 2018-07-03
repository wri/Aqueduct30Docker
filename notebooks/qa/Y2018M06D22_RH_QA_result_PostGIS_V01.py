
# coding: utf-8

# In[9]:

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
OUTPUT_VERSION = 3

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = 'y2018m06d28_rh_ws_full_range_ols_postgis_30spfaf06_v01_v02'
GEOM_TABLE = 'hybas06_v04'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)
OUTPUT_SCHEMA = "test"

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/qaData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input table: " + INPUT_TABLE_NAME,
      "\nOutput ec2: " + ec2_output_path,
      "\nOutput s3: " + s3_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

if OVERWRITE_OUTPUT:
    get_ipython().system('rm -r {ec2_output_path}')
    get_ipython().system('mkdir -p {ec2_output_path}')
else:
    get_ipython().system('mkdir -p {ec2_output_path}')
    


# In[4]:

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

pd.set_option('display.max_columns', 500)


# In[5]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))


# In[6]:

# What to compare

sql = "SELECT *"
sql +=" FROM {}".format(INPUT_TABLE_NAME)
sql +=" WHERE pfafid_30spfaf06 = 261492"
sql +=" AND temporal_resolution = 'year'"
sql +=" LIMIT 100000"
print(sql)
df = pd.read_sql(sql,engine)



# In[ ]:




# In[7]:

# Select 2014 for annual and monthly ols full range water stress

temporal_resolutions = ["year","month"]

year = 2014 

dfs = {}

for temporal_resolution in temporal_resolutions:
    if temporal_resolution == 'year':
        sql = "SELECT *"
        sql +=" FROM {}".format(INPUT_TABLE_NAME)
        sql +=" WHERE"
        # filter
        sql +=" year = {:04.0f}".format(year) 
        sql +=" AND temporal_resolution = 'year'"
        sql +=" LIMIT 100000"
        df = pd.read_sql(sql,engine)
        
        output_file_name = "full_range_ols_ws_{}_Y{:04.0f}.csv".format(temporal_resolution,year)
        output_file_path = ec2_output_path + "/" + output_file_name
        print(output_file_path)
        
        df = df.fillna(-9999)
        df.to_csv(output_file_path)
        dfs[output_file_name] = df
    elif temporal_resolution == 'month':
        for month in range(1,13):
            print(month)
            sql = "SELECT *"
            sql +=" FROM {}".format(INPUT_TABLE_NAME)
            sql +=" WHERE"
            # filter
            sql +=" year = {:04.0f}".format(year) 
            sql +=" AND temporal_resolution = 'month'"
            sql +=" AND month = {}".format(month)
            sql +=" LIMIT 100000"
            df = pd.read_sql(sql,engine)
            output_file_name = "full_range_ols_ws_{}_Y{:04.0f}M{:02.0f}.csv".format(temporal_resolution,year,month)
            output_file_path = ec2_output_path + "/" + output_file_name
            print(output_file_path)
            
            df = df.fillna(-9999)
            df.to_csv(output_file_path)
            dfs[output_file_name] = df
    else:
        break





# In[8]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:




# In[ ]:




# In[ ]:

df = dfs["full_range_ols_ws_year_Y2014.csv"]


# In[ ]:

df.dtypes


# In[ ]:

df2 = df.loc[df["ols_ols10_waterstress_dimensionless_30spfaf06"] == -9999]


# In[ ]:

df2.dtypes


# In[ ]:

dfs.keys()


# In[ ]:

ax1 = df.plot.scatter("year","waterstress_dimensionless_30spfaf06")
ax1.set_ylim(df["waterstress_dimensionless_30spfaf06"].min(),df["waterstress_dimensionless_30spfaf06"].max())


# In[ ]:

ax1 = df.plot.scatter("year","ols10_waterstress_dimensionless_30spfaf06")
ax1.set_ylim(df["ols10_waterstress_dimensionless_30spfaf06"].min(),df["ols10_waterstress_dimensionless_30spfaf06"].max())


# In[ ]:

ax1 = df.plot.scatter("year","ols_ols10_waterstress_dimensionless_30spfaf06")
ax1.set_ylim(df["ols_ols10_waterstress_dimensionless_30spfaf06"].min(),df["ols_ols10_waterstress_dimensionless_30spfaf06"].max())


# In[ ]:

palette = bokeh.palettes.Category20


# In[ ]:

output_notebook()


# In[ ]:

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



