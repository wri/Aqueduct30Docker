
# coding: utf-8

# In[1]:

""" Check if linear trend is implemented correctly.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180621
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.     

"""


OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M06D21_RH_QA_Linear_Trend_PostGIS_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME_RAW = 'y2018m05d29_rh_total_demand_postgis_30spfaf06_v01_v02'
INPUT_TABLE_NAME_MA = 'y2018m06d01_rh_moving_average_postgis_30spfaf06_v01_v03'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)
OUTPUT_SCHEMA_NAME = "test"

TEST_BASIN_PFAF_ID = 231607
MONTH = 5
TEMPORAL_RESOLUTION = "month"



print("Input Table RAW: " , INPUT_TABLE_NAME_RAW, 
      "\nInput Table MA: ", INPUT_TABLE_NAME_MA,
      "\nOutput Table: " , OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

# imports
import re
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)

from sklearn import linear_model
get_ipython().magic('matplotlib inline')


# In[ ]:




# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()


# # Raw Values

# In[5]:

sql_raw = ("SELECT * FROM {} "
           "WHERE pfafid_30spfaf06 = {} AND "
           "month = {} AND "
           "temporal_resolution = '{}'".format(INPUT_TABLE_NAME_RAW,TEST_BASIN_PFAF_ID,MONTH,TEMPORAL_RESOLUTION))


# In[6]:

print(sql_raw)


# In[7]:

df_raw = pd.read_sql(sql_raw,connection)


# In[8]:

df_raw.head()


# In[9]:

def simplify_df_raw(df):
    df_out = df[["year","ptotww_m_30spfaf06","ptotwn_m_30spfaf06","riverdischarge_m_30spfaf06"]]
    return df_out


# In[10]:

df_raw = simplify_df_raw(df_raw)


# In[11]:

df_raw.head()


# In[12]:

df_raw = df_raw.sort_values(by=["year"])


# In[13]:

ax1 = df_raw.plot.scatter("year","ptotww_m_30spfaf06")
ax1.set_ylim(df_raw["ptotww_m_30spfaf06"].min(),df_raw["ptotww_m_30spfaf06"].max())


# In[14]:

ax1 = df_raw.plot.scatter("year","riverdischarge_m_30spfaf06")
ax1.set_ylim(df_raw["riverdischarge_m_30spfaf06"].min(),df_raw["riverdischarge_m_30spfaf06"].max())


# # Moving Average

# In[15]:

sql_ma = ("SELECT * FROM {} "
           "WHERE pfafid_30spfaf06 = {} AND "
           "month = {} AND "
           "temporal_resolution = '{}'".format(INPUT_TABLE_NAME_MA,TEST_BASIN_PFAF_ID,MONTH,TEMPORAL_RESOLUTION))


# In[16]:

print(sql_ma)


# In[17]:

df_ma = pd.read_sql(sql_ma,connection)


# In[18]:

df_ma.head()


# In[19]:

def simplify_df_ma(df):
    df_out = df[["year","ma10_ptotww_m_30spfaf06","ma10_ptotwn_m_30spfaf06","ma10_riverdischarge_m_30spfaf06"]]
    return df_out


# In[20]:

df_ma = simplify_df_ma(df_ma)


# In[21]:

df_ma.head()


# In[22]:

df_ma = df_ma.sort_values(by=["year"])


# In[23]:

ax1 = df_ma.plot.scatter("year","ma10_ptotww_m_30spfaf06")
ax1.set_ylim(df_ma["ma10_ptotww_m_30spfaf06"].min(),df_ma["ma10_ptotww_m_30spfaf06"].max())


# In[24]:

ax1 = df_ma.plot.scatter("year","ma10_riverdischarge_m_30spfaf06")
ax1.set_ylim(df_ma["ma10_riverdischarge_m_30spfaf06"].min(),df_ma["ma10_riverdischarge_m_30spfaf06"].max())


# # Combined

# In[25]:

from bokeh.plotting import figure 
from bokeh.io import output_notebook, show
from bokeh.models import HoverTool


# In[26]:

output_notebook()


# In[27]:

p = figure(width=800, height=500)
p.line(x = df_raw["year"], y = df_raw['ptotww_m_30spfaf06'],line_color="#349e17",legend="ptotww_m_30spfaf06")
p.line(x = df_raw["year"], y = df_raw['ptotwn_m_30spfaf06'],line_color="#af4c1d",legend="ptotwn_m_30spfaf06")
p.line(x = df_raw["year"], y = df_raw['riverdischarge_m_30spfaf06'],line_color="#003baa",legend="riverdischarge_m_30spfaf06")
p.line(x = df_ma["year"], y = df_ma['ma10_ptotww_m_30spfaf06'],line_color="#3fea10",legend="ma10_ptotww_m_30spfaf06")
p.line(x = df_ma["year"], y = df_ma['ma10_ptotwn_m_30spfaf06'],line_color="#f4570e",legend="ma10_ptotwn_m_30spfaf06")
p.line(x = df_ma["year"], y = df_ma['ma10_riverdischarge_m_30spfaf06'],line_color="#01a8b7",legend="ma10_riverdischarge_m_30spfaf06")
p.legend.location = "top_left"
p.legend.click_policy="hide"

hover = HoverTool(tooltips = [('year', '@x'),
                             ('value',  '@y')])

p.add_tools(hover)
show(p)


# # Regression
# 
# there are multiple options for linear regression. 
# 
# 1. Withdrawal and river discharge separately: Use a moving window of 10 years, perform ols regression and determine the final year for each window.
#     Probably requires some thresholds for basins with a small number of valid years. (e.g. capped at max value, and minimum 0?)
# 1. Use a linear trend for demand and a moving average for riverdischarge
# 
# 
# 
# 
# 

# In[42]:

sql_reg = ("SELECT year,month,pfafid_30spfaf06, temporal_resolution,ptotww_m_30spfaf06 "
"FROM y2018m05d29_rh_total_demand_postgis_30spfaf06_v01_v02 " 
"WHERE pfafid_30spfaf06 = 231607 "
"AND month = 5")


# In[43]:

df_reg = pd.read_sql(sql_reg,connection)


# In[46]:

df_reg = df_reg.sort_values(by=["year"])


# In[47]:

df_reg.head()


# In[38]:

lm = linear_model.LinearRegression()


# In[52]:

# Fit line for year 1973 - 1982 (10 years)
df_reg_selection = df_reg[(df_reg["year"] >= 1973) & (df_reg["year"] <= 1982)]


# In[54]:

df_reg_selection


# In[70]:

import os


# In[73]:

print(os.getcwd())


# In[69]:

df_reg_selection.to_csv("temp.csv")


# In[59]:

# fit linear model. 


# In[64]:

x = pd.DataFrame(df_reg_selection["year"])
target = pd.DataFrame(df_reg_selection["ptotww_m_30spfaf06"])


# In[65]:

lm.fit(x,target)


# In[90]:

df_lm = pd.DataFrame()


# In[97]:

coef = lm.coef_[0][0]


# In[98]:

intercept = lm.intercept_[0]


# In[99]:

# projected value for 1982
y_p = 1982*coef + intercept


# In[100]:

print(y_p,coef,intercept)


# In[101]:

# check if I can accomplish the same using SQL
SELECT year,month,pfafid_30spfaf06, temporal_resolution,ptotww_m_30spfaf06,
    regr_slope(ptotww_m_30spfaf06,year) 
        OVER (PARTITION BY pfafid_30spfaf06, month, temporal_resolution ORDER BY year ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS slope
FROM y2018m05d29_rh_total_demand_postgis_30spfaf06_v01_v02 
WHERE year > 1970 AND
year < 1983 AND
year > 1972 AND
month = 5 AND
temporal_resolution = 'month'AND
pfafid_30spfaf06 = 231607


# In[ ]:



