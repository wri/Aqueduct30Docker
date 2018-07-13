
# coding: utf-8

# In[1]:

""" Compare several methods for setting arid and lowwateruse.
-------------------------------------------------------------------------------
Compare several methods for determining arid and lowwateruse per subbasins.

Goal: Finding a definition of arid and lowwateruse that performs well. 

Problem definition: Water Stress is a fraction and fractions are very sensitive
when the the denominator is very small. This is the case when riverdischarge is
low. 


Method 1:

Count number of months in time series that are considered arid and lowwater use

Count number of year in time series that are considered arid and lowwater use

set a threshold and visualize.


Method 2:

Use ols_ols10_xxx for use and withdrawal to set categorization upfront.


Key Takeaways:

Most subbasins will meet the thresholds for arid and lowwateruse at least
one month in the total of 660 months measured.

Using the ols approach reduces the total number of months that a subbasin meets
these criteria. 

Using method 2 drastically reduces complexity. 

Northwest China and Mongolia experience more arid months in January than in July. 
This is in agreement with a quick Google Search.


Author: Rutger Hofste
Date: 20180702
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M07D02_RH_QA_Arid_Low_Method_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = 'y2018m06d28_rh_ws_full_range_ols_postgis_30spfaf06_v02_v03'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

OUTPUT_SCHEMA = "qa"

print("Input table: " + INPUT_TABLE_NAME,
      "\nOutput table: " + OUTPUT_SCHEMA +"/"+ OUTPUT_TABLE_NAME)


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

def charts(case):
    print("case: ", case["description"])
    case["df"].hist(column=case["measure"],
                           bins = 10)


# # Method 1
# 
# Create a new table with the count of aridandlowwateruse. 
# 
# Sum Years Arid
# Sum Years Lowwateruse
# Sum Year Arid AND Lowwateruse
# 
# 
# 

# In[7]:

columns_1 = ["arid","lowwateruse","aridandlowwateruse"]
columns_2 = ["ols10_",""]

columns_of_interest = []
for column_1 in columns_1:
    for column_2 in columns_2:
        columns_of_interest.append("{}{}_boolean_30spfaf06".format(column_2,column_1))
        


# In[8]:

sql =" SELECT pfafid_30spfaf06,"
for column_of_interest in columns_of_interest:
    sql +=" SUM({}) AS sum_{},".format(column_of_interest,column_of_interest)
sql = sql[:-1]
sql +=" FROM {}".format(INPUT_TABLE_NAME)
sql +=" WHERE temporal_resolution = 'year'"
sql +=" GROUP BY temporal_resolution, pfafid_30spfaf06"
print(sql)



# In[9]:

# Warning, takes quite some time... couple of minutes to load
df_annual = pd.read_sql(sql,engine)


# In[10]:

df_annual.shape


# In[11]:

df_annual.head()


# ## Create Case: 1 Visualize Annual Arid

# In[12]:

cases = {}


# In[13]:

case = {}
case["description"] = "Annual Arid"
case["id"] = "annual_arid"
case["df"] = df_annual.copy()
case["measure"] = "sum_arid_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)'],
                                     [55,'rgb(0,0,0)']]
case["viz"] = create_viz(case)

cases[case["id"]] = case


# ## Create Case: 2 Visualize Annual Lowwateruse

# In[14]:

case = {}
case["description"] = "Annual Lowwateruse"
case["id"] = "annual_lowwateruse"
case["df"] = df_annual.copy()
case["measure"] = "sum_lowwateruse_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)'],
                                     [55,'rgb(0,0,0)']]
case["viz"] = create_viz(case)
cases[case["id"]] = case


# ## Create Case: 3 Visualize Annual Arid AND Lowwateruse

# In[15]:

case = {}
case["description"] = "Annual Arid AND Lowwateruse"
case["id"] = "annual_aridandlowwateruse"
case["df"] = df_annual.copy()
case["measure"] = "sum_aridandlowwateruse_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)'],
                                     [55,'rgb(0,0,0)']]
case["viz"] = create_viz(case)
cases[case["id"]] = case


# ## Create Case: 4 Grouped Monthly Arid

# In[16]:

sql =" SELECT pfafid_30spfaf06,"
for column_of_interest in columns_of_interest:
    sql +=" SUM({}) AS sum_{},".format(column_of_interest,column_of_interest)
sql = sql[:-1]
sql +=" FROM {}".format(INPUT_TABLE_NAME)
sql +=" WHERE temporal_resolution = 'month'"
sql +=" GROUP BY temporal_resolution, pfafid_30spfaf06"
print(sql)


# In[17]:

# Warning, takes quite some time... couple of minutes to load
df_groupedmonth = pd.read_sql(sql,engine)


# In[18]:

df_groupedmonth.shape


# In[19]:

case = {}
case["description"] = "Grouped Month Arid"
case["id"] = "groupedmonth_arid"
case["df"] = df_groupedmonth.copy()
case["measure"] = "sum_arid_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)'],
                                     [660,'rgb(0,0,0)']]
case["viz"] = create_viz(case)

cases[case["id"]] = case


# ## Create Case: 5 Grouped Monthly Lowwateruse

# In[20]:

case = {}
case["description"] = "Grouped Month Lowwateruse"
case["id"] = "groupedmonth_lowwateruse"
case["df"] = df_groupedmonth.copy()
case["measure"] = "sum_lowwateruse_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)'],
                                     [660,'rgb(0,0,0)']]
case["viz"] = create_viz(case)

cases[case["id"]] = case


# ## Create Case: 6 Grouped Monthly Aridandlowwateruse

# In[21]:

case = {}
case["description"] = "Grouped Month Arid And Lowwateruse"
case["id"] = "groupedmonth_aridandlowwateruse"
case["df"] = df_groupedmonth.copy()
case["measure"] = "sum_aridandlowwateruse_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)'],
                                     [660,'rgb(0,0,0)']]
case["viz"] = create_viz(case)

cases[case["id"]] = case


# ## Create Case: 7 Grouped Monthly Aridandlowwateruse ols vs normal

# In[22]:

df_groupedmonth_case7 = df_groupedmonth.copy()


# In[23]:

df_groupedmonth_case7["aridlowwateruse_dif_olsvsnormal"] = df_groupedmonth_case7["sum_ols10_aridandlowwateruse_boolean_30spfaf06"]-df_groupedmonth_case7["sum_aridandlowwateruse_boolean_30spfaf06"] 


# In[25]:

case = {}
case["description"] = "Grouped Month Arid And Lowwateruse ols vs normal"
case["id"] = "groupedmonth_aridandlowwateruse_olsvsnormal"
case["df"] = df_groupedmonth_case7.copy()
case["measure"] = "aridlowwateruse_dif_olsvsnormal"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)'],
                                     [660,'rgb(0,0,0)']]
case["viz"] = create_viz(case)

cases[case["id"]] = case


# In[ ]:




# ## Create Case: 8 Monthly January Arid

# In[26]:

sql =" SELECT pfafid_30spfaf06,month,"
for column_of_interest in columns_of_interest:
    sql +=" SUM({}) AS sum_{},".format(column_of_interest,column_of_interest)
sql = sql[:-1]
sql +=" FROM {}".format(INPUT_TABLE_NAME)
sql +=" WHERE temporal_resolution = 'month'"
sql +=" GROUP BY temporal_resolution, pfafid_30spfaf06,month"
print(sql)


# In[27]:

# Warning, takes quite some time... couple of minutes to load
df_month = pd.read_sql(sql,engine)


# In[28]:

df_month.shape


# In[29]:

df_jan = df_month.loc[df_month["month"]==1]


# In[30]:

df_jan.head()


# In[31]:

case = {}
case["description"] = "Month Jan Arid"
case["id"] = "month_jan_arid_ols"
case["df"] = df_jan.copy()
case["measure"] = "sum_ols10_arid_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)'],
                                     [55,'rgb(0,0,0)']]

case["viz"] = create_viz(case)
cases[case["id"]] = case


# In[ ]:




# ## Create Case: 9 Monthly July Arid

# In[32]:

df_jul = df_month.loc[df_month["month"]==7]


# In[33]:

case = {}
case["description"] = "Month Jul Arid"
case["id"] = "month_jul_arid_ols"
case["df"] = df_jul.copy()
case["measure"] = "sum_ols10_arid_boolean_30spfaf06"
case["dimension"] = "pfafid_30spfaf06"

df_temp = case["df"][[case["dimension"],case["measure"]]]

case["json"] = json.loads(df_temp.to_json(orient='records'))

case["color_stops"] = color_stops = [[0,'rgb(255,255,255)'],
                                     [1,'rgb(0,255,0)'],
                                     [55,'rgb(0,0,0)']]
case["viz"] = create_viz(case)

cases[case["id"]] = case


# # Running the cases

# In[34]:

cases.keys()


# In[35]:

# Number of years that a subbasin is classified as arid based on raw annual data.

viz = cases["annual_arid"]["viz"]
viz.show()


# In[36]:

# Number of years that a subbasin is classified as lowwateruse based on raw annual data.

viz = cases["annual_lowwateruse"]["viz"]
viz.show()


# In[37]:

# Number of years that a subbasin is classified as arid AND lowwateruse based on raw annual data.

viz = cases["annual_aridandlowwateruse"]["viz"]
viz.show()


# In[38]:

# Sum of arid months based on raw months values
viz = cases["groupedmonth_arid"]["viz"]
viz.show()


# In[39]:

# Sum of lowwater months based on raw months values
viz = cases["groupedmonth_lowwateruse"]["viz"]
viz.show()


# In[40]:

# Sum of Aird and lowwater months based on raw months values
viz = cases["groupedmonth_aridandlowwateruse"]["viz"]
viz.show()


# In[ ]:

# Difference in aridandlowwateruse ols10 vs normal
viz = cases["groupedmonth_aridandlowwateruse_olsvsnormal"]["viz"]
viz.show()


# In[41]:

# arid january
viz = cases["month_jan_arid_ols"]["viz"]
viz.show()


# In[42]:

# arid jul
viz = cases["month_jul_arid_ols"]["viz"]
viz.show()


# In[ ]:




# In[ ]:




# In[ ]:

# Basin Deep dive option
sql = "SELECT * FROM {} WHERE pfafid_30spfaf06 = 216080".format(INPUT_TABLE_NAME)
df_basin = pd.read_sql(sql,engine)


# In[ ]:

df_basin.head()


# In[ ]:




# In[ ]:

df_basin_selection = df_basin[["riverdischarge_m_30spfaf06","ols10_riverdischarge_m_30spfaf06","temporal_resolution","year","month","arid_boolean_30spfaf06","ols10_arid_boolean_30spfaf06"]]


# In[ ]:

df_basin_selection.head()


# In[ ]:

monthly_means = df_basin_selection.groupby("month").sum()


# In[ ]:

monthly_means


# In[ ]:

output_notebook()
p = figure(width=900, height=600)
p.line(x = df_basin_selection["year"], y = df_basin_selection["ols10_aridandlowwateruse_boolean_30spfaf06"],color="red",legend= "ols10_aridandlowwateruse_boolean_30spfaf06")
p.line(x = df_basin_selection["year"], y = df_basin_selection["aridandlowwateruse_boolean_30spfaf06"],color="black",legend= "aridandlowwateruse_boolean_30spfaf06")


p.legend.location = "top_left"
p.legend.click_policy="hide"
hover = HoverTool(tooltips = [('year', '@x'),
                             ('value',  '@y')])
p.add_tools(hover)

show(p)


# In[ ]:



