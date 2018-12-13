
# coding: utf-8

# In[18]:

"""
Inspect Overall Water Risk per sector.

WARNING: Script is extremely quick and dirty and contains inefficiencies. 

Nomenclature

['BWS',
 'BWD',
 'GTD',
 'IAV',
 'SEV',
 'DRR',
 'RFR',
 'CFR',
 'UCW',
 'CEP',
 'UDW',
 'USA',
 'RRI']

['Baseline water stress',
 'Baseline water depletion ',
 'Groundwater table decline ',
 'Interannual variability ',
 'Seasonal variability',
 'Drought risk',
 'Riverine flood risk ',
 'Coastal flood risk ',
 'Untreated collected wastewater',
 'Coastal eutrophication potential',
 'Unimproved/no drinking water ',
 'Unimproved/no sanitation',
 'RepRisk Index (RRI)']

['DEF', 'AGR', 'FNB', 'CHE', 'ELP', 'SMC', 'ONG', 'MIN', 'CON', 'TEX']

['Default',
 'Agriculture',
 'Food & Beverage',
 'Chemicals',
 'Electric Power',
 'Semiconductor',
 'Oil & Gas',
 'Mining',
 'Construction Materials',
 'Textile']

"""


# User Input
INDUSTRY = "def" # one of ['DEF', 'AGR', 'FNB', 'CHE', 'ELP', 'SMC', 'ONG', 'MIN', 'CON', 'TEX']
STRING_ID = '742498-USA.6_1-1335' # Use Shapefile to find string_id


# In[19]:

SCRIPT_NAME = 'Y2018M11D11_RH_QA_OWR_Inspector_Tool_V01'
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_IN = "y2018m12d11_rh_master_weights_gpd_v01_v02"

BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()


# In[20]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[21]:

import os
import collections
import pandas as pd
import numpy as np
from google.cloud import bigquery

import plotly.plotly as py
import plotly.graph_objs as go


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
get_ipython().magic('matplotlib inline')


# In[22]:

sql_nomenclature = """
SELECT
  *
FROM
  `aqueduct30.aqueduct30v01.y2018m12d06_rh_process_weights_bq_v01_v01`
ORDER BY
  id
"""


# In[23]:

df_nomenclature = pd.read_gbq(query=sql_nomenclature,dialect="standard")


# In[24]:

df_nomenclature


# In[25]:

indicators = list(df_nomenclature["indicator_short"].unique())
industries = list(df_nomenclature["industry_short"].unique())
indicators_full = list(df_nomenclature["indicator_name_full"].unique())
industries_full = list(df_nomenclature["industry_full"].unique())


# In[8]:

indicators


# In[9]:

indicators_full


# In[10]:

industries


# In[11]:

industries_full


# In[12]:

def get_hex_color(score):
    if score < 1:
        color = "#FFFF99"
    elif score < 2:
        color = "#FFE600"
    elif score < 3:
        color = "#FF9900"
    elif score < 4:
        color = "#FF1900"
    elif score <= 5:
        color = "#990000"
    else:
        color = "#4E4E4E"
    return color
    

def build_query(indicators,industry,string_id):
    sql = """
    SELECT
      aq30_id,
      string_id,
      pfaf_id,
      gid_1,
      aqid,
      area_km2,
      name_1,
      gid_0,
      name_0,
      delta_id,
    """
    for indicator in indicators:
        sql += "{}_score,".format(indicator)
        sql += "{}_{}_weight,".format(indicator,INDUSTRY)
        sql += "{}_{}_weightedscore,".format(indicator,INDUSTRY)
        
    sql += "{}_weight_sum,".format(INDUSTRY)
    sql += "{}_weightedscore_sum,".format(INDUSTRY)
    sql += "owr_{}_score".format(INDUSTRY)    

    sql += """
    FROM
      `{}.{}.{}`
    WHERE string_id = '{}'
    """.format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN,STRING_ID)
    return sql

sql = build_query(indicators,INDUSTRY,STRING_ID)
df_in = pd.read_gbq(query=sql,dialect="standard")

xs = []
ws = []
ss = []
for indicator in indicators:
    xs.append("{}".format(indicator))
    w = df_in.iloc[0]["{}_{}_weight".format(indicator,INDUSTRY)]
    s = df_in.iloc[0]["{}_score".format(indicator)]
    ws.append(w)
    ss.append(s)
    
df = pd.DataFrame({"x":xs,"weight":ws,"score":ss})
df = df.dropna()
df["cumweight"] = df["weight"].cumsum(axis=0)
df["offset"] = df["cumweight"] - (df["weight"]*0.5)
df["color"] = df["score"].apply(get_hex_color)

bar = go.Bar(name="Risk per indicator",
             x=df['offset'], # assign x as the dataframe column 'x'
             y=df['score'],
             width=df["weight"],
             text = df["x"],
             marker = {"color":list(df["color"])}
             )

owr_score = df_in.iloc[0]["owr_{}_score".format(INDUSTRY)]

line = go.Scatter(name = 'OWR {}'.format(INDUSTRY),
                  x = [0,df["cumweight"].max()],
                  y = [owr_score,owr_score],
                  line = {"dash":"dot",
                          "color":get_hex_color(owr_score)})
layout = go.Layout(
    barmode='stack',
    autosize=True,
    title='Risk Overview for {}'.format(STRING_ID),
    xaxis= {"title":"Weight for {}".format(INDUSTRY)},
    yaxis= {"title":"Score [0-5]"}
)
data = [bar, line]
fig = go.Figure(data=data, layout=layout)


# In[13]:

# plot Online


# In[14]:

url = py.plot(fig, filename='pandas-bar-chart-layout')


# In[15]:

url


# In[16]:

# Plot offline


# In[17]:

py.iplot(fig, filename='pandas-bar-chart-layout')

