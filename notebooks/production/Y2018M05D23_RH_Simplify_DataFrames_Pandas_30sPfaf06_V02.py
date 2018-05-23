
# coding: utf-8

# In[1]:

""" Combine and simplify demand and riverdischarge dataframes.
-------------------------------------------------------------------------------

Combine the area, demand and riverdischarge dataframes and put them in a 
simplified and cleaned format. A community question has been posted at: 
https://stackoverflow.com/questions/50486168/is-it-ok-to-split-value-and-parameter-in-database/50488411#50488411

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

TESTING = 1
OVERWRITE = 0
SCRIPT_NAME = "Y2018M05D23_RH_Simplify_DataFrames_Pandas_30sPfaf06_V02"
OUTPUT_VERSION = 3

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

# Area 
TABLE_NAME_AREA_30SPFAF06 = "area_m2_30spfaf06"

# Riverdischarge
S3_INPUT_PATH_RIVERDISCHARGE = "s3://wri-projects/Aqueduct30/processData/Y2018M05D16_RH_Final_Riverdischarge_30sPfaf06_V01/output_V03"

# Demand
S3_INPUT_PATH_DEMAND = "s3://wri-projects/Aqueduct30/processData/Y2018M04D22_RH_Zonal_Stats_Demand_EE_V01/output_V01"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nInput ec2: " + ec2_input_path,
      "\nInput postGIS table area: " + TABLE_NAME_AREA_30SPFAF06 ,
      "\nInput s3 riverdischarge: " + S3_INPUT_PATH_RIVERDISCHARGE,
      "\nInput s3 demand: " + S3_INPUT_PATH_DEMAND)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

if OVERWRITE:
    get_ipython().system('rm -r {ec2_input_path}')
    get_ipython().system('mkdir -p {ec2_input_path}')
    get_ipython().system('aws s3 cp {S3_INPUT_PATH_RIVERDISCHARGE} {ec2_input_path} --recursive --exclude="*" --include="*.pkl"')
    get_ipython().system('aws s3 cp {S3_INPUT_PATH_DEMAND} {ec2_input_path} --recursive --exclude="*" --include="*.pkl"')


# In[4]:

# imports
import re
import os
import numpy as np
import pandas as pd
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[5]:

def get_area_df():
    F = open("/.password","r")
    password = F.read().splitlines()[0]
    F.close()
    
    engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
    connection = engine.connect()

    if TESTING:
        query = "SELECT * FROM {} LIMIT 100".format(TABLE_NAME_AREA_30SPFAF06)
    else:
        query = "SELECT * FROM {}".format(TABLE_NAME_AREA_30SPFAF06)
    df_area = pd.read_sql(query,connection)
    return df_area

def pre_process_area(df):
    df_out = df[["pfafid_30spfaf06","area_m2_30spfaf06","count"]]
    df_out.rename(columns={"count":"area_count"},inplace=True)
    df_out.set_index("pfafid_30spfaf06",inplace=True)
    return df_out


# In[6]:

df_area = get_area_df()


# In[7]:

df_area = pre_process_area(df_area)


# In[8]:

df_area.head()


# In[9]:

file_names = os.listdir(ec2_input_path)


# In[13]:

temporal_resolution = "month"
year = 1973
month = 3


# In[14]:

matches_riverdisharge = []
matches_demand = []

riverdischarge_pattern = "global_historical_combinedriverdischarge_{}_millionm3_30sPfaf06_1960_2014_I\d\d\dY{:04.0f}M{:02.0f}.pkl".format(temporal_resolution,year,month)
demand_pattern = "global_historical_P....._{}_m_5min_1960_2014_I\d\d\dY{:04.0f}M{:02.0f}_reduced_06_30s_mean.pkl".format(temporal_resolution,year,month)

for file_name in file_names:
    if re.search(riverdischarge_pattern,file_name):
        matches_riverdisharge.append(file_name)
    elif re.search(demand_pattern,file_name):
        matches_demand.append(file_name)
        


# In[17]:

matches_riverdisharge


# In[16]:

matches_demand


# In[18]:

df_demand_test_path  = "{}/{}".format(ec2_input_path,matches_demand[0])




# In[67]:

df_demand_test = pd.read_pickle(df_demand_test_path)


# In[69]:

def pre_process_demand_df(df):
    """ rename dataframe column and drastically simplify dataframe.
    -------------------------------------------------------------------------------
    
    The column name will be in format: 
    domww_m_30spfaf06    
    {indicator}_{unit}_{spatial_aggregation}
    
    The temporal resolution is not added to the schema.   
        
    Args:
        df (pd.DataFrame) : input dataframe.
    
    Returns:
        df_out (pd.DataFrame) : 
    
    """
    
    df_in = df.copy()
    
    indicator = df_in.loc[0]["indicator"].lower()
    indicator = indicator[1:] # remove p from start
    unit = df_in.loc[0]["unit"].lower()
    zones_spatial_resolution = df_in.loc[0]["zones_spatial_resolution"]
    zones_pfaf_level = df_in.loc[0]["zones_pfaf_level"]    
    
    new_indicator_name = "{}_{}_{}pfaf{:02.0f}".format(indicator,unit,zones_spatial_resolution,zones_pfaf_level)
    new_count_name = "{}_count_{}pfaf{:02.0f}".format(indicator,zones_spatial_resolution,zones_pfaf_level)
    new_zones_name = "pfafid_{}pfaf{:02.0f}".format(zones_spatial_resolution,zones_pfaf_level)
    
    df_out = df_in[["zones","count","mean","temporal_resolution","year","month"]]
    df_out.rename(columns={"mean":new_indicator_name,
                           "count":new_count_name,
                           "zones":new_zones_name},
                  inplace=True)
    
    df_out[new_zones_name] = df_out[new_zones_name].astype(np.int64)
    df_out.set_index(new_zones_name,inplace=True)
    df_out.sort_index(axis=1, inplace=True)
    return df_out


# In[70]:

df2 = pre_process_demand_df(df_demand_test)


# In[71]:

df2.head()


# In[46]:

df_demand_test.head()


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

months = range(1,12+1)
years = range(1960,2014+1)
temporal_resolutions = ["year","month"]

if TESTING:
    months = [3]
    years = [1983]
    temporal_resolutions = ["year","month"]


# In[ ]:

for temporal_resolution in temporal_resolutions:
    for year in years:
        for month in months:
            riverdischarge_input_filename = "global_historical_combinedriverdischarge_month_millionm3_30sPfaf06_1960_2014_I003Y1960M04.pkl"
            


# In[ ]:



