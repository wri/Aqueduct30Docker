
# coding: utf-8

# In[1]:

""" Merge and simplify master table and annual scores based on months.
-------------------------------------------------------------------------------

Update 2020 02 05, changed version from 6 to 7
input version left 02 to 05
input version right 05 to 06

Author: Rutger Hofste
Date: 20180712
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

TESTING = 0
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M07D12_RH_Merge_Simplify_Tables_PostGIS_V01'
OUTPUT_VERSION = 7

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME_LEFT = "y2018m07d09_rh_apply_aridlowonce_mask_postgis_v01_v05"
INPUT_TABLE_NAME_RIGHT = "y2018m07d12_rh_annual_scores_from_months_postgis_v01_v06"
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

print("Input Table Left: " , INPUT_TABLE_NAME_LEFT, 
      "Input Table Right: " , INPUT_TABLE_NAME_RIGHT, 
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
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()

if OVERWRITE_OUTPUT:
    sql = "DROP TABLE IF EXISTS {};".format(OUTPUT_TABLE_NAME)
    print(sql)
    result = engine.execute(sql)


# In[5]:

columns_to_keep_left = ["pfafid_30spfaf06",
                        "temporal_resolution",
                        "year",
                        "month",
                        "area_m2_30spfaf06",
                        "area_count_30spfaf06"]


# In[6]:

columns_to_keep_right = []


# ## Raw Data and Decadal Statistics

# In[7]:

sectors = ["ptot",
           "pdom",
           "pind",
           "pirr",
           "pliv"]
use_types = ["ww","wn"]


# In[8]:

decadal_indicators = []
for sector in sectors:
    for use_type in use_types:
        decadal_indicators.append("{}{}".format(sector,use_type))


# In[9]:

decadal_indicators.append("riverdischarge")


# In[10]:

decadal_statistics = ["",
                   "ma10_",
                   "min10_",
                   "max10_",
                   "slope10_",
                   "intercept10_",
                   "ols10_",
                   "capped_ols10_"]


# In[11]:

for decadal_statistic in decadal_statistics:
    for decadal_indicator in decadal_indicators:
        indicator = "{}{}_m_30spfaf06".format(decadal_statistic,decadal_indicator)
        print(indicator)
        columns_to_keep_left.append(indicator)


# ## Statistics on Decadal Statistics

# In[12]:

tier2_decadal_indicators = ["ptotww",
                            "ptotwn",
                            "riverdischarge"]

tier2_decadal_statistics_0 = ["ols_","avg_","min_","max_","slope_","intercept_"]
tier2_decadal_statistics_1 = ["ma10_","ols10_","capped_ols10_"]

for tier2_decadal_indicator in tier2_decadal_indicators:
    for tier2_decadal_statistic_0 in tier2_decadal_statistics_0:
        for tier2_decadal_statistic_1 in tier2_decadal_statistics_1:
            indicator = "{}{}{}_m_30spfaf06".format(tier2_decadal_statistic_0,tier2_decadal_statistic_1,tier2_decadal_indicator)
            print(indicator)
            columns_to_keep_left.append(indicator)
            
    


# ## Complete TimeSeries Statistics

# In[13]:

# for ptotww, ptotwn and riverdischarge, statistics based on full time series are available.
complete_timeseries_statistics = ["avg_",
                                  "min_",
                                  "max_",
                                  "slope_",
                                  "intercept_",
                                  "ols_"]

complete_timeseries_indicators = ["ptotww",
                                  "ptotwn",
                                  "riverdischarge"
                                  ] 


# In[14]:

for complete_timeseries_statistic in complete_timeseries_statistics:
    for complete_timeseries_indicator in complete_timeseries_indicators:
        indicator = "{}{}_m_30spfaf06".format(complete_timeseries_statistic,complete_timeseries_indicator)
        print(indicator)
        columns_to_keep_left.append(indicator)


# ## Raw and Decadal Arid and Lowwater Use Columns

# In[15]:

arid_lowwateruse_indicators = ["arid",
                               "lowwateruse",
                               "aridandlowwateruse"]

arid_lowwateruse_statistics_tier0 = ["",
                                     "ma10_",
                                     "ols10_"]

for arid_lowwateruse_indicator in arid_lowwateruse_indicators:
    for arid_lowwateruse_statistic_tier0 in arid_lowwateruse_statistics_tier0:
        indicator = "{}{}_boolean_30spfaf06".format(arid_lowwateruse_statistic_tier0,arid_lowwateruse_indicator)
        print(indicator)
        columns_to_keep_left.append(indicator)



# ## Statistics on Decadal Statistics Arid and Lowwater Use

# In[16]:

lowarid_tier2_decadal_indicators = ["arid",
                                    "lowwateruse",
                                    "aridandlowwateruse"]

lowarid_tier2_decadal_statistics_0 = ["ols_"]
lowarid_tier2_decadal_statistics_1 = ["ols10_"]

for lowarid_tier2_decadal_indicator in lowarid_tier2_decadal_indicators:
    for lowarid_tier2_decadal_statistic_0 in lowarid_tier2_decadal_statistics_0:
        for lowarid_tier2_decadal_statistic_1 in lowarid_tier2_decadal_statistics_1:
            indicator = "{}{}{}_boolean_30spfaf06".format(lowarid_tier2_decadal_statistic_0,lowarid_tier2_decadal_statistic_1,lowarid_tier2_decadal_indicator)
            print(indicator)
            columns_to_keep_left.append(indicator)


# ## Water Stress Decadal

# In[17]:

waterstress_decadal_indicators = ["waterstress","waterdepletion"]

waterstress_decadal_statistics = ["",
                                  "ma10_",
                                  "ols10_",
                                  "capped_ols10_"]

for waterstress_decadal_indicator in waterstress_decadal_indicators:
    for waterstress_decadal_statistic in waterstress_decadal_statistics:
        indicator = "{}{}_dimensionless_30spfaf06".format(waterstress_decadal_statistic,waterstress_decadal_indicator)
        print(indicator)
        columns_to_keep_left.append(indicator)


# ## Statistics on Decadal Statistics Water Stress

# In[18]:

waterstress_tier2_decadal_indicators = ["waterstress","waterdepletion"]

waterstress_tier2_decadal_statistics_0 = ["avg_","min_","max_","slope_","intercept_","ols_"]
waterstress_tier2_decadal_statistics_1 = ["ols10_","ma10_","capped_ols10_"]


for waterstress_tier2_decadal_indicator in waterstress_tier2_decadal_indicators:
    for waterstress_tier2_decadal_statistic_0 in waterstress_tier2_decadal_statistics_0:        
        for waterstress_tier2_decadal_statistic_1 in waterstress_tier2_decadal_statistics_1:
            indicator = "{}{}{}_dimensionless_30spfaf06".format(waterstress_tier2_decadal_statistic_0,waterstress_tier2_decadal_statistic_1,waterstress_tier2_decadal_indicator)
            print(indicator)
            columns_to_keep_left.append(indicator)


# ## Water Stress Complete Timeseries

# In[19]:

waterstress_complete_indicators = ["waterstress","waterdepletion"]

waterstress_complete_statistics = ["min_",
                                   "max_",
                                   "avg_",
                                   "slope_",
                                   "intercept_",
                                   "ols_"]

for waterstress_complete_indicator in waterstress_complete_indicators:
    for waterstress_complete_statistic in waterstress_complete_statistics:
        indicator = "{}{}_dimensionless_30spfaf06".format(waterstress_complete_statistic,waterstress_complete_indicator)
        print(indicator)
        columns_to_keep_left.append(indicator)



# In[20]:

sql = "SELECT"
for column_to_keep_left in columns_to_keep_left:
    sql += " {},".format(column_to_keep_left)
sql = sql[:-1]
sql += " FROM {}".format(INPUT_TABLE_NAME_LEFT)
sql += " LIMIT 100"


# In[21]:

sql


# In[22]:

sql = "SELECT * FROM {} LIMIT 10".format(INPUT_TABLE_NAME_LEFT)


# In[23]:

df_complete = pd.read_sql(sql,engine)


# In[24]:

df_complete.head()


# In[25]:

all_columns = list(df_complete)


# In[26]:

len(all_columns)


# In[27]:

len(columns_to_keep_left)


# In[28]:

# What Columns are excluded?
missing_columns = set(all_columns) - set(columns_to_keep_left)


# In[29]:

columns_to_keep_right = ["avg1y_ols_capped_ols10_waterstress_dimensionless_30spfaf06",
                         "avg1y_ols_capped_ols10_weighted_waterstress_dimensionless_30spfaf06",
                         "avg1y_ols_capped_ols10_waterdepletion_dimensionless_30spfaf06",
                         "avg1y_ols_capped_ols10_weighted_waterdepletion_dimensionless_30spfaf06"]




# In[30]:

sql =  "CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)
sql += " SELECT "
for column_to_keep_left in columns_to_keep_left:
    sql += " l.{},".format(column_to_keep_left)
for column_to_keep_right in columns_to_keep_right:
    sql += " r.{},".format(column_to_keep_right)
sql = sql[:-1]
sql += " FROM {} l".format(INPUT_TABLE_NAME_LEFT)
sql += " INNER JOIN {} r ON".format(INPUT_TABLE_NAME_RIGHT)
sql += " CONCAT(l.pfafid_30spfaf06,l.year) = CONCAT(r.pfafid_30spfaf06,r.year)"
    


# In[31]:

sql


# In[32]:

result = engine.execute(sql)


# In[33]:

sql_index = "CREATE INDEX {}pfafid_30spfaf06 ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"pfafid_30spfaf06")


# In[34]:

result = engine.execute(sql_index)


# In[35]:

sql_index2 = "CREATE INDEX {}year ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"year")


# In[36]:

result = engine.execute(sql_index2)


# In[37]:

sql_index3 = "CREATE INDEX {}month ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"month")


# In[38]:

result = engine.execute(sql_index3)


# In[39]:

engine.dispose()


# In[40]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:21:34.564407  
# 0:23:37.884430  
# 0:26:40.223209  
# 0:26:38.212664

# In[ ]:



