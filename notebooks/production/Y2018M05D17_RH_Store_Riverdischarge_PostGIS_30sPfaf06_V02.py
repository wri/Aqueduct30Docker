
# coding: utf-8

# In[1]:

""" Store combined riverdischarge data in postGIS database.
-------------------------------------------------------------------------------

Store the combined riverdischarge results in the postGIS table. Use a 
consistent schema to allow other indicators to be stored as well.


Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

TESTING = 0
OVERWRITE = 1
SCRIPT_NAME = "Y2018M05D17_RH_Store_Riverdischarge_PostGIS_30sPfaf06_V02"
OUTPUT_VERSION = 3

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
TABLE_NAME = SCRIPT_NAME.lower()

COLUMNS_TO_SAVE = ["zones",
                   "riverdischarge_millionm3",
                   "count_sinks",
                   "count_mainchannel",
                   "first",
                   "temporal_resolution_mainchannel",
                   "temporal_resolution_sinks",
                   "output_version_mainchannel",
                   "output_version_sinks",
                   "sum",
                   "month_sinks",
                   "month_mainchannel",
                   "year_mainchannel",
                   "year_mainchannel"]

TABLE_NAME_AREA_30SPFAF06 = "area_m2_30spfaf06"

S3_INPUT_PATH_RIVERDISCHARGE = "s3://wri-projects/Aqueduct30/processData/Y2018M05D16_RH_Final_Riverdischarge_30sPfaf06_V01/output_V03"

ec2_input_path_riverdischarge = "/volumes/data/{}/input_riverdischarge_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nInput ec2 riverdischarge: " + ec2_input_path_riverdischarge,
      "\nInput s3 riverdischarge: " + S3_INPUT_PATH_RIVERDISCHARGE)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_input_path_riverdischarge}')
get_ipython().system('mkdir -p {ec2_input_path_riverdischarge}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_RIVERDISCHARGE} {ec2_input_path_riverdischarge} --recursive --exclude="*" --include="*.pkl"')


# In[5]:

import os
import numpy as np
import pandas as pd
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[6]:

def pre_process_riverdischarge(df,COLUMNS_TO_SAVE):
    df_out = df[COLUMNS_TO_SAVE]    
    df_out["zones"] = df_out["zones"].astype(np.int64)
    df_out.set_index("zones",inplace=True)
    df_out.sort_index(axis=1, inplace=True)
    # Other columns are not converted because they contain nan's. 
    # in a next version I might fill the nans with a no data value
    # and convert to integer. 
    return df_out

def pre_process_area(df):
    df_out = df[["pfafid_30spfaf06","area_m2_30spfaf06"]]
    df_out.set_index("pfafid_30spfaf06",inplace=True)
    return df_out
    
def volume_to_flux(df_merged):    
    df_merged["riverdischarge_m"] = (df_merged["riverdischarge_millionm3"]*1e6) / df_merged["area_m2_30spfaf06"]
    df_merged.sort_index(axis=1, inplace=True)
    return df_merged
    


# In[7]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()

if OVERWRITE:
    sql = text("DROP TABLE IF EXISTS {};".format(TABLE_NAME))
    result = engine.execute(sql)


# In[8]:

# read area dataframe
if TESTING:
    query = "SELECT * FROM {} LIMIT 100".format(TABLE_NAME_AREA_30SPFAF06)
else:
    query = "SELECT * FROM {}".format(TABLE_NAME_AREA_30SPFAF06)
df_area = pd.read_sql(query,connection)
df_area = pre_process_area(df_area)


# In[9]:

df_area.head()


# In[10]:

months = range(1,12+1)
years = range(1960,2014+1)
temporal_resolutions = ["year","month"]

input_file_names = os.listdir(ec2_input_path_riverdischarge)


# In[11]:

if TESTING:
    input_file_names = input_file_names[0:10]


# In[12]:

i = 0 
start_time = time.time()


for input_file_name in input_file_names:
    i = i + 1
    elapsed_time = time.time() - start_time 
    print("Index: {:03.0f} Elapsed: {}".format(i, timedelta(seconds=elapsed_time)))
    input_file_path = "{}/{}".format(ec2_input_path_riverdischarge,input_file_name)
    df_riverdischarge_raw = pd.read_pickle(input_file_path)
    df_riverdischarge_cleaned = pre_process_riverdischarge(df_riverdischarge_raw,COLUMNS_TO_SAVE)
    
    df_merged = df_area.merge(df_riverdischarge_cleaned,
                              how="left",
                              left_index=True,
                              right_index=True,
                              sort=True)
    
    df_out = volume_to_flux(df_merged)
    
    df_out["script_used"] = SCRIPT_NAME
    df_out["output_version"] = OUTPUT_VERSION
    
    df_out.to_pickle()
    df_out.to_sql(TABLE_NAME,engine,if_exists='append', index=False,chunksize=100000)


# In[13]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 2:16:22.809560  
# 2:15:24.235441
# 1:35:41.142818
# 
# 

# In[ ]:



