
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
OVERWRITE_INPUT = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = "Y2018M05D23_RH_Simplify_DataFrames_Pandas_30sPfaf06_V03"
OUTPUT_VERSION = 1
NODATA_VALUE = -9999

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

# Area 
TABLE_NAME_AREA_30SPFAF06 = "area_m2_30spfaf06"

# Riverdischarge
S3_INPUT_PATH_RIVERDISCHARGE = "s3://wri-projects/Aqueduct30/processData/Y2018M05D16_RH_Final_Riverdischarge_30sPfaf06_V01/output_V06"

# Demand
S3_INPUT_PATH_DEMAND = "s3://wri-projects/Aqueduct30/processData/Y2018M04D22_RH_Zonal_Stats_Demand_EE_V01/output_V01"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nInput ec2: " + ec2_input_path,
      "\nInput postGIS table area: " + TABLE_NAME_AREA_30SPFAF06 ,
      "\nInput s3 riverdischarge: " + S3_INPUT_PATH_RIVERDISCHARGE,
      "\nInput s3 demand: " + S3_INPUT_PATH_DEMAND,
      "\nOutput s3: " + s3_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

if OVERWRITE_INPUT:
    get_ipython().system('rm -r {ec2_input_path}')
    get_ipython().system('mkdir -p {ec2_input_path}')
    get_ipython().system('aws s3 cp {S3_INPUT_PATH_RIVERDISCHARGE} {ec2_input_path} --recursive --exclude="*" --include="*.pkl"')
    get_ipython().system('aws s3 cp {S3_INPUT_PATH_DEMAND} {ec2_input_path} --recursive --exclude="*" --include="*.pkl"')

if OVERWRITE_OUTPUT:
    get_ipython().system('rm -r {ec2_output_path}')
    get_ipython().system('mkdir -p {ec2_output_path}')
    


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
    df_out.rename(columns={"count":"area_count_30spfaf06"},inplace=True)
    df_out.set_index("pfafid_30spfaf06",inplace=True)
    return df_out


def get_file_names(file_names,temporal_resolution,year,month):
    """ Finds the filenames for riverdischarge and demand using regex.
    -------------------------------------------------------------------------------
    
    WARNING: Month is set to 1 for yearly (annual) data for riverdischarge
    whereas for demand month is set to 12. 
    
    
    Args:
        file_names (list) : list of all file names.
        temporal_resolution (string) : 'month' or 'year'
        year (integer) : year [1960:2014]
        month (integer) : month [1:12]. Not used if temporal_resolution is 'year'
    
    Returns:
        matching_file_names (dict) : dictionary with matching filenames for 
            demand and discharge.
    
    """   
    
    matching_file_names = {}    
    matching_file_names["riverdischarge"] = []
    matching_file_names["demand"] = []
    
    if temporal_resolution == "year":
        month_riverdischarge = 1
        month_demand = 12
        riverdischarge_pattern = "global_historical_combinedriverdischarge_{}_millionm3_30sPfaf06_1960_2014_I\d\d\dY{:04.0f}M{:02.0f}.pkl".format(temporal_resolution,year,month_riverdischarge)
        demand_pattern = "global_historical_P....._{}_m_5min_1960_2014_I\d\d\dY{:04.0f}M{:02.0f}_reduced_06_30s_mean.pkl".format(temporal_resolution,year,month_demand)      
    else:
        riverdischarge_pattern = "global_historical_combinedriverdischarge_{}_millionm3_30sPfaf06_1960_2014_I\d\d\dY{:04.0f}M{:02.0f}.pkl".format(temporal_resolution,year,month)
        demand_pattern = "global_historical_P....._{}_m_5min_1960_2014_I\d\d\dY{:04.0f}M{:02.0f}_reduced_06_30s_mean.pkl".format(temporal_resolution,year,month)

    for file_name in file_names:
        if re.search(riverdischarge_pattern,file_name):
            matching_file_names["riverdischarge"].append(file_name)
        elif re.search(demand_pattern,file_name):
            matching_file_names["demand"].append(file_name)
    return matching_file_names

def pre_process_df(df):
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



# In[6]:

df_area = get_area_df()
df_area = pre_process_area(df_area)


# In[7]:

df_area.head()


# In[8]:

file_names = os.listdir(ec2_input_path)


# In[9]:

temporal_resolutions = ["year","month"]
years = range(1960,2014+1)
months = range(1,12+1)


# In[10]:

if TESTING:
    temporal_resolutions = ["month","year"]
    years = range(1960,1962)
    months = range(1,3)
    


# In[11]:

def process_matchingfilenames(matching_file_names,df_area,ec2_input_path):
    """ Merge Area, Demand and riverdischarge 
    -------------------------------------------------------------------------------
    
    Uses area as left table and performs a left join of demand (8x) and 
    riverdischarge. 
    
    
    Args:
        matchingfile_names (dict) : Dictionary with list of strings with file names
            of demand and riverdischarge pickled dataframe.
        df_area (pd.DataFrame) : Pandas dataframe with area used as left table. 
        ec2_input_path (string) : ec2 input path. 
    
    Returns:
        df_merged (pd.DataFrame) : Merged Pandas DataFrame. 
    
    """
    df_merged = df_area.copy()
    for indicator, matching_file_names in matching_file_names.items():   
        for matching_file_name in matching_file_names:    
            file_path = "{}/{}".format(ec2_input_path,matching_file_name)
            df = pd.read_pickle(file_path)   

            if indicator == "riverdischarge":
                df.rename(columns={"count_mainchannel":"count",
                                   "riverdischarge_millionm3":"mean",
                                   "year_mainchannel":"year",
                                   "month_mainchannel":"month",
                                   "temporal_resolution_mainchannel":"temporal_resolution",
                                   "indicator_mainchannel":"indicator",
                                   "unit_mainchannel":"unit",
                                   "zones_spatial_resolution_mainchannel":"zones_spatial_resolution",
                                   "zones_pfaf_level_mainchannel":"zones_pfaf_level"},
                          inplace = True)  


            elif indicator == "demand":
                pass
            df_cleaned = pre_process_df(df)


            df_merged = df_merged.merge(right= df_cleaned,
                                         how="left",
                                         left_index =True,
                                         right_index = True,
                                         suffixes = ["","_duplicate"])

            try:
                # Take first non null 
                df_merged['month'].fillna(df_merged['month_duplicate'])
                df_merged['year'].fillna(df_merged['year_duplicate'])
                df_merged['temporal_resolution'].fillna(df_merged['temporal_resolution_duplicate'])
                
                
                df_merged = df_merged.drop(columns = ["month_duplicate",
                                                      "temporal_resolution_duplicate",
                                                      "year_duplicate"] ) 
                
                # Try to cast to integer                
                df_merged["month"] = df_merged["month"].astype(np.int64)
                df_merged["year"] = df_merged["year"].astype(np.int64)
                 
            except:
                pass
    df_merged["riverdischarge_m_30spfaf06"] = (df_merged["riverdischarge_millionm3_30spfaf06"] * 1e6) / df_merged["area_m2_30spfaf06"]
    df_merged.drop(columns=["riverdischarge_millionm3_30spfaf06"],inplace=True)
    df_merged.sort_index(axis=1, inplace=True)
    #df_merged.fillna(NODATA_VALUE, inplace=True) 
    
    return df_merged
    


# In[ ]:

i = 0
start_time = time.time()
for temporal_resolution in temporal_resolutions:
    if temporal_resolution == "month":
        for year in years:
            for month in months:
                i = i + 1
                elapsed_time = time.time() - start_time 
                print("Index: {:03.0f} Elapsed: {}".format(i, timedelta(seconds=elapsed_time)))
                print(i,temporal_resolution,year,month)
                matching_file_names = get_file_names(file_names,temporal_resolution,year,month) 
                df_merged = process_matchingfilenames(matching_file_names,df_area,ec2_input_path)
                output_file_name = "global_historical_merged_{}_m_30sPfaf06_1960_2014_Y{:04.0f}M{:02.0f}.pkl".format(temporal_resolution,year,month)
                output_path = "{}/{}".format(ec2_output_path,output_file_name)
                df_merged.to_pickle(output_path)        
    elif temporal_resolution == "year":
        for year in years:
            month = 1
            i = i + 1
            elapsed_time = time.time() - start_time 
            print("Index: {:03.0f} Elapsed: {}".format(i, timedelta(seconds=elapsed_time)))
            print(i,temporal_resolution,year,month)
            matching_file_names = get_file_names(file_names,temporal_resolution,year,month) 
            df_merged = process_matchingfilenames(matching_file_names,df_area,ec2_input_path)
            output_file_name = "global_historical_merged_{}_m_30sPfaf06_1960_2014_Y{:04.0f}M{:02.0f}.pkl".format(temporal_resolution,year,month)
            output_path = "{}/{}".format(ec2_output_path,output_file_name)
            df_merged.to_pickle(output_path)     
    else:
        pass
        
        
    


            

            
            

                     
            


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:39:39.668227  
# 0:42:53.025204  
# 0:47:08.975392  
# 0:31:06.810078
# 
