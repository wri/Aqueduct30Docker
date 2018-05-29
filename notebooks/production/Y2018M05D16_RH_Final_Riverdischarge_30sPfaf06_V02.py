
# coding: utf-8

# In[2]:

""" Combine riverdischarge in main channel and sinks. 
-------------------------------------------------------------------------------
Updated to get rid of the extra flow accumulation step.

If a sub-basin contains one or more sinks (coastal and endorheic), the sum 
of riverdischarge at those sinks will be used. If a subbasin does not contain
any sinks or is too small to be represented at 5min, the main channel 
riverdischarge (30s validfa_mask) will be used. 

Takes tabular result of main channel and sinks and combines them in one table.

Author: Rutger Hofste
Date: 20180516
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

TESTING = 1
SCRIPT_NAME = "Y2018M05D16_RH_Final_Riverdischarge_30sPfaf06_V01"
OUTPUT_VERSION = 4

EXTRA_PROPERTIES = {"nodata_value":-9999,
                    "ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION}

#S3_INPUT_PATH_MAINCHANNEL = "s3://wri-projects/Aqueduct30/processData/Y2018M05D04_RH_Zonal_Stats_Supply_EE_V01/output_V03"
S3_INPUT_PATH_MAINCHANNEL = "s3://wri-projects/Aqueduct30/processData/Y2018M05D28_RH_Riverdischarge_Mainchannel_30sPfaf06_EE_V01/output_V02"
S3_INPUT_PATH_SINKS = "s3://wri-projects/Aqueduct30/processData/Y2018M05D15_RH_Sum_Riverdischarge_Sinks_5min_EE_V01/output_V02"

#mainchannel_sample_file_name = "global_historical_riverdischarge_month_millionm3_5min_1960_2014_I357Y1989M10_reduced_06_30s_first.pkl"
#sinks_sample_file_name = "global_historical_riverdischarge_month_millionm3_5min_1960_2014_I357Y1989M10_reduced_06_5min_sum.pkl"

SEPARATOR = "_|-"
SCHEMA =["geographic_range",
         "temporal_range",
         "indicator",
         "temporal_resolution",
         "unit",
         "spatial_resolution",
         "temporal_range_min",
         "temporal_range_max",
         "reduced",
         "reduced_pfaf_level",
         "reduced_spatial_resolution",
         "reducer_type"]


ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input ec2: " + ec2_input_path,
      "\nOutput ec2: " + ec2_output_path,
      "\nOutput s3: " + s3_output_path,
      "\nInput s3 mainchannel: " + S3_INPUT_PATH_MAINCHANNEL,
      "\nInput s3 sinks: " + S3_INPUT_PATH_SINKS)


# In[3]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[4]:

ec2_input_path_mainchannel = "{}/mainchannel".format(ec2_input_path)
ec2_input_path_sinks = "{}/sinks".format(ec2_input_path)


# In[5]:

print(ec2_input_path_sinks)
print(ec2_input_path_mainchannel)


# In[6]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path_mainchannel}')
get_ipython().system('mkdir -p {ec2_input_path_sinks}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[7]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_MAINCHANNEL} {ec2_input_path_mainchannel} --recursive --exclude="*" --include="*.pkl"')


# In[8]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_SINKS} {ec2_input_path_sinks} --recursive --exclude="*" --include="*.pkl"')


# In[11]:

# Open Mainchannel dataframe

import pandas as pd
pd.set_option('display.max_columns', 500)
import os
import numpy as np
import aqueduct3

months = range(1,12+1)
years = range(1960,2014+1)
temporal_resolutions = ["year","month"]

mainchannel_file_names = os.listdir(ec2_input_path_mainchannel)


# In[18]:

def read_mainchannel(mainchannel_path):
    df_mainchannel = pd.read_pickle(mainchannel_path)
    df_mainchannel["count"] = df_mainchannel["count"].astype(np.int64)
    df_mainchannel["zones"] = df_mainchannel["zones"].astype(np.int64)
    
    return df_mainchannel

def read_sinks(sinks_path):
    df_sinks = pd.read_pickle(sinks_path)    
    return df_sinks


def combine_riverdischarge(df):
    """ Cereate new column with mainchannel or sink riverdischarge
    -------------------------------------------------------------------------------
    
    Use Sinks Discharge (5minPfaf06) if >0 sinks are present within a 5minPfaf06 
    subbasin. Use mainchannel riverdischarge otherwise.    
    
    Args:
        df (pd.Dataframe) : Merged pandas dataframe with mainchannel and sinks 
            supplies.
    
    Returns:
        df_out (pd.Dataframe) : Merged Dataframe with additional column with final
            riverdischarge.
    
    """
    
    df_out = df.copy()
    df_out['riverdischarge_millionm3'] = np.where(df_out['count_sinks']>0, df_out["sum"], df["max"])
    df_out.sort_index(axis=1,inplace=True)
    return df_out
    
    


# In[16]:

if TESTING:
    mainchannel_file_names = mainchannel_file_names[0:10]


# In[19]:

for mainchannel_file_name in mainchannel_file_names:
    mainchannel_path = "{}/{}".format(ec2_input_path_mainchannel,mainchannel_file_name)
    dictje = aqueduct3.split_key(key=mainchannel_file_name,
                                 schema = SCHEMA,
                                 separator = SEPARATOR)
    year = int(dictje["year"])
    month = int(dictje["month"])
    identifier = int(dictje["identifier"])
    
    df_mainchannel = read_mainchannel(mainchannel_path)
    
    sinks_file_name = "global_historical_riverdischarge_{}_millionm3_5min_1960_2014_I{:03.0f}Y{:04.0f}M{:02.0f}_reduced_06_5min_sum.pkl".format(dictje["temporal_resolution"],identifier,year,month)
    sinks_path = "{}/{}".format(ec2_input_path_sinks,sinks_file_name)
    
    df_sinks = read_sinks(sinks_path)
    
    df_merge = df_mainchannel.merge(right=df_sinks,
                                    how="outer",
                                    left_on="zones",
                                    right_on="zones",
                                    suffixes=["_mainchannel","_sinks"])
    df_out = combine_riverdischarge(df_merge)
    
    output_file_name = "global_historical_combinedriverdischarge_{}_millionm3_30sPfaf06_1960_2014_I{:03.0f}Y{:04.0f}M{:02.0f}.pkl".format(dictje["temporal_resolution"],identifier,year,month)
    df_out.to_pickle("{}/{}".format(ec2_output_path,output_file_name))
    


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:06:01.370454
