
# coding: utf-8

# # Add upstream, downstream and basin information to the dataframe
# 
# * Purpose of script: add contextual data to the dataframe. The script will sum the volumetric information of all upstream, downstream and basin parameters of the dataframe.  
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170915

# In[1]:

import time
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
print(dateString,timeString)


# In[2]:

S3_INPUT_PATH_EE  = "s3://wri-projects/Aqueduct30/processData/Y2017M09D14_RH_merge_EE_results_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M09D15_RH_Add_Basin_Data_V01/output/"

S3_INPUT_PATH_HYDROBASINS = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"

EC2_INPUT_PATH = "/volumes/data/Y2017M09D15_RH_Add_Basin_Data_V01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M09D15_RH_Add_Basin_Data_V01/output"

INPUT_FILENAME_EE =  "mergedZonalStatsEE_V12.pkl"
INPUT_FILENAME_HYDROBASINS =  "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V01.pkl"

OUTPUT_FILENAME = "Y2017M09D15_RH_Add_Basin_Data_V01"


# Note: There are two polygons with the same PFAF_ID (353020). This is caused by the fact that both poygons would otherwise cross the 180 degree meridian

# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH} ')
get_ipython().system('rm -r {EC2_OUTPUT_PATH} ')


# In[4]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH} ')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH} ')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_EE} {EC2_INPUT_PATH} --recursive')


# In[6]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_HYDROBASINS} {EC2_INPUT_PATH} --recursive --exclude "*" --include "*.pkl"')


# In[7]:

import os
import pandas as pd
import multiprocessing as mp
import pickle
import numpy as np
import itertools
import logging
import pprint
import ast


# In[8]:

inputLocationEE = os.path.join(EC2_INPUT_PATH,INPUT_FILENAME_EE)
inputLocationHydroBasins = os.path.join(EC2_INPUT_PATH,INPUT_FILENAME_HYDROBASINS)


# In[9]:

df_ee = pd.read_pickle(inputLocationEE)


# In[10]:

df_ee.index.names = ['PFAF_ID']


# In[11]:

df_HydroBasins = pd.read_pickle(inputLocationHydroBasins)


# In[12]:

df_complete = df_HydroBasins.merge(df_ee,how="left",left_index=True, right_index=True)


# Note: There are two polygons with the same PFAF_ID (353020). This is caused by the fact that both poygons would otherwise cross the 180 degree meridian. 

# In[13]:

df_complete = df_complete.drop_duplicates(subset='PFAF_ID', keep='first')


# ## Functions

# In[14]:

def calculateTotalDemand(useType,temporalResolution,year,month):
    # This function will add Dom Ind IrrLinear and Livestock of all basins in the input list
    
    if temporalResolution == "year":
        keyTotal = "local_sum_volumem3_Tot%s_%s_Y%0.4d" %(useType,temporalResolution,year)
    else:
        keyTotal = "local_sum_volumem3_Tot%s_%s_Y%0.4dM%0.2d" %(useType,temporalResolution,year,month)
    
    # Create Column with zeros
    dfDemand[keyTotal] = 0
    for demandType in demandTypes:
        if demandType == "IrrLinear" and temporalResolution == "year":
            key = "total_volume_%s%s_%sY%0.4d" %(demandType,useType,temporalResolution,year)
        else:
            key = "total_volume_%s%s_%sY%0.4dM%0.2d" %(demandType,useType,temporalResolution,year,month)
        dfDemand[keyTotal] = dfDemand[keyTotal] + df_complete[key]
    return dfDemand   


# This functions can take only one argument because I map them over the pooler.
def addUpstream2(listje):
    df_full_temp = df_complete.copy()
    df_part_temp = df_full_temp[df_full_temp.index.isin(listje)]
    df_part_temp2 = df_part_temp.copy()
    df_out = pd.DataFrame(index=df_part_temp2.index)
    
    i = 0
    for index, row in df_part_temp2.iterrows():
        i += 1
        print("i: ",i  ," index: ", index)
        try:
            upstreamCatchments = df_part_temp2.loc[index, "Upstream_PFAF_IDs"]
            upstreamCatchments = ast.literal_eval(upstreamCatchments)
            df_upstream = df_full_temp[df_full_temp.index.isin(upstreamCatchments)]
            # selecting columns based on regular expression
            df_upstream = df_upstream.filter(regex=("total*"))
            df_upstream = df_upstream.add_prefix("upstream_")
            sumSeries = df_upstream.sum(0)
            for key, value in sumSeries.iteritems():
                df_out.loc[index, key] = value
            df_out.loc[index, "errorCode"] = 0
        except:
            print("error")
            df_out.loc[index, "errorCode"] = 1
            pass
    
    return df_out
    
    
def addDownstream2(listje):
    df_full_temp = df_complete.copy()
    df_part_temp = df_full_temp[df_full_temp.index.isin(listje)]
    df_part_temp2 = df_part_temp.copy()
    df_out = pd.DataFrame(index=df_part_temp2.index)
    
    i = 0
    for index, row in df_part_temp2.iterrows():
        i += 1
        print("i: ",i  ," index: ", index)
        try:
            upstreamCatchments = df_part_temp2.loc[index, "Downstream_PFAF_IDs"]
            upstreamCatchments = ast.literal_eval(upstreamCatchments)
            df_upstream = df_full_temp[df_full_temp.index.isin(upstreamCatchments)]
            # selecting columns based on regular expression
            df_upstream = df_upstream.filter(regex=("total*"))
            df_upstream = df_upstream.add_prefix("downstream_")
            sumSeries = df_upstream.sum(0)
            for key, value in sumSeries.iteritems():
                df_out.loc[index, key] = value
            df_out.loc[index, "errorCode"] = 0
        except:
            print("error")
            df_out.loc[index, "errorCode"] = 1
            pass
    
    return df_out


def addBasin2(listje):
    df_full_temp = df_complete.copy()
    df_part_temp = df_full_temp[df_full_temp.index.isin(listje)]
    df_part_temp2 = df_part_temp.copy()
    #df_out = df_part_temp2.copy()
    df_out = pd.DataFrame(index=df_part_temp2.index)
    
    i = 0
    for index, row in df_part_temp2.iterrows():
        i += 1
        print("i: ",i  ," index: ", index)
        try:
            upstreamCatchments = df_part_temp2.loc[index, "Basin_PFAF_IDs"]
            upstreamCatchments = ast.literal_eval(upstreamCatchments)
            df_upstream = df_full_temp[df_full_temp.index.isin(upstreamCatchments)]
            # selecting columns based on regular expression
            df_upstream = df_upstream.filter(regex=("total*"))
            df_upstream = df_upstream.add_prefix("basin_")
            sumSeries = df_upstream.sum(0)
            for key, value in sumSeries.iteritems():
                df_out.loc[index, key] = value
            df_out.loc[index, "errorCode"] = 0
        except:
            print("error")
            df_out.loc[index, "errorCode"] = 1
            pass
    
    return df_out


# ## Script

# In[15]:

demandTypes = ["PDom","PInd","IrrLinear","PLiv"]
useTypes = ["WW","WN"]
temporalResolutions = ["year","month"]
years = [2014]


# In[16]:

dfDemand = pd.DataFrame(index=df_complete.index)
for temporalResolution in temporalResolutions:
    for useType in useTypes:
        for year in years:
            if temporalResolution == "year":
                month = 12
                print(useType,temporalResolution,year,month)
                dfDemand = calculateTotalDemand(useType,temporalResolution,year,month)
            else:
                for month in range(1,13):
                    print(useType,temporalResolution,year,month)
                    dfDemand = calculateTotalDemand(useType,temporalResolution,year,month)          


# In[17]:

dfDemand.head()


# In[18]:

df_complete = df_complete.merge(dfDemand,how="left",left_index=True,right_index=True)


# In[19]:

mp.cpu_count()


# In[20]:

indices_full = df_complete.index.values
indices_split = np.array_split(indices_full, mp.cpu_count())


# In[21]:

indices_split[1].shape


# In[22]:

mp.log_to_stderr()


# In[23]:

logger = mp.get_logger()
logger.setLevel(logging.INFO)


# In[24]:

pool = mp.Pool(mp.cpu_count())


# In[25]:

df_upstream = pd.concat(pool.map(addUpstream2, indices_split))


# In[26]:

df_downstream = pd.concat(pool.map(addDownstream2, indices_split))


# In[27]:

df_basin = pd.concat(pool.map(addBasin2, indices_split))


# In[28]:

pool.close()


# In[29]:

df_complete = df_complete.merge(df_upstream,how="left",left_index=True,right_index=True)


# In[30]:

df_complete = df_complete.merge(df_downstream,how="left",left_index=True,right_index=True)


# In[31]:

df_complete = df_complete.merge(df_basin,how="left",left_index=True,right_index=True)


# In[32]:

df_complete.to_pickle(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILENAME+".pkl"))


# In[33]:

df_complete.to_csv(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILENAME+".csv"))


# In[34]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[35]:

df_complete.head()

