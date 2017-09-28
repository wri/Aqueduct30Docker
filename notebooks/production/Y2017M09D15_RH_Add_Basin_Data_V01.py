
# coding: utf-8

# # Add upstream, downstream and basin information to the dataframe
# 
# * Purpose of script: add contextual data to the datafram. 
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

S3_INPUT_PATH_HYDROBASINS = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"

EC2_INPUT_PATH = "/volumes/data/Y2017M09D15_RH_Add_Basin_Data_V01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M09D15_RH_Add_Basin_Data_V01/output"

INPUT_FILENAME_EE =  "mergedZonalStatsEE_V12.pkl"
INPUT_FILENAME_HYDROBASINS =  "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V01.pkl"


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


# ## Functions

# In[13]:

def calculateTotalDemand(useType,temporalResolution,year,month):
    # This function will add Dom Ind IrrLinear and Livestock of all basins in the input list
    
    if temporalResolution == "year":
        keyTotal = "local_sum_volumem3_Tot%s_%sY%0.4d" %(useType,temporalResolution,year)
    else:
        keyTotal = "local_sum_volumem3_Tot%s_%sY%0.4dM%0.2d" %(useType,temporalResolution,year,month)
    dfDemand[keyTotal] = 42
    
    for demandType in demandTypes:
        if demandType == "IrrLinear" and temporalResolution == "year":
            key = "total_volume_%s%s_%s_Y%0.4d" %(demandType,useType,temporalResolution,year)
        else:
            key = "total_volume_%s%s_%s_Y%0.4dM%0.2d" %(demandType,useType,temporalResolution,year,month)
        dfDemand[keyTotal] = dfDemand[keyTotal] + df_complete[key]
    return dfDemand   


# ## Script

# In[14]:

demandTypes = ["PDom","PInd","IrrLinear","PLiv"]
useTypes = ["WW","WN"]
temporalResolutions = ["year","month"]
years = [2014]


# In[15]:


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


# In[ ]:

dfDemand.head()


# In[ ]:




# In[ ]:




# In[ ]:

def addUpstream(listje):
    df_full_temp = df_full.copy()
    df_part_temp = df_full_temp[df_full_temp.index.isin(listje)]
    df_part_temp2 = df_part_temp.copy()
    df_out = df_part_temp2.copy()
    i = 0
    for index, row in df_part_temp2.iterrows():
        i += 1
        print("i: ",i  ," index: ", index)
        try:
            upstreamCatchments = df_part_temp2.loc[index, "Upstream_PFAF_IDs"]
            upstreamCatchments = ast.literal_eval(upstreamCatchments)
            df_upstream = df_full_temp.loc[upstreamCatchments]
            area = df_upstream["countarea30sm2"] * df_upstream["meanarea30sm2"]

            df_new = pd.DataFrame()
            df_new["aream2"] = area

            for parameter in parameterList:
                df_new["count_" + parameter] = df_upstream["count" + parameter]
                df_new["volumem3_" + parameter] = area * df_upstream["mean" + parameter]

            sumSeries = df_new.sum()

            for key, value in sumSeries.iteritems():
                newKey = "upstream_sum_" + key
                df_out.loc[index, newKey] = value
            df_out.loc[index, "errorCode"] = 0
        except:
            print("error")
            df_out.loc[index, "errorCode"] = 1
            pass

    return df_out


# In[ ]:

mp.cpu_count()


# In[ ]:




# In[ ]:

print(inputLocation)


# In[ ]:




# In[ ]:

df_full.head()


# In[ ]:

indices_full = df_full.index.values
indices_split = np.array_split(indices_full, mp.cpu_count())


# In[ ]:

print(indices_split)


# In[ ]:

mp.log_to_stderr()


# In[ ]:

logger = mp.get_logger()
logger.setLevel(logging.INFO)


# In[ ]:

pool = mp.Pool(mp.cpu_count())


# In[ ]:

df_out = pd.concat(pool.map(addUpstream, indices_split))


# In[ ]:

df_out = addUpstream([1])


# In[ ]:

df_out.head()


# In[ ]:



