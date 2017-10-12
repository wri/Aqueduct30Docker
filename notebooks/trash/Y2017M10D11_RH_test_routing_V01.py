
# coding: utf-8

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

INPUT_VERSION = 18
OUTPUT_VERSION = 9

S3_INPUT_PATH_EE  = "s3://wri-projects/Aqueduct30/processData/Y2017M09D14_RH_merge_EE_results_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M10D11_RH_test_routing_V01/output/"

S3_INPUT_PATH_HYDROBASINS = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"

EC2_INPUT_PATH = "/volumes/data/Y2017M10D11_RH_test_routing_V01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M10D11_RH_test_routing_V01/output"

INPUT_FILENAME_EE =  "mergedZonalStatsEE_V%0.2d.pkl" %(INPUT_VERSION)
INPUT_FILENAME_HYDROBASINS =  "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V01.pkl"

OUTPUT_FILENAME = "Y2017M10D11_RH_test_routing_V%0.2d" %(OUTPUT_VERSION)


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


# In[13]:

df_complete = df_complete.drop_duplicates(subset='PFAF_ID', keep='first')


# In[14]:

df_complete.head()


# In[24]:

df_part_temp2 = df_complete.copy()


# In[25]:

df_part_temp2["rutger"] = df_part_temp2["Downstream_PFAF_IDs"].apply(lambda x: ast.literal_eval(x))


# In[35]:

df_part_temp2["numberUpstream"] = df_part_temp2["Downstream_PFAF_IDs"].apply(lambda x: len(ast.literal_eval(x)))


# In[40]:




# In[41]:

df_part_temp2


# In[ ]:

# Upstream runoff = max(0,local runoff + upstream runoff)
def calculateUpstream(df,temporalResolution):
    
    


# In[43]:

def naturalRunoff(localRunoff,upstreamRunoff):
    max(0,localRunoff+upstreamRunoff)
    return x * y


# In[ ]:

def availableRunoff(localRunoff,upstreamRunoff,upstreamConsumption):
    
    
    


# In[49]:

for numberUpstream in range(0,df_part_temp2["numberUpstream"].max()+1):
    df_part_temp2.loc[df_part_temp2["numberUpstream"] == 0, "test"] = df_part_temp2["total_volume_runoff_monthY2014M10"] + 


# In[45]:

df = df_part_temp2.copy()


# In[46]:

df['newcolumn'] = df.apply(lambda x: fxy(x['count_PLivWW_monthY2014M09'], x['count_PLivWW_monthY2014M10']), axis=1)


# In[47]:

df.head()


# In[16]:

upstreamCatchments = df_part_temp2.loc[111014, "Downstream_PFAF_IDs"]


# In[17]:

upstreamCatchments = ast.literal_eval(upstreamCatchments)


# In[18]:

len(upstreamCatchments)


# In[20]:

i = 0 


# In[ ]:



