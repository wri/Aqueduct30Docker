
# coding: utf-8

# ### Add downstream PFAFIDs to merged shapefile
# 
# * Purpose of script: Create a csv file with all PFAFID's downstream
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170823

# In[1]:

import pandas as pd
import numpy as np
import os, ftplib
from datetime import datetime, timedelta
from ast import literal_eval


# In[2]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D22_RH_Upstream_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Downstream_V01/output/"
EC2_INPUT_PATH = "/volumes/data/Y2017M08D23_RH_Downstream_V01/input/"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D23_RH_Downstream_V01/output/"
INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_V01.csv"
OUTPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_V01.csv"


# In[3]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[5]:

def stringToList(string):
    # input format : "[42, 42, 42]" , note the spaces after the commas, in this case I have a list of integers
    string = string[1:len(string)-1]
    try:
        if len(string) != 0: 
            tempList = string.split(", ")
            newList = list(map(lambda x: int(x), tempList))
        else:
            newList = []
    except:
        newList = [-9999]
    return(newList)


# In[6]:

df = pd.read_csv(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME))


# In[7]:

df["Upstream_HYBAS_IDs"] = df["Upstream_HYBAS_IDs"].apply(lambda x: stringToList(x))
df["Upstream_PFAF_IDs"] = df["Upstream_PFAF_IDs"].apply(lambda x: stringToList(x))


# In[8]:

header = df.dtypes

df["HYBAS_ID2"] = df["HYBAS_ID"]
df = df.set_index(["HYBAS_ID2"])


# testing purposes
#df = df[ 0:200]
df_out = df.copy()


df_out['Downstream_HYBAS_IDs'] = "Nodata"
df_out['Downstream_PFAF_IDs'] = "Nodata"

print(df_out.dtypes)

i = 1
for id in df.index:
    print("item: ", i, " id: ",id)
    i += 1
    writeID = id
    allDownID = []
    allDownPFAF = []    

    sinkHybasID = np.int64(df.loc[id]["NEXT_SINK"])
    sinkPfafID = np.int64(df.loc[sinkHybasID]["PFAF_ID"])


    while id != 0:
              
        series = df.loc[id]
        downId = np.int64(series["NEXT_DOWN"]) # Next down ID
        if downId != 0 :
            downSeries = df.loc[downId]
            pfafID = np.int64(downSeries["PFAF_ID"])
            allDownID.append(downId)
            allDownPFAF.append(pfafID)
            id = downId
        else:
            id = 0
            pass

    df_out.set_value(writeID, 'Downstream_HYBAS_IDs', allDownID)
    df_out.set_value(writeID, 'Downstream_PFAF_IDs',allDownPFAF)

    df_out.set_value(writeID, 'NEXT_SINK_PFAF', sinkPfafID )


df_out.to_csv(outputLocation)

print("done")


# In[9]:

def concatenateHybas(row):
    return row["Downstream_HYBAS_IDs"] + row["Upstream_HYBAS_IDs"] + [row["HYBAS_ID"]]


# In[10]:

df_out['Basin_HYBAS_IDs'] = df_out.apply(concatenateHybas, axis=1)


# In[11]:

def concatenatePFAF(row):
    return row["Downstream_PFAF_IDs"] + row["Upstream_PFAF_IDs"] + [row["PFAF_ID"]]


# In[12]:

df_out['Basin_PFAF_IDs'] = df_out.apply(concatenatePFAF, axis=1)


# In[13]:

df_out.tail()


# In[14]:

df_out.to_csv(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILENAME))


# In[15]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')

