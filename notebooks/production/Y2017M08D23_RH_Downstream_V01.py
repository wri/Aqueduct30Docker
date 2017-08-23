
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
import os, ftplib, urllib2
from datetime import datetime, timedelta


# In[22]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D22_RH_Upstream_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Downstream_V01/output/"
EC2_INPUT_PATH = "/volumes/data/Y2017M08D23_RH_Downstream_V01/input/"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D23_RH_Downstream_V01/output/"
INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_V01.csv"
OUTPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_V01.csv"


# In[17]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[9]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[18]:

inputLocation = os.path.join(EC2_INPUT_PATH,INPUT_FILENAME)
outputLocation = os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILENAME)


# In[19]:

df = pd.read_csv(inputLocation)


# In[20]:

df.head()


# In[21]:

header = df.dtypes

df["HYBAS_ID2"] = df["HYBAS_ID"]
df = df.set_index(["HYBAS_ID2"])


# testing purposes
#df = df[ 0:200]
df_out = df.copy()


df_out['Downstream_HYBAS_IDs'] = "Nodata"
df_out['Downstream_PFAF_IDs'] = "Nodata"
df_out['Basin_HYBAS_IDs'] = "Nodata"
df_out['Basin_PFAF_IDs'] = "Nodata"

print df_out.dtypes

i = 1
for id in df.index:
    print "item: ", i, " id: ",id
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
            # most downstream basin
            if (len(series["Upstream_HYBAS_IDs"]) == 2 ):
                allBasinIDs =  "[" + str(sinkHybasID)+ "]" #super ugly but it works
                allBasinPFAFs ="[" + str(sinkPfafID) + "]"
            else:
                allBasinIDs = series["Upstream_HYBAS_IDs"][:-1] + ", " + str(sinkHybasID) + "]"  # super ugly but it works
                allBasinPFAFs = series["Upstream_PFAF_IDs"][:-1] + ", " + str(sinkPfafID) + "]"
            id = 0
            pass



    df_out.set_value(writeID, 'Downstream_HYBAS_IDs', allDownID)
    df_out.set_value(writeID, 'Downstream_PFAF_IDs',allDownPFAF)
    df_out.set_value(writeID, 'Basin_HYBAS_IDs', allBasinIDs)
    df_out.set_value(writeID, 'Basin_PFAF_IDs',allBasinPFAFs)

    df_out.set_value(writeID, 'NEXT_SINK_PFAF', sinkPfafID )


df_out.to_csv(outputLocation)

print "done"


# In[24]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')

