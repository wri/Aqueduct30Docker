
# coding: utf-8

# In[1]:

""" Add downstream PFAFIDs to the merged hydrobasin csv file.
-------------------------------------------------------------------------------
Create a csv file with all PFAFID's downstream.

Revisited to apply normalization to database structure. 

Author: Rutger Hofste
Date: 20170823
Kernel: python27 -> python35
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
SCRIPT_NAME = "Y2017M08D23_RH_Downstream_V01"
OUTPUT_VERSION = 1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D22_RH_Upstream_V01/output_V04/"

INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_V01.csv"
OUTPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_V01.csv"


ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input ec2: " + ec2_input_path,
      "\nInput s3: " + S3_INPUT_PATH ,
      "\nOutput ec2: " + ec2_output_path,
      "\nOutput s3: " + s3_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import pandas as pd
import numpy as np
import os
import ftplib
from ast import literal_eval


# In[4]:

get_ipython().system('rm -r {ec2_input_path} ')
get_ipython().system('rm -r {ec2_output_path} ')
get_ipython().system('mkdir -p {ec2_input_path} ')
get_ipython().system('mkdir -p {ec2_output_path} ')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive')


# In[6]:

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


# In[7]:

df = pd.read_csv(os.path.join(ec2_input_path,INPUT_FILENAME))


# In[8]:

df["Upstream_HYBAS_IDs"] = df["Upstream_HYBAS_IDs"].apply(lambda x: stringToList(x))
df["Upstream_PFAF_IDs"] = df["Upstream_PFAF_IDs"].apply(lambda x: stringToList(x))


# In[9]:

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


# In[ ]:

def concatenateHybas(row):
    return row["Downstream_HYBAS_IDs"] + row["Upstream_HYBAS_IDs"] + [row["HYBAS_ID"]]


# In[ ]:

df_out['Basin_HYBAS_IDs'] = df_out.apply(concatenateHybas, axis=1)


# In[ ]:

def concatenatePFAF(row):
    return row["Downstream_PFAF_IDs"] + row["Upstream_PFAF_IDs"] + [row["PFAF_ID"]]


# In[ ]:

df_out['Basin_PFAF_IDs'] = df_out.apply(concatenatePFAF, axis=1)


# In[ ]:

df_out.tail()


# In[ ]:

df_out.to_csv(os.path.join(ec2_output_path,OUTPUT_FILENAME))


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 
