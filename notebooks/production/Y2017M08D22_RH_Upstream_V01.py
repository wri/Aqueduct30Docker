
# coding: utf-8

# ### Add upstream PFAFIDs to merged shapefile
# 
# * Purpose of script: Create a csv file with all PFAFID's updstream
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170822

# In[1]:

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta


# In[2]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D22_RH_Upstream_V01/output/"
EC2_INPUT_PATH = "/volumes/data/Y2017M08D22_RH_Upstream_V01/input/"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D22_RH_Upstream_V01/output/"
INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_V01.shp"
OUTPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_V01.csv"
OUTPUT_FILENAME_AUGMENTED = "hybas_lev06_v1c_merged_fiona_upstream_V01.csv"
OUTPUT_FILENAME_AUGMENTED2 = "hybas_lev06_v1c_merged_fiona_upstream_downstream_V01.csv"


# In[3]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# Copying the files from S3 to EC2 since Shapefiles consist of multiple files

# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive --exclude *.tif')


# In[5]:

ec2InputFile = os.path.join(EC2_INPUT_PATH,INPUT_FILENAME)
ec2OutputFile = os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILENAME)


# In[6]:

get_ipython().system('ogr2ogr -f CSV {ec2OutputFile} {ec2InputFile}')


# Test if the CSV is correct (without geometry)

# In[7]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# ## Functions

# In[8]:

def find_upstream_catchments(basin_ids, df):
    all_up_catchments =[]
    for i in np.arange(1, df.shape[0], 1):
        if not basin_ids:
            break
        else:
            up_catchments_adjacent = []
            for bid in basin_ids:
                bid = np.int64(bid)
                up_ids_idx = df[df['NEXT_DOWN'] == bid]
                up_ids_idx = up_ids_idx.index.tolist()
                for idx in up_ids_idx :
                    up_id = np.int64(df.HYBAS_ID[idx])
                    up_catchments_adjacent.append(up_id)
            basin_ids = up_catchments_adjacent
            all_up_catchments = all_up_catchments + basin_ids

    all_up_catchments_PFAF = df.loc[df.HYBAS_ID.isin(all_up_catchments)].PFAF_ID.tolist()
    #all_up_catchments_PFAF = map(lambda(x): np.int64(x), all_up_catchments_PFAF)    
    return all_up_catchments,all_up_catchments_PFAF

def generate_dictionary(df, outputLocation):
    df_temp = df
    up_catchments = []
    up_catchments_PFAF = []
    for bid in df.HYBAS_ID:
        bid = np.int64(bid)
        (all_up_catchments, all_up_catchments_PFAF) = find_upstream_catchments([bid], df)
        up_catchments.append(all_up_catchments)
        up_catchments_PFAF.append(all_up_catchments_PFAF)
        print(bid)


    df_temp['Upstream_HYBAS_IDs'] = up_catchments
    df_temp['Upstream_PFAF_IDs'] = up_catchments_PFAF
    df_temp.to_csv(outputLocation)
    print("DONE!")


# In[9]:

df = pd.read_csv(ec2OutputFile)


# In[10]:

df.head()


# In[11]:

outputLocationAugentedCSV =  os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILENAME_AUGMENTED)
outputLocationAugentedCSV2 = os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILENAME_AUGMENTED2)


# In[12]:

generate_dictionary(df, outputLocationAugentedCSV)


# In[13]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[14]:

df.head()


# In[ ]:



