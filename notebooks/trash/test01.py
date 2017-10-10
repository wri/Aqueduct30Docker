
# coding: utf-8

# In[1]:

INPUT_VERSION = 15
OUTPUT_VERSION = 3

S3_INPUT_PATH_EE  = "s3://wri-projects/Aqueduct30/processData/Y2017M09D14_RH_merge_EE_results_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/test01/output/"

S3_INPUT_PATH_HYDROBASINS = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"

EC2_INPUT_PATH = "/volumes/data/test01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017test01/output"

INPUT_FILENAME_EE =  "mergedZonalStatsEE_V%0.2d.pkl" %(INPUT_VERSION)
INPUT_FILENAME_HYDROBASINS =  "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V01.pkl"

OUTPUT_FILENAME = "Y2017M09D15_RH_Add_Basin_Data_V%0.2d" %(OUTPUT_VERSION)


# In[2]:

get_ipython().system('rm -r {EC2_INPUT_PATH} ')
get_ipython().system('rm -r {EC2_OUTPUT_PATH} ')


# In[3]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH} ')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH} ')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_EE} {EC2_INPUT_PATH} --recursive')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_HYDROBASINS} {EC2_INPUT_PATH} --recursive --exclude "*" --include "*.pkl"')


# In[6]:

import os
import pandas as pd
import multiprocessing as mp
import pickle
import numpy as np
import itertools
import logging
import pprint
import ast


# In[7]:

inputLocationEE = os.path.join(EC2_INPUT_PATH,INPUT_FILENAME_EE)


# In[8]:

df_ee = pd.read_pickle(inputLocationEE)


# In[9]:

df_ee.index.names = ['PFAF_ID']


# In[10]:

df_ee.head()


# In[11]:

dfTest = df_ee[df_ee["total_volume_reducedmeanrunoff_month_Y1960Y2014M01"] < 0 ]


# In[17]:

mask = df_ee["total_volume_reducedmeanrunoff_month_Y1960Y2014M01"] < 0

df_ee.loc[mask, "total_volume_reducedmeanrunoff_month_Y1960Y2014M01"] = 0


# In[19]:

df_ee.total_volume_reducedmeanrunoff_month_Y1960Y2014M01.min()


# In[ ]:



