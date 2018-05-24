
# coding: utf-8

# In[1]:

""" Add upstream PFAFIDs to the merged hydrobasin shp/csv file.
-------------------------------------------------------------------------------
Create a csv file with all PFAFID's upstream.

Revisited to apply normalization to database structure. 

Author: Rutger Hofste
Date: 20170822
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

SCRIPT_NAME = "Y2017M08D22_RH_Upstream_V01"
S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V01/output/"
INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_V01.shp"

OUTPUT_VERSION = 4

#S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D22_RH_Upstream_V01/output/"
#EC2_INPUT_PATH = "/volumes/data/Y2017M08D22_RH_Upstream_V01/input/"
#EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D22_RH_Upstream_V01/output/"

OUTPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_V01.csv"
OUTPUT_FILENAME_AUGMENTED = "hybas_lev06_v1c_merged_fiona_upstream_V01.csv"
OUTPUT_FILENAME_AUGMENTED2 = "hybas_lev06_v1c_merged_fiona_upstream_downstream_V01.csv"

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


# In[4]:

if 'GDAL_DATA' not in os.environ:
    os.environ['GDAL_DATA'] = r'/usr/share/gdal/2.1'


# In[5]:

get_ipython().system('rm -r {ec2_input_path} ')
get_ipython().system('rm -r {ec2_output_path} ')
get_ipython().system('mkdir -p {ec2_input_path} ')
get_ipython().system('mkdir -p {ec2_output_path} ')


# In[6]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --exclude *.tif')


# In[7]:

ec2InputFile = os.path.join(ec2_input_path,INPUT_FILENAME)
ec2OutputFile = os.path.join(ec2_output_path,OUTPUT_FILENAME)


# In[8]:

get_ipython().system('/opt/anaconda3/envs/python35/bin/ogr2ogr -f CSV {ec2OutputFile} {ec2InputFile}')


# Test if the CSV is correct (without geometry)

# In[9]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# ## Functions

# In[10]:

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


# In[11]:

df = pd.read_csv(ec2OutputFile)


# In[12]:

df.head()


# In[13]:

outputLocationAugentedCSV =  os.path.join(ec2_output_path,OUTPUT_FILENAME_AUGMENTED)
outputLocationAugentedCSV2 = os.path.join(ec2_output_path,OUTPUT_FILENAME_AUGMENTED2)


# In[14]:

generate_dictionary(df, outputLocationAugentedCSV)


# In[15]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[16]:

df.head()


# In[17]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:03:29.691124
# 

# In[ ]:



