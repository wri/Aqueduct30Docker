
# coding: utf-8

# ### Merge WWF's HydroBasins
# 
# * Purpose of script: This notebooks will copy the relevant files from raw ro process and merge the shapefiles of level 6 and level 00
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170802
# 

# ## Preparation
# 
# make sure you are authorized to use AWS S3
# 
# ## Origin of the WWF data
# 
# The Hydrosheds data has been downloaded from the [WWF Website](http://www.hydrosheds.org/download). A login is required for larger datasets. For Aqueduct we used the Standard version without lakes. Since the download is limited to 5GB we split up the download in two batches:  
# 
# 1. Africa, North American Arctic, Central & South-east Asia, Australia & Oceania, Europe & Middle East
# 1. Greenland, North America & Caribbean, South America, Siberia
# 
# Download URLs (no longer valid)  
# [link1](http://www.hydrosheds.org/tempdownloads/hydrosheds-3926b3742a77b18974ca.zip)  
# [link2](http://www.hydrosheds.org/tempdownloads/hydrosheds-a69872e3f4059aea2434.zip)
# 
# 
# The data was downloaded earlier but replicated here so the latest download data would be 2017/08/03 
# 
# The folders contain all levels but for this phase of Aqueduct we decided to use level 6. More information regarding this decision will be in the methodology document. 
# 
# 
# 

# ## Script
# copy the files from the raw data folder to the process data folder. The raw data folder contains pristine or untouched data and should not be used as a working directory
# 
# 

# In[4]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/WWF/HydroSheds30sComplete/"
S3_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V01/input/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V01/output/"
EC2_INPUT_PATH = "/volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/input/"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/output/"
GCS_OUTPUT = "gs://aqueduct30_v01/Y2017M08D02_RH_Merge_HydroBasins_V01/output/"
EE_OUTPUT_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V05/"


# In[10]:

import os
import fiona
import subprocess
import pandas as pd
import re


# ## functions

# In[16]:

def splitKey(key):
    # will yield the root file code and extension of a set of keys
    prefix, extension = key.split(".")
    fileName = prefix.split("/")[-1]
    values = re.split("_|-", fileName)
    keyz = ["indicator","spatial_resolution","WWFversion","geographic_range","library","spatial_resolution","version"]
    outDict = dict(zip(keyz, values))
    outDict["fileName"]=fileName
    outDict["extension"]=extension
    return outDict

def uploadEE(index,row):
    target = EE_OUTPUT_PATH + row.fileName
    source = GCS_OUTPUT + row.fileName + "." + row.extension
    metadata = "--nodata_value=%s -p extension=%s -p filename=%s -p geographic_range=%s -p indicator=%s -p spatial_resolution=%s -p units=%s -p ingested_by=%s -p exportdescription=%s" 
    get_ipython().magic('(row.nodata,row.extension,row.fileName,row.geographic_range,row.indicator,row.spatial_resolution,row.temporal_range_max,row.temporal_range_min, row.units, row.ingested_by, row.exportdescription)')
    command = "/opt/anaconda3/bin/earthengine upload image --asset_id %s %s %s" % (target, source,metadata)
    try:
        #response = subprocess.check_output(command, shell=True)
        outDict = {"command":command,"response":response,"error":0}
        df_errors2 = pd.DataFrame(outDict,index=[index])
        pass
    except:
        try:
            outDict = {"command":command,"response":response,"error":1}
        except:
            outDict = {"command":command,"response":-9999,"error":2}
        df_errors2 = pd.DataFrame(outDict,index=[index])
        print("error")
    return df_errors2


# In[2]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH}HydrobasinsStandardAfr-Eu.zip {S3_PATH}')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH}HydrobasinsStandardGR-SI.zip {S3_PATH}')


# In[29]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[16]:

get_ipython().system('mkdir -p /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/temp')


# In[8]:

get_ipython().system('aws s3 cp {S3_PATH} {EC2_INPUT_PATH} --recursive')


# Unzip shapefiles 

# In[23]:

os.chdir(EC2_INPUT_PATH)


# In[25]:

get_ipython().system("find . -name '*.zip' -exec unzip {} \\;")


# In[26]:

get_ipython().system("find / -name '*lev06_v1c.zip' -exec unzip -o {} \\;")
get_ipython().system("find / -name '*lev00_v1c.zip' -exec unzip -o {} \\;")


# Create output folder

# In[7]:

files = os.listdir(EC2_INPUT_PATH)


# In[33]:

meta = fiona.open('hybas_ar_lev06_v1c.shp').meta
with fiona.open(EC2_OUTPUT_PATH+"/hybas_lev06_v1c_merged_fiona_V01.shp", 'w', **meta) as output:
    for oneFile in files:    
        if oneFile.endswith("lev06_v1c.shp"):
            print(oneFile)
            for features in fiona.open(oneFile):
                output.write(features)    


# In[36]:

meta = fiona.open('hybas_ar_lev00_v1c.shp').meta
with fiona.open(EC2_OUTPUT_PATH+"/hybas_lev00_v1c_merged_fiona_V01.shp", 'w', **meta) as output:
    for oneFile in files:    
        if oneFile.endswith("lev00_v1c.shp"):
            print(oneFile)
            for features in fiona.open(oneFile):
                output.write(features)


# We also like to have rasterized versions of the shapefiles at 5min and 30s resolution (0.0833333 degrees and 0.00833333 degrees)

# In[38]:

lonSize5min = 4320
latSize5min = 2160
lonSize30s = 43200 
latSize30s = 21600


# Rasterizing on PFAF_ID and PFAF_12
# Layer name hybas_lev00_v1c_merged_fiona_V01
# 

# In[44]:

commands =[]
commands.append("gdal_rasterize -a PFAF_ID -ot Integer64 -of GTiff -te -180 -90 180 90 -ts %s %s -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6 -l hybas_lev06_v1c_merged_fiona_V01 -a_nodata -9999 %shybas_lev06_v1c_merged_fiona_V01.shp /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/output/hybas_lev06_v1c_merged_fiona__5min_V01.tif" %(lonSize5min,latSize5min,EC2_OUTPUT_PATH))
commands.append("gdal_rasterize -a PFAF_ID -ot Integer64 -of GTiff -te -180 -90 180 90 -ts %s %s -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6 -l hybas_lev06_v1c_merged_fiona_V01 -a_nodata -9999 %shybas_lev06_v1c_merged_fiona_V01.shp /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/output/hybas_lev06_v1c_merged_fiona_30s_V01.tif" %(lonSize30s,latSize30s,EC2_OUTPUT_PATH))
commands.append("gdal_rasterize -a PFAF_12 -ot Integer64 -of GTiff -te -180 -90 180 90 -ts %s %s -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6 -l hybas_lev00_v1c_merged_fiona_V01 -a_nodata -9999 %shybas_lev00_v1c_merged_fiona_V01.shp /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/output/hybas_lev00_v1c_merged_fiona_5min_V01.tif" %(lonSize5min,latSize5min,EC2_OUTPUT_PATH))
commands.append("gdal_rasterize -a PFAF_12 -ot Integer64 -of GTiff -te -180 -90 180 90 -ts %s %s -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6 -l hybas_lev00_v1c_merged_fiona_V01 -a_nodata -9999 %shybas_lev00_v1c_merged_fiona_V01.shp /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/output/hybas_lev00_v1c_merged_fiona_30s_V01.tif" %(lonSize30s,latSize30s,EC2_OUTPUT_PATH))


# Rasterizing (takes a while)

# In[46]:

for command in commands:
    #print(command)
    response = subprocess.check_output(command,shell=True)


# In[27]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive --quiet ')


# In[28]:

get_ipython().system('gsutil -m cp /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/output/*.tif {GCS_OUTPUT}')


# # HIER GEBLEVEN

# In[29]:

command = ("/opt/google-cloud-sdk/bin/gsutil ls %s") %(GCS_OUTPUT)
keys = subprocess.check_output(command,shell=True)
keys = keys.decode('UTF-8').splitlines()
print(keys)


# In[30]:

df = pd.DataFrame()
i = 0
for key in keys:
    i = i+1
    outDict = splitKey(key)
    df2 = pd.DataFrame(outDict,index=[i])
    df = df.append(df2)


# In[25]:

df


# In[ ]:

df["nodata"] = -9999
df["ingested_by"] ="RutgerHofste"
df["exportdescription"] = df["indicator"]
df["units"] = "PFAF_ID"

