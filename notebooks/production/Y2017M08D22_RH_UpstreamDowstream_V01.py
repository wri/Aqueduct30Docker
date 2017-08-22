
# coding: utf-8

# ### Add upstream and downstream PFAFIDs to merged shapefile
# 
# * Purpose of script: Create a csv file with all PFAFID's updstream and downstream
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170822

# In[9]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V01/output/"
EC2_INPUT_PATH = "/volumes/data/Y2017M08D22_RH_UpstreamDowstream_V01/input/"
FILENAME = "hybas_lev06_v1c_merged_fiona_V01.shp"


# In[12]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')


# Copying the files from S3 to EC2 since Shapefiles consist of multiple files

# In[14]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive --exclude *.tif')


# In[ ]:

get_ipython().system('ogr2ogr -f CSV "test.csv" "/volumes/data/Y2017M08D22_RH_UpstreamDowstream_V01/input/hybas_lev06_v1c_merged_fiona_V01.shp"')

