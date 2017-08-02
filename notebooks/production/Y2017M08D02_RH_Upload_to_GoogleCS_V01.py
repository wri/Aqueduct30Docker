
# coding: utf-8

# ### Upload data to Google Cloud Storage
# 
# * Purpose of script: This notebook will upload the geotiff files from the EC2 instance to Google Cloud Storage
# * Author: Rutger Hofste
# * Kernel used: python36
# * Date created: 20170802

# ## Preparation
# 
# run `gcloud init()` in your terminal and paste your login code
# 

# In[2]:

get_ipython().system('gsutil version')


# In[3]:

get_ipython().system('gcloud config set project aqueduct30')


# Before copying the files I created a folder on GCS with the name /Y2017M08D02_RH_Upload_to_GoogleCS_V01. Number of files are >8000 so this will take a couple of minutes. You could use `gsutil ls -lR gs://aqueduct30_v01` to check progress

# In[11]:

get_ipython().system('gsutil -m cp /volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V01/*.tif gs://aqueduct30_v01/Y2017M08D02_RH_Upload_to_GoogleCS_V01')


# 
