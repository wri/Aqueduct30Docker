
# coding: utf-8

# # Copy data from S3 raw to S3 process
# 
# * Purpose of script: Copy files from raw data folder to process data folder, all within S3. 
# * Author: Rutger Hofste
# * Kernel used: python36
# * Date created: 20170731

# Copying all netCDF4 files from the raw data folder to process data folder. Before you run this script, please make sure the data is not already in the s3://wri-projects/Aqueduct30/processData/01PCRGlobWBV01 folder

# ## Water Demand (18GB)

# In[1]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/rawData/Utrecht/yoshi20161219/waterdemand s3://wri-projects/Aqueduct30/processData/01PCRGlobWBV01 --recursive')


# ## River Discharge (5GB)

# In[2]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/rawData/Utrecht/yoshi20161219/wateravailability s3://wri-projects/Aqueduct30/processData/01PCRGlobWBV01 --recursive')


# Some files were later obtained from Rens van Beek to complete the analysis, that is why the files are from different sources 

# ## Water Stress (5GB)

# In[3]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/rawData/Utrecht/yoshi20161219/waterstress s3://wri-projects/Aqueduct30/processData/01PCRGlobWBV01 --recursive')


# ## Local Runoff (6GB)

# In[4]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/rawData/Utrecht/additionalFiles/wateravailability s3://wri-projects/Aqueduct30/processData/01PCRGlobWBV01 --recursive')


# ## Soil Moisture (4GB)

# In[5]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/rawData/Utrecht/additionalFiles/soilmoisture s3://wri-projects/Aqueduct30/processData/01PCRGlobWBV01 --recursive')


# In[ ]:




# 
