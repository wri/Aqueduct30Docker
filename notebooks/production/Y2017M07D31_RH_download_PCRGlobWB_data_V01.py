
# coding: utf-8

# ### Download PcrGlobWB data to instance
# 
# * Purpose of script: This notebook will download the data from S3 to the EC2 instance 
# * Author: Rutger Hofste
# * Kernel used: python36
# * Date created: 20170731
# 
# In this notebook we will copy the data for the first couple of steps from WRI's Amazon S3 Bucket. The data is large i.e. **40GB** so a good excuse to drink a coffee. The output in Jupyter per file is suppressed so you will only see a result after the file has been donwloaded. You can also run this command in your terminal and see the process per file

# Before you run this script, make sure you configure aws by running (in your terminal): `aws configure`

# Create folder to store the data

# In[6]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M07D31_RH_copy_S3raw_s3process_V01/output/"


# In[7]:

EC2_PATH = "/volumes/data/Y2017M07D31_RH_download_PCRGlobWB_data_V01/output/"


# In[8]:

get_ipython().system('mkdir -p {EC2_PATH}')


# Grab a coffee before you run the following command. This will copy the files from S3 to your EC2 instance. 

# In[9]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_PATH} --recursive')


# List files downloaded (32 in my case)

# In[12]:

get_ipython().system('find {EC2_PATH} -type f | wc -l')


# As you can see there are some zipped files. Unzipping

# Unzipping the file results in a 24GB file which is signifact. Therefore this step will take quite some time

# In[13]:

get_ipython().system('unzip {EC2_PATH}totalRunoff_monthTot_output.zip -d {EC2_PATH}')


# The total number of files should be around 25 but can change if the raw data changed. 

# In[14]:

get_ipython().system('ls -lah {EC2_PATH}')


# In the data that Yoshi provided there is only Livestock data for consumption (WN). However in an email he specified that the withdrawal (WW) equals the consumption (100% consumption) for livestock. Therefore we copy the WN Livestock files to WW to make looping over WN and WW respectively easier. 

# In[15]:

get_ipython().system('cp {EC2_PATH}/global_historical_PLivWN_month_millionm3_5min_1960_2014.nc4 {EC2_PATH}/global_historical_PLivWW_month_millionm3_5min_1960_2014.nc4')


# In[16]:

get_ipython().system('cp {EC2_PATH}/global_historical_PLivWN_year_millionm3_5min_1960_2014.nc4 {EC2_PATH}/global_historical_PLivWW_year_millionm3_5min_1960_2014.nc4')


# In[17]:

get_ipython().system('ls -lah {EC2_PATH}')


# In[18]:

import os
files = os.listdir(EC2_PATH)
print("Number of files: " + str(len(files)))


# Copy PLivWN to PLivWW because Livestock Withdrawal = Livestock Consumption (see Yoshi's email'). This will solve some lookping issues in the future. Copies 4GB of data so takes a while

# Some files that WRI received from Utrecht refer to water "Use" instead of WN (net). Renaming the relevant file. Renaming them

# In[19]:

get_ipython().system('mv {EC2_PATH}/global_historical_PDomUse_month_millionm3_5min_1960_2014.nc4 {EC2_PATH}/global_historical_PDomWN_month_millionm3_5min_1960_2014.nc4')
get_ipython().system('mv {EC2_PATH}/global_historical_PDomUse_year_millionm3_5min_1960_2014.nc4 {EC2_PATH}/global_historical_PDomWN_year_millionm3_5min_1960_2014.nc4')

get_ipython().system('mv {EC2_PATH}/global_historical_PIndUse_month_millionm3_5min_1960_2014.nc4 {EC2_PATH}/global_historical_PIndWN_month_millionm3_5min_1960_2014.nc4')
get_ipython().system('mv {EC2_PATH}/global_historical_PIndUse_year_millionm3_5min_1960_2014.nc4 {EC2_PATH}/global_historical_PIndWN_year_millionm3_5min_1960_2014.nc4')


# As you can see, the filename structure of the runoff files is different. Using Panoply to inspect the units, we rename the files accordingly. 
# 
# new names for annual:  
# 
# global_historical_runoff_year_myear_5min_1958_2014.nc
# 
# new name for monthly:  
# 
# global_historical_runoff_month_mmonth_5min_1958_2014.nc
# 

# In[20]:

get_ipython().system('mv {EC2_PATH}/totalRunoff_annuaTot_output.nc {EC2_PATH}/global_historical_runoff_year_myear_5min_1958_2014.nc')


# In[21]:

get_ipython().system('mv {EC2_PATH}/totalRunoff_monthTot_output.nc {EC2_PATH}/global_historical_runoff_month_mmonth_5min_1958_2014.nc')


# Final Folder strcuture

# In[22]:

get_ipython().system('ls {EC2_PATH}')


# 

# In[ ]:



