
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

# Running the following command will take a couple of minutes. it will copy the data (several GBs) from S3 to the EC2 instance.

# In[1]:

get_ipython().system('ls /volumes/data/PCRGlobWB20V01/')


# Total number of files in folder

# In[2]:

get_ipython().system('find /volumes/data/PCRGlobWB20V01/ -type f | wc -l')


# ---------ONLY RUN IF YOU WANT TO DELETE FILES IN FOLDER /volumes/data/PCRGlobWB20V01/ ON YOUR INSTANCE ------------

# In[3]:

#!rm -r /volumes/data/PCRGlobWB20V01/


# -----------------------------------------End danger zone------------------------------------------------------------------
# 
# 
# 
# 
# 

# Grab a coffee before you run the following command. This will copy the files from S3 to your EC2 instance. 

# In[4]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/processData/01PCRGlobWBV01 /volumes/data/PCRGlobWB20V01/ --recursive')


# List files downloaded (24 in my case)

# In[5]:

get_ipython().system('find /volumes/data/PCRGlobWB20V01/ -type f | wc -l')


# As you can see there are some zipped files. Unzipping

# Unzipping the file results in a 24GB file which is signifact. Therefore this step will take quite some time

# In[6]:

get_ipython().system('unzip /volumes/data/PCRGlobWB20V01/totalRunoff_monthTot_output.zip')


# The total number of files should be around 25 but can change if the raw data changed. 

# In[7]:

get_ipython().system('ls -lah /volumes/data/PCRGlobWB20V01/')


# In the data that Yoshi provided there is only Livestock data for consumption (WN). However in an email he specified that the withdrawal (WW) equals the consumption (100% consumption) for livestock. Therefore we copy the WN Livestock files to WW to make looping over WN and WW respectively easier. 

# In[8]:

get_ipython().system('cp /volumes/data/PCRGlobWB20V01/global_historical_PLivWN_month_millionm3_5min_1960_2014.nc4 /volumes/data/PCRGlobWB20V01/global_historical_PLivWW_month_millionm3_5min_1960_2014.nc4')


# In[9]:

get_ipython().system('cp /volumes/data/PCRGlobWB20V01/global_historical_PLivWN_year_millionm3_5min_1960_2014.nc4 /volumes/data/PCRGlobWB20V01/global_historical_PLivWW_year_millionm3_5min_1960_2014.nc4')


# In[10]:

get_ipython().system('ls -lah /volumes/data/PCRGlobWB20V01/')


# In[11]:

import os
pathName = "/volumes/data/PCRGlobWB20V01/"
files = os.listdir(pathName)
print("Number of files: " + str(len(files)))


# Copy PLivWN to PLivWW because Livestock Withdrawal = Livestock Consumption (see Yoshi's email'). This will solve some lookping issues in the future. Copies 4GB of data so takes a while

# In[12]:

get_ipython().system('cp /volumes/data/PCRGlobWB20V01/global_historical_PLivWN_month_millionm3_5min_1960_2014.nc4 /volumes/data/PCRGlobWB20V01/global_historical_PLivWW_month_millionm3_5min_1960_2014.nc4')


# In[13]:

get_ipython().system('cp /volumes/data/PCRGlobWB20V01/global_historical_PLivWN_year_millionm3_5min_1960_2014.nc4 /volumes/data/PCRGlobWB20V01/global_historical_PLivWW_year_millionm3_5min_1960_2014.nc4')


# Some files that WRI received from Utrecht refer to water "Use" instead of WN (net). Renaming the relevant file. Renaming them

# In[14]:

get_ipython().system('ls /volumes/data/PCRGlobWB20V01/')


# In[15]:

get_ipython().system('mv /volumes/data/PCRGlobWB20V01/global_historical_PDomUse_month_millionm3_5min_1960_2014.nc4 /volumes/data/PCRGlobWB20V01/global_historical_PDomWN_month_millionm3_5min_1960_2014.nc4')
get_ipython().system('mv /volumes/data/PCRGlobWB20V01/global_historical_PDomUse_year_millionm3_5min_1960_2014.nc4 /volumes/data/PCRGlobWB20V01/global_historical_PDomWN_year_millionm3_5min_1960_2014.nc4')

get_ipython().system('mv /volumes/data/PCRGlobWB20V01/global_historical_PIndUse_month_millionm3_5min_1960_2014.nc4 /volumes/data/PCRGlobWB20V01/global_historical_PIndWN_month_millionm3_5min_1960_2014.nc4')
get_ipython().system('mv /volumes/data/PCRGlobWB20V01/global_historical_PIndUse_year_millionm3_5min_1960_2014.nc4 /volumes/data/PCRGlobWB20V01/global_historical_PIndWN_year_millionm3_5min_1960_2014.nc4')


# In[16]:

get_ipython().system('ls /volumes/data/PCRGlobWB20V01/')


# In[ ]:



