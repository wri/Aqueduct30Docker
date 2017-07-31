
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

# In[5]:

get_ipython().system('ls /volumes/data/PCRGlobWB20V01/')


# Total number of files in folder

# In[6]:

get_ipython().system('find /volumes/data/PCRGlobWB20V01/ -type f | wc -l')


# ---------ONLY RUN IF YOU WANT TO DELETE FILES IN FOLDER /volumes/data/PCRGlobWB20V01/ ON YOUR INSTANCE ------------

# In[ ]:

get_ipython().system('rm -r /volumes/data/PCRGlobWB20V01/')


# -----------------------------------------End danger zone------------------------------------------------------------------
# 
# 
# 
# 
# 

# Grab a coffee before you run the following command. This will copy the files from S3 to your EC2 instance. 

# In[8]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/processData/01PCRGlobWBV01 /volumes/data/PCRGlobWB20V01/ --recursive')


# List files downloaded 

# In[3]:

get_ipython().system('find /volumes/data/PCRGlobWB20V01/ -type f | wc -l')


# As you can see there are some zipped files. Unzipping

# In[ ]:

Aqueduct30/processData/01PCRGlobWBV01/totalRunoff_monthTot_output.zip


# Back to Python

# In[6]:

import os


# In[7]:

pathName = "/volumes/data/PCRGlobWB20V01/waterdemand"


# In[8]:

files = os.listdir(pathName)


# In[9]:

print "Number of files: " + str(len(files))


# The total number of files should be 14. If your number of files is different, try running the first command (aws s3 sync) in a terminal. Remove any partially downloaded files. 
