
# coding: utf-8

# In this notebook we will copy the data for the first couple of steps from WRI's Amazon S3 Bucket. The data is large i.e. **17.8GB** so a good excuse to drink a coffee. The output in Jupyter per file is suppressed so you will only see a result after the file has been donwloaded. The largest file is 4.1GB. You can also run this command in your terminal and see the process per file

# In[ ]:


get_ipython().system('aws s3 sync s3://wri-projects/Aqueduct30/processData/01PCRGlobWBV01 /volumes/data/PCRGlobWB20V01/')


# List files downloaded 

# In[2]:


get_ipython().system(' ls /volumes/data/PCRGlobWB20V01/waterdemand')


# Back to Python

# In[17]:


import os


# In[18]:


pathName = "/volumes/data/PCRGlobWB20V01/waterdemand"


# In[19]:


files = os.listdir(pathName)


# In[20]:


print "Number of files: " + str(len(files))


# The total number of files should be 14. If your number of files is different, try running the first command (aws s3 sync) in a terminal. Remove any partially downloaded files. 
