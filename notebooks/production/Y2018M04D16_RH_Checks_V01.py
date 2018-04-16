
# coding: utf-8

# In[3]:

""" Run setup checks for credentials. 
-------------------------------------------------------------------------------
Aqueduct uses AWS, gsutil, earthengine and other command line interfaces 
or modules that require setup. 


Author: Rutger Hofste
Date: 20180416
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name


Returns:


"""

# Input Parameters

SCRIPT_NAME = "Y2018M04D16_RH_Checks_V01"


# In[4]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# Authorize the following services in your terminal:
# 
# 1. AWS  
# `aws configure`
# 1. gcloud  
# `gcloud init`  
# 1. earthengine  
# `earthengine authenticate`
# 
# 
# 

# In[ ]:




# In[ ]:




# In[5]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:24:15.930678    

# In[ ]:



