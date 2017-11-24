
# coding: utf-8

# # Y2017M11D24_RH_Prepare_Image_Collections_EE_V01
# 
# * Purpose of script: put all earth engine imagecollections in the same format (millionm^3  and dimensionless)
# * Kernel used: python27
# * Date created: 20171124  

# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[5]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"


# In[ ]:




# In[10]:

import ee
import re
import subprocess
from calendar import monthrange


# In[4]:

ee.Initialize()


# ICs not in right format: discharge (m^3 / s) and runoff (m/month or m/year)

# In[7]:

def readAsset(assetId):
    # this function will read both images and imageCollections 
    if ee.data.getInfo(assetId)["type"] == "Image":
        asset = ee.Image(assetId)
        assetType = "image"


    elif ee.data.getInfo(assetId)["type"] == "ImageCollection":
        asset = ee.ImageCollection(assetId)
        assetType = "imageCollection"
        
    else:
        print("error")        
    return {"assetId":assetId,"asset":asset,"assetType":assetType}


# In[8]:

command = "earthengine ls %s" %(EE_PATH)


# In[11]:

assetList = subprocess.check_output(command,shell=True).splitlines()


# In[ ]:

fileName = "global_historical_runoff_month_mmonth_5min_1958_2014"


# In[ ]:




# In[12]:

regexs = [""]


# In[ ]:



