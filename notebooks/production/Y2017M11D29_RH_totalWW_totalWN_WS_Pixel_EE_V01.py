
# coding: utf-8

# # Y2017M11D29_RH_totalWW_totalWN_WS_Pixel_EE_V01
# 
# * Purpose of script: calculate total demand WW WN and water stress at pixel level
# * Kernel used: python27
# * Date created: 20171129 

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[80]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M11D29_RH_totalWW_totalWN_WS_Pixel_EE_V01"

OUTPUT_VERSION = 1


# In[3]:

import ee


# In[4]:

ee.Initialize()


# In[71]:

sectors = ["PDom","PInd","PIrr","PLiv"]
demandTypes = ["WW","WN"]
temporalResolutions = ["year","month"]


# In[81]:


def totalDemand(year,month,demandType,temporalResolution):
    print(year,temporalResolution,demandType)
    d = {}
    keys = []
    properties = {"indicator":"PTot%s" %(demandType) ,
                  "temporal_range_max": 2014,
                  "ingested_by":"RutgerHofste",
                  "units":"millionm3",
                  "temporal_resolution":temporalResolution,
                  "exportdescription":"PTot%s_%sY%0.4dM%0.2d" %(demandType,temporalResolution,year,month),
                  "temporal_range_min":1960,
                  "month": month,
                  "script_used": SCRIPT_NAME,
                  "version": OUTPUT_VERSION
                 }
    
    
    for sector in sectors:
        
        key = "%s%s" %(sector,demandType)
        keys.append(key)
        ic = ee.ImageCollection("%s/global_historical_%s%s_%s_millionm3_5min_1960_2014" %(EE_PATH,sector,demandType,temporalResolution))
        
        if temporalResolution == "year":
            image = ee.Image(ic.filter(ee.Filter.eq("year",year)).first())
        elif temporalResolution == "month":
            image = ee.Image(ic.filter(ee.Filter.eq("year",year)).filter(ee.Filter.eq("month",month)).first())
        else:
            image = -9999
        d[key] = image
    
    
    totalImage = ee.Image(d[keys[0]].add(d[keys[1]]).add(d[keys[2]]).add(d[keys[3]]))
    totalImage = totalImage.set(properties)
    
    
    
    return d, totalImage
    
    


# In[82]:

demandType = "WW"
temporalResolution = "month"
year = 2014
month = 3


# In[83]:

d, totalImage = totalDemand(year,month,demandType,temporalResolution)


# In[84]:

print(totalImage.getInfo())


# In[ ]:



