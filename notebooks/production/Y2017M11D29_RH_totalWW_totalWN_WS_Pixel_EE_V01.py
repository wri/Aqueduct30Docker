
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


# In[2]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M11D29_RH_totalWW_totalWN_WS_Pixel_EE_V01"

OUTPUT_VERSION = 2

DIMENSION5MIN = "4320x2160"
DIMENSION30S = "43200x21600"
CRS = "EPSG:4326"

MAXPIXELS =1e10

YEARMIN = 1960
YEARMAX = 2014


# In[3]:

import ee
import subprocess
import pandas as pd
import logging


# In[4]:

ee.Initialize()


# In[5]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/%sV%0.2d.log" %(SCRIPT_NAME,OUTPUT_VERSION))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[6]:

sectors = ["PDom","PInd","PIrr","PLiv"]
demandTypes = ["WW","WN"]
temporalResolutions = ["year","month"]


# In[7]:

crsTransform = [
                0.0833333309780367,
                0,
                -179.99999491255934,
                0,
                -0.0833333309780367,
                90.00000254430942
              ]


# In[8]:

def createCollections(demandType,temporalResolution):
    icId = "global_historical_PTot%s_%s_millionm3_5min_1960_2014" %(demandType,temporalResolution)
    command = "earthengine create collection %s/%s" %(EE_PATH,icId) 
    result = subprocess.check_output(command,shell=True)
    print(command,result)

def createCollectionsWS(temporalResolution):
    icId = "global_historical_WS_%s_dimensionless_5min_1960_2014" %(temporalResolution)
    command = "earthengine create collection %s/%s" %(EE_PATH,icId) 
    result = subprocess.check_output(command,shell=True)
    print(command,result)    

def existing(year,month,temporalResolution,demandType):
    icID = "%s/global_historical_PTot%s_%s_millionm3_5min_1960_2014" %(EE_PATH,demandType,temporalResolution)
    
    assetID = "%s/global_historical_PTot%s_%s_millionm3_5min_1960_2014/global_historical_PTot%s_%s_millionm3_5min_1960_2014Y%0.4dM%0.2d" %(EE_PATH,demandType,temporalResolution,demandType,temporalResolution,year,month)
    image = ee.Image(assetID)
    try:
        if image.id().getInfo():
            exists = True
    except:
        exists = False
    return exists


def existingWS(year,month,temporalResolution):
    icID = "%s/global_historical_WS_%s_dimensionless_5min_1960_2014" %(EE_PATH,temporalResolution)
    
    assetID = "%s/global_historical_WS_%s_dimensionless_5min_1960_2014/global_historical_WS_%s_dimensionless_5min_1960_2014Y%0.4dM%0.2d" %(EE_PATH,temporalResolution,temporalResolution,year,month)
    image = ee.Image(assetID)
    try:
        if image.id().getInfo():
            exists = True
    except:
        exists = False
    return exists


    
def totalDemand(year,month,demandType,temporalResolution):
    elapsed = datetime.datetime.now() - startLoop
    print(year,month,demandType,temporalResolution)
    print(elapsed)
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
                  "year": year,
                  "demandType":demandType,
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
    
    description = "PTot%s_%sY%0.4dM%0.2dV%0.2d" %(demandType,temporalResolution,year,month,OUTPUT_VERSION)
    assetID = "%s/global_historical_PTot%s_%s_millionm3_5min_1960_2014/global_historical_PTot%s_%s_millionm3_5min_1960_2014Y%0.4dM%0.2d" %(EE_PATH,demandType,temporalResolution,demandType,temporalResolution,year,month)
    
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(totalImage),
        description = description,
        assetId = assetID,
        dimensions = DIMENSION5MIN,
        crs = CRS,
        crsTransform = crsTransform,
        maxPixels = MAXPIXELS     
    )
    task.start() 
    
    
def waterStressUncapped(year,month,temporalResolution):
    d = {}
    keys = []
    properties = {"indicator":"WS5min" ,
                  "temporal_range_max": 2014,
                  "ingested_by":"RutgerHofste",
                  "units":"dimensionless",
                  "temporal_resolution":temporalResolution,
                  "exportdescription":"WS5min_%sY%0.4dM%0.2d" %(temporalResolution,year,month),
                  "temporal_range_min":1960,
                  "month": month,
                  "year": year,
                  "script_used": SCRIPT_NAME,
                  "version": OUTPUT_VERSION
                 }
    
    icWW = ee.ImageCollection("%s/global_historical_PTotWW_%s_millionm3_5min_1960_2014" %(EE_PATH,temporalResolution))
    icDischarge = ee.ImageCollection("%s/global_historical_riverdischarge_%s_millionm3_5min_1960_2014" %(EE_PATH,temporalResolution))

    if temporalResolution == "year":
        imageWW = ee.Image(icWW.filter(ee.Filter.eq("year",year)).first())
        imageDischarge = ee.Image(icDischarge.filter(ee.Filter.eq("year",year)).first())
        image = imageWW.divide(imageDischarge)
    elif temporalResolution == "month":
        imageWW = ee.Image(icWW.filter(ee.Filter.eq("year",year)).filter(ee.Filter.eq("month",month)).first())
        imageDischarge = ee.Image(icDischarge.filter(ee.Filter.eq("year",year)).filter(ee.Filter.eq("month",month)).first())
        image = imageWW.divide(imageDischarge)
    else:
        image = -9999
        
    image = image.set(properties)
    description = "WS_%sY%0.4dM%0.2dV%0.2d" %(temporalResolution,year,month,OUTPUT_VERSION)
    assetID = "%s/global_historical_WS_%s_dimensionless_5min_1960_2014/global_historical_WS_%s_dimensionless_5min_1960_2014Y%0.4dM%0.2d" %(EE_PATH,temporalResolution,temporalResolution,year,month)

    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(image),
        description = description,
        assetId = assetID,
        dimensions = DIMENSION5MIN,
        crs = CRS,
        crsTransform = crsTransform,
        maxPixels = MAXPIXELS     
    )
    task.start()


# In[9]:

for demandType in demandTypes:
    for temporalResolution in temporalResolutions:
        createCollections(demandType,temporalResolution)


# Earth Engine sometimes encounters internal server issues. Running this loop 2-3 times until the size of the imageCollections is the same as the input. 

# In[10]:

"""
startLoop = datetime.datetime.now()
for demandType in demandTypes:
    for temporalResolution in temporalResolutions:
        try:            
            if temporalResolution == "year":
                month = 12
                for year in range(YEARMIN,YEARMAX+1):
                    if existing(year,month,temporalResolution,demandType):
                        logger.debug("exists %0.4d %0.2d %s %s" %(year,month,temporalResolution,demandType))
                    else: 
                        logger.debug("exists %0.4d %0.2d %s %s" %(year,month,temporalResolution,demandType))
                        totalDemand(year,month,demandType,temporalResolution)
            elif temporalResolution == "month":
                for year in range(YEARMIN,YEARMAX+1): 
                    for month in range(1,13):
                        if existing(year,month,temporalResolution,demandType):
                            pass
                            logger.debug("exists %0.4d %0.2d %s %s" %(year,month,temporalResolution,demandType))
                        else:
                            logger.debug("exists %0.4d %0.2d %s %s" %(year,month,temporalResolution,demandType))
                            totalDemand(year,month,demandType,temporalResolution)
        except:
            logger.error("error")
"""


# ### Water Stress at Pixel level

# In[11]:

for temporalResolution in temporalResolutions:
    createCollectionsWS(temporalResolution)


# In[12]:

startLoop = datetime.datetime.now()
for temporalResolution in temporalResolutions:
    try:
        if temporalResolution == "year":
            month = 12
            for year in range(YEARMIN,YEARMAX+1):
                if existingWS(year,month,temporalResolution):
                    logger.debug("exists %0.4d %0.2d %s" %(year,month,temporalResolution))

                else:
                    waterStressUncapped(year,month,temporalResolution)
                    logger.debug("exists %0.4d %0.2d %s" %(year,month,temporalResolution))
        elif temporalResolution == "month":
            for year in range(YEARMIN,YEARMAX+1): 
                for month in range(1,13):
                    if existingWS(year,month,temporalResolution):
                        logger.debug("exists %0.4d %0.2d %s" %(year,month,temporalResolution))

                    else:
                        waterStressUncapped(year,month,temporalResolution)
                        logger.debug("exists %0.4d %0.2d %s" %(year,month,temporalResolution))
    except:
        logger.exception("error  %0.4d %0.2d %s" %(year,month,temporalResolution))


# In[13]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

