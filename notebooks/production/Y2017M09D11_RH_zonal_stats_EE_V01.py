
# coding: utf-8

# ### Zonal Statistics using EE with PCRGlobWB data and hydroBasin level 6
# 
# * Purpose of script: This script will perform a zonal statistics calculation using earth engine, PCRGlobWB data and Hydrobasin level 6 at 30s resolution. 
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170911

# In[1]:

import time
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
print(dateString,timeString)


# In[2]:

import ee
import numpy as np
import subprocess
from retrying import retry
import itertools
import re
from pprint import *


# In[3]:

ee.Initialize()


# ## Settings

# In[4]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

GCS_BUCKET= "aqueduct30_v01"
GCS_OUTPUT_PATH = "Y2017M09D11_RH_zonal_stats_EE_V01/"

HYBASLEVEL = 6

DIMENSION5MIN = "4320x2160"
DIMENSION30S = "43200x21600"
CRS = "EPSG:4326"

VERSION = 10

HYDROBASINS = "projects/WRI-Aquaduct/PCRGlobWB20V07/hybas_lev00_v1c_merged_fiona_30s_V01"

AREA5min = "projects/WRI-Aquaduct/PCRGlobWB20V07/area_5min_m2V11" 
AREA30s = "projects/WRI-Aquaduct/PCRGlobWB20V07/area_30s_m2V11"
ONES5MIN ="projects/WRI-Aquaduct/PCRGlobWB20V07/ones_5minV11"
ONES30s = "projects/WRI-Aquaduct/PCRGlobWB20V07/ones_30sV11"



# In[5]:

reducers = ee.Reducer.mean().combine(reducer2= ee.Reducer.count(), sharedInputs= True)
weightedReducers = reducers.splitWeights()


# In[6]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )
geometry = ee.Geometry.Polygon(coords=[[-10.0, -10.0], [10,  -10.0], [10, 10], [-10,10]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# ## Functions

# In[7]:

def prepareZonalRaster(image):
    image    = ee.Image(image)
    newImage = ee.Image(image.divide(ee.Number(10).pow(ee.Number(12).subtract(HYBASLEVEL))).floor())
    newImage = newImage.copyProperties(image)
    newImage = ee.Image(newImage).toInt64().select(["b1"],["PfafID"])
    newImage = newImage.set({"exportdescription":"Hybas%0.2d" %(HYBASLEVEL)})     
    return newImage

def readAsset(assetId):
    try:
        if ee.data.getInfo(assetId)["type"] == "Image":
            asset = ee.Image(assetId)
            assetType = "image"
        elif ee.data.getInfo(assetId)["type"] == "ImageCollection":
            asset = ee.ImageCollection(assetId)
            assetType = "imageCollection"
        else:
            print("error")
    except:
        print("error",assetId)
    return {"assetId":assetId,"asset":asset,"assetType":assetType}

def addSuffix(fc,suffix):
    namesOld = ee.Feature(fc.first()).toDictionary().keys()
    namesNew = namesOld.map(lambda x:ee.String(x).cat("_").cat(ee.String(suffix)))
    return fc.select(namesOld, namesNew)

def zonalStats(valueImage, weightImage, zonesImage):
    # ee export function is client side. getInfo required
    suffix = ee.Image(valueImage).get("exportdescription").getInfo() 
    scale = zonesImage.projection().nominalScale()
    totalImage = ee.Image(valueImage).addBands(ee.Image(weightImage)).addBands(ee.Image(zonesImage))
    resultsList = ee.List(totalImage.reduceRegion(
        geometry= geometry,
        reducer= weightedReducers.group(groupField= 2, groupName= "PfafID"),
        scale= scale,
        maxPixels= 1e10
        ).get("groups"))
    fc = ee.FeatureCollection(resultsList.map(lambda listItem: ee.Feature(None,listItem)))
    fc2 = addSuffix(fc, suffix)
    fc2 = fc2.copyProperties(valueImage)
    return fc2

#@retry(wait_exponential_multiplier=10000, wait_exponential_max=100000)
def export(fc):
    # Make sure your fc has an attribute called exportdescription.    
    # There is a bug in ee that adds ee_export.csv to your filename. Will remove in next steps
    fc = ee.FeatureCollection(fc)
    description = fc.get("exportdescription").getInfo() + "V%0.2d" %(VERSION)
    fileName = fc.get("exportdescription").getInfo() + "V%0.2d" %(VERSION)
    myExportFC = ee.batch.Export.table.toCloudStorage(collection=fc,
                                                    description= description,
                                                    bucket = GCS_BUCKET,
                                                    fileNamePrefix= GCS_OUTPUT_PATH  + fileName ,
                                                    fileFormat="CSV")
    myExportFC.start()
    


# In[8]:

command = "earthengine ls %s" %(EE_PATH)


# In[9]:

assetList = subprocess.check_output(command,shell=True).splitlines()


# ## Script
# 
# This script consists of several parts:
# 1. Auxiliary data  
#     This includes area cell count, avarage cell size
# 1. Supply Data  
#     zonal stats for long term average supply (1960-2014)
# 1. Demand Data  
#     zonal stats for all combinations of Dom, Ind, Irr, IrrLinear, Liv and WW, WN
# 
# 

# ### Auxiliary Data

# In[ ]:




# In[10]:

d ={}


# In[11]:

d["zones"] = readAsset(HYDROBASINS) 
d["area5min"] = readAsset(AREA5min)
d["area30s"] = readAsset(AREA30s)
d["ones5min"] = readAsset(ONES5MIN)
d["ones30s"] = readAsset(ONES30s)

d["zones"]["image"] = prepareZonalRaster(ee.Image(HYDROBASINS))


# In[12]:

print(d)


# In[13]:

dOut ={}


# In[14]:

dOut["fcArea"] = zonalStats(d["area30s"]["asset"], d["ones30s"]["asset"],d["zones"]["asset"])


# ### PCRGLOBWB Data

# In[15]:

sectors = ["Dom","Ind","Irr","IrrLinear","Liv"]
parameters = ["WW","WN"]
temporalScales = ["year","month"]


# In[16]:

for r in itertools.product(sectors,parameters, temporalScales): 
    regex = "%s%s_%s" %(r[0],r[1],r[2])
    for assetId in assetList:
        if re.search(regex,assetId):
            d[regex] = readAsset(assetId)
            d[regex]["PCRGlobWB"] = 1
    

    


# In[17]:

pprint(d)


# In[ ]:



