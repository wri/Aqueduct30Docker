
# coding: utf-8

# ### Zonal Statistics using EE with PCRGlobWB data and hydroBasin level 6
# 
# * Purpose of script: This script will perform a zonal statistics calculation using earth engine, PCRGlobWB data and Hydrobasin level 6 at 30s resolution. 
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170911

# In[1]:

import time, datetime
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
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

TESTING = 0

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

HYBASLEVEL = 6

DIMENSION5MIN = "4320x2160"
DIMENSION30S = "43200x21600"
CRS = "EPSG:4326"

VERSION = 16

GCS_BUCKET= "aqueduct30_v01"
GCS_OUTPUT_PATH = "Y2017M09D11_RH_zonal_stats_EE_V%0.2d/" %(VERSION)

HYDROBASINS = "projects/WRI-Aquaduct/PCRGlobWB20V07/hybas_lev00_v1c_merged_fiona_30s_V01"

AREA5MIN = "projects/WRI-Aquaduct/PCRGlobWB20V07/area_5min_m2V11" 
AREA30S = "projects/WRI-Aquaduct/PCRGlobWB20V07/area_30s_m2V11"
ONES5MIN ="projects/WRI-Aquaduct/PCRGlobWB20V07/ones_5minV11"
ONES30S = "projects/WRI-Aquaduct/PCRGlobWB20V07/ones_30sV11"

YEAR_MIN = 2014
YEAR_MAX = 2014

PATTERN = "Y2014"


# In[5]:

reducers = ee.Reducer.mean().combine(reducer2= ee.Reducer.count(), sharedInputs= True)
weightedReducers = reducers.splitWeights()


# In[6]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )

if TESTING ==1:
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

def volumeToFlux(image):
    image = ee.Image(image)
    image = image.divide(ee.Image(AREA5MIN)).multiply(1e6).copyProperties(image)
    image = image.set("units","m")
    image = image.set("convertedToFlux", 1)
    return image

def addWeightImage(d):
    dOut = d
    if d['PCRGlobWB'] == 1:
        dOut["weightAsset30s"] = ee.Image(AREA30s)
    else: 
        pprint(d)
        dOut["weightAsset30s"] = ee.Image(ONES30s)
    return dOut

@retry(wait_exponential_multiplier=10000, wait_exponential_max=100000)
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

# In[10]:

regexList = []


# In[11]:

#auxList = ["zones","area_5min","area_30s","ones_5min","ones_30s"]
auxList = ["zones","area_30s","ones_30s"]


# In[12]:

sectors = ["Dom","Ind","Irr","IrrLinear","Liv"]
parameters = ["WW","WN"]
temporalScales = ["year","month"]
runoffparameters = ["runoff","reducedmeanrunoff"]


# In[13]:

if TESTING:
    sectors = ["Dom"]
    parameters = ["WW"]
    temporalScales = ["year","month"]
    runoffparameters = ["runoff","reducedmeanrunoff"]


# In[14]:

demandList = []
for r in itertools.product(sectors,parameters, temporalScales): 
    regex = "%s%s_%s" %(r[0],r[1],r[2])
    demandList = demandList + [regex]


# In[15]:

supplyList = []
for r in itertools.product(runoffparameters, temporalScales): 
    regex = "%s_%s" %(r[0],r[1])
    supplyList = supplyList + [regex]


# In[16]:

regexList = regexList + auxList + demandList + supplyList


# In[17]:

d = dict(zip(regexList,[{}]*len(regexList)))


# In[18]:

for regex in regexList:
    # item is also the regular expression
    print(regex)
    if regex == "zones":
        d[regex] = readAsset(HYDROBASINS)
        d[regex]["asset"] = prepareZonalRaster(ee.Image(HYDROBASINS))
    else:
        for assetId in assetList:
            if re.search(regex,assetId):
                d[regex] = readAsset(assetId)


# In[19]:

zonesImage = d["zones"]["asset"]


# In[20]:

a = []

for key, nestedDict in d.iteritems():
    try:
        if key in auxList:
            print(key, " using ones30s as weight")
            weightsImage = ee.Image(ONES30S)

        else:
            print(key, " using area30s as weight")
            weightsImage = ee.Image(AREA30S)



        if nestedDict["assetType"] == "image":
            image = nestedDict["asset"]
            units = image.get("units").getInfo()
            if units == "millionm3":
                image = volumeToFlux(image) 


            fcOut = zonalStats(nestedDict["asset"],weightsImage,zonesImage)
            export(fcOut)
        elif nestedDict["assetType"] == "imageCollection": 
            imagesList = ee.data.getList({"id":"%s" %(nestedDict["assetId"])} )
            for item in imagesList:
                imageId = item["id"]
                # Filter 2014 images
                image = ee.Image(imageId)
                units = image.get("units").getInfo()
                if units == "millionm3":
                    image = volumeToFlux(image)           


                if re.search(PATTERN,imageId):
                    fcOut = zonalStats(ee.Image(image),weightsImage,zonesImage)
                    export(fcOut)
        else:
            print("error")
    except:
        print("error",key)
        


# In[21]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

