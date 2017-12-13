
# coding: utf-8

# # Y2017M12D12_RH_Zonal_Areas_EE_V01
# 
# * Purpose of script: create raster images with the area per basin at 5min and 30s resolution
# * Kernel used: python27
# * Date created: 20171212

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M12D12_RH_Zonal_Areas_EE_V01"

PFAF_LEVEL = 6

OUTPUT_VERSION = 1


CRS = "EPSG:4326"


# In[3]:

import ee
import logging
import pandas as pd
import subprocess


# In[4]:

ee.Initialize()


# In[5]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[6]:

area30s = ee.Image("%s/area_30s_m2V11" %(EE_PATH))
zones30s = ee.Image("%s/hybas_lev00_v1c_merged_fiona_30s_V01" %(EE_PATH))
zones30s = zones30s.divide(ee.Number(10).pow(ee.Number(12).subtract(PFAF_LEVEL))).floor().toInt64();
crs30s = area30s.projection()


# In[7]:

area5min = ee.Image("%s/area_5min_m2V11" %(EE_PATH))
zones5min = ee.Image("%s/hybas_lev00_v1c_merged_fiona_5min_V01" %(EE_PATH))
zones5min = zones5min.divide(ee.Number(10).pow(ee.Number(12).subtract(PFAF_LEVEL))).floor().toInt64();
crs5min = area5min.projection()


# For the delineation of zones from 30s to 5min we used nearest neighbor in fiona and mode in Y2017M12D07_RH_ZonalStats_MaxQ_toImage_EE_V01. This might result in slightly different behavior. 

# In[8]:

zones5minMode = ee.Image(ee.ImageCollection("%s/global_historical_availableriverdischarge_year_millionm3_5minPfaf6_1960_2014"%(EE_PATH) ).first()).select(["zones_mode_pfaf6"])


# In[9]:

def ensure_default_properties(obj): 
    obj = ee.Dictionary(obj)
    default_properties = ee.Dictionary({"mean": -9999,"count": -9999})
    return default_properties.combine(obj)

def mapList(results, key):
    newResult = results.map(lambda x: ee.Dictionary(x).get(key))
    return newResult


def zonalStatsToRaster(image,zonesImage,geometry,maxPixels,reducerType):
    # reducertype can be mean, max, sum, first. Count is always included for QA
    # the resolution of the zonesimage is used for scale

    reducer = ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"mean"),ee.Reducer.mean(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"max"),ee.Reducer.max(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"sum"),ee.Reducer.sum(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"first"),ee.Reducer.first(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"mode"),ee.Reducer.mode(),"error"))))
    )
    reducer = ee.Reducer(reducer).combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName="zones") 

    scale = zonesImage.projection().nominalScale().getInfo()
    zonesImage = zonesImage.select(zonesImage.bandNames(),["zones"])

    totalImage = ee.Image(image).addBands(zonesImage)
    resultsList = ee.List(totalImage.reduceRegion(
        geometry= geometry, 
        reducer= reducer,
        scale= scale,
        maxPixels=maxPixels
        ).get("groups"))

    resultsList = resultsList.map(ensure_default_properties); 
    zoneList = mapList(resultsList, 'zones');
    countList = mapList(resultsList, 'count');
    valueList = mapList(resultsList, reducerType);

    valueImage = zonesImage.remap(zoneList, valueList).select(["remapped"],[reducerType])
    countImage = zonesImage.remap(zoneList, countList).select(["remapped"],["count"])
    newImage = zonesImage.addBands(countImage).addBands(valueImage)
    return newImage,zoneList,valueList,countList

def exportAsset(imageOut,imageName,dimensions,properties):
    assetID = "%s/%sV%0.2d"  %(EE_PATH,imageName,OUTPUT_VERSION) 
    imageOut = imageOut.set(properties)
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(imageOut),
        description = imageName,
        assetId = assetID,
        dimensions = dimensions,
        crs = CRS,
        crsTransform = imageOut.projection().getInfo()["transform"],
        maxPixels = 1e10     
    )
    print(assetID)
    task.start()


# In[10]:

area30sBasin,zoneList,valueList,countList =  zonalStatsToRaster(area30s,zones30s,geometry,1e10,"sum")


# In[11]:

area5minBasin,zoneList,valueList,countList =  zonalStatsToRaster(area5min,zones5min,geometry,1e10,"sum")


# In[12]:

area5minBasinMode,zoneList,valueList,countList =  zonalStatsToRaster(area5min,zones5minMode,geometry,1e10,"sum")


# In[ ]:




# In[13]:

properties = {"script_used":SCRIPT_NAME,"ingested_by":"RutgerHofste","units":"m2","version":OUTPUT_VERSION}


# In[14]:

exportAsset(area30sBasin,"area_30spfaf%0.2d_m2_V%0.2d" %(PFAF_LEVEL,OUTPUT_VERSION),"43200x21600",properties)


# In[15]:

exportAsset(area5minBasin,"area_5minpfaf%0.2d_m2_V%0.2d" %(PFAF_LEVEL,OUTPUT_VERSION),"4320x2160",properties)


# In[16]:

exportAsset(area5minBasinMode,"area_5minpfaf%0.2dMode_m2_V%0.2d" %(PFAF_LEVEL,OUTPUT_VERSION),"4320x2160",properties)


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

