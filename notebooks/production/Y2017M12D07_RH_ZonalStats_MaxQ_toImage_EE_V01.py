
# coding: utf-8

# # Y2017M12D07_RH_ZonalStats_MaxQ_toImage_EE_V01
# 
# * Purpose of script: find conservative and global max discharge value per Hydrobasin level 6 and export to imageCollection. 
# * Kernel used: python27
# * Date created: 20171207

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M12D07_RH_ZonalStats_MaxQ_toImage_EE_V01"

OUTPUT_VERSION = 3

TESTING =0

CRS = "EPSG:4326"

DIMENSION5MIN = "4320x2160"

YEARMIN = 1960
YEARMAX = 2014


# In[3]:

import ee
import logging
import pandas as pd
import subprocess


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

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )

if TESTING ==1:
    geometry = ee.Geometry.Polygon(coords=[[-10.0, -10.0], [10,  -10.0], [10, 10], [-10,10]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[7]:

icQ_year = ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_riverdischarge_year_millionm3_5min_1960_2014")
icQ_month = ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_riverdischarge_month_millionm3_5min_1960_2014")


# In[8]:

searchImage = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/Y2017M12D06_RH_Conservative_Basin_Sinks_EE_V01")
zones = searchImage.select(["zones_mode_pfaf6"])
sinks = searchImage.select(["q_search_mask"]).gte(1) 



# In[9]:

scale5min = zones.projection().nominalScale().getInfo()


# In[10]:

crsTransform = [
                0.0833333309780367,
                0,
                -179.99999491255934,
                0,
                -0.0833333309780367,
                90.00000254430942
              ]


# In[11]:

def createIndicatorDataFrame():
    indicatorDf = pd.DataFrame()
    for temporalResolution in temporalResolutions:
        newRow = {}
        newRow["temporalResolution"] = temporalResolution
        newRow["icID"] = "%s/global_historical_availableriverdischarge_%s_millionm3_5minPfaf06_1960_2014" %(EE_PATH,temporalResolution)
        newRow["units"] = "millionm3"
        
        indicatorDf = indicatorDf.append(newRow,ignore_index=True)
    return indicatorDf

def createCollections():
    icId = row["icID"]        
    command = "earthengine create collection %s" %(icId) 
    result = subprocess.check_output(command,shell=True)
    if result:
        logger.error(result)
    return icId

def ensure_default_properties(obj): 
    obj = ee.Dictionary(obj)
    default_properties = ee.Dictionary({"mean": -9999,"count": -9999,"max":-9999})
    return default_properties.combine(obj)

def mapList(results, key):
    newResult = results.map(lambda x: ee.Dictionary(x).get(key))
    return newResult


def zonalStatsToImage(valueImage,zoneImage,stat,geometry,scale):   
    if stat == "mean":
        reducer = ee.Reducer.mean().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")
    elif stat == "max":
        reducer = ee.Reducer.max().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")
    elif stat == "sum":
        reducer = ee.Reducer.sum().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")

    
    # image with band1 = values, band2 = zones   
    totalImage = ee.Image(valueImage).addBands(ee.Image(zoneImage))
    resultsList = ee.List(
      ee.Image(totalImage).reduceRegion(
        geometry= geometry,
        reducer= reducer,
        scale= scale,
        maxPixels=1e10
      ).get("groups")
    )
    resultsList = resultsList.map(ensure_default_properties)
    zoneList = mapList(resultsList, 'zones')
    
    statList = mapList(resultsList,stat)
    statImage = ee.Image(zoneImage).remap(zoneList, statList).select(["remapped"],[stat])
    
    countList = mapList(resultsList,"count")
    countImage = ee.Image(zoneImage).remap(zoneList, countList).select(["remapped"],["count"])

    
    resultImage = zoneImage.addBands(countImage) .addBands(statImage) 
    #resultImage = resultImage.copyProperties(valueImage)    
        
    properties = {"script_used":SCRIPT_NAME,
                  "output_version":OUTPUT_VERSION,
                  "reducer":stat,
                  "units":"reduced see script"
                  }
    
    resultImage = resultImage.set(properties)    
    return ee.Image(resultImage)


def combineStats(image):
    allQ = image
    sinksQ = allQ.mask(sinks) 
    sumQSinks = zonalStatsToImage(sinksQ,zones,"sum",geometry,scale5min).select(["sum"])
    maxQglobal = zonalStatsToImage(image,zones,"max",geometry,scale5min).select(["max"])
    totalImage = zones.addBands(sumQSinks).addBands(maxQglobal)
    
    properties = {"description":"combined",
                  "year": year,
                  "month": month,
                  "temporalResolution":row["temporalResolution"],
                  "parameter":"reducedDischarge",
                  "version":OUTPUT_VERSION}
    totalImage = totalImage.set(properties) 
    
    return totalImage

def exportAsset(imageOut):
    assetID = "%s/global_historical_availableriverdischarge_%s_millionm3_5minPfaf6_1960_2014Y%0.4dM%0.2d" %(newIcID,row["temporalResolution"],year,month)
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(imageOut),
        description = "global_historical_availableriverdischarge_%s_millionm3_5minPfaf6_1960_2014Y%0.4dM%0.2d" %(row["temporalResolution"],year,month),
        assetId = assetID,
        dimensions = DIMENSION5MIN,
        crs = CRS,
        crsTransform = crsTransform,
        maxPixels = 1e10     
    )
    print(assetID)
    task.start()


# In[12]:

zones_5min_mode = zones.reduceResolution(
      reducer = ee.Reducer.mode(),
      maxPixels =  1024
    ).reproject(
      crs =   zones.projection()
    );


# In[13]:

temporalResolutions = ["year","month"]


# In[14]:

df = createIndicatorDataFrame()


# In[15]:

df.head()


# In[16]:

for index, row in df.iterrows():
    newIcID = createCollections()
    if row["temporalResolution"] == "year":
        month =12
        for year in range(YEARMIN,YEARMAX+1):
            image = ee.Image(icQ_year.filter(ee.Filter.eq("year",year)).first())
            imageOut = combineStats(image)
            exportAsset(imageOut)
            logger.debug("%s Year %0.4d Month %0.4d" %(index,year,month))
                                
    elif row["temporalResolution"] == "month":
        for year in range(YEARMIN,YEARMAX+1): 
            for month in range(1,13):
                image = ee.Image(icQ_month.filter(ee.Filter.eq("year",year)).filter(ee.Filter.eq("month",month)).first())
                imageOut = combineStats(image)
                exportAsset(imageOut)
                logger.debug("%s Year %0.4d Month %0.4d" %(index,year,month))


# In[17]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


#  
