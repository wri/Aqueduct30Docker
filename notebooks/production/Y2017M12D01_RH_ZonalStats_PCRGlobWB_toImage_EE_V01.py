
# coding: utf-8

# # Y2017M12D01_RH_ZonalStats_PCRGlobWB_toImage_EE_V01
# 
# * Purpose of script: calculate sectoral demand, total demand, runoff and discharge per Hydrobasin level 6, export to imageCollections
# * Kernel used: python27
# * Date created: 20171201

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M12D01_RH_ZonalStats_PCRGlobWB_toImage_EE_V01"

OUTPUT_VERSION = 3

PFAF_LEVEL = 6

TESTING = 0

DIMENSION5MIN = {}
DIMENSION5MIN["x"] = 4320
DIMENSION5MIN["y"] = 2160

DIMENSION30S = {}
DIMENSION30S["x"] = 43200
DIMENSION30S["y"] = 21600

CRS = "EPSG:4326"

YEARMIN = 1960
YEARMAX = 2014


# In[3]:

import ee
import subprocess
import pandas as pd
import logging
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


# In[7]:

area5min = ee.Image("%s/area_5min_m2V11" %(EE_PATH))
dimensions5min = "%sx%s" %(DIMENSION5MIN["x"],DIMENSION5MIN["y"])
dimensions30s = "%sx%s" %(DIMENSION30S["x"],DIMENSION30S["y"])


# In[9]:

crsTransform5min = [
                0.0833333309780367,
                0,
                -179.99999491255934,
                0,
                -0.0833333309780367,
                90.00000254430942
              ]


# In[10]:

crsTransform30s = [
                0.008333333333333333,
                0,
                -180,
                0,
                -0.008333333333333333,
                90
              ]


# In[11]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )

if TESTING ==1:
    geometry = ee.Geometry.Polygon(coords=[[-10.0, -10.0], [10,  -10.0], [10, 10], [-10,10]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[12]:

demandSectors = ["PDom","PInd","PIrr","PLiv","PTot"]
demandTypes = ["WW","WN"]
temporalResolutions = ["year","month"]

supplySectors = ["runoff","riverdischarge"]


# Because running takes so long I will run the most important datasets first

# In[23]:

demandSectors = ["PTot"]
demandTypes = ["WW"]
temporalResolutions = ["year"]

supplySectors = ["riverdischarge"]


# In[24]:

def createIndicatorDataFrame():
    indicatorDf = pd.DataFrame()
    for temporalResolution in temporalResolutions:
        for demandSector in demandSectors:
            for demandType in demandTypes:        
                newRow = {}
                newRow["sector"] = demandSector
                newRow["demandType"] = demandType
                newRow["temporalResolution"] = temporalResolution
                newRow["units"] = "millionm3"
                newRow["icID"] = "%s/global_historical_%s%s_%s_millionm3_5min_1960_2014" %(EE_PATH,demandSector,demandType,temporalResolution)
                indicatorDf = indicatorDf.append(newRow,ignore_index=True)
        for supplySector in supplySectors:
                newRow = {}
                newRow["sector"] =supplySector
                newRow["demandType"] = ""
                newRow["temporalResolution"] = temporalResolution
                newRow["units"] = "millionm3"
                if supplySector == "riverdischarge":
                    newRow["icID"] = "%s/global_historical_%s_%s_millionm3_5min_1960_2014" %(EE_PATH,supplySector,temporalResolution)
                elif supplySector == "runoff":
                    newRow["icID"] = "%s/global_historical_%s_%s_millionm3_5min_1958_2014" %(EE_PATH,supplySector,temporalResolution)
                indicatorDf = indicatorDf.append(newRow,ignore_index=True)        
        
    return indicatorDf

def createBasinsImage(PfafLevel):
    HydroBASINSimage = ee.Image("users/rutgerhofste/PCRGlobWB20V04/support/global_Standard_lev00_30sGDALv01")
    HydroBASINSimageProjection = HydroBASINSimage.projection()
    HydroBASINSimageNominalScale = HydroBASINSimageProjection.nominalScale()
    hydroBasin = HydroBASINSimage.divide(ee.Number(10).pow(ee.Number(12).subtract(PfafLevel))).floor()
    hydroBasin = hydroBasin.toInt64()
    return ee.Image(hydroBasin), HydroBASINSimageNominalScale.getInfo()

def volumeToFlux5min(image):
    fluxImage = ee.Image(image).divide(area5min).multiply(1e6)
    fluxImage = fluxImage.copyProperties(image)
    fluxImage = fluxImage.set("units","m")
    return ee.Image(fluxImage)


def ensure_default_properties(obj): 
    obj = ee.Dictionary(obj)
    default_properties = ee.Dictionary({"mean": -9999,"count": -9999})
    return default_properties.combine(obj)

def mapList(results, key):
    newResult = results.map(lambda x: ee.Dictionary(x).get(key))
    return newResult

def createCollections(sector,demandType,temporalResolution):
    icId = "%s/global_historical_%s%s_%s_m_pfaf%0.2d_1960_2014" %(EE_PATH,sector,demandType,temporalResolution,PFAF_LEVEL)
        
    command = "earthengine create collection %s" %(icId) 
    result = subprocess.check_output(command,shell=True)
    if result:
        logger.error(result)
    return icId
        
def zonalStatsToImage(image):     
    imageFlux = volumeToFlux5min(image)
    totalImage = imageFlux.addBands(hydroBasin)
    totalImage = totalImage.select(totalImage.bandNames(),["flux","zones"])
    resultsList = ee.List(
      totalImage.reduceRegion(
        geometry= geometry,
        reducer= reducer,
        scale= hybasScale,
        maxPixels=1e10
      ).get("groups")
    )
    resultsList = resultsList.map(ensure_default_properties)
    zoneList = mapList(resultsList, 'zones')
    
    meanList = mapList(resultsList,"mean")
    meanImage = hydroBasin.remap(zoneList, meanList)
    meanImage = ee.Image(meanImage).select(["remapped"],["mean"])
    
    countList = mapList(resultsList,"count")
    countImage = hydroBasin.remap(zoneList, countList)
    countImage = ee.Image(countImage).select(["remapped"],["count"])
    
    resultImage = meanImage.addBands(countImage)    
    resultImage = resultImage.copyProperties(image)    
        
    exportdescription = "%s%s_%sY%0.4dM%0.2d" %(row["sector"],row["demandType"],row["temporalResolution"],year,month)
    properties = {"units":"m",
                  "script_used":SCRIPT_NAME,
                  "output_version":OUTPUT_VERSION,
                  "reducer":"mean_and_count",
                  "Pfaf_Level":PFAF_LEVEL,
                  "exportdescription":exportdescription
                  }
    
    resultImage = resultImage.set(properties)
    
    newAssetID = "%s/global_historical_%s%s_%s_m_pfaf%0.2d_1960_2014Y%0.4dM%0.2d" %(newIcID,row["sector"],row["demandType"],row["temporalResolution"],PFAF_LEVEL,year,month)
    logger.debug(newAssetID)
    description = "%sV%0.2d" %(exportdescription, OUTPUT_VERSION)
    
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(resultImage),
        description = description,
        assetId = newAssetID,
        dimensions = dimensions30s,
        crs = CRS,    
        crsTransform = crsTransform30s,
        maxPixels = 1e10    
    )
    task.start()     
    
    return ee.Image(resultImage)


# In[25]:

indicatorDf = createIndicatorDataFrame()


# In[26]:

hydroBasin, hybasScale = createBasinsImage(PFAF_LEVEL)


# In[27]:

indicatorDf


# In[28]:

reducer = ee.Reducer.mean().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")


# In[21]:

for index, row in indicatorDf.iterrows():
    print(row["icID"])
    newIcID = createCollections(row["sector"],row["demandType"],row["temporalResolution"])
    ic = ee.ImageCollection(row["icID"])
    
    if row["temporalResolution"] == "year":
        for year in range(YEARMIN,YEARMAX+1):        
            logger.debug("%s %0.4d" %(index,year))
            month = 12
            image = ee.Image(ic.filter(ee.Filter.eq("year",year)).first())
            resultImage = zonalStatsToImage(image)
            
    if row["temporalResolution"] == "month":
        for year in range(YEARMIN,YEARMAX+1):
            for month in range(1,13):
                logger.debug("%s Year %0.4d Month %0.4d" %(index,year,month))
                image = ee.Image(ic.filter(ee.Filter.eq("year",year)).filter(ee.Filter.eq("month",month)).first())
                resultImage = zonalStatsToImage(image)   
    


# In[22]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

