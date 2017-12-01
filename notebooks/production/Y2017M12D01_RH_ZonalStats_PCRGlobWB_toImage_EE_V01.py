
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


# In[40]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M12D01_RH_ZonalStats_PCRGlobWB_toImage_EE_V01"

OUTPUT_VERSION = 1

PFAF_LEVEL = 6

TESTING = 0

DIMENSION5MIN = {}
DIMENSION5MIN["x"] = 4320
DIMENSION5MIN["y"] = 2160

DIMENSION30S = {}
DIMENSION30S["x"] = 43200
DIMENSION30S["y"] = 21600

CRS = "EPSG:4326"

REDUCER_NAME = "mean"

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


# In[6]:

area5min = ee.Image("%s/area_5min_m2V11" %(EE_PATH))


# In[7]:

dimensions5min = "%sx%s" %(DIMENSION5MIN["x"],DIMENSION5MIN["y"])


# In[8]:

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


# In[41]:

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
    default_properties = ee.Dictionary({REDUCER_NAME: -9999})
    return default_properties.combine(obj)

def mapList(results, key):
    newResult = results.map(lambda x: ee.Dictionary(x).get(key))
    return newResult

def createCollections(sector,demandType,temporalResolution):
    icId = "global_historical_%s%s_%s_m_pfaf%0.2d_1960_2014" %(sector,demandType,temporalResolution,PFAF_LEVEL)
    command = "earthengine create collection %s/%s" %(EE_PATH,icId) 
    result = subprocess.check_output(command,shell=True)
    if result:
        logger.error(result)


# In[23]:

indicatorDf = createIndicatorDataFrame()


# In[42]:

hydroBasin, hybasScale = createBasinsImage(PFAF_LEVEL)


# In[24]:

indicatorDf


# Test for one image

# In[38]:

indicatorDf = indicatorDf[0:2]


# In[46]:

reducer = ee.Reducer.mean().group(groupField=1, groupName= "zones")


# In[47]:

for index, row in indicatorDf.iterrows():
    #command = "earthengine ls %s" %(row["icID"])
    #assetList = subprocess.check_output(command,shell=True).splitlines()
    # get properties from first image 
    firstImage = ee.Image(ee.ImageCollection(row["icID"]).first())

    #createCollections(row["sector"],row["demandType"],row["temporalResolution"])
    ic = ee.ImageCollection(row["icID"])

    if row["temporalResolution"] == "year":
        #for year in range(YEARMIN,YEARMAX+1):
        for year in range(YEARMIN,1961):
            image = ee.Image(ic.filter(ee.Filter.eq("year",year)).first())
            # function ? 
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
            reducedList = mapList(resultsList,REDUCER_NAME)
            resultImage = hydroBasin.remap(zoneList, reducedList)
            resultImage = ee.Image(resultImage).select(["remapped"],["b1"])
            resultImage = resultImage.copyProperties(image)
            properties = {"units":"meanflux","script_used":SCRIPT_NAME,"output_version":OUTPUT_VERSION}
            resultImage = resultImage.set(properties)
            print()


# In[ ]:

indicatorDftest


# In[ ]:

icID = indicatorDftest.loc[0]["icID"]


# In[ ]:

ic = ee.ImageCollection(icID)


# In[ ]:

year = 2014


# In[ ]:

imageFlux = volumeToFlux5min(image)


# In[ ]:

totalImage = imageFlux.addBands(hydroBasin)
totalImage = totalImage.select(totalImage.bandNames(),["flux","zones"])


# In[ ]:




# In[ ]:

resultsList = ee.List(
  totalImage.reduceRegion(
    geometry= geometry,
    reducer= reducer,
    scale= 1000,
    maxPixels=1e10
  ).get("groups")
)


# In[ ]:

resultsList = resultsList.map(ensure_default_properties)


# In[ ]:

zoneList = mapList(resultsList, 'zones')


# In[ ]:

reducedList = mapList(resultsList,REDUCER_NAME)


# In[ ]:

resultImage = hydroBasin.remap(zoneList, reducedList)


# In[ ]:

resultImage = resultImage.copyProperties(image)


# In[ ]:

properties = {"units":"meanflux","script_used":SCRIPT_NAME,"output_version":OUTPUT_VERSION}


# In[ ]:

resultImage = resultImage.set(properties)


# In[ ]:

resultImage = ee.Image(resultImage).select(["remapped"],["b1"])


# In[ ]:

assetID = "users/rutgerhofste/testZones03"


# In[ ]:

task = ee.batch.Export.image.toAsset(
    image =  ee.Image(resultImage),
    description = "test",
    assetId = assetID,
    dimensions = dimensions30s,
    #dimensions = DIMENSION30S,
    crs = CRS,    
    crsTransform = crsTransform30s,
    
    # for testing purposes -----
    #scale = 10000, 
    #region = geometry,
    # end testing  --------
    
    maxPixels = 1e10    
)
task.start() 


# In[ ]:




# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

