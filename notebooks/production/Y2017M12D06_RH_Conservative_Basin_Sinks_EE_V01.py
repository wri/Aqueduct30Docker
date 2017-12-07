
# coding: utf-8

# # Y2017M12D06_RH_Conservative_Basin_Sinks_EE_V01
# 
# * Purpose of script: find conservative discharge point
# * Kernel used: python27
# * Date created: 20171206

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M12D06_RH_Conservative_Basin_Sinks_EE_V01"

OUTPUT_VERSION = 3

TESTING =0

CRS = "EPSG:4326"

DIMENSION5MIN = "4320x2160"


# In[3]:

import ee
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

crsTransform = [
                0.0833333309780367,
                0,
                -179.99999491255934,
                0,
                -0.0833333309780367,
                90.00000254430942
              ]


# In[7]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )

if TESTING ==1:
    geometry = ee.Geometry.Polygon(coords=[[-10.0, -10.0], [10,  -10.0], [10, 10], [-10,10]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[8]:

zones = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/hybas_lev06_v1c_merged_fiona_30s_V01")
FA5min = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/accumulateddrainagearea_05min_km2")
ldd5min = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/lddsound_05min_numpad")


# In[9]:

scale5min = FA5min.projection().nominalScale().getInfo()


# Steps 
# 
# 1. use mode to resample hydrobasins to 5min  
# 1. find maximum FA per basin (grouped zonal stats)  
# 1. mask maximum FA per basin  
# 1. add coastal and endorheac basins  
# 1. find maximum FA per basin after masked  
# 1. mask discharge using 4)+coastal/endo and perform zonal stats again. 
# 
# 
# 
# 

# In[10]:

zones_5min_mode = zones.reduceResolution(
      reducer = ee.Reducer.mode(),
      maxPixels =  1024
    ).reproject(
      crs =   FA5min.projection()
    );
    


# In[11]:

def ensure_default_properties(obj): 
    obj = ee.Dictionary(obj)
    default_properties = ee.Dictionary({"mean": -9999,"count": -9999,"max":-9999})
    return default_properties.combine(obj)

def mapList(results, key):
    newResult = results.map(lambda x: ee.Dictionary(x).get(key))
    return newResult


def zonalStatsToImage(valueImage,zoneImage,geometry,reducer,scale):   
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
    
    maxList = mapList(resultsList,"max")
    maxImage = ee.Image(zoneImage).remap(zoneList, maxList)
    maxImage = ee.Image(maxImage).select(["remapped"],["max"])
    
    countList = mapList(resultsList,"count")
    countImage = ee.Image(zoneImage).remap(zoneList, countList)
    countImage = ee.Image(countImage).select(["remapped"],["count"])
    
    resultImage = zoneImage.addBands(countImage) .addBands(maxImage) 
    resultImage = resultImage.copyProperties(valueImage)    
        
    properties = {"script_used":SCRIPT_NAME,
                  "output_version":OUTPUT_VERSION,
                  "reducer":"max_and_count",
                  }
    
    resultImage = resultImage.set(properties)    
    return ee.Image(resultImage)


# In[12]:

reducer = ee.Reducer.max().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")


# In[13]:

maxFA = zonalStatsToImage(FA5min,zones_5min_mode,geometry,reducer,scale5min).select(["max"])


# This step will mask out the cells of FA5min = maxFA but include the pixels that are sinks

# In[14]:

FAmask = (maxFA.eq(FA5min).subtract(ldd5min.eq(5))).neq(1)


# In[15]:

FAMasked = FA5min.mask(FAmask)


# In[16]:

maxFAMasked = zonalStatsToImage(FAMasked,zones_5min_mode,geometry,reducer,scale5min).select(["max"])


# The final step will create a boolean raster with all cells that are used to find the available discherge per basins. Non-Endo basins will have 1 pixel whereas endo basins will have multiple. 

# In[17]:

QSearchMask = (maxFAMasked.eq(FA5min).add(ldd5min.eq(5)))


# Create an image with three bands: zones (mode 5min), globalMaxFA, SearchMask

# In[18]:

imageOut = zones_5min_mode.addBands(maxFAMasked).addBands(QSearchMask)


# In[19]:

imageOut = imageOut.select([u'b1', u'max', u'max_1'],["zones_mode_pfaf6","masked_max_fa","q_search_mask"])


# In[20]:

properties = {"created_by":"Rutger Hofste",
             "script_used":SCRIPT_NAME,
             "version":OUTPUT_VERSION,
             "resolution":"5min",
             "units": "pfaf_id, km2, boolean",
             "description":"image with three bands. 1 containing the resampled zones, 2 with the maximum flow accumulation at 5min after masked and 3 search mask for discharge"}


# In[21]:

imageOut = imageOut.set(properties)


# In[22]:

task = ee.batch.Export.image.toAsset(
    image =  ee.Image(imageOut),
    description = SCRIPT_NAME,
    assetId = EE_PATH + "/" +SCRIPT_NAME,
    dimensions = DIMENSION5MIN,
    crs = CRS,    
    crsTransform = crsTransform,
    maxPixels = 1e10    
)
task.start() 

