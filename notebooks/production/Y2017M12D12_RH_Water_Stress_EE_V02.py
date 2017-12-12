
# coding: utf-8

# # Y2017M12D12_RH_Water_Stress_V02
# 
# * Purpose of script: Calculate water stress using maximum discharge and total withdrawals  
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

SCRIPT_NAME = "Y2017M12D12_RH_Water_Stress_EE_V02"

OUTPUT_VERSION = 1

PFAF_LEVEL = 6

YEARMIN = 1960
YEARMAX = 1960


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

geometrySmall = ee.Geometry.Polygon(coords=[[-180.0, -81.0], [180,  -81.0], [180, 81], [-180,81]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[21]:

geometrySmall = ee.Geometry.Polygon(coords=[[-10, -10.0], [10,  -10.0], [10, 10], [-10,10]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[9]:

def createIndicatorDataFrame():
    df = pd.DataFrame()
    for temporalResolution in temporalResolutions:
        newRow = {}
        newRow["temporalResolution"] = temporalResolution
        newRow["icWW"] = "%s/global_historical_PTotWW_%s_m_pfaf%0.2d_1960_2014" %(EE_PATH,temporalResolution,PFAF_LEVEL)
        newRow["icQ"] = "%s/global_historical_availableriverdischarge_%s_millionm3_5minPfaf%0.1d_1960_2014" %(EE_PATH,temporalResolution,PFAF_LEVEL)
        newRow["newIcId"] = "%s/global_historical_WS_%s_dimensionless_30sPfaf%0.2d_1960_2014" %(EE_PATH,temporalResolution,PFAF_LEVEL)
        df = df.append(newRow,ignore_index=True)
    return df


def createCollections(newIcId):        
    command = "earthengine create collection %s" %(newIcId) 
    print(command)
    result = subprocess.check_output(command,shell=True)
    if result:
        pass
        logger.error(result)
        
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



def fluxToVolume(image):
    newImage = ee.Image(image).multiply(area_30sPfaf06).divide(1e6)
    newImage = newImage.copyProperties(image)
    newImage = newImage.set({"units":"millionm3","script_used":SCRIPT_NAME})
    return newImage
    
    
    

    


# In[10]:

area30s = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/area_30s_m2V11")
zones30s = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/hybas_lev00_v1c_merged_fiona_30s_V01")
zones30s = zones30s.divide(ee.Number(10).pow(ee.Number(12).subtract(PFAF_LEVEL))).floor().toInt64();

crs30s = area30s.projection()


# In[12]:

area30sPfaf6,zoneList,valueList,countList = zonalStatsToRaster(area30s,zones30s,geometrySmall,1e10,"sum")


# In[13]:

area30sPfaf6_m2 = area30sPfaf6.select(["sum"]) # image at 30s with area in m^2 per basin


# In[14]:

temporalResolutions = ["year","month"]


# In[15]:

df = createIndicatorDataFrame()


# In[16]:

df.head()


# In[22]:

for index, row in df.iterrows():
    if row["temporalResolution"] == "year":
        createCollections(row["newIcId"])        
        icQ = ee.ImageCollection(row["icQ"])
        icWW_flux = ee.ImageCollection(row["icWW"])
        
        #icWW_volume =icWW_flux.map(fluxToVolume)
            
        month =12
        for year in range(YEARMIN,YEARMAX+1):
            iQ = ee.Image(icQ.filter(ee.Filter.eq("year",year)).first()) #
            iQmax = iQ.select(["max"])  # maximum discharge per basin in million m^3
            iQsum = iQ.select(["sum"])  # sum discharge per basin in million m^3 of all sinks as identied. 
            
            iQsum30s,zoneList,valueList,countList = zonalStatsToRaster(iQsum,zones30s,geometrySmall,1e10,"mode")
            iQmax30s,zoneList2,valueList2,countList2 = zonalStatsToRaster(iQmax,zones30s,geometrySmall,1e10,"mode")
            
            
            # WS = WW/BA
            
            WS = iQsum30s.divide(icWW_flux)
            
            
        
    elif row["temporalResolution"] == "month":      
        createCollections(row["newIcId"])
        icQ = ee.ImageCollection(row["icQ"])
        icWW_flux = ee.ImageCollection(row["icWW"])
        
        #icWW_volume =icWW_flux.map(fluxToVolume)
        
        for year in range(YEARMIN,YEARMAX+1): 
            for month in range(1,13):
                print(year,month)


# In[23]:

dictionary = dict(zip(zoneList.getInfo(), zoneList.getInfo()))


# In[24]:




# In[ ]:



