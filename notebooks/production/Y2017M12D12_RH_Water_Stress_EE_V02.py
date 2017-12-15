
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

OUTPUT_VERSION = 4

PFAF_LEVEL = 6

YEARMIN = 1960
YEARMAX = 2014

THRESHOLD = 1.25 #Qmax > 1.25 Qsum -> use Qsum, else use max(Qmax,Qsum) 

DIMENSIONS30SSMALL = "43200x19440"

CRS = "EPSG:4326"

CRS_TRANSFORM30S_SMALL = [0.008333333333333333, 0.0, -180.0, 0.0, -0.008333333333333333, 81.0]


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


# In[7]:

def createIndicatorDataFrame():
    df = pd.DataFrame()
    for temporalResolution in temporalResolutions:
        newRow = {}
        newRow["temporalResolution"] = temporalResolution
        newRow["icWW"] = "%s/global_historical_PTotWW_%s_m_pfaf%0.2d_1960_2014" %(EE_PATH,temporalResolution,PFAF_LEVEL)
        newRow["icQ"] = "%s/global_historical_availableriverdischarge_%s_millionm3_5minPfaf%0.1d_1960_2014" %(EE_PATH,temporalResolution,PFAF_LEVEL)
        newRow["icWN"] = "%s/global_historical_PTotWN_%s_m_pfaf%0.2d_1960_2014" %(EE_PATH,temporalResolution,PFAF_LEVEL)
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
        
        
def zonalStatsToRaster(image,zonesImage,geometry,maxPixels,reducerType,scale):
    # reducertype can be mean, max, sum, first. Count is always included for QA
    # the resolution of the zonesimage is used for scale

    reducer = ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"mean"),ee.Reducer.mean(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"max"),ee.Reducer.max(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"sum"),ee.Reducer.sum(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"first"),ee.Reducer.first(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"mode"),ee.Reducer.mode(),"error"))))
    )
    reducer = ee.Reducer(reducer).combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName="zones") 

    
    zonesImage = zonesImage.select(zonesImage.bandNames(),["zones"])

    totalImage = ee.Image(image).addBands(zonesImage)
    resultsList = ee.List(totalImage.reduceRegion(
        geometry= geometry, 
        reducer= reducer,
        scale= scale,
        maxPixels=maxPixels,
        bestEffort =True
        ).get("groups"))

    resultsList = resultsList.map(ensure_default_properties); 
    zoneList = mapList(resultsList, 'zones');
    countList = mapList(resultsList, 'count');
    valueList = mapList(resultsList, reducerType);

    valueImage = zonesImage.remap(zoneList, valueList).select(["remapped"],[reducerType])
    countImage = zonesImage.remap(zoneList, countList).select(["remapped"],["count"])
    newImage = zonesImage.addBands(countImage).addBands(valueImage)
    return newImage



def fluxToVolume(image):
    newImage = ee.Image(image).multiply(area30s_pfaf06).divide(1e6)
    newImage = newImage.copyProperties(image)
    newImage = newImage.set({"units":"millionm3","script_used":SCRIPT_NAME})
    return newImage
  
    
def calculateWS(iQ,iWW,iWN,scale30s):            
    iQmax = iQ.select(["max"])  # maximum discharge per basin in million m^3
    iQsum = iQ.select(["sum"])  # sum discharge per basin in million m^3 of all sinks as identied. 

    iQsum30s = zonalStatsToRaster(iQsum,zones30s,geometrySmall,1e10,"mode",scale30s).select(["mode"])
    iQmax30s = zonalStatsToRaster(iQmax,zones30s,geometrySmall,1e10,"mode",scale30s).select(["mode"])

    dif = (iQmax30s.subtract(iQsum30s)).divide(iQsum30s)
    useLargest = dif.lte(THRESHOLD)  # use largest of max(sum,max) when difference below threshold
    useSum = dif.gt(THRESHOLD) # use sum when above threshold. If max is much larger than sum, this is an indication of a confluence at the edge of a basin. 

    iQ =  useLargest.multiply(iQmax30s.max(iQsum30s))
    iQ = iQ.add(useSum.multiply(iQsum30s))
    
    iQ  = iQ .select(iQ.bandNames(),["Q_millionm3"])    
    iWN = iWN.select(iWN.bandNames(),["WN_millionm3"])
    iWW = iWW.select(iWW.bandNames(),["WW_millionm3"])
    
    # add local consumption to available supply. No threshold is used so WN can be larger than Q per cell. 
    iBA = iQ.add(iWN)            
    iWS = iWW.divide(iBA)
    iWS = iWS.select(iWS.bandNames(),["WS_dimensionless"])
    
    imageOut = iWS.addBands(iQ).addBands(iWW).addBands(iWN)
    return imageOut
    


def exportAsset(imageOut,assetID,dimensions,properties,CRS_TRANSFORM30S_SMALL):
    try:
        ee.Image(assetID).id().getInfo()
        nonExisting = False
    except:
        nonExisting = True
    
    if  nonExisting:  
        imageOut = imageOut.set(properties)
        description = "global_historical_WS_%s_dimensionless_30sPfaf%0.2d_1960_2014Y%0.4dM%0.2dV%0.2d" %(row["temporalResolution"],PFAF_LEVEL,year,month,OUTPUT_VERSION)

        task = ee.batch.Export.image.toAsset(
            image =  ee.Image(imageOut),
            description = description,
            assetId = assetID,
            dimensions = dimensions,
            crs = CRS,
            crsTransform = CRS_TRANSFORM30S_SMALL,
            maxPixels = 1e10     
        )
        print(assetID)
        logger.debug(assetID)
        task.start()
    

    


# In[8]:

area30s = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/area_30s_m2V11")
zones30s = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/hybas_lev00_v1c_merged_fiona_30s_V01")
zones30s = zones30s.divide(ee.Number(10).pow(ee.Number(12).subtract(PFAF_LEVEL))).floor().toInt64();

crs30s = area30s.projection()

area30s_pfaf06 = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/area_30spfaf06_m2_V01V01").select(["sum"])

scale30s = zones30s.projection().nominalScale().getInfo()


# In[9]:

area30sPfaf6 = zonalStatsToRaster(area30s,zones30s,geometrySmall,1e10,"sum",scale30s)


# In[10]:

area30sPfaf6_m2 = area30sPfaf6.select(["sum"]) # image at 30s with area in m^2 per basin


# In[11]:

temporalResolutions = ["year","month"]


# In[12]:

temporalResolutions = ["month"]


# In[13]:

df = createIndicatorDataFrame()


# In[ ]:

df.head()


# In[ ]:



for index, row in df.iterrows():
    if row["temporalResolution"] == "year":
        createCollections(row["newIcId"])        
        icQ = ee.ImageCollection(row["icQ"])
        icWW_flux = ee.ImageCollection(row["icWW"])
        icWN_flux = ee.ImageCollection(row["icWN"])
        
        icWW_volume =icWW_flux.map(fluxToVolume) # units millionm3
        icWN_volume =icWN_flux.map(fluxToVolume) # units millionm3
        
        month =12
        for year in range(YEARMIN,YEARMAX+1):
            iQ = ee.Image(icQ.filter(ee.Filter.eq("year",year)).first()) 
            iWW = ee.Image(icWW_volume.filter(ee.Filter.eq("year",year)).first()).select(["mean"])
            iWN = ee.Image(icWN_volume.filter(ee.Filter.eq("year",year)).first()).select(["mean"])
            
            iWS = calculateWS(iQ,iWW,iWN,scale30s)
                                  
            properties = {"script_used":SCRIPT_NAME,
                          "ingested_by":"RutgerHofste",
                          "units":"dimensionless",
                          "version":OUTPUT_VERSION,
                          "year":year,
                          "month":month,
                          "threshold":THRESHOLD,
                          "temporalResolution":row["temporalResolution"]
                         }
            assetID = "%s/global_historical_WS_%s_dimensionless_30sPfaf%0.2d_1960_2014Y%0.4dM%0.2d" %(row["newIcId"],row["temporalResolution"],PFAF_LEVEL,year,month)
            
            exportAsset(iWS,assetID,DIMENSIONS30SSMALL,properties,CRS_TRANSFORM30S_SMALL) 
             
    elif row["temporalResolution"] == "month":      
        createCollections(row["newIcId"])
        icQ = ee.ImageCollection(row["icQ"])
        icWW_flux = ee.ImageCollection(row["icWW"])
        icWN_flux = ee.ImageCollection(row["icWN"])
        
        icWW_volume =icWW_flux.map(fluxToVolume) # units millionm3
        icWN_volume =icWN_flux.map(fluxToVolume) # units millionm3
                
        for year in range(YEARMIN,YEARMAX+1): 
            for month in range(1,13):
                iQ = ee.Image(icQ.filter(ee.Filter.eq("year",year)).filter(ee.Filter.eq("month",month)).first()) 
                iWW = ee.Image(icWW_volume.filter(ee.Filter.eq("year",year)).filter(ee.Filter.eq("month",month)).first()).select(["mean"])
                iWN = ee.Image(icWN_volume.filter(ee.Filter.eq("year",year)).filter(ee.Filter.eq("month",month)).first()).select(["mean"])
                    
                iWS = calculateWS(iQ,iWW,iWN,scale30s)

                properties = {"script_used":SCRIPT_NAME,
                              "ingested_by":"RutgerHofste",
                              "units":"dimensionless",
                              "version":OUTPUT_VERSION,
                              "year":year,
                              "month":month,
                              "threshold":THRESHOLD,
                              "temporalResolution":row["temporalResolution"]
                             }
                assetID = "%s/global_historical_WS_%s_dimensionless_30sPfaf%0.2d_1960_2014Y%0.4dM%0.2d" %(row["newIcId"],row["temporalResolution"],PFAF_LEVEL,year,month)
                
                exportAsset(iWS,assetID,DIMENSIONS30SSMALL,properties,CRS_TRANSFORM30S_SMALL) 
                
                
                
                


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:




# In[ ]:



