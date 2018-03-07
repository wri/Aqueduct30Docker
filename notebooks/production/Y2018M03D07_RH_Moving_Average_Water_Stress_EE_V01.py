
# coding: utf-8

# # Y2018M03D07_RH_Moving_Average_Water_Stress_EE_V01
# 
# * Purpose of script: Calculate Water Stress at pfaf6 30s level by dividing WW and Q 10 year moving averages. Converts WW to volumes  
# 
# * Script exports to: TO DO
# 
# 
# * Kernel used: python35
# * Date created: 20170307
# 
# 

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[24]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2018M03D07_RH_Moving_Average_Water_Stress_EE_V01"

WW_INPUT_VERSION = 2
WN_INPUT_VERSION = 2
Q_INPUT_VERSION = 2



OUTPUT_VERSION = 1

TESTING = 1

CRS = "EPSG:4326"

PFAF_LEVEL = 6

DIMENSION30S = {}
DIMENSION30S["x"] = 43200
DIMENSION30S["y"] = 21600


# In[6]:

import ee
import os
import logging
import pandas as pd
import subprocess


# In[7]:

ee.Initialize()


# In[8]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/%sV%0.2d.log" %(SCRIPT_NAME,OUTPUT_VERSION))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[9]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[10]:

crsTransform30sSmall = [
    360.0 / DIMENSION30S["x"], 
    0,
    -180,
    0,
    -162.0 / (0.9* DIMENSION30S["y"]),
    81   
]

dimensions30sSmall = "{}x{}".format(DIMENSION30S["x"],int(0.9*DIMENSION30S["y"]))


# In[25]:

area30s = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/area_30s_m2V11")
zones30s = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/hybas_lev00_v1c_merged_fiona_30s_V01")
zones30s = zones30s.divide(ee.Number(10).pow(ee.Number(12).subtract(PFAF_LEVEL))).floor().toInt64();

crs30s = area30s.projection()

area30s_pfaf06 = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/area_30spfaf06_m2_V01V01").select(["sum"])

scale30s = zones30s.projection().nominalScale().getInfo()


# In[21]:

def create_collection(assetid):
    """ Create image collection in earth engine asset folder
    
    This function will only work if the folder in which the
    new imageCollection will be created is valid
    
    
    Args:
        assetid (string) : asset id for the new image collection
    
    Returns: 
        result (string) : captured message from command line
    
    """   
    
    command = "earthengine create collection {}".format(assetid) 
    result = subprocess.check_output(command,shell=True)
    if result:
        logger.error(result)
    return result

def zonal_stats_to_raster(image,zonesImage,geometry,maxPixels,reducerType,scale):
    """ Zonal statistics using rasterized zones
    
    Args:
        image (ee.Image) : input image with values (Check the units)
        zonesImage (ee.Image) : integer image with the zones
        geometry (ee.Geometry) : geometry indicating the extent of the calculation. Note if geometry is geodesic
        maxPixels (integer) : maximum numbers of pixels within geometry
        reducerType (string) : options include 'mean', 'max', 'sum', 'first' en 'mode' 
    
    
    
    # reducertype can be mean, max, sum, first. Count is always included for QA
    # the resolution of the zonesimage is used for scale
    """
    
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


# In[11]:

months = range(1,13)
years = range(1960+9,2014+1)
indicators = ["WS"]


# In[12]:

df = pd.DataFrame()

for indicator in indicators:
    for month in months:
        for year in years:
            newRow = {}
            newRow["month"] = month
            newRow["year"] = year
            newRow["output_ic_filename"] = "global_historical_{}_month_none_pfaf06_1960_2014_movingaverage_10y_V{:02.0f}".format(indicator,OUTPUT_VERSION)
            newRow["output_ic_assetid"] = "{}/{}".format(EE_PATH,newRow["output_ic_filename"])
            newRow["output_i_filename"] = "global_historical_{}_month_none_pfaf06_Y{:04.0f}M{:02.0f}_movingaverage_10y_V{:02.0f}".format(indicator,year,month,OUTPUT_VERSION)
            newRow["output_i_assetid"] = "{}/{}".format(newRow["output_ic_assetid"],newRow["output_i_filename"])
            newRow["indicator"] = indicator
            newRow["exportdescription"] = "{}_month_Y{:04.0f}M{:02.0f}_movingaverage_10y".format(indicator,year,month)
            df= df.append(newRow,ignore_index=True)


# In[16]:

df.head()


# In[17]:

if TESTING:
    df = df[0:2]


# In[20]:

for output_ic_assetid in df["output_ic_assetid"].unique():
    result = create_collection(output_ic_assetid)
    print(result)


# In[ ]:




# In[22]:

area30sPfaf6 = zonal_stats_to_raster(area30s,zones30s,geometrySmall,1e10,"sum",scale30s)


# In[ ]:



