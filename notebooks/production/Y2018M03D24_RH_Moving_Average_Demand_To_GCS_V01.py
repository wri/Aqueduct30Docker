
# coding: utf-8

# # Y2018M03D24_RH_Moving_Average_Demand_To_GCS_V01
# 
# * Purpose of script: store moving average results for demand in a CSV file in GCS.
# 
# * Script exports to: 
# * Kernel used: python35
# * Date created: 20180324
# 
# 
# 

# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[66]:

SCRIPT_NAME = "Y2018M03D24_RH_Moving_Average_Demand_To_GCS_V01"

INPUT_VERSION = 3

OUTPUT_VERSION = 1

GCS_OUTPUT_PATH = "{}/outputV{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
GCS_BUCKET = "aqueduct30_v01"


# In[29]:

import ee
import pandas as pd


# In[5]:

ee.Initialize()


# In[68]:

# Functions

def ensure_default_properties(obj):
    """ Sets the properties mean, count and sum for an object
    
    if a reduceRegion operation returns no values in earth engine
    one cannot join the result list with a list of the zones without
    setting a  nodata (-9999) value first
    
    Args:
        obj (ee.Dictionary) : earth engine object.
    
    Returns
        obj (ee.Dictionary) : dictionary with mean, count and sum 
                              set to -9999.    
    
    """
    obj = ee.Dictionary(obj)
    default_properties = ee.Dictionary({"mean": -9999,"count": -9999,"sum":-9999})
    return default_properties.combine(obj)

def mapList(results, key):
    """ filter results by key
    server side function.
    
    Args:
        results (ee.List) : list of result dictionaries.
        key (string) : key name.
    
    Returns:
        newResult (ee.List) : List of values
    
    
    """
    newResult = results.map(lambda x: ee.Dictionary(x).get(key))
    return newResult


def dict_to_feature(d):
    """ Convert a dictionary to an earth engine feature
    server side
    
    Args:
        d (ee.Dictionary) : server side dictionary
    Returns
        f (ee.Feature) : server side earth engine feature    
    """
    f = ee.Feature(None,ee.Dictionary(d))
    return f
    
    


def zonalStatsToRaster(image,zonesImage,geometry,maxPixels,reducerType):
    """ Zonal statistics with rasters as input and rasters and lists as output
    
    Args:
        image (ee.Image) : image with value. Make sure the dimensions and units are 
                           compatible with the zones image.
        zonesImage (ee.Image) : image with zones stores as unique integers. Dimensions
                                must match image argument.
        geometry (ee.Geometry) : geometry specifying the extent of the calculation.
        maxPixels (integer) : Maximum number of pixels in calculation. Defaults to 1e10.
        reducerType (string) : reducer type. Options include mean max sum first and mode.
        
    Returns:
        newImage (ee.Image) : Image with zones, count and values as bands
        zoneList (ee.List) : list with zones
        countList : (ee.List) : list with counts per zone
        valueList : (ee.List) : list with values per zone       
    
    
    """
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


def zonalStatsToFeatureCollection(image,zonesImage,geometry,maxPixels,reducerType):
    """ Zonal statistics with rasters as input and rasters and lists as output
    
    Args:
        image (ee.Image) : image with value. Make sure the dimensions and units are 
                           compatible with the zones image.
        zonesImage (ee.Image) : image with zones stores as unique integers. Dimensions
                                must match image argument.
        geometry (ee.Geometry) : geometry specifying the extent of the calculation.
        maxPixels (integer) : Maximum number of pixels in calculation. Defaults to 1e10.
        reducerType (string) : reducer type. Options include mean max sum first and mode.
        
    Returns:
        resultList (ee.List) : List of dictionaries     
    
    
    """
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
    fc = ee.FeatureCollection(resultList.map(dict_to_feature))

    return fc


def export_asset(fc):
    """ Export a google earth engine featureCollection to an asset folder
    
    function will start a new task. To view the status of the task
    check the javascript API or query tasks script. Function is used 
    as mapped function so other arguments need to be set globally. 
    
    Args:
        fc (ee.FeatureCollection) : featureCollection to export
        
    Returns:
           
    """
    
    task = ee.batch.Export.table.toCloudStorage(
        collection =  ee.FeatureCollection(fc),
        description = "test_description",
        bucket = GCS_BUCKET,
        fileNamePrefix = GCS_OUTPUT_PATH  + "fileName",
        fileFormat = "CSV"
    )
    task.start()



# In[11]:

temp_image = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/area_30spfaf06_m2_V01V01")


# In[12]:

area_30spfaf06_m2 = temp_image.select(["sum"])


# In[13]:

zones_30spfaf06 = temp_image.select(["zones"])


# In[21]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[15]:

ic_WN = ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_PTotWN_month_m_pfaf06_1960_2014_movingaverage_10y_V{:02.0f}".format(INPUT_VERSION))


# In[17]:

test_image = ee.Image(ic_WN.first())


# In[24]:

newImage,zoneList,valueList,countList = zonalStatsToRaster(test_image,zones_30spfaf06,geometry,1e10,"mode")


# In[54]:

fc = zonalStatsToFeatureCollection(test_image,zones_30spfaf06,geometry,1e10,"mode")


# In[69]:

export_asset(fc)


# In[ ]:



