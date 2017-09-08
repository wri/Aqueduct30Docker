
# coding: utf-8

# # Calculate linear trend for Agricultural Demand
# 
# * Purpose of script: Calculate linear trend for Agricultural Demand for 2004 - 2014 due to the high sensitivity of the model to ag demand. 
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170901

# In[1]:

import time
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
print(dateString,timeString)


# In[2]:

import os
import ee
import folium
from folium_gee import *
import subprocess
import itertools


# Settings:

# The Standardized format to store assets on Earth Engine is EE_INPUT_PATH / EE_IC_NAME / EE_I_NAME and every image should have the property expertdescription that would allow to export the data to a table header. 

# In[3]:

EE_INPUT_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07/"

YEAR_MIN = 2004
YEAR_MAX = 2014

DIMENSION5MIN = "4320x2160"
CRS = "EPSG:4326"

VERSION = 18

UNITS = "millionm3"
MAXPIXELS =1e10


# In[4]:

temporalScales = ["year","month"]
parameters = ["WW","WN"]


# In[5]:

ee.Initialize()


# In[6]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 89], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# posted question in EE dev forum. Apparently it is easier to print the tranform in Javascipt and paste it into this script. 

# In[7]:

crsTransform = [
                0.0833333309780367,
                0,
                -179.99999491255934,
                0,
                -0.0833333309780367,
                90.00000254430942
              ]


# In[8]:

def addValidProperties(image,d):
    nestedNewDict = {}
    #remove non string or real properties
    for nestedKey, nestedValue in d.iteritems():
        if isinstance(nestedValue,str) or isinstance(nestedValue,int):
            nestedNewDict[nestedKey] = nestedValue
        else:
            pass
            #print("removing property: ",nestedKey )
    image = ee.Image(image).set(nestedNewDict)
    return image

def createTimeBand(image):
    # Adds a timeband to the single band image. band is "b1" 
    year = ee.Number(ee.Image(image).get("year"))
    newImage = ee.Image.constant(year).toDouble().select(["constant"],["independent"])
    image = image.toDouble().select(["b1"],["dependent"])
    return image.addBands(newImage)   

def linearTrend(ic,yearmin,yearmax):
    icTimeband = ic.map(createTimeBand)
    imageFinalYear = ee.Image(ic.filter(ee.Filter.calendarRange(yearmin,yearmax,"year")).first())
    fit = icTimeband.select(["independent","dependent"]).reduce(ee.Reducer.linearFit())
    offset = fit.select(["offset"])
    scale = fit.select(["scale"]) #Note that this definition of scale is a as in y = ax+b
    newImageYearMax = scale.multiply(yearmax).add(offset).select(["scale"],["newValue"])
    return ee.Image(newImageYearMax)

def exportToAsset(image,outputIcName,outputIName):    
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(image),
        description = outputIName,
        assetId = EE_INPUT_PATH + outputIcName + "/" + outputIName ,
        dimensions = DIMENSION5MIN,
        crs = CRS,
        crsTransform = crsTransform,
        maxPixels = MAXPIXELS     
    )
    task.start()

def iterateFunction(parameter,temporalScale):
    inputIcName = "global_historical_PIrr%s_%s_millionm3_5min_1960_2014" %(r[0],r[1])
    outputIcName = "global_historical_PIrr%slinear_%s_millionm3_5min_%0.4d_%0.4dV%0.2d" %(r[0],r[1],YEAR_MIN,YEAR_MAX,VERSION)
    
    # properties independent on temporal_scale
    properties = {}
    properties = {"units":UNITS,
                  "parameter":"Irr%slinear_%s" %(r[0],r[1]),
                  "year":YEAR_MAX,
                  "range_min":YEAR_MIN,
                  "range_max":YEAR_MAX,
                  "creation":"RutgerHofste_%s_Python27" %(dateString),
                  "regression":"linear",
                  "version":VERSION,
                  "nodata_value":-9999,
                  "script_used":"Y2017M09D01_RH_linear_trend_Ag_Demand_EE_V01"
             }
    
    ic = ee.ImageCollection(os.path.join(EE_INPUT_PATH,inputIcName))
    sampleImage = ee.Image(ic.first())
    icFiltered = ic.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year"))
    
    command = ("earthengine create collection %s") %(os.path.join(EE_INPUT_PATH,outputIcName))
    subprocess.check_output(command,shell=True)
    
    if r[1] == "year":
        properties["exportdescription"] = "Irr%sLinear_%sY%0.4d" %(r[0],r[1],YEAR_MAX)
        properties["temporal_scale"] = r[1]
        properties["time_start"] = "%04d-%0.2d-%0.2d" %(YEAR_MAX,12,1)
        print(properties["exportdescription"])
        newImageYearMax = linearTrend(icFiltered,YEAR_MIN,YEAR_MAX)
        newImageYearMax = addValidProperties(newImageYearMax,properties)        
        outputIName = "global_historical_PIrr%slinear_%s_millionm3_5min_%0.4d_%0.4dV%0.2d" %(r[0],r[1],YEAR_MIN,YEAR_MAX,VERSION)   
        exportToAsset(newImageYearMax,outputIcName,outputIName)
        
    elif r[1] =="month":
        for month in range(1,2):
            properties["exportdescription"] = "Irr%sLinear_%sY%0.4dM%0.2d" %(r[0],r[1],YEAR_MAX,month)
            properties["month"] = month
            properties["temporal_scale"] = r[1]   
            properties["time_start"] = "%04d-%0.2d-%0.2d" %(YEAR_MAX,month,1)
            print(properties["exportdescription"])
            icMonths = icFiltered.filter(ee.Filter.eq("month",ee.Number(month)))
            newImageYearMax = linearTrend(icMonths,YEAR_MIN,YEAR_MAX)
            newImageYearMax = addValidProperties(newImageYearMax,properties)
            
            outputIName = "global_historical_PIrr%slinear_%s_millionm3_5min_%0.4d_%0.4dM%0.2dV%0.2d" %(r[0],r[1],YEAR_MIN,YEAR_MAX,month,VERSION)
            exportToAsset(newImageYearMax,outputIcName,outputIName)
    else:
        print("Error, check script")
    
   


# In[9]:

for r in itertools.product(parameters, temporalScales): 
    r =list(r)
    parameter = r[0]
    temporalScale = r[1]
    iterateFunction(parameter,temporalScale)
    
    


# In[ ]:



