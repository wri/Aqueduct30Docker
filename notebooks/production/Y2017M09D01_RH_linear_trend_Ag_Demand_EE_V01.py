
# coding: utf-8

# # Calculate linear trend for Agricultural Demand
# 
# * Purpose of script: Calculate linear trend for Agricultural Demand for 2004 - 2014 due to the high sensitivity of the model to ag demand. 
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170901

# In[1]:

import os
import ee
import folium
from folium_gee import *
import subprocess


# Settings:

# In[48]:

EE_INPUT_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07/"
INPUT_FILE_NAME_WW_ANNUAL = "global_historical_PIrrWW_year_millionm3_5min_1960_2014"
INPUT_FILE_NAME_WN_ANNUAL = "global_historical_PIrrWN_year_millionm3_5min_1960_2014"
INPUT_FILE_NAME_WW_MONTH = "global_historical_PIrrWW_month_millionm3_5min_1960_2014"
INPUT_FILE_NAME_WN_MONTH = "global_historical_PIrrWN_month_millionm3_5min_1960_2014"
YEAR_MIN = 2004
YEAR_MAX = 2014

FILE_NAME_MONTH_WW = "global_historical_PIrrWWlinear_month_millionm3_5min_2004_2014"
FILE_NAME_MONTH_WN = "global_historical_PIrrWNlinear_month_millionm3_5min_2004_2014"



# In[3]:

ee.Initialize()


# In[4]:

propertiesWWannua = {"units":"millionm3","parameter":"IrrWWlinear_year","year":2014,"month":12,"exportdescription":"IrrWWLinear_annuaY2014M12"}
propertiesWNannua = {"units":"millionm3","parameter":"IrrWNlinear_year","year":2014,"month":12,"exportdescription":"IrrWNLinear_annuaY2014M12"}


# In[5]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 89], [180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[6]:

print(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WW_ANNUAL))


# In[7]:

icWWannua = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WW_ANNUAL));
icWNannua = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WN_ANNUAL));


# In[8]:

irrWW2014 = ee.Image(icWWannua.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year")).first())
irrWN2014 = ee.Image(icWNannua.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year")).first())


# In[37]:

def createTimeBand(image):
    # Adds a timeband to the single band image. band is "b1" 
    year = ee.Number(ee.Image(image).get("year"))
    newImage = ee.Image.constant(year).toDouble().select(["constant"],["independent"])
    image = image.toDouble().select(["b1"],["dependent"])
    return image.addBands(newImage)   
    
    
def linearTrendAnnual(ic,yearmin,yearmax,outputFileName,units,exportdescription):
    nominalScale = ee.Image(ic.first()).projection().nominalScale().getInfo()
    icFiltered = ic.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year"))
    icFilteredTimeband = icFiltered.map(createTimeBand)
    imageFinalYear = ee.Image(ic.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year")).first())
    fit = icFilteredTimeband.select(["independent","dependent"]).reduce(ee.Reducer.linearFit())
    offset = fit.select(["offset"])
    scale = fit.select(["scale"]) #Note that this definition of scale is a as in ax+b
    newImageYearMax = scale.multiply(YEAR_MAX).add(offset).select(["scale"],["newValue"])
    spatialScale = imageFinalYear.projection().nominalScale()
    exportImageToAsset(newImageYearMax,outputFileName,units,exportdescription,nominalScale)
    return newImageYearMax


def exportImageToAsset(image,outputFileName,units,exportdescription,scale):
    image = image.set("rangeMin",ee.Number(YEAR_MIN)).set("rangeMax",ee.Number(YEAR_MAX)).set("units",units).set("exportdescription",exportdescription).set("creation","RutgerHofste_20170901_Python27")
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(image),
        description = outputFileName,
        assetId = EE_INPUT_PATH + outputFileName,
        scale = scale,
        region = geometry.bounds().getInfo()['coordinates'][0],
        maxPixels = 1e10
    )
    task.start()
    
def iterateMonths(month):
    # parameters defined prior to running function!
    linearTrendMonth(ic,yearmin,yearmax,outputFileName,units,exportdescription,month)
    
    
    
def linearTrendMonth(ic,yearmin,yearmax,outputFileName,units,exportdescription,month):
    nominalScale = ee.Image(ic.first()).projection().nominalScale().getInfo()
    icFiltered = ic.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year"))
    icFiltered = icFiltered.filter(ee.Filter.eq("month",ee.Number(month)))
    icFilteredTimeband = icFiltered.map(createTimeBand)
    imageFinalYear = ee.Image(ic.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year")).first())
    fit = icFilteredTimeband.select(["independent","dependent"]).reduce(ee.Reducer.linearFit())
    offset = fit.select(["offset"])
    scale = fit.select(["scale"]) #Note that this definition of scale is a as in ax+b
    newImageYearMax = scale.multiply(YEAR_MAX).add(offset).select(["scale"],["newValue"])
    spatialScale = imageFinalYear.projection().nominalScale()
    exportImageToAsset(newImageYearMax,outputFileName,units,exportdescription,nominalScale)
    return newImageYearMax


# Usage: (ic,yearmin,yearmax,outputFileName,units,exportdescription)

# In[39]:

newImage2014WW = linearTrendAnnual(icWWannua,YEAR_MIN,YEAR_MAX,"global_historical_PIrrWWlinear_year_millionm3_5min_2004_2014","millionm3","IrrWWLinear_yearY2014M12")


# In[40]:

newImage2014WN = linearTrendAnnual(icWNannua,YEAR_MIN,YEAR_MAX,"global_historical_PIrrWNlinear_year_millionm3_5min_2004_2014","millionm3","IrrWNLinear_yearY2014M12")


# The monthly results should be stored in imageCollections instead of single images. Creating them first

# In[43]:

icMonthPathWW = os.path.join(EE_INPUT_PATH,FILE_NAME_MONTH_WW)
command = ("earthengine create collection %s") %icMonthPathWW
print(command)


# In[44]:

subprocess.check_output(command,shell=True)


# In[45]:

icMonthPathWN = os.path.join(EE_INPUT_PATH,FILE_NAME_MONTH_WN)
command = ("earthengine create collection %s") %icMonthPathWN
print(command)


# In[46]:

subprocess.check_output(command,shell=True)


# In[47]:

months = list(range(1,13))


# In[49]:

icWWmonth = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WW_MONTH));
icWNmonth = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WN_MONTH));


# ## Define all parameters prior to running the mapping function

# In[52]:

ic = icWWmonth
yearmin = YEAR_MIN
yearmax = YEAR_MAX
outputFileName = "global_historical_PIrrWWlinear_month_millionm3_5min_2004_2014"
units = "millionm3"
exportdescription = "IrrWWLinear_monthY2014"


# In[ ]:

map()

