
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

# The Standardized format to store assets on Earth Engine is EE_INPUT_PATH / EE_IC_NAME / EE_I_NAME and every image should have the property expertdescription that would allow to export the data to a table header. 

# In[2]:

EE_INPUT_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07/"

YEAR_MIN = 2004
YEAR_MAX = 2014

DIMENSION5MIN = "4320x2160"

VERSION = 17

INPUT_FILE_NAME_WW_ANNUAL = "global_historical_PIrrWW_year_millionm3_5min_1960_2014"
INPUT_FILE_NAME_WN_ANNUAL = "global_historical_PIrrWN_year_millionm3_5min_1960_2014"

INPUT_FILE_NAME_WW_MONTH = "global_historical_PIrrWW_month_millionm3_5min_1960_2014"
INPUT_FILE_NAME_WN_MONTH = "global_historical_PIrrWN_month_millionm3_5min_1960_2014"

EE_IC_NAME_ANNUAL_WW = "global_historical_PIrrWWlinear_year_millionm3_5min_%0.4d_%0.4dV%0.2d" %(YEAR_MIN,YEAR_MAX,VERSION)
EE_IC_NAME_ANNUAL_WN = "global_historical_PIrrWNlinear_year_millionm3_5min_%0.4d_%0.4dV%0.2d" %(YEAR_MIN,YEAR_MAX,VERSION)

EE_IC_NAME_MONTH_WW = "global_historical_PIrrWWlinear_month_millionm3_5min_%0.4d_%0.4dV%0.2d" %(YEAR_MIN,YEAR_MAX,VERSION)
EE_IC_NAME_MONTH_WN = "global_historical_PIrrWNlinear_month_millionm3_5min_%0.4d_%0.4dV%0.2d" %(YEAR_MIN,YEAR_MAX,VERSION)

EE_I_NAME_ANNUAL_WW = "global_historical_PIrrWWlinear_year_millionm3_5min_%0.4d_%0.4dY%0.4d" %(YEAR_MIN,YEAR_MAX,YEAR_MAX) #add Vxx (e.g. Y2014V10) in the script
EE_I_NAME_ANNUAL_WN = "global_historical_PIrrWNlinear_year_millionm3_5min_%0.4d_%0.4dY%0.4d" %(YEAR_MIN,YEAR_MAX,YEAR_MAX) #add Vxx (e.g. Y2014V10) in the script

EE_I_NAME_MONTH_WW = "global_historical_PIrrWWlinear_month_millionm3_5min_%0.4d_%0.4dY%0.4d" %(YEAR_MIN,YEAR_MAX,YEAR_MAX) #add MxxVxx (e.g. M01V10) in the script
EE_I_NAME_MONTH_WN = "global_historical_PIrrWNlinear_month_millionm3_5min_%0.4d_%0.4dY%0.4d" %(YEAR_MIN,YEAR_MAX,YEAR_MAX) #add MxxVxx (e.g. M01V10) in the script

ANNUAL_EXPORTDESCRIPTION_WW = "IrrWWLinear_yearY%0.4d" %(YEAR_MAX)  # add Yxxxx e.g. Y2014 
ANNUAL_EXPORTDESCRIPTION_WN = "IrrWNLinear_yearY%0.4d" %(YEAR_MAX)  # add Yxxxx e.g. Y2014 
MONTHLY_EXPORTDESCRIPTION_WW = "IrrWWLinear_monthY%0.4d" %(YEAR_MAX)#add YxxxxMxx e.g. Y2014M01
MONTHLY_EXPORTDESCRIPTION_WN = "IrrWNLinear_monthY%0.4d" %(YEAR_MAX)#add YxxxxMxx e.g. Y2014M01

UNITS = "millionm3"


# In[3]:

ee.Initialize()


# In[4]:

propertiesWWannua = {"units":"millionm3","parameter":"IrrWWlinear_year","year":2014,"month":12,"exportdescription":"IrrWWLinear_yearY2014M12"}
propertiesWNannua = {"units":"millionm3","parameter":"IrrWNlinear_year","year":2014,"month":12,"exportdescription":"IrrWNLinear_yearY2014M12"}


# In[5]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 89], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[6]:

icWWannua = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WW_ANNUAL));
icWNannua = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WN_ANNUAL));


# In[7]:

irrWW2014 = ee.Image(icWWannua.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year")).first())
irrWN2014 = ee.Image(icWNannua.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year")).first())


# Create imageCollections (4)

# In[8]:

path = os.path.join(EE_INPUT_PATH,EE_IC_NAME_ANNUAL_WW)
command = ("earthengine create collection %s") %path
print(command)
subprocess.check_output(command,shell=True)


# In[9]:

path = os.path.join(EE_INPUT_PATH,EE_IC_NAME_ANNUAL_WN)
command = ("earthengine create collection %s") %path
print(command)
subprocess.check_output(command,shell=True)


# In[10]:

path = os.path.join(EE_INPUT_PATH,EE_IC_NAME_MONTH_WW)
command = ("earthengine create collection %s") %path
print(command)
subprocess.check_output(command,shell=True)


# In[11]:

path = os.path.join(EE_INPUT_PATH,EE_IC_NAME_MONTH_WN)
command = ("earthengine create collection %s") %path
print(command)
subprocess.check_output(command,shell=True)


# In[12]:

def createTimeBand(image):
    # Adds a timeband to the single band image. band is "b1" 
    year = ee.Number(ee.Image(image).get("year"))
    newImage = ee.Image.constant(year).toDouble().select(["constant"],["independent"])
    image = image.toDouble().select(["b1"],["dependent"])
    return image.addBands(newImage)   
   
def linearTrendAnnual(ic,yearmin,yearmax,eeIcNameAnnual,eeIName,units,exportdescription,parameter,version):
    nominalScale = ee.Image(ic.first()).projection().nominalScale().getInfo()
    icFiltered = ic.filter(ee.Filter.calendarRange(yearmin,yearmax,"year"))
    icFilteredTimeband = icFiltered.map(createTimeBand)
    imageFinalYear = ee.Image(ic.filter(ee.Filter.calendarRange(yearmin,yearmax,"year")).first())
    fit = icFilteredTimeband.select(["independent","dependent"]).reduce(ee.Reducer.linearFit())
    offset = fit.select(["offset"])
    scale = fit.select(["scale"]) #Note that this definition of scale is a as in y = ax+b
    newImageYearMax = scale.multiply(yearmax).add(offset).select(["scale"],["newValue"])
    newImageYearMax = newImageYearMax.set("time_start", "%04d-%0.2d-%0.2d" %(YEAR_MAX,12,1) )
    exportImageToAsset(newImageYearMax,eeIcNameAnnual,eeIName,units,exportdescription,nominalScale,parameter,version)
    return newImageYearMax


def exportImageToAsset(image,eeIcName,eeIName,units,exportdescription,scale,parameter,version):
    properties = {"rangeMin":YEAR_MIN,
                  "rangeMax":YEAR_MAX,
                  "units":units,
                  "exportdescription":exportdescription,
                  "creation":"RutgerHofste_20170902_Python27",
                  "parameter":parameter,
                  "nodata_value":-9999,
                  "method":"lineartrend"
                 }
    image = image.set(properties)    
    
    eeIName = eeIName + "V%0.2d" %(version)  
    assetId = EE_INPUT_PATH + eeIcName +"/" + eeIName
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(image),
        description = eeIName ,
        assetId = assetId,
        dimensions = DIMENSION5MIN ,
        region = geometry.bounds().getInfo()['coordinates'][0],
        maxPixels = 1e10
    )
    print("exportdescription: ", exportdescription)
    task.start()
    return 1
    
def iterateMonths(month):
    # parameters defined prior to running function!
    newImageYearMax = linearTrendMonth(ic,yearmin,yearmax,eeIcName,eeIName,units,exportdescription,month,parameter,version)
    return newImageYearMax
    
    
def linearTrendMonth(ic,yearmin,yearmax,eeIcName,eeIName,units,exportdescription,month,parameter,version):
    eeIName = eeIName + "M%0.2d" %(month)
    exportdescription = exportdescription + "M%0.2d" %(month)
    nominalScale = ee.Image(ic.first()).projection().nominalScale().getInfo()
    icFiltered = ic.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year"))
    icFiltered = icFiltered.filter(ee.Filter.eq("month",ee.Number(month)))
    icFilteredTimeband = icFiltered.map(createTimeBand)
    imageFinalYear = ee.Image(ic.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year")).first())
    fit = icFilteredTimeband.select(["independent","dependent"]).reduce(ee.Reducer.linearFit())
    offset = fit.select(["offset"])
    scale = fit.select(["scale"]) #Note that this definition of scale is a as in ax+b
    newImageYearMax = scale.multiply(YEAR_MAX).add(offset).select(["scale"],["newValue"])
    newImageYearMax = newImageYearMax.set("month",month)
    newImageYearMax = newImageYearMax.set("time_start", "%04d-%0.2d-%0.2d" %(YEAR_MAX,month,1) )
    exportImageToAsset(newImageYearMax,eeIcName,eeIName,units,exportdescription,nominalScale,parameter,version)
    return newImageYearMax


# In[13]:

parameter = "IrrWWlinear_year"
image = linearTrendAnnual(icWWannua,YEAR_MIN,YEAR_MAX,EE_IC_NAME_ANNUAL_WW,EE_I_NAME_ANNUAL_WW,UNITS,ANNUAL_EXPORTDESCRIPTION_WW,parameter,VERSION)


# In[14]:

parameter = "IrrWNlinear_year"
image = linearTrendAnnual(icWNannua,YEAR_MIN,YEAR_MAX,EE_IC_NAME_ANNUAL_WN,EE_I_NAME_ANNUAL_WN,UNITS,ANNUAL_EXPORTDESCRIPTION_WN,parameter,VERSION)


# In[15]:

months = list(range(1,13))


# In[16]:

icWWmonth = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WW_MONTH));
icWNmonth = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WN_MONTH));


# ## Define all parameters prior to running the mapping function

# In[17]:

ic = icWWmonth
yearmin = YEAR_MIN
yearmax = YEAR_MAX
eeIcName = EE_IC_NAME_MONTH_WW
eeIName = EE_I_NAME_MONTH_WW
units = "millionm3"
exportdescription = MONTHLY_EXPORTDESCRIPTION_WW 
parameter = "IrrWWlinear_month"
version = VERSION


# In[18]:

map(iterateMonths,months)


# In[19]:

ic = icWNmonth
yearmin = YEAR_MIN
yearmax = YEAR_MAX
eeIcName = EE_IC_NAME_MONTH_WN
eeIName = EE_I_NAME_MONTH_WN
units = "millionm3"
exportdescription = MONTHLY_EXPORTDESCRIPTION_WN
parameter = "IrrWNlinear_month"
version = VERSION


# In[20]:

map(iterateMonths,months)

