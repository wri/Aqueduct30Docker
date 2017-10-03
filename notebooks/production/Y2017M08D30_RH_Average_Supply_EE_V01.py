
# coding: utf-8

# # Calculate average PCRGlobWB supply using EE
# 
# * Purpose of script: This script will calculate baseline supply based on runoff for 1960-2014 at 5min resolution
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170830

# In[1]:

import time
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
print(dateString,timeString)


# In[2]:

import os
import ee
import subprocess
from pprint import *
from itertools import chain


# In[3]:

ee.Initialize()


# In[4]:

EE_INPUT_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07/"

# Unfortunately specifying the dimensions caused the script to crash (internal error on Google's side) Specify scale instead.

DIMENSION5MIN = {}
DIMENSION5MIN["x"] = 4320
DIMENSION5MIN["y"] = 2160

INPUT_FILE_NAME_ANNUAL = "global_historical_runoff_year_myear_5min_1958_2014"
INPUT_FILE_NAME_MONTH = "global_historical_runoff_month_mmonth_5min_1958_2014"

EE_IC_NAME_ANNUAL =  "global_historical_reducedmeanrunoff_year_myear_5min_Y1960_Y2014"
EE_IC_NAME_MONTH =   "global_historical_reducedmeanrunoff_month_mmonth_5min_Y1960_Y2014"

EE_I_NAME_ANNUAL = EE_IC_NAME_ANNUAL
EE_I_NAME_MONTH = EE_IC_NAME_MONTH

YEAR_MIN = 1960
YEAR_MAX = 2014

ANNUAL_UNITS = "m/year"
MONTHLY_UNITS = "m/month"

ANNUAL_EXPORTDESCRIPTION = "reducedmeanrunoff_year" #final format reducedmeanrunoff_yearY1960Y2014
MONTHLY_EXPORTDESCRIPTION = "reducedmeanrunoff_month" #final format reducedmeanrunoff_monthY1960Y2014M01
VERSION = 21

MAXPIXELS =1e10


# The Standardized format to store assets on Earth Engine is EE_INPUT_PATH / EE_IC_NAME / EE_I_NAME and every image should have the property expertdescription that would allow to export the data to a table header. 

# In[5]:

dimensions = "%sx%s" %(DIMENSION5MIN["x"],DIMENSION5MIN["y"])


# In[6]:

print(dimensions)


# In[7]:

sampleImage = ee.Image(ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_ANNUAL)).first())


# In[8]:

projection = sampleImage.projection().getInfo()


# In[9]:

crs = sampleImage.projection().crs().getInfo()


# posted question in EE dev forum. Apparently it is easier to print the tranform in Javascipt and paste it into this script. 

# In[10]:

crsTransform = [
                0.0833333309780367,
                0,
                -179.99999491255934,
                0,
                -0.0833333309780367,
                90.00000254430942
              ]


# In[11]:

scale = ee.Image(ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_ANNUAL)).first()).projection().nominalScale().getInfo()


# In[12]:

def geometryFromProj(projection,dimensions):
    coords = {}
    coords["xmin"]=projection["transform"][2]
    coords["xmax"]=projection["transform"][2]+dimensions["x"]*projection["transform"][0]
    coords["ymin"]=projection["transform"][5]+dimensions["y"]*projection["transform"][4]
    coords["ymax"]=projection["transform"][5]
    geometry = ee.Geometry.Polygon(coords=[[coords["xmin"], coords["ymin"]], 
                                           [coords["xmax"], coords["ymin"]],
                                           [coords["xmax"], coords["ymax"]],
                                           [coords["xmin"], coords["ymax"]]],
                                            proj= ee.Projection('EPSG:4326'),geodesic=False )
    return geometry

def reduceMean(ic,yearMin,yearMax):
    dateFilterMin = ee.Filter.gte("year",yearMin)
    dateFilterMax = ee.Filter.lte("year",yearMax)
    filteredIc = ee.ImageCollection(ic.filter(dateFilterMin).filter(dateFilterMax))
    reducedImage = ee.Image(filteredIc.reduce(ee.Reducer.mean()))
    return reducedImage

def exportToAsset(image,description,assetId,dimensions,region,maxPixels):
    #print(image.propertyNames().getInfo())
    task = ee.batch.Export.image.toAsset(
        image =  image,
        description = description,
        assetId = assetId,
        dimensions = dimensions,
        #scale = scale,
        crs = crs,
        crsTransform = crsTransform,
        #region = geometry.bounds().getInfo()['coordinates'][0],
        maxPixels = maxPixels
    )
    #print(assetId)
    task.start()
    return 1

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

def createImageCollections(d):
    command = ("earthengine create collection %s%s") %(EE_INPUT_PATH,d["ic_name"])
    response = subprocess.check_output(command,shell=True)
    print(response)
    
    


# In[13]:

geometry = geometryFromProj(projection,DIMENSION5MIN)


# In[14]:

d = {}


# In[15]:

commonProperties = {"rangeMin":YEAR_MIN,
                    "rangeMax":YEAR_MAX,
                    "creation":"RutgerHofste_%s_Python27" %(dateString),
                    "nodata_value":-9999,
                    "reducer":"mean",
                    "version":VERSION,
                    "year":2014,
                    "year_warning":"rangeNotOneYear",
                    "script_used":"Y2017M08D30_RH_Average_Supply_EE_V01"
                   }


# In[16]:

dYearOrig = commonProperties
dMonthOrig = commonProperties


# In[ ]:




# In[17]:

dYearExtra = {"ic": ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_ANNUAL)),
                    "ic_name": EE_IC_NAME_ANNUAL+"V%0.2d" %(VERSION) ,
                    "image_name": EE_IC_NAME_ANNUAL+"V%0.2d" %(VERSION),
                    "temporal_resolution":"year",
                    "units":ANNUAL_UNITS,
                    "exportdescription": ANNUAL_EXPORTDESCRIPTION + "_Y%sY%s" %(YEAR_MIN,YEAR_MAX),
                    "time_start": "%04d-%0.2d-%0.2d" %(YEAR_MAX,12,1)
                    }


# In[18]:

dMonthExtra = {"ic": ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_MONTH)),
                     "ic_name": EE_IC_NAME_MONTH +"V%0.2d" %(VERSION),
                     "temporal_resolution":"month",
                     "units":MONTHLY_UNITS,
                     "nodata_value":-9999,
                     # add month , image_name and exportdexription
                     }


# These commands will only work in python 2!

# In[19]:

d["year"] = dict(dYearOrig.items() + dYearExtra.items())


# In[20]:

d["month"] = dict(dMonthOrig.items() + dMonthExtra.items())


# In[21]:

for key, value in d.iteritems():
    createImageCollections(value)


# In[22]:

newDict = {}
for key, value in d.iteritems():
    newDict[key] = value
    reducedImage = reduceMean(value["ic"],YEAR_MIN,YEAR_MAX)
    if value["temporal_resolution"] == "year":
        reducedImage = reduceMean(value["ic"],value["rangeMin"],newDict[key]["rangeMax"])
        validImage = addValidProperties(reducedImage,newDict[key])
        assetId = EE_INPUT_PATH+newDict[key]["ic_name"]+"/"+newDict[key]["image_name"]
        exportToAsset(validImage,newDict[key]["exportdescription"]+"_V%s"%(newDict[key]["version"]),assetId,dimensions,geometry,MAXPIXELS)
        
    elif value["temporal_resolution"] == "month":
        for month in range(1,13):
            newDict[key]["month"] = month
            newDict[key]["image_name"] = EE_IC_NAME_MONTH +"M%0.2dV%0.2d" %(month,VERSION)
            newDict[key]["exportdescription"] = MONTHLY_EXPORTDESCRIPTION + "_Y%sY%sM%0.2d" %(YEAR_MIN,YEAR_MAX,month)
            
            reducedImage = reduceMean(value["ic"],newDict[key]["rangeMin"],newDict[key]["rangeMax"])
            validImage = addValidProperties(reducedImage,newDict[key])
            assetId = EE_INPUT_PATH+newDict[key]["ic_name"]+"/"+newDict[key]["image_name"]
            exportToAsset(validImage,value["exportdescription"]+"V%s" %(newDict[key]["version"]),assetId,dimensions,geometry,MAXPIXELS)            
        pass

