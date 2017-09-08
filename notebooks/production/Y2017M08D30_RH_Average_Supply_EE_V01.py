
# coding: utf-8

# # Calculate average PCRGlobWB supply using EE
# 
# * Purpose of script: This script will calculate baseline supply based on runoff for 1960-2014 at 5min resolution
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170830

# In[1]:

import os
import ee
import folium
from folium_gee import *
import subprocess


# In[2]:

ee.Initialize()


# In[3]:

EE_INPUT_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07/"

# Unfortunately specifying the dimensions caused the script to crash (internal error on Google's side) Specify scale instead.

DIMENSION5MIN = {}
DIMENSION5MIN["x"] = 4320
DIMENSION5MIN["y"] = 2160

INPUT_FILE_NAME_ANNUAL = "global_historical_runoff_year_myear_5min_1958_2014"
INPUT_FILE_NAME_MONTH = "global_historical_runoff_month_mmonth_5min_1958_2014"

EE_IC_NAME_ANNUAL =  "global_historical_reducedmeanrunoff_year_myear_5min_1960_2014"
EE_IC_NAME_MONTH =   "global_historical_reducedmeanrunoff_month_mmonth_5min_1960_2014"

EE_I_NAME_ANNUAL = EE_IC_NAME_ANNUAL
EE_I_NAME_MONTH = EE_IC_NAME_MONTH

YEAR_MIN = 1960
YEAR_MAX = 2014

ANNUAL_UNITS = "m/year"
MONTHLY_UNITS = "m/month"

ANNUAL_EXPORTDESCRIPTION = "reducedmeanrunoff_year" #final format reducedmeanrunoff_yearY1960Y2014
MONTHLY_EXPORTDESCRIPTION = "reducedmeanrunoff_month" #final format reducedmeanrunoff_monthY1960Y2014M01
VERSION = 16

MAXPIXELS =1e10


# The Standardized format to store assets on Earth Engine is EE_INPUT_PATH / EE_IC_NAME / EE_I_NAME and every image should have the property expertdescription that would allow to export the data to a table header. 

# In[4]:

dimensions = "%sx%s" %(DIMENSION5MIN["x"],DIMENSION5MIN["y"])


# In[5]:

print(dimensions)


# In[6]:

sampleImage = ee.Image(ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_ANNUAL)).first())


# In[7]:

projection = sampleImage.projection().getInfo()


# In[8]:

crs = sampleImage.projection().crs().getInfo()


# posted question in EE dev forum. Apparently it is easier to print the tranform in Javascipt and paste it into this script. 

# In[9]:

crsTransform = [
                0.0833333309780367,
                0,
                -179.99999491255934,
                0,
                -0.0833333309780367,
                90.00000254430942
              ]


# In[10]:

scale = ee.Image(ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_ANNUAL)).first()).projection().nominalScale().getInfo()


# In[11]:

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
    print(dimensions)
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(image),
        description = description,
        assetId = assetId,
        dimensions = dimensions,
        #scale = scale,
        crs = crs,
        crsTransform = crsTransform,
        #region = geometry.bounds().getInfo()['coordinates'][0],
        maxPixels = maxPixels
    )
    print(assetId)
    task.start()
    return 1

def addValidProperties(image,d):
    nestedNewDict = {}
    #remove non string or real properties
    for nestedKey, nestedValue in d.iteritems():
        if isinstance(nestedValue,str) or isinstance(nestedValue,int):
            newDict[nestedKey] = nestedValue
        else:
            pass
            #print("removing property: ",nestedKey )
    image = ee.Image(image).set(nestedNewDict)
    return image

def createImageCollections(d):
    command = ("earthengine create collection %s%s") %(EE_INPUT_PATH,d["ic_name"])
    response = subprocess.check_output(command,shell=True)
    print(response)
    
    


# In[12]:

geometry = geometryFromProj(projection,DIMENSION5MIN)


# In[13]:

d = {}


# In[14]:

d["annual"] = {"ic": ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_ANNUAL)),
                        "ic_name": EE_IC_NAME_ANNUAL+"V%0.2d" %(VERSION) ,
                        "image_name": EE_IC_NAME_ANNUAL+"V%0.2d" %(VERSION),
                        "temporal_resolution":"year",
                        "rangeMin":YEAR_MIN,
                        "rangeMax":YEAR_MAX,
                        "units":ANNUAL_UNITS,
                        "exportdescription": ANNUAL_EXPORTDESCRIPTION + "Y%sY%s" %(YEAR_MIN,YEAR_MAX),
                        "creation":"RutgerHofste_20170901_Python27",
                        "nodata_value":-9999,
                        "reducer":"mean",
                        "time_start": "%04d-%0.2d-%0.2d" %(YEAR_MAX,12,1),
                        "version":VERSION
                        }


# In[15]:

d["monthly"] = {"ic": ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_MONTH)),
                         "ic_name": EE_IC_NAME_MONTH +"V%0.2d" %(VERSION),
                         "temporal_resolution":"month",
                         "rangeMin":YEAR_MIN,
                         "rangeMax":YEAR_MAX,
                         "units":MONTHLY_UNITS,
                         "creation":"RutgerHofste_20170901_Python27",
                         "nodata_value":-9999,
                         "reducer":"mean",                         
                         "version":VERSION,
                         # add month , image_name and exportdexription
                        }


# In[16]:

for key, value in d.iteritems():
    createImageCollections(value)


# In[17]:

newDict = {}
for key, value in d.iteritems():
    newDict[key] = value
    reducedImage = reduceMean(value["ic"],YEAR_MIN,YEAR_MAX)
    if value["temporal_resolution"] == "year":
        reducedImage = reduceMean(value["ic"],value["rangeMin"],newDict[key]["rangeMax"])
        validImage = addValidProperties(reducedImage,value)
        assetId = EE_INPUT_PATH+newDict[key]["ic_name"]+"/"+newDict[key]["image_name"]
        exportToAsset(validImage,newDict[key]["exportdescription"]+"V%s"%(newDict[key]["version"]),assetId,dimensions,geometry,MAXPIXELS)
        
    if value["temporal_resolution"] == "month":
        for month in range(1,13):
            newDict[key]["month"] = month
            newDict[key]["image_name"] = EE_IC_NAME_MONTH +"M%0.2dV%0.2d" %(month,VERSION)
            newDict[key]["exportdescription"] = ANNUAL_EXPORTDESCRIPTION + "Y%sY%sM%0.d" %(YEAR_MIN,YEAR_MAX,month)
            
            reducedImage = reduceMean(value["ic"],newDict[key]["rangeMin"],newDict[key]["rangeMax"])
            validImage = addValidProperties(reducedImage,value)
            assetId = EE_INPUT_PATH+newDict[key]["ic_name"]+"/"+newDict[key]["image_name"]
            exportToAsset(validImage,value["exportdescription"]+"V%s" %(newDict[key]["version"]),assetId,dimensions,geometry,MAXPIXELS)            
        pass
        
        
    
    


# In[ ]:



