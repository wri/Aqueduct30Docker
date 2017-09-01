
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
INPUT_FILE_NAME_ANNUAL = "global_historical_runoff_year_myear_5min_1958_2014"
INPUT_FILE_NAME_MONTH = "global_historical_runoff_month_mmonth_5min_1958_2014"
YEAR_MIN = 1960
YEAR_MAX = 2014
ANNUAL_UNITS = "m/year"
MONTHLY_UNITS = "m/month"
ANNUAL_EXPORTDESCRIPTION = "runoff_annua_myear_reduced"
MONTHLY_EXPORTDESCRIPTION = "runoff_month_mmonth_reduced"
VERSION = "30"


# In[4]:

def exportToAssetMonth(month):
    scale = ee.Image(icMonthly.first()).projection().nominalScale().getInfo()
    monthFilter = ee.Filter.eq("month",ee.Number(month))
    monthlyImage = filteredMonthlyCollection.reduce(ee.Reducer.mean())
    monthlyImage  = monthlyImage.set("month",ee.Number(month)).set("rangeMin",ee.Number(YEAR_MIN)).set("rangeMax",ee.Number(YEAR_MAX)).set("reducer",ee.String("mean")).set("units","m/month")
    exportdescription = MONTHLY_EXPORTDESCRIPTION + "_M%s" %(month)
    monthlyImage  = monthlyImage.set("exportdescription",exportdescription)
    monthlyImage  = monthlyImage.set("creation","RutgerHofste_20170901_Python27") 

    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(monthlyImage),
        description = exportdescription,
        assetId = icMonthPath + "/" + exportdescription,
        scale = scale,
        region = geometry.bounds().getInfo()['coordinates'][0],
        maxPixels = 1e10
    )
    task.start()
    print(month)
    return 1


def exportToAssetAnnual(ic):
    annualImage = ee.Image(ic.reduce(ee.Reducer.mean()))
    annualImage = annualImage.set("rangeMin",ee.Number(YEAR_MIN)).set("rangeMax",ee.Number(YEAR_MAX)).set("units",ANNUAL_UNITS).set("exportdescription",ANNUAL_EXPORTDESCRIPTION).set("creation","RutgerHofste_20170901_Python27")
    scale = ee.Image(icAnnual.first()).projection().nominalScale().getInfo()
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(annualImage),
        description = ANNUAL_EXPORTDESCRIPTION,
        assetId = EE_INPUT_PATH + ANNUAL_EXPORTDESCRIPTION,
        scale = scale,
        region = geometry.bounds().getInfo()['coordinates'][0],
        maxPixels = 1e10
    )
    task.start()
    print("done")
    return 1
    
    


# In[5]:

icAnnual = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_ANNUAL))
icMonthly = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_MONTH))


# In[6]:

dateFilterMin = ee.Filter.gte("year",YEAR_MIN)
dateFilterMax = ee.Filter.lte("year",YEAR_MAX)


# In[7]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 89], [180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[8]:

filteredAnnualCollection = ee.ImageCollection(icAnnual.filter(dateFilterMin).filter(dateFilterMax))
filteredMonthlyCollection = ee.ImageCollection(icMonthly.filter(dateFilterMin).filter(dateFilterMax))


# In[9]:

icMonthPath = os.path.join(EE_INPUT_PATH,MONTHLY_EXPORTDESCRIPTION+ "V" + VERSION)
command = ("earthengine create collection %s") %icMonthPath
print(command)


# In[10]:

subprocess.check_output(command,shell=True)


# In[11]:

months = list(range(1,13))


# ### Run the functions

# In[12]:

exportToAssetAnnual(filteredAnnualCollection)


# In[13]:

map(exportToAssetMonth,months)


# In[ ]:



