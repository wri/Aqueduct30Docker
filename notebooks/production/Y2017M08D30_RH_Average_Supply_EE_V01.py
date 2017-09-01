
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

EE_IC_NAME_ANNUAL =  "global_historical_reducedmeanrunoff_year_myear_5min_1960_2014"
EE_IC_NAME_MONTH =   "global_historical_reducedmeanrunoff_month_mmonth_5min_1960_2014"

EE_I_NAME_ANNUAL = EE_IC_NAME_ANNUAL
EE_I_NAME_MONTH = EE_IC_NAME_MONTH


INPUT_FILE_NAME_ANNUAL = "global_historical_runoff_year_myear_5min_1958_2014"
INPUT_FILE_NAME_MONTH = "global_historical_runoff_month_mmonth_5min_1958_2014"

YEAR_MIN = 1960
YEAR_MAX = 2014
ANNUAL_UNITS = "m/year"
MONTHLY_UNITS = "m/month"
ANNUAL_EXPORTDESCRIPTION = "reducedmeanrunoff_year" #final format reducedmeanrunoff_yearY1960Y2014
MONTHLY_EXPORTDESCRIPTION = "reducedmeanrunoff_month" #final format reducedmeanrunoff_monthY1960Y2014M01
VERSION = "33"


# The Standardized format to store assets on Earth Engine is EE_INPUT_PATH / EE_IC_NAME / EE_I_NAME and every image should have the property expertdescription that would allow to export the data to a table header. 

# In[4]:

def exportToAssetAnnual(ic):
    annualExportDescription = ANNUAL_EXPORTDESCRIPTION + "Y%sY%s" %(YEAR_MIN,YEAR_MAX)
    annualImage = ee.Image(ic.reduce(ee.Reducer.mean()))
    annualImage = annualImage.set("rangeMin",ee.Number(YEAR_MIN)).set("rangeMax",ee.Number(YEAR_MAX)).set("units",ANNUAL_UNITS).set("exportdescription",annualExportDescription).set("creation","RutgerHofste_20170901_Python27")
    scale = ee.Image(icAnnual.first()).projection().nominalScale().getInfo()
    assetId = EE_INPUT_PATH + EE_IC_NAME_ANNUAL + "V" +VERSION+ "/" + EE_I_NAME_ANNUAL + "V" + VERSION    
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(annualImage),
        description = EE_I_NAME_ANNUAL + "V" + VERSION,
        assetId = assetId,
        scale = scale,
        region = geometry.bounds().getInfo()['coordinates'][0],
        maxPixels = 1e10
    )
    task.start()
    return 1


def exportToAssetMonth(month):
    monthlyExportDescription = MONTHLY_EXPORTDESCRIPTION + "Y%sY%sM%0.2d" %(YEAR_MIN,YEAR_MAX,month)
    monthlyImage = filteredMonthlyCollection.filter(ee.Filter.eq("month",ee.Number(month)))
    monthlyImage = filteredMonthlyCollection.reduce(ee.Reducer.mean())
    monthlyImage  = monthlyImage.set("month",ee.Number(month)).set("rangeMin",ee.Number(YEAR_MIN)).set("rangeMax",ee.Number(YEAR_MAX)).set("reducer",ee.String("mean")).set("units",MONTHLY_UNITS).set("exportdescription",monthlyExportDescription).set("creation","RutgerHofste_20170901_Python27")
    scale = ee.Image(icMonthly.first()).projection().nominalScale().getInfo()

    assetId = EE_INPUT_PATH + EE_IC_NAME_MONTH + "V" +VERSION+ "/" + EE_I_NAME_MONTH + "M%0.2dV%s" %(month,VERSION)  
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(monthlyImage),
        description = EE_I_NAME_MONTH + "M%0.2dV%s" %(month,VERSION) ,
        assetId = assetId,
        scale = scale,
        region = geometry.bounds().getInfo()['coordinates'][0],
        maxPixels = 1e10
    )
    task.start()
    print(month)
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

icAnnualPath = os.path.join(EE_INPUT_PATH,EE_IC_NAME_ANNUAL+ "V" + VERSION)
command = ("earthengine create collection %s") %icAnnualPath
print(command)


# In[10]:

subprocess.check_output(command,shell=True)


# In[11]:

icMonthPath = os.path.join(EE_INPUT_PATH,EE_IC_NAME_MONTH+ "V" + VERSION)
command = ("earthengine create collection %s") %icMonthPath
print(command)


# In[12]:

subprocess.check_output(command,shell=True)


# In[ ]:




# ### Run the functions

# In[13]:

exportToAssetAnnual(filteredAnnualCollection)


# In[14]:

months = list(range(1,13))


# In[15]:

map(exportToAssetMonth,months)


# In[ ]:



