
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

DIMENSION5MIN = "4320x2160"

INPUT_FILE_NAME_ANNUAL = "global_historical_runoff_year_myear_5min_1958_2014"
INPUT_FILE_NAME_MONTH = "global_historical_runoff_month_mmonth_5min_1958_2014"

YEAR_MIN = 1960
YEAR_MAX = 2014
ANNUAL_UNITS = "m/year"
MONTHLY_UNITS = "m/month"
ANNUAL_EXPORTDESCRIPTION = "reducedmeanrunoff_year" #final format reducedmeanrunoff_yearY1960Y2014
MONTHLY_EXPORTDESCRIPTION = "reducedmeanrunoff_month" #final format reducedmeanrunoff_monthY1960Y2014M01
VERSION = "36"


# The Standardized format to store assets on Earth Engine is EE_INPUT_PATH / EE_IC_NAME / EE_I_NAME and every image should have the property expertdescription that would allow to export the data to a table header. 

# In[4]:

def exportToAssetAnnual(ic):
    annualExportDescription = ANNUAL_EXPORTDESCRIPTION + "Y%sY%s" %(YEAR_MIN,YEAR_MAX)
    annualImage = ee.Image(ic.reduce(ee.Reducer.mean()))
    properties = {"rangeMin":ee.Number(YEAR_MIN),
                  "rangeMax":ee.Number(YEAR_MAX),
                  "units":ANNUAL_UNITS,
                  "exportdescription":annualExportDescription,
                  "creation":"RutgerHofste_20170901_Python27",
                  "nodata_value":-9999,
                  "reducer":"mean",
                  "time_start": "%04d-%0.2d-%0.2d" %(YEAR_MAX,12,1) 
                 }
    annualImage = annualImage.set(properties)
    scale = ee.Image(icAnnual.first()).projection().nominalScale().getInfo()
    assetId = EE_INPUT_PATH + EE_IC_NAME_ANNUAL + "V" +VERSION+ "/" + EE_I_NAME_ANNUAL + "V" + VERSION    
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(annualImage),
        description = EE_I_NAME_ANNUAL + "V" + VERSION,
        assetId = assetId,
        dimensions = DIMENSION5MIN,
        region = geometry.bounds().getInfo()['coordinates'][0],
        maxPixels = 1e10
    )
    task.start()
    print(assetId)
    return annualImage


def exportToAssetMonth(month):
    monthlyExportDescription = MONTHLY_EXPORTDESCRIPTION + "Y%sY%sM%0.2d" %(YEAR_MIN,YEAR_MAX,month)
    monthlyImage = filteredMonthlyCollection.filter(ee.Filter.eq("month",ee.Number(month)))
    monthlyImage = filteredMonthlyCollection.reduce(ee.Reducer.mean())
    properties = {"month":month,
                  "reducer":"mean",
                  "rangeMin":ee.Number(YEAR_MIN),
                  "rangeMax":ee.Number(YEAR_MAX),
                  "units":MONTHLY_UNITS,
                  "exportdescription":monthlyExportDescription,
                  "creation":"RutgerHofste_20170901_Python27",
                  "nodata_value":-9999,
                  "time_start": "%04d-%0.2d-%0.2d" %(YEAR_MAX,month,1) 
                 }
    monthlyImage  =  monthlyImage.set(properties)

    
    scale = ee.Image(icMonthly.first()).projection().nominalScale().getInfo()

    assetId = EE_INPUT_PATH + EE_IC_NAME_MONTH + "V" +VERSION+ "/" + EE_I_NAME_MONTH + "M%0.2dV%s" %(month,VERSION)  
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(monthlyImage),
        description = EE_I_NAME_MONTH + "M%0.2dV%s" %(month,VERSION) ,
        assetId = assetId,
        dimensions = DIMENSION5MIN,
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

sampleImage = ee.Image(filteredAnnualCollection.first())


# In[10]:

icAnnualPath = os.path.join(EE_INPUT_PATH,EE_IC_NAME_ANNUAL+ "V" + VERSION)
command = ("earthengine create collection %s") %icAnnualPath
print(command)


# In[11]:

subprocess.check_output(command,shell=True)


# In[12]:

icMonthPath = os.path.join(EE_INPUT_PATH,EE_IC_NAME_MONTH+ "V" + VERSION)
command = ("earthengine create collection %s") %icMonthPath
print(command)


# In[13]:

subprocess.check_output(command,shell=True)


# ### Run the functions

# In[16]:

annualImage = exportToAssetAnnual(filteredAnnualCollection)


# In[ ]:

months = list(range(1,13))


# In[ ]:

map(exportToAssetMonth,months)


# ## check results

# In[ ]:

annualImage = annualImage.reproject('EPSG:4326',sampleImage.projection().getInfo())


# In[ ]:

lat = 39.495159
lon = -107.3689237
zoom_start=5


# In[ ]:

m = folium.Map(location=[lat, lon], tiles="OpenStreetMap", zoom_start=zoom_start)


# In[ ]:

vis_params = {'min':0.0, 'max':100, 'palette':'00FFFF,0000FF'}


# In[ ]:

folium_gee_layer(m,annualImage,vis_params=vis_params,folium_kwargs={'overlay':True,'name':'reducedRunoff'})


# In[ ]:

m.add_child(folium.LayerControl())
m


# In[ ]:



