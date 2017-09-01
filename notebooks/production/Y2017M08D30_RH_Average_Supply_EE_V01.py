
# coding: utf-8

# # Calculate average PCRGlobWB supply using EE
# 
# * Purpose of script: This script will calculate baseline supply based on runoff for 1960-2014 at 5min resolution
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170830

# In[5]:

import os
import ee
import folium
from folium_gee import *


# In[6]:

ee.Initialize()


# In[7]:

EE_INPUT_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V05/"
INPUT_FILE_NAME_ANNUAL = "global_historical_runoff_year_myear_5min_1958_2014"
INPUT_FILE_NAME_MONTH = "global_historical_runoff_month_mmonth_5min_1958_2014"
YEAR_MIN = 1960
YEAR_MAX = 2014
ANNUAL_UNITS = "m/year"
MONTHLY_UNITS = "m/month"
ANNUAL_EXPORTDESCRIPTION = "runoff_annua"
MONTHLY_EXPORTDESCRIPTION = "runoff_month"
VERSION = "30"


# In[ ]:

def exportMeanToAsset(month)


# Folium map starting parameters

# In[8]:

lat = 38.899089
lon = -77.008173
zoom_start=10


# In[9]:

months = list(range(1,13))


# In[12]:

m = folium.Map(location=[lat, lon], tiles="Mapbox Bright", zoom_start=zoom_start)


# In[13]:

icAnnual = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_ANNUAL))


# In[14]:

icMonthly = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_MONTH))


# In[15]:

dateFilterMin = ee.Filter.gte("year",YEAR_MIN)
dateFilterMax = ee.Filter.lte("year",YEAR_MAX)


# In[16]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 89], [180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[17]:

filteredAnnualCollection = ee.ImageCollection(icAnnual.filter(dateFilterMin).filter(dateFilterMax))


# In[18]:

filteredMonthlyCollection = ee.ImageCollection(icMonthly.filter(dateFilterMin).filter(dateFilterMax))


# In[19]:

annualImage = ee.Image(filteredAnnualCollection.reduce(ee.Reducer.mean()))


# In[20]:

annualImage = annualImage.set("rangeMin",ee.Number(YEAR_MIN)).set("rangeMax",ee.Number(YEAR_MAX)).set("units",ANNUAL_UNITS).set("exportdescription",ANNUAL_EXPORTDESCRIPTION)


# In[21]:

scale = ee.Image(icAnnual.first()).projection().nominalScale().getInfo()


# In[22]:

task = ee.batch.Export.image.toAsset(
    image =  ee.Image(annualImage),
    description = 'mean1960tm2014totalRunoff_annuaTot_outputV'+VERSION,
    assetId = EE_INPUT_PATH + 'mean1960tm2014totalRunoff_annuaTot_outputV'+VERSION,
    scale = scale,
    region = geometry.bounds().getInfo()['coordinates'][0],
    maxPixels = 1e10
)


# In[ ]:

task.start()


# In[23]:

vis_params = {'min':0.0, 'max':1, 'palette':'00FFFF,0000FF'}


# In[24]:

folium_gee_layer(m,annualImage,vis_params=vis_params,folium_kwargs={'overlay':True,'name':'Test'})
m.add_child(folium.LayerControl())
m


# In[ ]:



