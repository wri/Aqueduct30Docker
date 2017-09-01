
# coding: utf-8

# # Calculate linear trend for Agricultural Demand
# 
# * Purpose of script: Calculate linear trend for Agricultural Demand for 2004 - 2014 due to the high sensitivity of the model to ag demand. 
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170901

# In[9]:

import os
import ee
import folium
from folium_gee import *
import subprocess


# Settings:

# In[25]:

EE_INPUT_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07/"
INPUT_FILE_NAME_WW_ANNUAL = "global_historical_PIrrWW_year_millionm3_5min_1960_2014"
INPUT_FILE_NAME_WN_ANNUAL = "global_historical_PIrrWN_year_millionm3_5min_1960_2014"
INPUT_FILE_NAME_WW_MONTHLY = "global_historical_PIrrWW_month_millionm3_5min_1960_2014"
INPUT_FILE_NAME_WN_MONTHLY = "global_historical_PIrrWN_month_millionm3_5min_1960_2014"
YEAR_MIN = 2004
YEAR_MAX = 2014


# In[26]:

ee.Initialize()


# In[27]:

propertiesWWannua = {"units":"millionm3","parameter":"IrrWWlinear_year","year":2014,"month":12,"exportdescription":"IrrWWLinear_annuaY2014M12"}
propertiesWNannua = {"units":"millionm3","parameter":"IrrWNlinear_year","year":2014,"month":12,"exportdescription":"IrrWNLinear_annuaY2014M12"}


# In[28]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 89], [180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[29]:

print(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WW_ANNUAL))


# In[30]:

icWWannua = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WW_ANNUAL));
icWNannua = ee.ImageCollection(os.path.join(EE_INPUT_PATH,INPUT_FILE_NAME_WN_ANNUAL));


# In[32]:

irrWW2014 = ee.Image(icWWannua.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year")).first())
irrWN2014 = ee.Image(icWNannua.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year")).first())


# In[38]:

icWWRange = icWWannua.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year"))
icWNRange = icWNannua.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year"))


# In[39]:

def createTimeBand(image):
    # Adds a timeband to the single band image. band is "b1" 
    year = ee.Number(ee.Image(image).get("year"))
    newImage = ee.Image.constant(year).toDouble().select(["constant"],["independent"])
    image = image.toDouble().select(["b1"],["dependent"])
    return image.addBands(newImage)   
    
    
def linearTrendAnnual(ic,yearmin,yearmax):
    ic_filtered = ic.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year"))
    imageFinalYear = ee.Image(ic.filter(ee.Filter.calendarRange(YEAR_MIN,YEAR_MAX,"year")).first())
    
    


# In[40]:

icWWRange2 = icWWRange.map(createTimeBand)
icWNRange2 = icWNRange.map(createTimeBand)


# In[41]:

fitWW = icWWRange2.select(["independent","dependent"]).reduce(ee.Reducer.linearFit())


# In[42]:

fitWN = icWNRange2.select(["independent","dependent"]).reduce(ee.Reducer.linearFit())


# In[43]:

offsetWW = fitWW.select(["offset"])
scaleWW = fitWW.select(["scale"])


# In[ ]:



