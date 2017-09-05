
# coding: utf-8

# ### Create Area Image EE
# 
# * Purpose of script: This notebooks create an area image (30s) in m2 to go from flux to volume and vice versa
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170905

# In[1]:

import ee
import numpy as np


# In[2]:

XSIZE5MIN = 4320
YSIZE5MIN = XSIZE5MIN/2
EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07/"


# In[3]:

ee.Initialize()


# In[4]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 89], [180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[5]:

cellsize_05min_m2 = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V05/cellsize_05min_m2")


# In[6]:

onesRaster = ee.Image.constant(1)
areaRaster = ee.Image.pixelArea()


# In[7]:

def exportToAsset(image,eePath, eeIName, scale, geometry,properties):
    image = image.set(properties)    
    assetId = eePath + eeIName
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(image),
        description = eeIName ,
        assetId = assetId,
        scale = scale,
        region = geometry.bounds().getInfo()['coordinates'][0],
        maxPixels = 1e10
        )
    task.start()


# In[8]:

properties5min = {"spatial_resolution":"5min",
                 "ingested_by":"RutgerHofste" ,
                  "exportdescription": "ones_5min" ,
                  "units": "dimensionless"              
                 }


# In[9]:

exportToAsset(onesRaster,EE_PATH, "ones_5min", ee.Image(cellsize_05min_m2).projection().nominalScale().getInfo(), geometry,properties5min)


# In[10]:

properties30s = {"spatial_resolution":"30s",
                 "ingested_by":"RutgerHofste" ,
                  "exportdescription": "ones_30s" ,
                  "units": "dimensionless"              
                 }


# In[11]:

exportToAsset(onesRaster,EE_PATH, "ones_30s", ee.Image(cellsize_05min_m2).projection().nominalScale().getInfo()/10, geometry,properties30s)


# In[12]:

properties5minArea = {"spatial_resolution":"5min",
                 "ingested_by":"RutgerHofste" ,
                  "exportdescription": "area_5min_m2" ,
                  "units": "m2"              
                 }


# In[ ]:

exportToAsset(areaRaster,EE_PATH, "area_5min_m2", ee.Image(cellsize_05min_m2).projection().nominalScale().getInfo(), geometry,properties5minArea)


# In[ ]:



